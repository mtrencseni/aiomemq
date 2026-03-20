use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use rand::seq::IteratorRandom;

struct Topic {
    subscribers: HashMap<usize, mpsc::UnboundedSender<Value>>,
    cache: VecDeque<Value>,
    next_index: i64,
}

impl Topic {
    fn new(cache_size: usize) -> Self {
        Self {
            subscribers: HashMap::new(),
            cache: VecDeque::with_capacity(cache_size),
            next_index: 0,
        }
    }
}

struct SharedState {
    topics: HashMap<String, Topic>,
    cache_size: usize,
    next_session_id: usize,
}

type State = Arc<Mutex<SharedState>>;

async fn send_raw(writer: &mut BufWriter<WriteHalf<TcpStream>>, data: &str) -> tokio::io::Result<()> {
    writer.write_all(data.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn send_cmd(writer: &mut BufWriter<WriteHalf<TcpStream>>, cmd: &Value) -> tokio::io::Result<()> {
    let s = serde_json::to_string(cmd).unwrap();
    send_raw(writer, &s).await
}

async fn send_success(writer: &mut BufWriter<WriteHalf<TcpStream>>) -> tokio::io::Result<()> {
    send_cmd(writer, &json!({"success": true})).await
}

async fn send_failure(writer: &mut BufWriter<WriteHalf<TcpStream>>, reason: &str) -> tokio::io::Result<()> {
    send_cmd(writer, &json!({"success": false, "reason": reason})).await
}

fn validate_subscribe(obj: &serde_json::Map<String, Value>) -> bool {
    let allowed = ["command", "topic", "last_seen", "cache"];
    if !obj.keys().all(|k| allowed.contains(&k.as_str())) { return false; }
    if !obj.get("command").map_or(false, |v| v.is_string()) { return false; }
    if !obj.get("topic").map_or(false, |v| v.is_string()) { return false; }
    if let Some(ls) = obj.get("last_seen") {
        if !ls.is_i64() { return false; }
    }
    if let Some(c) = obj.get("cache") {
        if !c.is_boolean() { return false; }
    }
    true
}

fn validate_unsubscribe(obj: &serde_json::Map<String, Value>) -> bool {
    let allowed = ["command", "topic"];
    if !obj.keys().all(|k| allowed.contains(&k.as_str())) { return false; }
    if obj.len() != 2 { return false; }
    if !obj.get("command").map_or(false, |v| v.is_string()) { return false; }
    if !obj.get("topic").map_or(false, |v| v.is_string()) { return false; }
    true
}

fn validate_send(obj: &serde_json::Map<String, Value>) -> bool {
    let allowed = ["command", "topic", "msg", "delivery", "cache"];
    if !obj.keys().all(|k| allowed.contains(&k.as_str())) { return false; }
    if !obj.get("command").map_or(false, |v| v.is_string()) { return false; }
    if !obj.get("topic").map_or(false, |v| v.is_string()) { return false; }
    if !obj.get("msg").map_or(false, |v| v.is_string()) { return false; }
    if !obj.get("delivery").map_or(false, |v| {
        v.as_str().map_or(false, |s| s == "all" || s == "one")
    }) { return false; }
    if let Some(c) = obj.get("cache") {
        if !c.is_boolean() { return false; }
    }
    true
}

fn verify_command(v: &Value) -> bool {
    let obj = match v.as_object() {
        Some(o) => o,
        None => return false,
    };
    let cmd = match obj.get("command").and_then(|c| c.as_str()) {
        Some(s) => s,
        None => return false,
    };

    match cmd {
        "subscribe" => validate_subscribe(obj),
        "unsubscribe" => validate_unsubscribe(obj),
        "send" => validate_send(obj),
        _ => false,
    }
}

struct Session {
    id: usize,
    state: State,
    subscribed_topics: Vec<String>,
    tx: mpsc::UnboundedSender<Value>,
}

impl Session {
    async fn new(state: State, tx: mpsc::UnboundedSender<Value>) -> Self {
        let mut s = state.lock().await;
        let id = s.next_session_id;
        s.next_session_id += 1;
        Self {
            id,
            state: Arc::clone(&state),
            subscribed_topics: Vec::new(),
            tx,
        }
    }

    async fn handle_subscribe(&mut self, writer: &mut BufWriter<WriteHalf<TcpStream>>, obj: &serde_json::Map<String, Value>) -> tokio::io::Result<()> {
        let topic_name = obj.get("topic").unwrap().as_str().unwrap().to_string();
        let last_seen = obj.get("last_seen").and_then(|v| v.as_i64()).unwrap_or(-1);
        let want_cache = obj.get("cache").and_then(|v| v.as_bool()).unwrap_or(true);

        let mut to_send = Vec::new();
        {
            let mut s = self.state.lock().await;
            let cache_size = s.cache_size;
            let topic = s.topics.entry(topic_name.clone()).or_insert_with(|| Topic::new(cache_size));
            
            topic.subscribers.insert(self.id, self.tx.clone());
            self.subscribed_topics.push(topic_name.clone());

            if want_cache {
                for m in &topic.cache {
                    if let Some(idx) = m["index"].as_i64() {
                        if idx > last_seen {
                            to_send.push(m.clone());
                        }
                    }
                }
                
                let mut new_cache = VecDeque::with_capacity(cache_size);
                for m in &topic.cache {
                    let idx = m["index"].as_i64().unwrap();
                    if idx <= last_seen || m["delivery"] == "all" {
                        new_cache.push_back(m.clone());
                    }
                }
                topic.cache = new_cache;
            }
        }

        send_success(writer).await?;
        writer.flush().await?;
        for m in to_send {
            send_cmd(writer, &m).await?;
            writer.flush().await?;
        }
        Ok(())
    }

    async fn handle_unsubscribe(&mut self, writer: &mut BufWriter<WriteHalf<TcpStream>>, obj: &serde_json::Map<String, Value>) -> tokio::io::Result<()> {
        let topic_name = obj.get("topic").unwrap().as_str().unwrap();
        {
            let mut s = self.state.lock().await;
            if let Some(topic) = s.topics.get_mut(topic_name) {
                topic.subscribers.remove(&self.id);
            }
        }
        self.subscribed_topics.retain(|t| t != topic_name);
        send_success(writer).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn handle_send(&mut self, writer: &mut BufWriter<WriteHalf<TcpStream>>, val: Value) -> tokio::io::Result<()> {
        let obj = val.as_object().unwrap();
        let topic_name = obj.get("topic").unwrap().as_str().unwrap().to_string();
        let delivery = obj.get("delivery").unwrap().as_str().unwrap();
        let mut do_cache = obj.get("cache").and_then(|v| v.as_bool()).unwrap_or(true);

        {
            let mut s = self.state.lock().await;
            let cache_size = s.cache_size;
            let topic = s.topics.entry(topic_name.clone()).or_insert_with(|| Topic::new(cache_size));
            
            let mut msg = val.clone();
            let index = topic.next_index;
            topic.next_index += 1;
            msg["index"] = json!(index);

            if delivery == "all" {
                for sub in topic.subscribers.values() {
                    let _ = sub.send(msg.clone());
                }
            } else {
                let mut rng = rand::thread_rng();
                if let Some(sub) = topic.subscribers.values().choose(&mut rng) {
                    let _ = sub.send(msg.clone());
                    do_cache = false;
                }
            }

            if do_cache {
                topic.cache.push_back(msg);
                if topic.cache.len() > cache_size {
                    topic.cache.pop_front();
                }
            }
        }
        send_success(writer).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn process_line(&mut self, writer: &mut BufWriter<WriteHalf<TcpStream>>, mut buffer: Vec<u8>) -> tokio::io::Result<bool> {
        while buffer.last().map_or(false, |&b| b == b'\r' || b == b'\n') {
            buffer.pop();
        }
        if buffer == b"quit" { return Ok(false); }
        if buffer.is_empty() { return Ok(true); }

        let line = match String::from_utf8(buffer) {
            Ok(s) => s,
            Err(_) => {
                send_failure(writer, "Could not decode input as UTF-8").await?;
                return Ok(true);
            }
        };

        let val: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => {
                send_failure(writer, "Could not parse json").await?;
                return Ok(true);
            }
        };

        if !verify_command(&val) {
            send_failure(writer, "Malformed json message").await?;
            return Ok(true);
        }

        let obj = val.as_object().unwrap();
        let cmd = obj.get("command").unwrap().as_str().unwrap();

        match cmd {
            "subscribe" => self.handle_subscribe(writer, obj).await?,
            "unsubscribe" => self.handle_unsubscribe(writer, obj).await?,
            "send" => self.handle_send(writer, val).await?,
            _ => unreachable!(),
        }

        Ok(true)
    }

    async fn cleanup(self) {
        let mut s = self.state.lock().await;
        for t in self.subscribed_topics {
            if let Some(topic) = s.topics.get_mut(&t) {
                topic.subscribers.remove(&self.id);
            }
        }
    }
}

async fn handle_client(socket: TcpStream, state: State) -> tokio::io::Result<()> {
    let (reader, writer) = tokio::io::split(socket);
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);
    let (tx, mut rx) = mpsc::unbounded_channel::<Value>();
    
    let mut session = Session::new(state, tx).await;

    loop {
        let mut buffer = Vec::new();
        tokio::select! {
            res = reader.read_until(b'\n', &mut buffer) => {
                if res? == 0 { break; }
                if !session.process_line(&mut writer, buffer).await? {
                    break;
                }
            }
            msg = rx.recv() => {
                match msg {
                    Some(m) => {
                        send_cmd(&mut writer, &m).await?;
                        writer.flush().await?;
                    }
                    None => break,
                }
            }
        }
    }

    session.cleanup().await;
    Ok(())
}

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let port = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(7000);
    let cache_size = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100);

    let state = Arc::new(Mutex::new(SharedState {
        topics: HashMap::new(),
        cache_size,
        next_session_id: 0,
    }));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    eprintln!("Listening on 127.0.0.1:{}", port);

    loop {
        let (socket, _) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, state).await {
                eprintln!("Error handling client: {}", e);
            }
        });
    }
}
