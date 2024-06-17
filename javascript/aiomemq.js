'use strict';

const net = require('net');
const readline = require('readline');
const { StringDecoder } = require('string_decoder');
const decoder = new StringDecoder('utf8');
const { once } = require('events');
const logging = console;

class DefaultDict {
    constructor(defaultInit) {
        return new Proxy({}, {
            get: (target, name) => name in target ?
                target[name] :
                (target[name] = typeof defaultInit === 'function' ?
                    defaultInit() : // Call the function directly
                    defaultInit)
        });
    }
}

class Deque extends Array {
    constructor(maxlen) {
        super();
        this.maxlen = maxlen;
    }

    push(...args) {
        if (args.length + this.length > this.maxlen) {
            this.splice(0, args.length + this.length - this.maxlen);
        }
        return super.push(...args);
    }
}

function sendCmd(socket, cmd) {
    socket.write(JSON.stringify(cmd) + "\r\n", 'utf8');
}

function sendSuccess(socket) {
    const cmd = { success: true };
    sendCmd(socket, cmd);
}

function sendFailure(socket, reason) {
    const cmd = { success: false, reason: reason };
    sendCmd(socket, cmd);
}

const template_subscribe = {
    command: { type: "string", required: true },
    topic: { type: "string", required: true },
    last_seen: { type: "number", required: false },
    cache: { type: "boolean", required: false }
};

const template_unsubscribe = {
    command: { type: "string", required: true },
    topic: { type: "string", required: true }
};

const template_send = {
    command: { type: "string", required: true },
    topic: { type: "string", required: true },
    msg: { type: "string", required: true },
    delivery: { type: "string", required: true, values: ["all", "one"] },
    cache: { type: "boolean", required: false }
};

function templateMatch(template, obj) {
    const objKeys = new Set(Object.keys(obj));
    const templateKeys = new Set(Object.keys(template));
    
    if (![...objKeys].every(key => templateKeys.has(key))) {
        return false;
    }

    for (const [key, props] of Object.entries(template)) {
        if (props.required && !(key in obj)) {
            return false;
        }
        if (key in obj && typeof obj[key] !== props.type) {
            return false;
        }
        if ('values' in props && !props.values.includes(obj[key])) {
            return false;
        }
    }
    
    return true;
}

function verifyCommand(cmd) {
    const templates = {
        subscribe: template_subscribe,
        unsubscribe: template_unsubscribe,
        send: template_send
    };

    if (!cmd.hasOwnProperty("command") || typeof cmd.command !== "string") {
        return false;
    }

    const template = templates[cmd.command];
    if (!template || !templateMatch(template, cmd)) {
        return false;
    }

    return true;
}

function sendCached(socket, topic, lastSeen) {
    const cache = caches[topic];

    // Send commands from the cache that have an index greater than lastSeen
    for (const cmd of cache) {
        if (cmd.index > lastSeen) {
            sendCmd(socket, cmd);
        }
    }

    // Create a new cache and populate it with the appropriate commands
    const newCache = new Deque(cacheSize);
    for (const cmd of cache) {
        if (cmd.index <= lastSeen) {
            newCache.push(cmd);
        } else if (cmd.delivery === "all") {
            newCache.push(cmd);
        }
    }

    caches[topic] = newCache;
}

function handleSubscribe(cmd, socket) {
    if (!topics[cmd.topic]) {
        topics[cmd.topic] = new Set();
    }
    if (!topics_reverse[socket]) {
        topics_reverse[socket] = new Set();
    }
    
    topics[cmd.topic].add(socket);
    topics_reverse[socket].add(cmd.topic);
    
    const lastSeen = cmd.hasOwnProperty('last_seen') ? parseInt(cmd.last_seen, 10) : -1;

    sendSuccess(socket);

    if (cmd.hasOwnProperty('cache') ? cmd.cache : true) {
        sendCached(socket, cmd.topic, lastSeen);
    }
}

function handleUnsubscribe(cmd, socket) {
    if (topics[cmd.topic]) {
        topics[cmd.topic].delete(socket);
    }
    if (topics_reverse[socket]) {
        topics_reverse[socket].delete(cmd.topic);
    }
    sendSuccess(socket);
}

function handleSend(cmd, socket) {
    cmd.index = indexs[cmd.topic];
    indexs[cmd.topic] += 1;

    let doCache = cmd.hasOwnProperty('cache') ? cmd.cache : true;
    let subscribers;

    if (cmd.delivery === 'all') {
        subscribers = topics[cmd.topic];
    } else {  // delivery === 'one'
        if (topics[cmd.topic].size === 0) {
            subscribers = new Set();
        } else {
            const which = Math.floor(Math.random() * topics[cmd.topic].size);
            subscribers = new Set([Array.from(topics[cmd.topic])[which]]);
            doCache = false;
        }
    }

    if (doCache) {
        caches[cmd.topic].push(cmd);
    }

    for (const subscriber of subscribers) {
        sendCmd(subscriber, cmd);
    }

    sendSuccess(socket);
}

function handleCommand(cmd, socket) {
    const handlers = {
        subscribe: handleSubscribe,
        unsubscribe: handleUnsubscribe,
        send: handleSend
    };

    const handler = handlers[cmd.command];
    if (handler) {
        handler(cmd, socket);
    }
}

function handleClient(socket) {
    logging.info("New client connected...");

    const rl = readline.createInterface({
        input: socket,
        crlfDelay: Infinity
    });

    socket.on('error', (err) => {
        logging.error('Socket error on connection with', socket.remoteAddress, ':', err);
    });

    rl.on('line', (line) => {
        line = line.trim();
        if (line === "quit") {
            rl.close();
            return;
        }
        if (!line) {
            logging.info("Client disconnected (EOF)");
            rl.close();
            return;
        }
        try {
            line = decoder.write(Buffer.from(line, 'utf8')).trim();
        } catch (e) {
            sendFailure(socket, "Could not decode input as UTF-8");
            return;
        }
        if (line === "") return;
        logging.info(`Received: ${line}`);
        let cmd;
        try {
            cmd = JSON.parse(line);
        } catch (e) {
            sendFailure(socket, "Could not parse json");
            return;
        }
        try {
            if (verifyCommand(cmd)) {
                handleCommand(cmd, socket);
            } else {
                sendFailure(socket, "Malformed json message");
            }
        } catch (e) {
            logging.error(e);
            sendFailure(socket, "Internal exception");
        }
    });

    function cleanupSocket(socket) {
        if (topics_reverse[socket]) {
            for (let topic of topics_reverse[socket]) {
                topics[topic].delete(socket);
            }
            delete topics_reverse[socket];
        }
        logging.info("Client disconnected...");
    }
    
    socket.on('end', () => cleanupSocket(socket));
    socket.on('close', () => cleanupSocket(socket));
}

function runServer(host, port) {
    const server = net.createServer(handleClient);
    server.listen(port, host, () => {
        logging.info(`Listening on ${host}:${port}...`);
    });
}

const DEFAULT_PORT = 7000;
const DEFAULT_CACHE_SIZE = 100;

let port = DEFAULT_PORT;
let cacheSize = DEFAULT_CACHE_SIZE;

if (process.argv.length <= 4) {
    port = parseInt(process.argv[2], 10);
    if (isNaN(port) || port <= 0) {
       port = DEFAULT_PORT;
    }
}
if (process.argv.length == 4) {
    cacheSize = parseInt(process.argv[3], 10);
    if (isNaN(cacheSize) || cacheSize <= 0) {
       cacheSize = DEFAULT_CACHE_SIZE;
    }
}

/* global topics */
let topics = new DefaultDict(() => new Set());
/* global topics_reverse */
let topics_reverse = new DefaultDict(() => new Set());
/* global caches */
let caches = new DefaultDict(() => new Deque(cacheSize));
/* global indexs */
let indexs = new DefaultDict(() => 0);

process.on('unhandledRejection', (reason, promise) => {
    logging.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

runServer('localhost', port);
