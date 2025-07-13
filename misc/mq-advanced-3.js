const net = require('net');

class DefaultDict {
    constructor(defaultInit) {
        return new Proxy({}, {
            get: (target, name) => name in target ?
                target[name] :
                (target[name] = typeof defaultInit === 'function' ?
                    new defaultInit().valueOf() :
                    defaultInit)
        })
    }
}

const topics = new DefaultDict(Set);
const caches = new DefaultDict(Array);
const indexs = new DefaultDict(Number);
const MAX_CACHE_LENGTH = 100

// {"command":"subscribe", "topic":"foo"}
// {"command":"subscribe", "topic":"foo", "last_seen": 2}
// {"command":"send", "topic":"foo", "msg":"1", "delivery": "one"}
// {"command":"send", "topic":"foo", "msg":"2", "delivery": "all"}
// {"command":"send", "topic":"foo", "msg":"3", "delivery": "all"}
// {"command":"send", "topic":"foo", "msg":"4", "delivery": "all"}

function writeCommand(socket, cmd) {
    socket.write(JSON.stringify(cmd) + '\n\r');
}

function handleClient(socket) {
    console.log('New client connected...');

    function sendCached(socket, topic, last_seen) {
        if (!(topic in caches))
            return;
        // construct a new array cache
        let new_cache = new Array();
        caches[topic].forEach(cmd => {
            // write all cached messages the client has not seen yet
            if (cmd.index > last_seen)
                writeCommand(socket, cmd);
            // only keep ones with delivery=all
            if (cmd.index <= last_seen || cmd.delivery === 'all')
                new_cache.push(cmd);

        });
        caches[topic] = new_cache;
    }
    
    function handleSubscribe(socket, cmd) {
        topics[cmd.topic].add(socket);
        const last_seen = 'last_seen' in cmd ? cmd.last_seen : -1;
        sendCached(socket, cmd.topic, last_seen);
    }
    
    function handleSend(cmd) {
        cmd.index = indexs[cmd.topic];
        indexs[cmd.topic] += 1;
        var recipients = Array.from(topics[cmd.topic]);
        if (recipients.length > 0 && cmd.delivery === 'one')
            recipients = recipients.slice(0, 1);
        recipients.forEach(socket => {
            writeCommand(socket, cmd);
        });
        if (cmd.delivery === 'all' || recipients.length == 0) {
            // add to cache
            caches[cmd.topic].push(cmd);
            if (caches[cmd.topic].length > MAX_CACHE_LENGTH)
                caches[cmd.topic].shift();
        }        
    }

    socket.on('data', (data) => {
        const line = data.toString().trim();
        if (line === '')
            return;
        if (line != 'quit') {
            console.log(`Received: ${line}`);
            try {
                let cmd = JSON.parse(line);
                if (cmd.command === 'subscribe')
                    handleSubscribe(socket, cmd);
                else if (cmd.command === 'send')
                    handleSend(cmd);
            } catch (e) {
                socket.write('Error processing command\n\r');
            }
        } else {
            socket.end();
        }
    });
    
    socket.on('end', () => {
        console.log('Client disconnected...');
        Object.keys(topics).forEach(topic => {
            topics[topic].delete(socket);
        });
    });
}

const server = net.createServer(handleClient);

const port = process.argv[2];
server.listen(port, () => {
    console.log(`Listening on localhost:${port}...`);
});
