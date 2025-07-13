const net = require('net');

const topics = {};
const caches = {};
const MAX_CACHE_LENGTH = 100

// {"command":"subscribe", "topic":"foo"}
// {"command":"subscribe", "topic":"bar"}
// {"command":"send", "topic":"foo", "msg":"1", "delivery": "one"}
// {"command":"send", "topic":"foo", "msg":"2", "delivery": "all"}
// {"command":"send", "topic":"foo", "msg":"3", "delivery": "all"}
// {"command":"send", "topic":"foo", "msg":"4", "delivery": "all"}

function handleClient(socket) {
    console.log('New client connected...');

    function sendCached(socket, topic) {
        if (!(topic in caches))
            return;
        // construct a new array cache
        let new_cache = new Array();
        caches[topic].forEach(line => {
            // write all cached messages
            socket.write(line + '\n\r');
            // only keep ones with delivery=all
            let cmd = JSON.parse(line);
            if (cmd.delivery === 'all')
                new_cache.push(line);
        });
        caches[topic] = new_cache;
    }

    socket.on('data', (data) => {
        const line = data.toString().trim();
        if (line === '')
            return;
        if (line != 'quit') {
            console.log(`Received: ${line}`);
            let cmd = JSON.parse(line);
            if (!(cmd.topic in topics))
                topics[cmd.topic] = new Set();
            if (cmd.command === 'subscribe') {
                topics[cmd.topic].add(socket);
                sendCached(socket, cmd.topic);
            }
            else if (cmd.command === 'send') {
                var recipients = Array.from(topics[cmd.topic]);
                if (recipients.length > 0 && cmd.delivery === 'one')
                    recipients = recipients.slice(0, 1);
                recipients.forEach(recipient => {
                    recipient.write(line + '\n\r');
                });
                if (cmd.delivery === 'all' || recipients.length == 0) {
                    // add to cache
                    if (!(cmd.topic in caches))
                        caches[cmd.topic] = new Array();
                    caches[cmd.topic].push(line);
                    if (caches[cmd.topic].length > MAX_CACHE_LENGTH)
                        caches[cmd.topic].shift();
                }
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
