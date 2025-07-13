const net = require('net');

const topics = {};

// {"command":"subscribe", "topic":"foo"}
// {"command":"subscribe", "topic":"bar"}
// {"command":"send", "topic":"foo", "msg":"blah"}
// {"command":"send", "topic":"foo", "msg":"bar"}
// {"command":"send", "topic":"foo", "msg":"bar"}

function handleClient(socket) {
    console.log('New client connected...');

    socket.on('data', (data) => {
        const line = data.toString().trim();
        if (line === '')
            return;
        
        if (line != 'quit') {
            console.log(`Received: ${line}`);
            let cmd = JSON.parse(line);
            if (cmd.command === 'subscribe') {
                if (!topics[cmd.topic])
                    topics[cmd.topic] = new Set();
                topics[cmd.topic].add(socket);
            }
            else if (cmd.command === 'send') {
                const subscribers = topics[cmd.topic];
                if (subscribers) {
                    subscribers.forEach(subscriber => {
                        subscriber.write(data);
                    });
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
