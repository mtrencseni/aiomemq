from collections import defaultdict
import pytest
import subprocess
import socket
import random
import string
import time
import json
import os

SERVER_HOST = 'localhost'
SERVER_PORT = 7000
CACHE_SIZE = 2

@pytest.fixture(scope="module")
def server():
    # Start the server as a separate process
    server_process = subprocess.Popen(['python3', 'aiomemq.py', str(SERVER_PORT), str(CACHE_SIZE)])
    time.sleep(1)  # Give the server some time to start
    yield
    server_process.terminate()
    server_process.wait()

def receive(sock):
    response = sock.recv(64*1024).decode('utf-8').strip()
    return json.loads(response)

def receive_many(sock, timeout=None, allow_trailing_bytes=False):
    sock.settimeout(timeout)
    response = ''
    try:
        response = sock.recv(64*1024).decode('utf-8').strip()
    except TimeoutError:
        pass
    sock.settimeout(None)
    messages = response.split("\r\n")
    response = []
    if allow_trailing_bytes:
        for msg in messages:
            try:
                response.append(json.loads(msg))
            except:
                return response
    else:
        response = [json.loads(msg) for msg in messages if msg]
    return response

def receive_until(sock, expected_count, timeout=0.05, allow_trailing_bytes=False):
    end_time = time.time() + timeout
    responses = []
    while time.time() < end_time:
        try:
            responses.extend(receive_many(sock, timeout, allow_trailing_bytes))
            if len(responses) >= expected_count:
                break
        except socket.timeout:
            break
    return responses

def send(sock, message):
    if isinstance(message, str):
        sock.sendall((message + "\r\n").encode('utf-8'))
    elif isinstance(message, bytes):
        sock.sendall(message + b"\r\n")
    else:
        sock.sendall((json.dumps(message) + "\r\n").encode('utf-8'))

def send_and_receive(sock, message):
    send(sock, message)
    time.sleep(0.01)
    return receive(sock)

def send_and_receive_many(sock, message, allow_trailing_bytes=False):
    send(sock, message)
    time.sleep(0.01)
    return receive_many(sock, timeout=None, allow_trailing_bytes=allow_trailing_bytes)

def send_and_receive_until(sock, message, expected_count, timeout=0.05, allow_trailing_bytes=False):
    send(sock, message)
    time.sleep(0.01)
    return receive_until(sock, expected_count, timeout, allow_trailing_bytes)

def generate_random_topic():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_random_bytes(length):
    return os.urandom(length)

def _test_random_bytes(length):
    random_bytes = generate_random_bytes(length)
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client:
        response = send_and_receive_many(client, random_bytes, allow_trailing_bytes=True)
        # the random bytes could generate multiple responses
        for r in response: r == {'success': False, 'reason': 'Could not decode input as UTF-8'}

def _test_random_string(length):
    random_bytes = generate_random_string(length)
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client:
        response = send_and_receive_many(client, random_bytes, allow_trailing_bytes=True)
        # the random bytes could generate multiple responses
        for r in response: r == {'success': False, 'reason': 'Could not parse json'}

def test_random_bytes_100(server):
    return _test_random_bytes(100)

def test_random_bytes_1k(server):
    return _test_random_bytes(1024)

def test_random_bytes_10k(server):
    return _test_random_bytes(10*1024)

# server reads in 64k chunks
def test_random_bytes_64k(server):
    return _test_random_bytes(64*1024-2)

def test_random_string_100(server):
    return _test_random_string(100)

def test_random_string_1k(server):
    return _test_random_string(1024)

def test_random_string_10k(server):
    return _test_random_string(10*1024)

# server reads in 64k chunks
def test_random_string_64k(server):
    return _test_random_string(64*1024-2)

def test_subscribe_validation(server):
    topic = generate_random_topic()
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client1:
        # missing key
        response = send_and_receive(client1, {'command': 'subscribe'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # extra key
        response = send_and_receive(client1, {'command': 'subscribe', 'topic': topic, 'extra_key': 'extra_value'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # bad type
        response = send_and_receive(client1, {'command': 123, 'topic': topic})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # bad type
        response = send_and_receive(client1, {'command': 'subscribe', 'topic': 123})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # bad type
        response = send_and_receive(client1, {'command': 'subscribe', 'topic': topic, 'last_seen': "123"})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # bad type
        response = send_and_receive(client1, {'command': 'subscribe', 'topic': topic, 'cache': 123})
        assert response == {'success': False, 'reason': 'Malformed json message'}

def test_unsubscribe_validation(server):
    topic = generate_random_topic()
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client1:
        # missing key
        response = send_and_receive(client1, {'command': 'unsubscribe'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # extra key
        response = send_and_receive(client1, {'command': 'unsubscribe', 'topic': topic, 'extra_key': 'extra_value'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # bad type
        response = send_and_receive(client1, {'command': 123, 'topic': topic})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # bad type
        response = send_and_receive(client1, {'command': 'unsubscribe', 'topic': 123})
        assert response == {'success': False, 'reason': 'Malformed json message'}

def test_send_validation(server):
    topic = generate_random_topic()
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client1:
        # missing key (command)
        response = send_and_receive(client1, {'topic': topic, 'msg': 'hello', 'delivery': 'all'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # missing key (topic)
        response = send_and_receive(client1, {'command': 'send', 'msg': 'hello', 'delivery': 'all'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # missing key (msg)
        response = send_and_receive(client1, {'command': 'send', 'topic': topic, 'delivery': 'all'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # missing key (delivery)
        response = send_and_receive(client1, {'command': 'send', 'topic': topic, 'msg': 'hello'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # extra key
        response = send_and_receive(client1, {'command': 'send', 'topic': topic, 'msg': 'hello', 'delivery': 'all', 'extra_key': 'extra_value'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # bad type (command)
        response = send_and_receive(client1, {'command': 123, 'topic': topic, 'msg': 'hello', 'delivery': 'all'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # bad type (topic)
        response = send_and_receive(client1, {'command': 'send', 'topic': 123, 'msg': 'hello', 'delivery': 'all'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # bad type (msg)
        response = send_and_receive(client1, {'command': 'send', 'topic': topic, 'msg': 123, 'delivery': 'all'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # bad type (delivery)
        response = send_and_receive(client1, {'command': 'send', 'topic': topic, 'msg': 'hello', 'delivery': 123})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # invalid value (delivery)
        response = send_and_receive(client1, {'command': 'send', 'topic': topic, 'msg': 'hello', 'delivery': 'invalid'})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # bad type (cache)
        response = send_and_receive(client1, {'command': 'send', 'topic': topic, 'msg': 'hello', 'delivery': 'all', 'cache': 'not_a_bool'})
        assert response == {'success': False, 'reason': 'Malformed json message'}

def test_quoting(server):
    strings_with_quotes = ["''''''", '""""""', "'\"'\"'\""]
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client1:
        for s in strings_with_quotes:
            # Subscribe to the topic with quotes
            response = send_and_receive(client1, {'command': 'subscribe', 'topic': s})
            assert response == {'success': True}
            # Send a message to the topic with quotes
            message = {'command': 'send', 'topic': s, 'msg': s, 'delivery': 'all'}
            response = send_and_receive_until(client1, message, 2)
            assert {'success': True} in response
            assert {
                'command': 'send',
                'topic': s,
                'msg': s,
                'index': 0,
                'delivery': 'all'
            } in response

def _test_topic_length(length):
    topic = "a" * length
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client1:
        # Subscribe to the topic
        response = send_and_receive(client1, {'command': 'subscribe', 'topic': topic})
        assert response == {'success': True}
        # Send a message to the topic
        message = {'command': 'send', 'topic': topic, 'msg': 'test message', 'delivery': 'all'}
        response = send_and_receive_until(client1, message, 2)
        assert {'success': True} in response
        assert {
            'command': 'send',
            'topic': topic,
            'msg': 'test message',
            'index': 0,
            'delivery': 'all'
        } in response

def test_topic_length_min(server):
    _test_topic_length(1)

def test_topic_length_max_256(server):
    _test_topic_length(256)

def test_topic_length_max_1024(server):
    _test_topic_length(1024)

def _test_concurrent_clients(delivery):
    topic = generate_random_topic()
    clients = []
    # Create multiple clients
    for i in range(3):
        client = socket.create_connection((SERVER_HOST, SERVER_PORT))
        clients.append(client)
        # Subscribe each client to the topic
        response = send_and_receive(client, {'command': 'subscribe', 'topic': topic})
        assert response == {'success': True}
    # Send a message to the topic
    sender = socket.create_connection((SERVER_HOST, SERVER_PORT))
    message = {'command': 'send', 'topic': topic, 'msg': 'test message', 'delivery': delivery}
    response = send_and_receive(sender, message)
    assert response == {'success': True}
    # Verify message delivery based on the delivery mode
    if delivery == 'all':
        for client in clients:
            received_message = receive(client)
            expected_message = {
                'command': 'send',
                'topic': topic,
                'msg': 'test message',
                'index': 0,
                'delivery': 'all'
            }
            assert received_message == expected_message
    elif delivery == 'one':
        received_count = 0
        for client in clients:
            received_messages = receive_until(client, expected_count=1)
            if len(received_messages) == 0:
                continue
            assert received_messages[0] == {
                'command': 'send',
                'topic': topic,
                'msg': 'test message',
                'index': 0,
                'delivery': 'one'
            }
            received_count += 1
        # Ensure only one client received the message
        assert received_count == 1
    # Close all client connections
    for client in clients:
        client.close()
    sender.close()

def test_concurrent_clients_all(server):
    _test_concurrent_clients('all')

def test_concurrent_clients_one(server):
    _test_concurrent_clients('one')

def test_non_existing_commands(server):
    topic = generate_random_topic()
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client1:
        # Non-existing command
        response = send_and_receive(client1, {'command': 'non_existing_command', 'topic': topic})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # Another non-existing command
        response = send_and_receive(client1, {'command': 'invalid_command', 'topic': topic})
        assert response == {'success': False, 'reason': 'Malformed json message'}
        # Yet another non-existing command
        response = send_and_receive(client1, {'command': 'fake_command', 'topic': topic})
        assert response == {'success': False, 'reason': 'Malformed json message'}

def _test_many_connections(server, num_clients, num_messages):
    clients = []
    topics = []
    expected_counts = defaultdict(int)
    # Step 1: All clients connect and create a topic for themselves
    for i in range(num_clients):
        client = socket.create_connection((SERVER_HOST, SERVER_PORT))
        topic = generate_random_topic()
        topics.append(topic)
        clients.append((client, topic))
        # Subscribe to its own topic
        response = send_and_receive(client, {'command': 'subscribe', 'topic': topic})
        assert response == {'success': True}
    # Step 2: All clients select another random client and send it K messages
    for sender_id in range(num_clients):
        sender, sender_topic = clients[sender_id]
        for _ in range(num_messages):
            recipient_id = random.choice([i for i in range(num_clients) if i != sender_id])
            recipient_topic = topics[recipient_id]
            message = {'command': 'send', 'topic': recipient_topic, 'msg': f'test message from {sender_id} to {recipient_id}', 'delivery': 'all'}
            # Just send the message, don't read responses, since sent messages could also be arriving
            send(sender, message)
            expected_counts[sender_id] += 1    # the {'success': True}
            expected_counts[recipient_id] += 1 # the actual message
    time.sleep(1)
    # Step 3: Clients ensure they only receive messages addressed to them
    total_received_messages = 0
    for recipient_id in range(num_clients):
        client, topic = clients[recipient_id]
        # received_messages = receive_many(client)
        received_messages = receive_until(client, expected_count=expected_counts[recipient_id], timeout=0.1, allow_trailing_bytes=False)
        assert len(received_messages) == expected_counts[recipient_id]
        assert {'success': True} in received_messages
        # Check that all received messages were addressed to this client
        for msg in received_messages:
            if 'command' in msg:
                assert msg['command'] == 'send'
                assert msg['topic'] == topic
                total_received_messages += 1
    # Close all client connections
    for client, _ in clients:
        client.close()
    # Step 4: Check that in total, all N*K messages were received
    assert total_received_messages == num_clients * num_messages

def test_many_connections_10_1(server):
    _test_many_connections(server, num_clients=10, num_messages=1)

def test_many_connections_10_10(server):
    _test_many_connections(server, num_clients=10, num_messages=10)

def test_many_connections_10_100(server):
    _test_many_connections(server, num_clients=10, num_messages=100)

def test_many_connections_100_1(server):
    _test_many_connections(server, num_clients=100, num_messages=1)

def test_many_connections_100_10(server):
    _test_many_connections(server, num_clients=100, num_messages=10)

def test_many_connections_100_100(server):
    _test_many_connections(server, num_clients=100, num_messages=100)

def test_many_connections_1k_1(server):
    _test_many_connections(server, num_clients=1000, num_messages=1)

def test_many_connections_1k_10(server):
    _test_many_connections(server, num_clients=1000, num_messages=10)

def test_many_connections_1k_100(server):
    _test_many_connections(server, num_clients=1000, num_messages=100)

def test_many_connections_10k_1(server):
    _test_many_connections(server, num_clients=10*1000, num_messages=1)

def test_many_connections_10k_10(server):
    _test_many_connections(server, num_clients=10*1000, num_messages=10)

def test_cache_behavior(server):
    topic = generate_random_topic()
    # Step 1: Sender connects and sends messages to the topic
    sender = socket.create_connection((SERVER_HOST, SERVER_PORT))
    for i in range(CACHE_SIZE):
        message = {'command': 'send', 'topic': topic, 'msg': f'test message {i}', 'delivery': 'all'}
        response = send_and_receive(sender, message)
        assert response == {'success': True}
    sender.close()
    # Step 2: Receiver connects with cache set to False and should not receive any messages
    receiver_no_cache = socket.create_connection((SERVER_HOST, SERVER_PORT))
    response = send_and_receive(receiver_no_cache, {'command': 'subscribe', 'topic': topic, 'cache': False})
    assert response == {'success': True}
    # Ensure no more messages are received
    received_messages = receive_many(receiver_no_cache, timeout=0.1)
    assert received_messages == []
    receiver_no_cache.close()
    # Step 3: Another receiver connects with cache set to True and should receive all messages
    receiver_with_cache = socket.create_connection((SERVER_HOST, SERVER_PORT))
    received_messages = send_and_receive_many(receiver_with_cache, {'command': 'subscribe', 'topic': topic, 'cache': True})
    # Ensure all cached messages are received
    expected_messages = [
        {'command': 'send', 'topic': topic, 'msg': f'test message {i}', 'index': i, 'delivery': 'all'}
        for i in range(CACHE_SIZE)
    ] + [{'success': True}]
    assert len(received_messages) == len(expected_messages)
    for msg in expected_messages:
        assert msg in received_messages
    receiver_with_cache.close()

def test_simple_send(server):
    topic = generate_random_topic()
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client1, \
         socket.create_connection((SERVER_HOST, SERVER_PORT)) as client2:
        response = send_and_receive(client1, {'command': 'subscribe', 'topic': topic})
        assert response == {'success': True}
        response = send_and_receive(client2, {'command': 'send', 'topic': topic, 'msg': 'hello', 'delivery': 'all'})
        assert response == {'success': True}
        response = receive(client1)
        assert response == {'command': 'send', 'topic': topic, 'msg': 'hello', 'delivery': 'all', 'index': 0}

def test_cache_size(server):
    topic = generate_random_topic()
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client1:
        # Client 1 sends 5 messages to the random topic
        for i in range(5):
            message = {'command': 'send', 'topic': topic, 'msg': f'hello{i}', 'delivery': 'all'}
            response = send_and_receive(client1, message)
            assert response == {'success': True}
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client2:
        # Client 2 subscribes to the random topic
        message = {'command': 'subscribe', 'topic': topic, 'cache': True}
        response = send_and_receive_until(client2, message, 3)
        assert len(response) == 3
        # Client 2 should receive the subscription success message
        assert response[0] == {'success': True}
        # Client 2 should only receive the last 2 messages
        expected_messages = [
            {'command': 'send', 'topic': topic, 'msg': 'hello3', 'index': 3, 'delivery': 'all'},
            {'command': 'send', 'topic': topic, 'msg': 'hello4', 'index': 4, 'delivery': 'all'},
        ]
        assert response[-2:] == expected_messages

def test_delivery_semantics(server):
    topic = generate_random_topic()
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client1:
        # Client 1 sends 5 messages to the random topic
        for i in range(5):
            message = {'command': 'send', 'topic': topic, 'msg': f'hello{i}', 'delivery': 'one' if i < 4 else 'all'}
            response = send_and_receive(client1, message)
            assert response == {'success': True}
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client2:
        # Client 2 subscribes to the random topic
        message = {'command': 'subscribe', 'topic': topic, 'cache': True}
        response = send_and_receive_until(client2, message, 3)
        assert len(response) == 3
        # Client 2 should receive the subscription success message
        assert response[0] == {'success': True}
        # Client 2 should only receive the last 2 messages
        expected_messages = [
            {'command': 'send', 'topic': topic, 'msg': 'hello3', 'index': 3, 'delivery': 'one'},
            {'command': 'send', 'topic': topic, 'msg': 'hello4', 'index': 4, 'delivery': 'all'},
        ]
        assert response[-2:] == expected_messages
    with socket.create_connection((SERVER_HOST, SERVER_PORT)) as client3:
        # Client 2 subscribes to the random topic
        message = {'command': 'subscribe', 'topic': topic, 'cache': True}
        response = send_and_receive_until(client3, message, 2)
        assert len(response) == 2
        # Client 2 should receive the subscription success message
        assert response[0] == {'success': True}
        # Client 2 should only receive the last message that had delivery=all
        expected_messages = [
            {'command': 'send', 'topic': topic, 'msg': 'hello4', 'index': 4, 'delivery': 'all'},
        ]
        assert response[-1:] == expected_messages

def test_last_seen_behavior(server):
    topic = generate_random_topic()
    # Step 1: Sender connects and sends messages to the topic
    sender = socket.create_connection((SERVER_HOST, SERVER_PORT))
    for i in range(5):
        message = {'command': 'send', 'topic': topic, 'msg': f'test message {i}', 'delivery': 'all'}
        response = send_and_receive(sender, message)
        assert response == {'success': True}
    sender.close()
    # Step 2: Receiver connects with last_seen set to 2 and should only receive messages with index > 2
    receiver = socket.create_connection((SERVER_HOST, SERVER_PORT))
    received_messages = send_and_receive_many(receiver, {'command': 'subscribe', 'topic': topic, 'last_seen': 2})
    # Ensure only messages with index 3 and 4 are received
    assert received_messages == [
        {'success': True},
        {'command': 'send', 'topic': topic, 'msg': 'test message 3', 'index': 3, 'delivery': 'all'},
        {'command': 'send', 'topic': topic, 'msg': 'test message 4', 'index': 4, 'delivery': 'all'}
    ]
    receiver.close()
    # Step 3: Another receiver connects with last_seen set to 4 and should receive no messages
    receiver_no_messages = socket.create_connection((SERVER_HOST, SERVER_PORT))
    response = send_and_receive(receiver_no_messages, {'command': 'subscribe', 'topic': topic, 'last_seen': 4})
    assert response == {'success': True}
    # Ensure no messages are received
    received_messages = receive_many(receiver_no_messages, timeout=0.1)
    assert received_messages == [], "Receiver with last_seen=4 should not receive any messages"
    receiver_no_messages.close()
