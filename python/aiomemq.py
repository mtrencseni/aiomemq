import sys
import asyncio
import random
import json
import traceback
import logging
from collections import defaultdict, deque
from typing import Any, Callable, Deque, Dict, Optional, Union

logging.basicConfig(level=logging.CRITICAL + 1)

DEFAULT_PORT = 7000
DEFAULT_CACHE_SIZE = 100

topics: Dict[str, set] = defaultdict(set)
topics_reverse: Dict[asyncio.StreamWriter, set] = defaultdict(set)
caches: Dict[str, deque] = defaultdict(lambda: deque(maxlen=DEFAULT_CACHE_SIZE))
indexs: Dict[str, int] = defaultdict(int)

template_subscribe: Dict[str, Dict[str, Union[type, bool]]] = {
    "command": {"type": str, "required": True},
    "topic": {"type": str, "required": True},
    "last_seen": {"type": int, "required": False},
    "cache": {"type": bool, "required": False},
}

template_unsubscribe: Dict[str, Dict[str, Union[type, bool]]] = {
    "command": {"type": str, "required": True},
    "topic": {"type": str, "required": True},
}

template_send: Dict[str, Dict[str, Union[type, bool, list]]] = {
    "command": {"type": str, "required": True},
    "topic": {"type": str, "required": True},
    "msg": {"type": str, "required": True},
    "delivery": {"type": str, "required": True, "values": ["all", "one"]},
    "cache": {"type": bool, "required": False},
}


def template_match(template: Dict[str, Dict[str, Any]], obj: Dict[str, Any]) -> bool:
    if not set(obj.keys()).issubset(template.keys()):
        return False
    for key, props in template.items():
        if props["required"] and key not in obj:
            return False
        if key in obj and not isinstance(obj[key], props["type"]):
            return False
        if "values" in props and obj[key] not in props["values"]:
            return False
    return True


def send_cmd(writer: asyncio.StreamWriter, cmd: Dict[str, Any]) -> None:
    writer.write((json.dumps(cmd) + "\r\n").encode("utf8"))


def send_success(writer: asyncio.StreamWriter) -> None:
    cmd = {"success": True}
    send_cmd(writer, cmd)


def send_failure(writer: asyncio.StreamWriter, reason: str) -> None:
    cmd = {"success": False, "reason": reason}
    send_cmd(writer, cmd)


def send_cached(writer: asyncio.StreamWriter, topic: str, last_seen: int) -> None:
    for cmd in caches[topic]:
        if cmd["index"] > last_seen:
            send_cmd(writer, cmd)
    new_cache: Deque[Dict[str, Any]] = deque(maxlen=cache_size)
    for cmd in caches[topic]:
        if cmd["index"] <= last_seen:
            new_cache.append(cmd)
        else:
            if cmd["delivery"] == "all":
                new_cache.append(cmd)
    caches[topic] = new_cache


def handle_subscribe(cmd: Dict[str, Any], writer: asyncio.StreamWriter) -> None:
    topics[cmd["topic"]].add(writer)
    topics_reverse[writer].add(cmd["topic"])
    last_seen = int(cmd["last_seen"]) if "last_seen" in cmd else -1
    send_success(writer)
    if cmd.get("cache", True):
        send_cached(writer, cmd["topic"], last_seen)


def handle_unsubscribe(cmd: Dict[str, Any], writer: asyncio.StreamWriter) -> None:
    topics[cmd["topic"]].remove(writer)
    topics_reverse[writer].remove(cmd["topic"])
    send_success(writer)


def handle_send(cmd: Dict[str, Any], writer: asyncio.StreamWriter) -> None:
    cmd["index"] = indexs[cmd["topic"]]
    indexs[cmd["topic"]] += 1
    do_cache = cmd.get("cache", True)
    if cmd["delivery"] == "all":
        subscribers = topics[cmd["topic"]]
    else:  # delivery == 'one':
        if len(topics[cmd["topic"]]) == 0:
            subscribers = set([])
        else:
            which = random.randint(0, len(topics[cmd["topic"]]) - 1)
            subscribers = set([list(topics[cmd["topic"]])[which]])
            do_cache = False
    if do_cache:
        caches[cmd["topic"]].append(cmd)
    for subscriber in subscribers:
        send_cmd(subscriber, cmd)
    send_success(writer)


def verify_command(cmd: Dict[str, Any]) -> bool:
    templates: Dict[str, Dict[str, Dict[str, Any]]] = {
        "subscribe": template_subscribe,
        "unsubscribe": template_unsubscribe,
        "send": template_send,
    }
    if "command" not in cmd or not isinstance(cmd["command"], str):
        return False
    template: Optional[Dict[str, Dict[str, Any]]] = templates.get(cmd["command"])
    if template is None or not template_match(template, cmd):
        return False
    return True


def handle_command(cmd: Dict[str, Any], writer: asyncio.StreamWriter) -> None:
    handlers = {
        "subscribe": handle_subscribe,
        "unsubscribe": handle_unsubscribe,
        "send": handle_send,
    }
    handler = handlers.get(cmd["command"])
    if handler:
        handler(cmd, writer)


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    logging.info("New client connected...")
    line = str()
    try:
        while line.strip() != "quit":
            line = await reader.readline()
            if not line:  # Check for EOF
                logging.info("Client disconnected (EOF)")
                break
            try:
                line = line.decode("utf8").strip()
            except UnicodeDecodeError:
                send_failure(writer, "Could not decode input as UTF-8")
                continue
            if line == "":
                continue
            logging.info(f"Received: {line}")
            try:
                cmd = json.loads(line)
            except json.JSONDecodeError:
                send_failure(writer, "Could not parse json")
            else:
                try:
                    if verify_command(cmd):
                        handle_command(cmd, writer)
                    else:
                        send_failure(writer, "Malformed json message")
                except Exception as e:
                    logging.info(traceback.format_exc())
                    send_failure(writer, "Internal exception")
    except Exception as e:
        logging.info(traceback.format_exc())
    finally:
        if writer in topics_reverse:
            for topic in topics_reverse[writer]:
                topics[topic].remove(writer)
            del topics_reverse[writer]
        writer.close()
        await writer.wait_closed()
        logging.info("Client disconnected...")


async def run_server(host: str, port: int) -> None:
    server = await asyncio.start_server(handle_client, host, port)
    logging.info(f"Listening on {host}:{port}...")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    if len(sys.argv) not in [1, 2, 3]:
        logging.info(f"Usage: python3 aiomemq.py <port> <cache_size>")
        logging.info(f"  <port>       - optional, default {DEFAULT_PORT}")
        logging.info(f"  <cache_size> - optional, default {DEFAULT_CACHE_SIZE}")
        sys.exit(1)
    port = int(sys.argv[1]) if len(sys.argv) == 2 else DEFAULT_PORT
    cache_size = int(sys.argv[2]) if len(sys.argv) == 3 else DEFAULT_CACHE_SIZE
    caches = defaultdict(lambda: deque(maxlen=cache_size))
    asyncio.run(run_server(host="localhost", port=port))
