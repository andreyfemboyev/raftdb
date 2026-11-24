"""Run a RaftDB node or query a cluster."""

import argparse
import hashlib
import http.client
import json
from pathlib import Path
import sys
import time
from uuid import uuid4

from .raft import Node, PROPOSAL_TIMEOUT, rpc
from .server import Server, valid_request_id, valid_sql
from .storage import Storage


def load_cluster(path):
    with Path(path).open(encoding="utf-8") as source:
        config = json.load(source)
    if not isinstance(config, dict) or set(config) != {"cluster", "nodes"}:
        raise ValueError("Config requires cluster and nodes fields")
    name = config["cluster"]
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Expected a non-empty cluster name")
    members = {}
    if not isinstance(config["nodes"], dict) or not config["nodes"]:
        raise ValueError("Expected a non-empty map of node IDs to host:port strings")
    for node, binding in config["nodes"].items():
        if not valid_request_id(node) or not isinstance(binding, str):
            raise ValueError("Invalid node ID or address")
        host, separator, port = binding.partition(":")
        if not host or not separator or not port.isascii() or not port.isdecimal():
            raise ValueError(f"Invalid host:port for {node}")
        if len(port) > 5 or not 0 < int(port) < 65536 or any(char.isspace() for char in host):
            raise ValueError(f"Invalid host:port for {node}")
        members[node] = (host, int(port))
    if len(set(members.values())) != len(members):
        raise ValueError("Each node requires a distinct address")
    canonical = json.dumps({"cluster": name, "nodes": members}, sort_keys=True)
    return members, hashlib.sha256(canonical.encode()).hexdigest()


def serve(args, members, cluster):
    if args.node not in members:
        raise ValueError(f"Unknown node {args.node}")
    host, port = members[args.node]
    storage = Storage(args.data)
    try:
        node = Node(args.node, members, cluster, storage)
        with Server((args.bind or host, port), node) as server:
            node.start()
            print(f"Node {args.node} listening on {args.bind or host}:{port}", flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                node.stop()
    finally:
        storage.lock.close()
    return 0


def query(args, members):
    sql = args.sql if args.sql is not None else sys.stdin.read()
    if not valid_sql(sql):
        raise ValueError("Expected one non-empty SQL statement, at most 16384 UTF-8 bytes")
    request_id = args.request_id if args.request_id is not None else uuid4().hex
    if not valid_request_id(request_id):
        raise ValueError("Invalid request ID")
    deadline = time.monotonic() + args.timeout
    candidates = list(members)
    last_error = "No cluster nodes responded"
    payload = {"sql": sql, "request_id": request_id}
    while time.monotonic() < deadline:
        if not candidates:
            candidates = list(members)
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
        target = candidates.pop(0)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            status, result = rpc(
                members[target], "/sql", payload, timeout=min(PROPOSAL_TIMEOUT + 0.5, remaining)
            )
        except (OSError, http.client.HTTPException, ValueError) as error:
            last_error = str(error)
            continue
        if not isinstance(result, dict):
            continue
        if status in (200, 400, 409, 422):
            print(json.dumps(result, ensure_ascii=True, indent=2))
            return 0 if status == 200 else 2
        last_error = result.get("error", last_error)
        hint = result.get("leader")
        if isinstance(hint, str) and hint in candidates:
            candidates.remove(hint)
            candidates.insert(0, hint)

    print(json.dumps({
        "ok": False, "code": "unconfirmed", "request_id": request_id, "error": last_error,
    }, indent=2))
    return 2


def status(args, members):
    if args.node is not None and args.node not in members:
        raise ValueError(f"Unknown node {args.node}")
    states = {}
    for node in [args.node] if args.node is not None else members:
        connection = http.client.HTTPConnection(*members[node], timeout=0.5)
        try:
            connection.request("GET", "/status")
            with connection.getresponse() as response:
                states[node] = json.load(response)
        except (OSError, http.client.HTTPException, ValueError) as error:
            states[node] = {"error": str(error)}
        finally:
            connection.close()
    print(json.dumps(states, indent=2))
    return 2 if all("error" in state for state in states.values()) else 0


def positive_timeout(raw):
    value = float(raw)
    if not 0 < value <= 3600:
        raise argparse.ArgumentTypeError("Timeout must be greater than zero and at most 3600 seconds")
    return value


def main():
    parser = argparse.ArgumentParser(prog="python3 -m raftdb")
    sub = parser.add_subparsers(dest="operation", required=True)
    server = sub.add_parser("serve", help="Start one replica")
    server.add_argument("--node", required=True)
    server.add_argument("--data", required=True)
    server.add_argument("--bind", help="Override the local bind host while retaining the configured port")

    client = sub.add_parser("sql", help="Execute a SQL statement, given as an argument or on stdin")
    client.add_argument("sql", nargs="?")
    client.add_argument("--request-id", help="Reuse this ID when retrying the same operation")
    client.add_argument("--timeout", type=positive_timeout, default=6.0)

    inspector = sub.add_parser("status", help="Show observed leader and replica progress")
    inspector.add_argument("--node")
    for command in (server, client, inspector):
        command.add_argument("--config", default="cluster.json")
    args = parser.parse_args()

    try:
        members, cluster = load_cluster(args.config)
        if args.operation == "serve":
            return serve(args, members, cluster)
        if args.operation == "sql":
            return query(args, members)
        return status(args, members)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"raftdb: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
