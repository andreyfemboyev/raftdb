"""Load the shared fixed-membership cluster configuration."""

import hashlib
import json
from pathlib import Path

from .server import valid_request_id


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
