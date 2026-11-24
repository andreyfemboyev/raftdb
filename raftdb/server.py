"""HTTP endpoints for Raft RPCs, SQL requests, and node status."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
import traceback

from .raft import BATCH_SIZE, MAX_REQUEST_ID, MAX_SQL_BYTES


MAX_BODY = 2 * 1024 * 1024
ID = re.compile(r"[a-zA-Z0-9_.:-]{1," + str(MAX_REQUEST_ID) + r"}\Z")


def valid_request_id(value):
    return isinstance(value, str) and ID.fullmatch(value) is not None


def valid_sql(value):
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        return len(value.encode("utf-8")) <= MAX_SQL_BYTES
    except UnicodeError:
        return False


def unsigned(value):
    return type(value) is int and 0 <= value < 2**63


def valid_entry(entry, term):
    if not isinstance(entry, dict) or not unsigned(entry.get("term")):
        return False
    if not 0 < entry["term"] <= term:
        return False
    if entry.keys() == {"term"}:
        return True
    return (
        entry.keys() == {"term", "request_id", "sql"}
        and valid_request_id(entry["request_id"]) and valid_sql(entry["sql"])
    )


def valid_raft_message(node, path, data):
    source = data.get("source")
    term = data.get("term")
    if (
        data.get("cluster") != node.cluster or not isinstance(source, str)
        or source not in node.peers or not unsigned(term) or term == 0
    ):
        return False
    if path == "/raft/vote":
        return (
            unsigned(data.get("last_index")) and unsigned(data.get("last_term"))
            and data["last_term"] <= term
        )
    if not all(unsigned(data.get(field)) for field in ("prev_index", "prev_term", "commit")):
        return False
    entries = data.get("entries")
    if not isinstance(entries, list) or len(entries) > BATCH_SIZE or data["prev_term"] > term:
        return False
    last_term = data["prev_term"]
    for entry in entries:
        if not valid_entry(entry, term) or entry["term"] < last_term:
            return False
        last_term = entry["term"]
    return True


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        self.request.settimeout(3)
        super().setup()

    def do_GET(self):
        if self.path == "/status":
            self.send_json(200, self.server.node.status())
        else:
            self.error_json(404, "unknown_route", "Unknown route")

    def do_POST(self):
        if self.path not in ("/sql", "/raft/vote", "/raft/append"):
            self.error_json(404, "unknown_route", "Unknown route")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > MAX_BODY:
                raise ValueError("Invalid Content-Length")
            data = json.loads(self.rfile.read(length))
            if not isinstance(data, dict):
                raise ValueError("Expected an object")
        except (ValueError, RecursionError, OSError):
            self.error_json(400, "bad_request", "Expected a bounded JSON object")
            return
        node = self.server.node
        if self.path == "/sql":
            request_id, sql = data.get("request_id"), data.get("sql")
            if not valid_request_id(request_id) or not valid_sql(sql):
                self.error_json(400, "bad_request", "Expected a valid request_id and a SQL statement")
                return
            self.dispatch(node.propose, request_id, sql)
        elif valid_raft_message(node, self.path, data):
            method = node.request_vote if self.path == "/raft/vote" else node.append_entries
            self.dispatch(lambda: (200, method(data)))
        else:
            self.error_json(400, "bad_request", "Invalid Raft RPC")

    def dispatch(self, method, *args):
        try:
            status, response = method(*args)
        except Exception:
            traceback.print_exc()
            os._exit(70)
        self.send_json(status, response)

    def error_json(self, status, code, message):
        self.send_json(status, {"ok": False, "code": code, "error": message})

    def send_json(self, status, response):
        body = json.dumps(response, separators=(",", ":")).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass

    def log_message(self, *args):
        pass


class Server(ThreadingHTTPServer):
    def __init__(self, address, node):
        self.node = node
        super().__init__(address, Handler)
