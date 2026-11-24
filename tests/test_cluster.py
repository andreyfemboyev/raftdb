from concurrent.futures import ThreadPoolExecutor
import http.client
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from raftdb.raft import rpc


ROOT = Path(__file__).resolve().parents[1]


class ClusterTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.config = self.directory / "cluster.json"
        self.members = {}
        self.processes = {}
        self.addCleanup(self.stop_all)
        sockets = []
        try:
            for node in ("a", "b", "c"):
                reservation = socket.socket()
                sockets.append(reservation)
                reservation.bind(("127.0.0.1", 0))
                self.members[node] = reservation.getsockname()
        finally:
            for reservation in sockets:
                reservation.close()
        self.config.write_text(json.dumps({
            "cluster": "integration",
            "nodes": {node: f"{host}:{port}" for node, (host, port) in self.members.items()},
        }))
        for node in self.members:
            self.start(node)

    def start(self, node):
        with (self.directory / f"{node}.log").open("ab") as log:
            self.processes[node] = subprocess.Popen(
                [sys.executable, "-m", "raftdb", "serve", "--node", node,
                 "--data", str(self.directory / node), "--config", str(self.config)],
                cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
            )

    def stop(self, node):
        process = self.processes.pop(node)
        if process.poll() is None:
            process.kill()
        process.wait(timeout=3)

    def stop_all(self):
        for node in list(self.processes):
            self.stop(node)

    def get_status(self, node):
        connection = http.client.HTTPConnection(*self.members[node], timeout=0.25)
        try:
            connection.request("GET", "/status")
            with connection.getresponse() as response:
                return json.load(response)
        except (OSError, http.client.HTTPException, ValueError):
            return {}
        finally:
            connection.close()

    def wait_for(self, description, check, timeout=8):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            result = check()
            if result:
                return result
            time.sleep(0.05)
        statuses = {node: self.get_status(node) for node in self.processes}
        logs = {node: (self.directory / f"{node}.log").read_text() for node in self.members}
        self.fail(f"Timed out waiting for {description}. Status: {statuses}. Logs: {logs}")

    def leader(self, *, among=None):
        def elected():
            states = [self.get_status(node) for node in (among or self.processes)]
            leaders = [state["node"] for state in states if state.get("role") == "leader"]
            return leaders[0] if len(leaders) == 1 else None

        return self.wait_for("one leader", elected)

    def caught_up(self, nodes, index):
        self.wait_for(
            f"{nodes} to apply index {index}",
            lambda: all(self.get_status(node).get("last_applied", 0) >= index for node in nodes),
        )

    def submit(self, node, request_id, sql, status=200):
        response_status, response = rpc(
            self.members[node], "/sql", {"request_id": request_id, "sql": sql}, timeout=3,
        )
        self.assertEqual(response_status, status, response)
        return response

    def cli(self, *args):
        result = subprocess.run(
            [sys.executable, "-m", "raftdb", *args, "--config", str(self.config)],
            cwd=ROOT, capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        return json.loads(result.stdout)

    def test_leader_crash_concurrent_retries_and_full_restart(self):
        leader = self.leader()
        self.cli("sql", "CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT)")
        followers = [node for node in self.members if node != leader]
        response = self.submit(followers[0], "follower-only", "SELECT * FROM events", status=503)
        self.assertEqual(response["code"], "not_leader")

        write = "INSERT INTO events VALUES (1, 'confirmed')"
        with ThreadPoolExecutor(max_workers=8) as workers:
            results = list(workers.map(lambda _: self.submit(leader, "write", write), range(8)))
        self.assertTrue(all(result == results[0] for result in results))
        self.submit(leader, "write", "DELETE FROM events", status=409)

        self.stop(leader)
        successor = self.leader()
        self.assertNotEqual(successor, leader)
        self.assertEqual(self.submit(successor, "write", write), results[0])
        read = self.submit(successor, "read", "SELECT * FROM events")
        self.assertEqual(read["rows"], [[1, "confirmed"]])

        self.start(leader)
        self.caught_up(list(self.members), read["index"])
        self.stop_all()
        for node in self.members:
            self.start(node)
        recovered = self.leader()
        self.assertEqual(self.submit(recovered, "write", write), results[0])
        self.assertEqual(self.cli("sql", "SELECT COUNT(*) FROM events")["rows"], [[1]])

    def test_minority_cannot_confirm_and_divergent_suffix_is_repaired(self):
        leader = self.leader()
        created = self.submit(leader, "create", "CREATE TABLE test (id INTEGER PRIMARY KEY)")
        self.caught_up(list(self.members), created["index"])
        followers = [node for node in self.members if node != leader]
        for node in followers:
            self.stop(node)

        blocked = self.submit(leader, "lost-write", "INSERT INTO test VALUES (41)", status=503)
        self.assertEqual(blocked["code"], "commit_timeout")
        blocked = self.submit(leader, "lost-read", "SELECT * FROM test", status=503)
        self.assertEqual(blocked["code"], "commit_timeout")
        isolated = self.get_status(leader)
        self.assertEqual(isolated["commit_index"], created["index"])
        self.assertGreater(isolated["last_log_index"], isolated["commit_index"])

        self.stop(leader)
        for node in followers:
            self.start(node)
        successor = self.leader(among=followers)
        self.submit(successor, "new-write", "INSERT INTO test VALUES (42)")
        read = self.submit(successor, "new-read", "SELECT * FROM test")
        self.assertEqual(read["rows"], [[42]])

        self.start(leader)
        self.caught_up(list(self.members), read["index"])
        log = json.loads((self.directory / leader / "raft.json").read_text())["log"]
        self.assertNotIn("lost-write", [entry.get("request_id") for entry in log])
        self.assertNotIn("lost-read", [entry.get("request_id") for entry in log])


if __name__ == "__main__":
    unittest.main()
