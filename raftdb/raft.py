"""Fixed-membership Raft and its replicated SQL state machine."""

import http.client
import json
import os
import random
import threading
import time
import traceback

from .sql import Database, SQLError
from .storage import Storage


HEARTBEAT = 0.075
ELECTION = (0.45, 0.85)
RPC_TIMEOUT = 0.23
PROPOSAL_TIMEOUT = 1.75
BATCH_SIZE = 16
MAX_SQL_BYTES = 16_384
MAX_REQUEST_ID = 128


def rpc(address, path, data, timeout=RPC_TIMEOUT):
    connection = http.client.HTTPConnection(*address, timeout=timeout)
    try:
        connection.request(
            "POST", path, json.dumps(data), {"Content-Type": "application/json"}
        )
        with connection.getresponse() as response:
            return response.status, json.load(response)
    finally:
        connection.close()


class Node:
    def __init__(self, node_id, members, cluster, storage: Storage):
        self.id = node_id
        self.members = members
        self.cluster = cluster
        self.peers = {key: address for key, address in members.items() if key != self.id}
        self.quorum = len(members) // 2 + 1
        self.storage = storage
        self.changed = threading.Condition()
        self.stopped = threading.Event()
        self.threads = []
        self.role = "follower"
        self.leader = None
        self.votes = set()
        self.next_index = {}
        self.match_index = {}
        self.pending = {}
        self.db = Database()
        self.completed = {}
        self.applied = 0
        self.identity = {"version": 1, "cluster": cluster, "node": node_id}

        state = storage.load()
        if state is None:
            self.term, self.voted_for, self.commit = 0, None, 0
            # Index zero is a sentinel, so indices match list offsets.
            self.log = [{"term": 0}]
            self.persist()
        else:
            if state["identity"] != self.identity:
                raise ValueError("The data directory belongs to a different node or cluster")
            self.term = state["term"]
            self.voted_for = state["voted_for"]
            self.commit = state["commit"]
            self.log = state["log"]
            if (
                not 0 <= self.commit < len(self.log)
                or self.log[0] != {"term": 0}
                or self.term < self.log[-1]["term"]
            ):
                raise ValueError("Invalid persisted log")
        self.reset_election()
        self.apply()

    def persist(self):
        self.storage.write({
            "identity": self.identity, "term": self.term, "voted_for": self.voted_for,
            "commit": self.commit, "log": self.log,
        })

    def reset_election(self):
        self.election_at = time.monotonic() + random.uniform(*ELECTION)

    def start(self):
        jobs = [(self.election_loop, ())]
        jobs.extend((self.peer_loop, (peer, address)) for peer, address in self.peers.items())
        for target, args in jobs:
            thread = threading.Thread(target=self.run_worker, args=(target, args), daemon=True)
            self.threads.append(thread)
            thread.start()

    @staticmethod
    def run_worker(target, args):
        try:
            target(*args)
        except Exception:
            traceback.print_exc()
            os._exit(70)

    def stop(self):
        self.stopped.set()
        with self.changed:
            self.changed.notify_all()
        for thread in self.threads:
            thread.join(timeout=1)

    def election_loop(self):
        while not self.stopped.wait(0.025):
            with self.changed:
                if self.role != "leader" and time.monotonic() >= self.election_at:
                    self.begin_election()

    def begin_election(self):
        self.term += 1
        self.role = "candidate"
        self.leader = None
        self.voted_for = self.id
        self.votes = {self.id}
        self.reset_election()
        self.persist()
        if len(self.votes) >= self.quorum:
            self.become_leader()

    def become_follower(self, term, leader=None):
        newer = term > self.term
        if newer:
            self.term = term
            self.voted_for = None
        self.role = "follower"
        self.leader = leader
        self.pending.clear()
        self.reset_election()
        if newer:
            self.persist()
        self.changed.notify_all()

    def become_leader(self):
        self.role = "leader"
        self.leader = self.id
        self.pending.clear()
        self.next_index = dict.fromkeys(self.peers, len(self.log))
        self.match_index = dict.fromkeys(self.peers, 0)
        # A current-term no-op lets the leader commit any preceding entries.
        self.log.append({"term": self.term})
        self.persist()
        self.advance_commit()

    def apply(self):
        while self.applied < self.commit:
            self.applied += 1
            entry = self.log[self.applied]
            if "request_id" not in entry:
                continue
            request_id, sql = entry["request_id"], entry["sql"]
            if request_id not in self.completed:
                try:
                    result = {"ok": True, **self.db.execute(sql)}
                except SQLError as error:
                    result = {"ok": False, "code": "sql_error", "error": str(error)}
                self.completed[request_id] = (sql, self.applied, result)
            self.pending.pop(request_id, None)

    def advance_commit(self):
        matches = sorted([len(self.log) - 1, *self.match_index.values()], reverse=True)
        index = matches[self.quorum - 1]
        if index > self.commit and self.log[index]["term"] == self.term:
            self.commit = index
            self.persist()
            self.apply()
            self.changed.notify_all()

    def peer_loop(self, peer, address):
        voted_term = 0
        delay = HEARTBEAT
        while not self.stopped.wait(delay):
            delay = HEARTBEAT
            with self.changed:
                term, role = self.term, self.role
                if role == "candidate" and voted_term != term:
                    path = "/raft/vote"
                    params = {"last_index": len(self.log) - 1, "last_term": self.log[-1]["term"]}
                elif role == "leader":
                    path = "/raft/append"
                    start = self.next_index[peer]
                    prev = start - 1
                    entries = self.log[start:start + BATCH_SIZE]
                    params = {
                        "prev_index": prev, "prev_term": self.log[prev]["term"],
                        "entries": entries, "commit": self.commit,
                    }
                else:
                    continue
                message = {**params, "cluster": self.cluster, "source": self.id, "term": term}
            try:
                status, result = rpc(address, path, message)
            except (OSError, http.client.HTTPException, ValueError):
                continue
            if status != 200 or not isinstance(result, dict) or type(result.get("term")) is not int:
                continue
            with self.changed:
                if result["term"] > self.term:
                    self.become_follower(result["term"])
                    continue
                if self.term != term or self.role != role or result["term"] != term:
                    continue
                if role == "candidate":
                    voted_term = term
                    if result.get("granted") is True:
                        self.votes.add(peer)
                        if len(self.votes) >= self.quorum:
                            self.become_leader()
                elif result.get("success") is True:
                    matched = prev + len(entries)
                    self.match_index[peer] = matched
                    self.next_index[peer] = matched + 1
                    self.advance_commit()
                    if self.next_index[peer] < len(self.log):
                        delay = 0
                elif type(result.get("next_index")) is int:
                    self.next_index[peer] = max(1, min(start - 1, result["next_index"]))
                    delay = 0 if start > 1 else HEARTBEAT

    def request_vote(self, message):
        term, candidate = message["term"], message["source"]
        with self.changed:
            if term > self.term:
                self.become_follower(term)
            candidate_tip = (message["last_term"], message["last_index"])
            own_tip = (self.log[-1]["term"], len(self.log) - 1)
            granted = (
                term == self.term and self.voted_for in (None, candidate)
                and candidate_tip >= own_tip
            )
            if granted:
                if self.voted_for != candidate:
                    self.voted_for = candidate
                    self.persist()
                self.reset_election()
            return {"term": self.term, "granted": granted}

    def append_entries(self, message):
        term, leader = message["term"], message["source"]
        prev, prev_term = message["prev_index"], message["prev_term"]
        entries = message["entries"]
        with self.changed:
            if term < self.term:
                return {"term": self.term, "success": False}
            if term > self.term or self.role != "follower":
                self.become_follower(term, leader)
            self.leader = leader
            self.reset_election()

            if prev >= len(self.log):
                return {"term": self.term, "success": False, "next_index": len(self.log)}
            if self.log[prev]["term"] != prev_term:
                conflict_term = self.log[prev]["term"]
                first = prev
                while first > 1 and self.log[first - 1]["term"] == conflict_term:
                    first -= 1
                return {"term": self.term, "success": False, "next_index": first}

            modified = False
            for offset, entry in enumerate(entries):
                index = prev + 1 + offset
                if index == len(self.log) or self.log[index]["term"] != entry["term"]:
                    if index <= self.commit:
                        raise RuntimeError("Raft attempted to overwrite a committed entry")
                    self.log[index:] = entries[offset:]
                    modified = True
                    break
            # Only the prefix matched by this RPC is safe to advance.
            commit = min(message["commit"], prev + len(entries))
            if commit > self.commit:
                self.commit = commit
                modified = True
            if modified:
                self.persist()
                self.apply()
                self.changed.notify_all()
            return {"term": self.term, "success": True}

    def unavailable(self, code, error):
        return 503, {"ok": False, "code": code, "error": error, "leader": self.leader}

    def propose(self, request_id, sql):
        with self.changed:
            if self.role != "leader":
                return self.unavailable("not_leader", "Send this request to the leader")
            if request_id in self.completed:
                return self.result(request_id, sql)
            if request_id in self.pending:
                original, index = self.pending[request_id]
                if original != sql:
                    return self.id_conflict()
            else:
                index = len(self.log)
                self.log.append({"term": self.term, "sql": sql, "request_id": request_id})
                self.pending[request_id] = (sql, index)
                self.persist()
                self.advance_commit()
            term = self.term
            deadline = time.monotonic() + PROPOSAL_TIMEOUT
            while True:
                if request_id in self.completed:
                    return self.result(request_id, sql)
                if self.role != "leader" or self.term != term or self.stopped.is_set():
                    return self.unavailable("not_leader", "Leadership changed before confirmation")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return self.unavailable("commit_timeout", f"Log entry {index} is unconfirmed")
                self.changed.wait(remaining)

    @staticmethod
    def id_conflict():
        return 409, {"ok": False, "code": "id_conflict", "error": "Request ID used for different SQL"}

    def result(self, request_id, sql):
        original, index, result = self.completed[request_id]
        if original != sql:
            return self.id_conflict()
        status = 200 if result["ok"] else 422
        return status, {**result, "request_id": request_id, "index": index}

    def status(self):
        with self.changed:
            return {
                "node": self.id, "role": self.role, "term": self.term, "leader": self.leader,
                "commit_index": self.commit, "last_applied": self.applied,
                "last_log_index": len(self.log) - 1, "quorum": self.quorum,
            }
