from tempfile import TemporaryDirectory
import unittest

from raftdb.journal import CorruptionError
from raftdb.raft import Node
from raftdb.storage import Storage


class RaftTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = temporary.name
        self.members = {node: ("127.0.0.1", port) for port, node in enumerate("abc", 8091)}
        self.open_node()

    def open_node(self):
        self.storage = Storage(self.directory)
        self.addCleanup(self.storage.close)
        self.node = Node("a", self.members, "raft-unit-test", self.storage)
        self.addCleanup(self.node.db.store.close)

    def append(self, term, prev, prev_term, entries, commit):
        return self.node.append_entries({
            "source": "b", "term": term, "prev_index": prev, "prev_term": prev_term,
            "entries": entries, "commit": commit,
        })

    def test_new_leader_needs_a_current_term_entry_to_commit_old_entries(self):
        command = {"term": 1, "request_id": "create", "sql": "CREATE TABLE items (id INTEGER PRIMARY KEY)"}
        self.assertTrue(self.append(1, 0, 0, [command], 0)["success"])
        with self.node.changed:
            self.node.begin_election()
            self.node.votes.add("b")
            self.node.become_leader()
            self.node.match_index["b"] = 1
            self.node.advance_commit()
            self.assertEqual(self.node.commit, 0)
            self.node.match_index["b"] = 2
            self.node.advance_commit()
        self.assertEqual(self.node.commit, 2)
        self.assertIn("items", self.node.db.tables)

    def test_heartbeat_commits_only_the_matching_prefix(self):
        self.append(1, 0, 0, [{"term": 1}, {"term": 1}], 0)
        self.assertTrue(self.append(2, 1, 1, [], 2)["success"])
        self.assertEqual(self.node.commit, 1)
        self.assertEqual(self.node.log[2]["term"], 1)
        self.assertTrue(self.append(2, 1, 1, [{"term": 2}], 2)["success"])
        self.assertEqual(self.node.commit, 2)
        self.assertEqual(self.node.log[2]["term"], 2)

    def test_invalid_edits_and_hard_state_leave_the_durable_prefix_intact(self):
        self.append(1, 0, 0, [{"term": 1}], 1)
        size = self.node.log.journal.disk_bytes()
        with self.assertRaises(CorruptionError):
            self.storage.edit(1, [{"term": 1}])
        with self.assertRaises(CorruptionError):
            self.storage.edit(2, [{"term": 2}])
        with self.assertRaises(CorruptionError):
            self.storage.hard(1, None, 0)
        self.assertEqual(self.node.log.journal.disk_bytes(), size)
        self.storage.verify()

    def test_vote_survives_restart_and_rejects_an_outdated_log(self):
        self.append(1, 0, 0, [{"term": 1}], 1)
        ballot = {"source": "b", "term": 2, "last_term": 1, "last_index": 1}
        self.assertTrue(self.node.request_vote(ballot)["granted"])
        self.node.db.store.close()
        self.storage.close()
        self.open_node()
        self.assertFalse(self.node.request_vote(ballot | {"source": "c"})["granted"])
        self.assertFalse(self.node.request_vote(
            {"source": "c", "term": 3, "last_term": 0, "last_index": 0},
        )["granted"])
        self.assertTrue(self.node.request_vote(ballot | {"term": 3})["granted"])


if __name__ == "__main__":
    unittest.main()
