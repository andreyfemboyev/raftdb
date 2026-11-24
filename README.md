# RaftDB

RaftDB is a distributed relational database implemented with the Python standard library. It includes a Raft consensus implementation, durable replicated storage, a SQL parser and executor, and a command-line client. A three-node cluster can continue serving new statements after losing one replica.

## Run three replicas

Use Python 3.10 or newer on a POSIX system with directory `fsync` and file locking support. Run the commands from this directory. The default configuration uses loopback ports 8091 through 8093.

```sh
# Terminal 1
python3 -m raftdb serve --node n1 --data ./data/n1

# Terminal 2
python3 -m raftdb serve --node n2 --data ./data/n2

# Terminal 3
python3 -m raftdb serve --node n3 --data ./data/n3
```

In a fourth terminal, run these commands. The SQL client discovers the leader and follows leader hints as it retries.

```sh
python3 -m raftdb status
python3 -m raftdb sql "CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, done BOOLEAN)"
python3 -m raftdb sql --request-id seed-tasks "INSERT INTO tasks VALUES (1, 'Write tests', TRUE), (2, 'Review results', FALSE)"
python3 -m raftdb sql "SELECT id, title FROM tasks WHERE done = FALSE ORDER BY id LIMIT 10"
```

The query returns the columns `id` and `title` with the row `[2, "Review results"]`. Stop the reported leader with Ctrl+C, run `status` again after the election, and issue another query. Restart the stopped replica using the original data directory so it can replay its committed log and catch up.

For separate hosts, give every process the same cluster configuration with a unique cluster name and reachable IPv4 hostnames or addresses and ports. Pass it to commands using `--config /path/to/cluster.json`. Give each replica its own data directory. Use `--bind` on a server if its listening address differs from its advertised address. Run all nodes on a private, trusted network because peer and client requests share an unauthenticated HTTP listener.

## SQL dialect

| Statement | Example |
| --- | --- |
| Create a table | `CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, done BOOLEAN)` |
| Insert multiple rows | `INSERT INTO tasks (id, title) VALUES (3, 'Add notes'), (4, 'It''s ready')` |
| Read and sort | `SELECT id, title FROM tasks WHERE done IS NULL ORDER BY id DESC LIMIT 5` |
| Count matching rows | `SELECT COUNT(*) FROM tasks WHERE id > 1 AND done IS NOT NULL` |
| Update matching rows | `UPDATE tasks SET done = TRUE WHERE id = 2` |
| Delete matching rows | `DELETE FROM tasks WHERE id = 3` |

A table requires one primary key. Types are signed 64-bit `INTEGER`, `TEXT`, and `BOOLEAN`. Missing values on inserts default to `NULL` where allowed. The engine accepts one statement per request, with an optional trailing semicolon. Identifiers are unquoted and case-insensitive. String values use single quotes, with doubled quotes for an embedded apostrophe.

Filters support literal comparisons, `IS [NOT] NULL`, and `AND`. Comparisons involving a null do not match. Nulls sort first in ascending order and last in descending order. Use `ORDER BY` when row order matters. `UPDATE` and `DELETE` apply to all rows if the statement omits `WHERE`. Each mutation is atomic, including multi-row inserts and updates that fail constraint validation.

## HTTP interface and retries

`GET /status` reports the observed role, term, leader hint, and replica indices. A node that still reports itself as leader may have lost contact with a quorum.

`POST /sql` accepts a JSON object containing a unique `request_id` and one `sql` string of up to 16 KiB in UTF-8. Successful responses contain `ok`, `request_id`, the committed log `index`, and either `affected` or `columns` and `rows`.

| HTTP status | Meaning |
| --- | --- |
| 200 | The statement committed and executed successfully |
| 400 | The request envelope is invalid |
| 409 | The request ID is already bound to different SQL |
| 422 | The statement committed and returned a SQL error |
| 503 | The contacted node was unable to confirm the operation |

On a timeout or connection failure, the outcome may still be pending. Retry the exact same SQL with the same request ID. The state machine deduplicates the request and returns the original outcome, including across leader changes and full process restarts. The CLI generates an ID by default and prints it on an unconfirmed outcome. A fresh read needs a fresh request ID.

## Implementation

- `raftdb/raft.py` handles randomized leader election, durable votes, AppendEntries, log repair, and majority commit. A leader commits by counting replicas for an entry in its current term, thereby also committing its preceding log.
- `raftdb/storage.py` persists the term, vote, commit index, and log with file and directory `fsync` and atomic replacement. Each node reconstructs its relational state and request outcomes by replaying its committed prefix at startup.
- `raftdb/sql.py` tokenizes SQL, builds statement objects, validates schemas and constraints, and executes primary-key lookups, scans, filters, projections, sorting, and counts.
- `raftdb/server.py` exposes the HTTP interface. `raftdb/__main__.py` runs the node and the retrying client.

New reads and writes go through the same Raft log. Applying each statement at its committed position gives fresh reads and writes a single consistent order, even during leader changes. This trades read throughput for a straightforward consistency model.

This implementation is sized for a small cluster and datasets that fit in memory. Membership is fixed, and a full durable log image is written on each persistence event. The log and request outcome cache grow with use. The SQL subset omits joins, secondary indexes, and multi-statement transactions. The server is designed to tolerate process crashes and network outages among trusted replicas.

## Verification

```sh
python3 -m unittest discover -s tests -v
```

The suite starts three real node processes on ephemeral ports. It checks leader replacement, durable recovery after a full cluster crash, concurrent retries, refusal to confirm fresh reads and writes on a minority, and repair of a conflicting uncommitted log suffix.
