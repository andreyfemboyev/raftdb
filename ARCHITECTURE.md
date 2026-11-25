# Architecture

One HTTP server process owns one replica directory. The process holds an exclusive POSIX file lock for the lifetime of that replica. A node-wide condition lock serializes Raft and relational changes, including online maintenance. A worker per peer sends votes, AppendEntries batches, and InstallSnapshot chunks. RPCs run outside the condition lock. New entries and advancing commits wake peer workers immediately, with periodic heartbeats when idle.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `raftdb/raft.py` | Election, fixed membership, replication progress, quorum commit, SQL proposal, snapshot creation and installation |
| `raftdb/storage.py` | Absolute Raft indices, term and vote, commit, journal suffix edits, and snapshot generation publication |
| `raftdb/journal.py` | CRC32 framing, rotation, replay, fsync, and atomic generation switching |
| `raftdb/kv.py` | Bitcask-style key directory and atomic transactions across binary records |
| `raftdb/relational.py` | Durable schemas, typed primary keys, values, outcomes, and application checkpoint |
| `raftdb/sql.py` | Lexer, parser, statement objects, type and constraint checks, execution against table and row mappings |
| `raftdb/server.py`, `raftdb/client.py` | HTTP envelope validation, request retries, value binding, batched results, and import/export |

The on-disk primary key is also the lookup key for its row value. Reopening the store reconstructs a key directory of byte keys and file offsets. Row values, schema contents, and outcomes are loaded individually from their frames as needed. Iteration visits the ordered key directory and fetches matching row values. Full scans, sorts, and multi-row writes can use substantial working memory. Secondary indexes can be added as new key namespaces updated in the same statement batch. Multi-table planning and transactional isolation need separate executor and consensus work.

## Write and recovery ordering

1. The leader fsyncs the indexed entry before counting itself. Each follower fsyncs the relevant term and suffix edit before replying to AppendEntries.
2. A leader advances commit with a majority match for a current-term entry. The leader and each receiving follower persist the commit index before applying it.
3. SQL execution stages changes in an overlay and validates the entire statement. The relational journal fsyncs a single transaction containing the changes, original outcome, and new applied index. A failed SQL statement stores its error and checkpoint in the transaction.
4. The leader returns the durable outcome. Startup replays only the committed suffix above the relational checkpoint, so a crash between Raft commit and SQL application cannot repeat an already applied mutation.

A follower limits its commit advance to the prefix proven by the particular AppendEntries request. Edits are framed atomic suffix replacements above the committed index. The device may retain obsolete bytes until compaction. Elections use absolute index and term, including the snapshot boundary.

## Snapshots and compaction

Snapshots capture the applied point under the state lock. The snapshot and remaining Raft suffix are durable before the Raft generation pointer references them. The relational store can then compact independently by rewriting its live keys, including deduplication state, and atomically publishing a new generation.

A leader streams an immutable snapshot in 128 KiB chunks when a peer's next index precedes the available log. A receiver stages the file, verifies the size, SHA-256 and record framing, and checks that its embedded relational checkpoint matches the advertised index. The receiver preserves its suffix only when the local entry at the snapshot boundary has the advertised term. It publishes the Raft generation before replacing relational state. If a crash separates these steps, startup restores the authoritative snapshot before applying later committed entries. The follower acknowledges the installation only after both publications.

An automatic snapshot uses the configured applied-entry interval. Leaders cap outstanding uncommitted entries, and obsolete hard-state and suffix-edit frames have a separate rewrite threshold. Snapshot and relational compaction need temporary disk space and pause local processing. Request-outcome retention keeps the live keyspace growing over time.

## Migration

The original `raft.json` remains the source until an entire native Raft generation has been published. Migration carries original SQL dialect flags with the imported entries, preserving rejected statements and acknowledgements during replay. The new reader subsequently uses the native generations. The CLI retains the legacy `serve`, `sql`, and `status` syntax and the SQL response fields. Upgrade a stopped cluster together because native snapshots and migration flags extend the version 1 peer protocol.
