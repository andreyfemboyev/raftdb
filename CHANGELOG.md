# Changelog

## v0.2.0 · Native Storage Engine

- Replace full JSON Raft rewrites with CRC32-framed segmented logs and an explicit suffix-edit format.
- Persist relational schemas, rows, primary keys, request outcomes, and the applied checkpoint with atomic SQL statement batches.
- Add automatic and manual snapshots, chunked InstallSnapshot, and durable generation switching for compaction.
- Import v0.1 JSON state with its original dialect and election history.
- Extend SQL with DROP TABLE, OFFSET, and aggregate functions.
- Add the Python client, value parameters, paginated results, CSV and JSON interchange, and optional pandas and NumPy conversion.
- Add the SQL shell, storage stats, maintenance and verification commands, and reproducible comparison benchmarks.

## Original JSON implementation

The initial RaftDB supported three-node Raft, a JSON-backed persistent Raft log, in-memory SQL state reconstructed by replay, quorum-committed reads and writes, and the `serve`, `sql`, and `status` CLI.
