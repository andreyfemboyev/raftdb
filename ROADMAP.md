# Roadmap

## v0.2.0

The native storage milestone includes durable binary Raft and relational journals, migration from the JSON log, primary-key lookup, durable request deduplication, snapshot transfer, maintenance, and the documented SQL subset.

## Next storage and consensus work

- Move large snapshot and compaction jobs off the Raft critical section using pinned generations and a carefully validated publication protocol.
- Design a request-outcome retention contract and a disk-backed or sharded key directory for larger datasets.
- Add group commit and a confirmed quorum read path to reduce durable write amplification for reads.
- Exercise storage recovery under filesystem or block-device power-loss simulation in addition to process-kill tests.
- Design authenticated client and replica transport, membership changes, and an online upgrade protocol.

## Query and analytics work

- Add secondary indexes with atomic maintenance in one statement batch and planning for index-backed scans.
- Introduce an explicit read snapshot and server-side cursors for bounded-memory exports.
- Design joins, transaction isolation, and larger analytical workloads with explicit resource bounds.
- Expand the benchmark matrix for high concurrency, larger data, multiple file systems, and geographically distributed replicas.
