# Working on RaftDB

RaftDB is the primary repository. The separate [BitcaskDB repository](https://github.com/andreyfemboyev/bitcask-db) is the design reference. Implement changes here and preserve source attribution. Check both projects' licensing before copying additional source.

## Invariants

- Persist term and vote before sending a granted vote. A durable Raft edit can replace only uncommitted entries. Count a follower's match only after a successful reply to an indexed RPC.
- A leader advances commit only using a majority match for an entry in its current term. A follower advances commit only through the prefix the received RPC proved.
- Keep `snapshot_index <= applied_index <= commit_index <= last_log_index` after recovery. SQL changes, original request outcome, and applied index share one relational transaction.
- Fresh reads need a quorum-committed entry. A request ID binds one SQL string and one result. Retrying a completed request returns its historical outcome.
- Publish generation pointers only after file and directory fsync. Keep old data until after the new pointer is durable. Recover a relational store lagging a published snapshot before taking requests.
- Keep SQL and key ordering deterministic across replicas. Preserve the original dialect on migrated entries. Treat checksum failures as errors and limit automatic tail repair to incomplete active frames.

## Development

Use Python 3.10+ and the standard library in the database core. Keep POSIX locking and fsync behavior explicit in persistence changes. Write focused regressions for changed consensus or recovery behavior and use real subprocesses for distributed tests and crash boundaries. Keep SQL, HTTP, CLI, and disk format documentation synchronized with behavior.

```sh
python -m compileall -q raftdb tests benchmarks
python -W error::ResourceWarning -m unittest discover -s tests -v
python -m raftdb benchmark --baseline /path/to/legacy-raftdb
python benchmarks/storage.py /path/to/raftdb
```

Upcoming work includes bounded deduplication, background compaction, authenticated transport, and index-aware query execution. See [ROADMAP.md](ROADMAP.md).
