# Benchmarks

Run the cluster benchmark on Linux with Python 3.10+. Each variant starts three disposable loopback replicas and runs a seeded single-client workload. Defaults are 384 inserts, 128 indexed reads, 128 updates, and 24 all-replica update samples with seed 42 and 256-byte insert payloads. Tests use each implementation's default durable acknowledgements. Native replication wakes as soon as a write is ready. The baseline sends new work on heartbeat ticks.

```sh
git worktree add --detach /tmp/raftdb-json 7477321054526680df4652414c9b7a33abbc5bc3
./benchmarks/run.sh --baseline /tmp/raftdb-json
python benchmarks/storage.py .
python benchmarks/storage.py /tmp/raftdb-json
```

Use `--dataset PATH` to save the exact CREATE and INSERT input to a new SQL file. The cluster benchmark reports nearest-rank p50, p95, and p99 client latencies, observed time until three replicas have applied a write, recovery from a full SIGKILL to a successful quorum read, Linux RSS, on-disk bytes across all replica directories, and the time and size change of explicit compaction. Recovery includes election. The all-replica metric includes status polling. The baseline has no compaction operation.

The storage benchmark isolates durable Raft work. Each sample appends one entry and advances commit in a separate durable write without SQL, snapshotting, or RPC scheduling. It measures Raft journal reopen and verifies the final entry. End-to-end and storage-only results describe different workloads.
