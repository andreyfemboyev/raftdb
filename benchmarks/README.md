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

## Measured results

This run used Python 3.12.14 and the disposable Linux cloud workspace filesystem on November 25, 2026. The benchmark compared native revision `ae435aee571e7a8dc74d3b98dc95b13cd178f7d2` with JSON baseline `7477321054526680df4652414c9b7a33abbc5bc3`. It used the default workload above and no concurrent benchmark clients. Latencies are milliseconds.

| Cluster metric | Native | JSON baseline |
| --- | ---: | ---: |
| Inserts per second | 378.36 | 12.64 |
| Insert p50 / p95 / p99 | 2.556 / 3.197 / 4.136 | 79.010 / 81.202 / 82.660 |
| Indexed reads per second | 376.51 | 12.44 |
| Read p50 / p95 / p99 | 2.484 / 3.023 / 12.687 | 80.153 / 81.858 / 84.390 |
| Updates per second | 382.86 | 12.38 |
| Update p50 / p95 / p99 | 2.532 / 3.251 / 4.569 | 80.558 / 81.758 / 82.844 |
| All-replica apply p50 / p95 / p99 | 3.923 / 10.058 / 10.120 | 159.480 / 163.269 / 165.375 |
| Full restart to quorum read | 572.759 | 743.008 |
| Cluster RSS before restart, bytes | 57,917,440 | 58,888,192 |
| Highest node RSS peak, bytes | 19,345,408 | 19,865,600 |
| Cluster disk before compaction, bytes | 2,016,252 | 466,035 |
| Sequential compaction of three replicas | 38.739 | Unavailable |
| Disk before and after explicit compaction, bytes | 2,017,845 → 1,757,841 | Unavailable |

Both clusters recovered all 384 rows. Native disk usage includes persisted relational rows, request outcomes and snapshots. The baseline reconstructs rows and outcomes from its log. The end-to-end throughput gap includes immediate replication scheduling in the native version.

| Storage-only metric, 1,200 operations | Native | JSON baseline |
| --- | ---: | ---: |
| Durable append-plus-commit operations per second | 2,455.40 | 285.55 |
| Append-plus-commit p50 / p95 / p99 | 0.350 / 0.561 / 1.536 | 3.468 / 5.599 / 6.374 |
| Raft journal reopen | 6.514 | 1.007 |
| Raft journal bytes | 450,422 | 399,905 |

These measurements characterize one small loopback workload, journal length, and filesystem. Actual latency and space amplification vary with dataset size, request sizes, concurrency and storage. [Raw cluster results](results/cluster.json) · [Native journal results](results/storage-native.json) · [JSON journal results](results/storage-json.json)
