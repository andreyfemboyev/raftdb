"""Compare two durable writes per statement without SQL, HTTP or election timing."""

import argparse
import json
import math
from pathlib import Path
import random
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import perf_counter


def measure(source, rows, payload_bytes, seed):
    source = source.resolve()
    sys.path.insert(0, str(source))
    from raftdb.storage import Storage

    native = hasattr(Storage, "initialize")
    identity = {"version": 1, "cluster": "storage-benchmark", "node": "a"}
    generator = random.Random(seed)
    entries = [
        {"term": 1, "request_id": f"key-{key}", "sql": (
            f"INSERT INTO items VALUES ({key}, '{generator.randbytes(payload_bytes).hex()[:payload_bytes]}')"
        )}
        for key in range(rows)
    ]
    with TemporaryDirectory(prefix="raftdb-storage-bench-") as root:
        store = Storage(root)
        close = Storage.close if native else lambda storage: storage.lock.close()
        state = {"identity": identity, "term": 1, "voted_for": "a", "commit": 0, "log": [{"term": 0}]}
        try:
            if native:
                store.initialize(identity)
                store.hard(1, "a", 0)
            else:
                store.write(state)
            samples = []
            began = perf_counter()
            for index, entry in enumerate(entries, 1):
                start = perf_counter()
                if native:
                    store.edit(index, [entry])
                    store.hard(1, "a", index)
                else:
                    state["log"].append(entry)
                    store.write(state)
                    state["commit"] = index
                    store.write(state)
                samples.append(perf_counter() - start)
            elapsed = perf_counter() - began
        finally:
            close(store)

        disk_bytes = sum(file.stat().st_size for file in Path(root).rglob("*") if file.is_file())
        began = perf_counter()
        reopened = Storage(root)
        try:
            commit = reopened.log.commit if native else reopened.load()["commit"]
            restart = perf_counter() - began
            tip = reopened.log[commit] if native else reopened.load()["log"][commit]
            if commit != rows or tip != entries[-1]:
                raise ValueError("Recovered raft tip does not match the benchmark")
        finally:
            close(reopened)

    sorted_samples = sorted(samples)
    return {
        "operations": rows, "operations_per_second": round(rows / elapsed, 2),
        **{f"p{p}_ms": round(1000 * sorted_samples[math.ceil(rows * p / 100) - 1], 3) for p in (50, 95, 99)},
        "restart_ms": round(restart * 1000, 3), "disk_bytes": disk_bytes,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Native or legacy RaftDB checkout")
    parser.add_argument("--rows", type=int, default=1200)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.rows <= 0 or not 0 < args.payload_bytes <= 15_000:
        parser.error("Use positive rows and payload size between 1 and 15000")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.source, text=True).strip()
    print(json.dumps({
        "revision": revision, "seed": args.seed, "payload_bytes": args.payload_bytes,
        **measure(args.source, args.rows, args.payload_bytes, args.seed),
    }, indent=2))


if __name__ == "__main__":
    main()
