# Contributing

Use Linux with Python 3.10 or newer. Clone the repository and run `python -m unittest discover -s tests -v` from the repository root. Runtime dependencies are unnecessary. To test the packaged command, build a wheel with `python -m pip wheel --no-deps --wheel-dir /tmp/raftdb-wheels .`.

Read [AGENTS.md](AGENTS.md) and the [architecture notes](ARCHITECTURE.md) before touching durable state or consensus. Include regressions that exercise observable behavior and name the relevant crash boundary in persistence tests. For benchmark changes, include the command, source revisions, workload, and raw measurements. Keep durable format changes documented in [STORAGE_FORMAT.md](STORAGE_FORMAT.md).

Open changes against `main` and describe the scope, compatibility impact, and tests run.
