"""Durable Raft hard state and log, using atomic file replacement on POSIX."""

import fcntl
import json
import os
from pathlib import Path
import sys


class Storage:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        missing = []
        ancestor = self.directory
        while not ancestor.exists():
            missing.append(ancestor)
            ancestor = ancestor.parent
        self.directory.mkdir(parents=True, exist_ok=True)
        # Make each new directory entry durable before acknowledging any writes.
        for directory in reversed(missing):
            self.sync_directory(directory.parent)
        self.path = self.directory / "raft.json"
        self.lock = (self.directory / "lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise

    @staticmethod
    def sync_directory(directory):
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def load(self):
        try:
            with self.path.open(encoding="utf-8") as source:
                return json.load(source)
        except FileNotFoundError:
            return None

    def write(self, state):
        temporary = self.directory / "raft.tmp"
        try:
            with temporary.open("w", encoding="utf-8") as target:
                json.dump(state, target, separators=(",", ":"))
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, self.path)
            self.sync_directory(self.directory)
        except OSError as error:
            # Continuing after an ambiguous durable write can violate Raft safety.
            print(f"Fatal storage error: {error}", file=sys.stderr, flush=True)
            os._exit(74)
