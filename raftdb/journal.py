"""Checksummed segmented files and atomic generation publication on POSIX."""

import os
import re
import shutil
import struct
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4
from zlib import crc32


DEFAULT_SEGMENT_SIZE = 16 * 1024 * 1024
MAX_FRAME = 128 * 1024 * 1024
HEADER = struct.Struct(">4sBQI")
CRC = struct.Struct(">I")
MAGIC = b"RDB2"
GENERATION = re.compile(r"gen-[0-9a-f]{32}")
SEGMENT = re.compile(r"[0-9]{20}\.seg")


class CorruptionError(ValueError):
    """Durable bytes failed validation."""


class TornFrame(CorruptionError):
    """The input ended before a complete frame."""


@dataclass(frozen=True, slots=True)
class Location:
    segment: int
    offset: int


def sync_directory(directory):
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def ensure_directory(directory):
    if not directory.exists():
        ensure_directory(directory.parent)
        directory.mkdir(exist_ok=True, mode=0o700)
        sync_directory(directory.parent)


def write_frame(file, kind, data):
    if len(data) > MAX_FRAME:
        raise ValueError("Journal frame is too large")
    header = HEADER.pack(MAGIC, kind, len(data), crc32(data))
    for part in (header, CRC.pack(crc32(header)), data):
        if file.write(part) != len(part):
            raise OSError(f"Short write in {file.name}")


def read_frame(file):
    position = file.tell()
    header = file.read(HEADER.size)
    if not header:
        return None
    check = file.read(CRC.size)
    if len(header) != HEADER.size or len(check) != CRC.size:
        raise TornFrame(f"{file.name}:{position}: incomplete frame header")
    magic, kind, length, payload_crc = HEADER.unpack(header)
    if magic != MAGIC or crc32(header) != CRC.unpack(check)[0] or length > MAX_FRAME:
        raise CorruptionError(f"{file.name}:{position}: invalid frame header or CRC32")
    payload = file.read(length)
    if len(payload) != length:
        raise TornFrame(f"{file.name}:{position}: incomplete frame payload")
    if crc32(payload) != payload_crc:
        raise CorruptionError(f"{file.name}:{position}: invalid payload CRC32")
    return kind, payload


class Journal:
    def __init__(self, directory, segment_size, *, create=False):
        self.directory = directory
        self.segment_size = segment_size
        self.active = None
        if create:
            directory.mkdir(mode=0o700)
            self.number = 1
            self.active = self.create_segment(1)
        else:
            if not directory.is_dir():
                raise CorruptionError(f"Missing generation {directory}")
            segments = sorted(p for p in directory.iterdir() if SEGMENT.fullmatch(p.name))
            if not segments or any(path != self.path(n) for n, path in enumerate(segments, 1)):
                raise CorruptionError(f"Missing segment in {directory}")
            self.number = len(segments)
            self.active = segments[-1].open("r+b")
        self.size = self.active.seek(0, os.SEEK_END)

    def path(self, number):
        return self.directory / f"{number:020d}.seg"

    def create_segment(self, number):
        file = self.path(number).open("x+b")
        try:
            os.fsync(file.fileno())
            sync_directory(self.directory)
        except BaseException:
            file.close()
            raise
        return file

    def frames(self, *, repair=False):
        for number in range(1, self.number + 1):
            mode = "r+b" if repair and number == self.number else "rb"
            with self.path(number).open(mode) as file:
                while True:
                    offset = file.tell()
                    try:
                        frame = read_frame(file)
                    except TornFrame:
                        if mode != "r+b":
                            raise
                        file.truncate(offset)
                        file.flush()
                        os.fsync(file.fileno())
                        self.size = offset
                        break
                    if frame is None:
                        break
                    yield Location(number, offset), *frame

    def append(self, kind, data):
        size = HEADER.size + CRC.size + len(data)
        if self.size and self.size + size > self.segment_size:
            self.sync()
            new = self.create_segment(self.number + 1)
            self.active.close()
            self.active = new
            self.number += 1
            self.size = 0
        self.active.seek(self.size)
        location = Location(self.number, self.size)
        write_frame(self.active, kind, data)
        self.size += size
        return location

    def read(self, location):
        if location.segment == self.number:
            self.active.seek(location.offset)
            frame = read_frame(self.active)
        else:
            with self.path(location.segment).open("rb") as file:
                file.seek(location.offset)
                frame = read_frame(file)
        if frame is None:
            raise CorruptionError("Journal index points past end of segment")
        return frame

    def sync(self):
        self.active.flush()
        os.fsync(self.active.fileno())

    def close(self):
        if self.active is not None:
            self.active.close()

    def disk_bytes(self):
        return sum(self.path(n).stat().st_size for n in range(1, self.number + 1))


class Generations:
    """Publish a replacement only after all its file entries are durable."""

    def __init__(self, root: Path, segment_size: int, *, existing=False):
        if type(segment_size) is not int or segment_size <= 0:
            raise ValueError("Segment size must be a positive integer")
        self.root = root
        self.segment_size = segment_size
        if not existing:
            ensure_directory(root)
        self.existing = existing
        self.pointer = root / "CURRENT"

    def open(self):
        try:
            raw = self.pointer.read_bytes()
        except FileNotFoundError:
            if self.existing:
                raise CorruptionError(f"Missing CURRENT in {self.root}") from None
            for directory in self.root.iterdir():
                if GENERATION.fullmatch(directory.name) and any(
                    file.stat().st_size for file in directory.iterdir()
                ):
                    raise CorruptionError(f"Missing CURRENT in nonempty store {self.root}")
            journal = self.create()
            try:
                self.publish(journal)
            except BaseException:
                journal.close()
                raise
        else:
            try:
                name = raw.decode("ascii").removesuffix("\n")
            except UnicodeError as error:
                raise CorruptionError(f"Invalid CURRENT in {self.root}") from error
            if raw != (name + "\n").encode("ascii") or not GENERATION.fullmatch(name):
                raise CorruptionError(f"Invalid CURRENT in {self.root}")
            journal = Journal(self.root / name, self.segment_size)
            if not self.existing:
                try:
                    sync_directory(self.root)
                except BaseException:
                    journal.close()
                    raise
        return journal

    def create(self):
        return Journal(self.root / f"gen-{uuid4().hex}", self.segment_size, create=True)

    def publish(self, journal):
        journal.sync()
        sync_directory(journal.directory)
        temporary = self.root / "CURRENT.tmp"
        with temporary.open("w", encoding="ascii") as target:
            target.write(journal.directory.name + "\n")
            target.flush()
            os.fsync(target.fileno())
        sync_directory(self.root)
        os.replace(temporary, self.pointer)
        sync_directory(self.root)

    def retire(self, journal):
        for directory in self.root.iterdir():
            if directory != journal.directory and GENERATION.fullmatch(directory.name):
                shutil.rmtree(directory, ignore_errors=True)
