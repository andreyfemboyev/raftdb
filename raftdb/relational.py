"""Persistent schemas, primary rows, and replicated request outcomes."""

from collections.abc import MutableMapping
from dataclasses import asdict
import json
import struct

from .journal import CorruptionError
from .kv import KV, snapshot_records
from .sql import Column, Database, SQLError, Table


SCHEMA, ROW, REQUEST, CHUNK = b"S", b"R", b"D", b"C"
CHECKPOINT = b"M\x00applied"
U64 = struct.Struct(">Q")
I64 = struct.Struct(">q")
MAX_RESULT = 16 * 1024 * 1024


def pack(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def checkpoint(data):
    if data is None:
        return 0
    if len(data) != U64.size:
        raise CorruptionError("Invalid relational checkpoint")
    return U64.unpack(data)[0]


def schema_key(name):
    return SCHEMA + name.encode("ascii")


def row_prefix(name):
    # SQL identifiers contain ASCII letters, digits and underscores.
    return ROW + name.encode("ascii") + b"\0"


def primary_bytes(key):
    if type(key) is int:
        return b"i" + I64.pack(key)
    if type(key) is bool:
        return b"b" + bytes([key])
    if type(key) is str:
        return b"s" + key.encode("utf-8")
    if key is None:
        return b"n"  # Lookup only. SQL validation forbids a NULL primary key.
    raise TypeError("Invalid primary key type")


def primary_value(encoded):
    kind, value = encoded[:1], encoded[1:]
    if kind == b"i" and len(value) == I64.size:
        return I64.unpack(value)[0]
    if kind == b"b" and value in (b"\0", b"\1"):
        return value == b"\1"
    if kind == b"s":
        return value.decode("utf-8")
    raise CorruptionError("Invalid persisted primary key")


class Overlay:
    def __init__(self, store):
        self.store = store
        self.changes = {}

    def get(self, key):
        if key in self.changes:
            return self.changes[key]
        return self.store.get(key)

    def keys(self, prefix):
        for key in self.store.keys(prefix):
            if self.changes.get(key, b"") is not None:
                yield key
        for key, value in self.changes.items():
            if key.startswith(prefix) and key not in self.store.index and value is not None:
                yield key


class Rows(MutableMapping):
    def __init__(self, view, name, columns):
        self.view = view
        self.prefix = row_prefix(name)
        self.columns = columns

    def __getitem__(self, key):
        data = self.view.get(self.prefix + primary_bytes(key))
        if data is None:
            raise KeyError(key)
        return dict(zip(self.columns, json.loads(data), strict=True))

    def __contains__(self, key):
        encoded = self.prefix + primary_bytes(key)
        if encoded in self.view.changes:
            return self.view.changes[encoded] is not None
        return encoded in self.view.store.index

    def __setitem__(self, key, row):
        self.view.changes[self.prefix + primary_bytes(key)] = pack([row[name] for name in self.columns])

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        self.view.changes[self.prefix + primary_bytes(key)] = None

    def __iter__(self):
        return (primary_value(key[len(self.prefix):]) for key in self.view.keys(self.prefix))

    def __len__(self):
        return sum(1 for _ in self.view.keys(self.prefix))


class Tables(MutableMapping):
    def __init__(self, view):
        self.view = view

    def __getitem__(self, name):
        data = self.view.get(schema_key(name))
        if data is None:
            raise KeyError(name)
        primary, specs = json.loads(data)
        columns = {spec["name"]: Column(**spec) for spec in specs}
        return Table(columns, primary, Rows(self.view, name, columns))

    def __setitem__(self, name, table):
        self.view.changes[schema_key(name)] = pack([
            table.primary, [asdict(column) for column in table.columns.values()],
        ])

    def __delitem__(self, name):
        if name not in self:
            raise KeyError(name)
        for key in list(self.view.keys(row_prefix(name))):
            self.view.changes[key] = None
        self.view.changes[schema_key(name)] = None

    def __iter__(self):
        return (key[1:].decode("ascii") for key in self.view.keys(SCHEMA))

    def __len__(self):
        return sum(1 for _ in self.view.keys(SCHEMA))


class Relational:
    def __init__(self, directory, segment_size, *, repair=True):
        self.store = KV(directory, segment_size, repair=repair)
        try:
            self.applied = checkpoint(self.store.get(CHECKPOINT))
        except BaseException:
            self.store.close()
            raise

    @property
    def tables(self):
        return Tables(Overlay(self.store))

    @staticmethod
    def chunk_key(request, number):
        return CHUNK + request + b"\0" + U64.pack(number)

    def result(self, request_id):
        request = request_id.encode("ascii")
        data = self.store.get(REQUEST + request)
        if data is None:
            return None
        if data.startswith(b"\0"):
            chunks = []
            for number in range(checkpoint(data[1:])):
                chunk = self.store.get(self.chunk_key(request, number))
                if chunk is None:
                    raise CorruptionError("Missing request outcome chunk")
                chunks.append(chunk)
            data = b"".join(chunks)
        return json.loads(data)

    def apply(self, index, entry):
        if index != self.applied + 1:
            raise CorruptionError("Cannot apply a nonconsecutive Raft entry")
        view = Overlay(self.store)
        request_id = entry.get("request_id")
        if request_id is not None:
            request = request_id.encode("ascii")
            key = REQUEST + request
            if key not in self.store.index:
                sql, legacy = entry["sql"], entry.get("legacy", False)
                try:
                    engine = Database(Tables(view), max_result=None if legacy else MAX_RESULT)
                    result = {"ok": True, **engine.execute(sql, legacy=legacy)}
                    data = pack([sql, index, result])
                    if not legacy and len(data) > MAX_RESULT:
                        raise SQLError("SQL result exceeds the size limit")
                except SQLError as error:
                    view.changes.clear()
                    data = pack([sql, index, {"ok": False, "code": "sql_error", "error": str(error)}])
                if len(data) > MAX_RESULT:
                    # Preserve results of old statements that had no size limit.
                    chunks = 0
                    for offset in range(0, len(data), MAX_RESULT):
                        view.changes[self.chunk_key(request, chunks)] = data[offset:offset + MAX_RESULT]
                        chunks += 1
                    data = b"\0" + U64.pack(chunks)
                view.changes[key] = data
        view.changes[CHECKPOINT] = U64.pack(index)
        self.store.write(view.changes)
        self.applied = index

    @staticmethod
    def validate_snapshot(path, index):
        seen = set()
        applied = None
        for record in snapshot_records(path):
            if record.key in seen:
                raise CorruptionError("Duplicate key in snapshot")
            seen.add(record.key)
            if record.key == CHECKPOINT:
                applied = checkpoint(record.value)
        if applied != index:
            raise CorruptionError("Snapshot checkpoint does not match its Raft index")

    def verify(self):
        self.store.verify()
        tables = self.tables
        schemas = {name: tables[name] for name in tables}
        for key in self.store.index:
            namespace = key[:1]
            if namespace == ROW:
                table, separator, primary = key[1:].partition(b"\0")
                name = table.decode("ascii")
                if not separator or name not in schemas:
                    raise CorruptionError("Primary row refers to a missing table")
                definition = schemas[name]
                value = primary_value(primary)
                row = definition.rows[value]
                actual = row[definition.primary]
                if type(actual) is not type(value) or actual != value:
                    raise CorruptionError("Primary key does not match the row")
                for column, value in row.items():
                    definition.validate(column, value)
            elif namespace == REQUEST:
                _, index, _ = self.result(key[1:].decode("ascii"))
                if type(index) is not int or not 0 < index <= self.applied:
                    raise CorruptionError("Request outcome lies outside the applied prefix")
            elif namespace not in (SCHEMA, CHUNK) and key != CHECKPOINT:
                raise CorruptionError("Unknown relational key namespace")

    def restore(self, path, index):
        self.store.replace(snapshot_records(path))
        self.applied = checkpoint(self.store.get(CHECKPOINT))
        if self.applied != index:
            raise CorruptionError("Restored checkpoint does not match its Raft index")
