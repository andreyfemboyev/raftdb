"""Retrying client, typed value bindings, and result interchange."""

from collections.abc import Mapping
import csv
import http.client
import json
import re
import time
from uuid import uuid4

from .config import load_cluster
from .raft import MAX_SQL_BYTES, PROPOSAL_TIMEOUT, rpc
from .server import valid_request_id, valid_sql
from .sql import Parser, Select


PLACEHOLDER = re.compile(r"'(?:[^']|'')*'|['?]")
IDENTIFIER = re.compile(r"[a-zA-Z_][a-zA-Z_0-9]*\Z")
NULL = r"\N"


class QueryError(ValueError):
    """A rejected or unconfirmed statement, with its retryable request ID."""

    def __init__(self, status, result):
        super().__init__(result.get("error", f"HTTP {status}"))
        self.status = status
        self.result = result
        self.request_id = result.get("request_id")


def literal(value):
    if value is None:
        return "NULL"
    if type(value) is bool:
        return "TRUE" if value else "FALSE"
    if type(value) is int and -(2**63) <= value < 2**63:
        return str(value)
    if type(value) is str:
        return "'" + value.replace("'", "''") + "'"
    raise ValueError("Expected NULL, BOOLEAN, signed 64-bit INTEGER or TEXT")


def bind(sql, parameters):
    """Bind positional question marks outside strings to SQL scalar literals."""
    values = iter(parameters)

    def replace(match):
        token = match.group()
        if token == "'":
            raise ValueError("Unterminated SQL string")
        if token != "?":
            return token
        try:
            value = next(values)
        except StopIteration as error:
            raise ValueError("Too few SQL parameters") from error
        return literal(value)

    result = PLACEHOLDER.sub(replace, sql)
    sentinel = object()
    if next(values, sentinel) is not sentinel:
        raise ValueError("Too many SQL parameters")
    return result


class Client:
    def __init__(self, members, *, cluster=None, timeout=6.0):
        if not members:
            raise ValueError("At least one cluster member is required")
        if not 0 < timeout <= 3600:
            raise ValueError("Timeout must be greater than zero and at most 3600 seconds")
        self.members = dict(members)
        self.timeout = timeout
        self.cluster = cluster
        self.leader = None

    @classmethod
    def from_config(cls, path="cluster.json", *, timeout=6.0):
        members, cluster = load_cluster(path)
        return cls(members, cluster=cluster, timeout=timeout)

    def request(self, sql, *, request_id=None, page=None):
        if not valid_sql(sql):
            raise ValueError(f"Expected one non-empty SQL statement, at most {MAX_SQL_BYTES} UTF-8 bytes")
        request_id = uuid4().hex if request_id is None else request_id
        if not valid_request_id(request_id):
            raise ValueError("Invalid request ID")
        deadline = time.monotonic() + self.timeout
        last_error = "No cluster nodes responded"
        payload = {"sql": sql, "request_id": request_id}
        if page is not None:
            payload["page"] = page
        candidates = self.candidates()
        while time.monotonic() < deadline:
            if not candidates:
                candidates = self.candidates()
                time.sleep(min(0.1, max(0, deadline - time.monotonic())))
            target = candidates.pop(0)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                status, result = rpc(
                    self.members[target], "/sql", payload,
                    timeout=min(PROPOSAL_TIMEOUT + 0.5, remaining),
                )
            except (OSError, http.client.HTTPException, ValueError) as error:
                last_error = str(error)
                continue
            if not isinstance(result, dict):
                continue
            if status in (200, 400, 409, 422):
                if status in (200, 409, 422):
                    self.leader = target
                return status, result
            last_error = result.get("error", last_error)
            hint = result.get("leader")
            if isinstance(hint, str) and hint in candidates:
                candidates.remove(hint)
                candidates.insert(0, hint)

        return 503, {
            "ok": False, "code": "unconfirmed", "request_id": request_id, "error": last_error,
        }

    def candidates(self):
        candidates = list(self.members)
        if self.leader in candidates:
            candidates.remove(self.leader)
            candidates.insert(0, self.leader)
        return candidates

    @staticmethod
    def checked(status, result, request_id=None):
        if status != 200:
            if request_id is not None:
                result = {"request_id": request_id, **result}
            raise QueryError(status, result)
        return result

    def execute(self, sql, parameters=None, *, request_id=None):
        """Execute one statement. Supply a stable ID when retrying a mutation."""
        sql = sql if parameters is None else bind(sql, parameters)
        return self.checked(*self.request(sql, request_id=request_id), request_id)

    def query_batches(self, sql, parameters=None, *, batch_size=1000, request_id=None):
        """Yield pages of one committed SELECT result, including an empty first page."""
        if type(batch_size) is not int or not 1 <= batch_size <= 10_000:
            raise ValueError("Batch size must be between 1 and 10000")
        query = sql if parameters is None else bind(sql, parameters)
        if not isinstance(Parser(query).parse(), Select):
            raise ValueError("Batched results require SELECT")
        offset = 0
        request_id = uuid4().hex if request_id is None else request_id
        index = None
        while offset is not None:
            page = self.checked(*self.request(
                query, request_id=request_id, page={"offset": offset, "size": batch_size},
            ), request_id)
            if "next_offset" not in page:
                raise QueryError(502, {"error": "Node does not support result pages", "request_id": request_id})
            if index is None:
                index = page["index"]
            elif page["index"] != index:
                raise QueryError(502, {"error": "Result index changed between pages", "request_id": request_id})
            yield page
            offset = page["next_offset"]

    def inspect(self, operation="status", *, node=None):
        if operation not in ("status", "stats", "compact", "snapshot", "verify"):
            raise ValueError(f"Unknown inspection operation {operation}")
        if node is not None and node not in self.members:
            raise ValueError(f"Unknown node {node}")
        states = {}
        for member in [node] if node is not None else self.members:
            try:
                if operation in ("status", "stats"):
                    states[member] = self.get(member, f"/{operation}")
                else:
                    status, result = rpc(
                        self.members[member], f"/admin/{operation}", {"cluster": self.cluster},
                        timeout=self.timeout,
                    )
                    states[member] = self.checked(status, result)
            except (OSError, http.client.HTTPException, ValueError) as error:
                states[member] = {"error": str(error)}
        return states

    def get(self, node, path):
        connection = http.client.HTTPConnection(*self.members[node], timeout=0.5)
        try:
            connection.request("GET", path)
            with connection.getresponse() as response:
                return self.checked(response.status, json.load(response))
        finally:
            connection.close()

    def export_csv(self, target, sql, parameters=None, *, batch_size=1000):
        writer = csv.writer(target)
        for number, page in enumerate(self.query_batches(sql, parameters, batch_size=batch_size)):
            if number == 0:
                writer.writerow(page["columns"])
            writer.writerows([csv_value(value) for value in row] for row in page["rows"])

    def export_json(self, target, sql, parameters=None, *, batch_size=1000):
        first_row = True
        for number, page in enumerate(self.query_batches(sql, parameters, batch_size=batch_size)):
            if number == 0:
                target.write('{"columns":')
                json.dump(page["columns"], target)
                target.write(',"rows":[')
            for row in page["rows"]:
                if not first_row:
                    target.write(",")
                json.dump(row, target)
                first_row = False
        target.write("]}\n")

    def import_csv(self, table, source, types: Mapping, *, batch_size=100, request_id=None):
        """Insert a CSV with named columns. Each batch commits independently."""
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("Batch size must be a positive integer")
        reader = csv.reader(source, strict=True)
        names = next(reader, [])
        if (
            not names or not IDENTIFIER.fullmatch(table)
            or not all(IDENTIFIER.fullmatch(name) for name in names)
            or len(set(name.lower() for name in names)) != len(names)
            or set(names) != set(types)
            or any(kind not in ("INTEGER", "BOOLEAN", "TEXT") for kind in types.values())
        ):
            raise ValueError("CSV header and types must match valid column names and supported SQL types")
        prefix = f"INSERT INTO {table} ({', '.join(names)}) VALUES "
        kinds = [types[name] for name in names]
        base_id = uuid4().hex if request_id is None else request_id
        if not isinstance(base_id, str) or not valid_request_id(f"{base_id}:0"):
            raise ValueError("Invalid CSV import request ID prefix")
        rows, length, affected, number = [], len(prefix), 0, 0
        for values in reader:
            if len(values) != len(names):
                raise ValueError(f"CSV record ending at line {reader.line_num} has an incorrect column count")
            row = "(" + ", ".join(literal(parse_csv(value, kind)) for value, kind in zip(values, kinds)) + ")"
            size = len(row.encode("utf-8"))
            if len(prefix) + size > MAX_SQL_BYTES:
                raise ValueError(f"CSV record ending at line {reader.line_num} exceeds the SQL size limit")
            if rows and (len(rows) == batch_size or length + 2 + size > MAX_SQL_BYTES):
                affected += self.execute(prefix + ", ".join(rows), request_id=f"{base_id}:{number}")["affected"]
                rows, length, number = [], len(prefix), number + 1
            length += size + (2 if rows else 0)
            rows.append(row)
        if rows:
            affected += self.execute(prefix + ", ".join(rows), request_id=f"{base_id}:{number}")["affected"]
        return affected

    def to_pandas(self, sql, parameters=None):
        import pandas

        result = self.read_result(sql, parameters)
        return pandas.DataFrame(result["rows"], columns=result["columns"])

    def to_numpy(self, sql, parameters=None):
        import numpy

        result = self.read_result(sql, parameters)
        return numpy.array(result["rows"], dtype=object).reshape(-1, len(result["columns"]))

    def read_result(self, sql, parameters):
        query = sql if parameters is None else bind(sql, parameters)
        if not isinstance(Parser(query).parse(), Select):
            raise ValueError("Conversion requires SELECT")
        return self.execute(query)


def csv_value(value):
    if value is None:
        return NULL
    if isinstance(value, str) and value.startswith("\\"):
        return "\\" + value
    if type(value) is bool:
        return "TRUE" if value else "FALSE"
    return value


def parse_csv(value, kind):
    if value == NULL:
        return None
    if kind == "TEXT":
        return value[1:] if value.startswith("\\\\") else value
    if kind == "BOOLEAN" and value.upper() in ("TRUE", "FALSE"):
        return value.upper() == "TRUE"
    if kind == "INTEGER" and re.fullmatch(r"-?[0-9]+", value):
        return int(value)
    raise ValueError(f"CSV value is not {kind}")
