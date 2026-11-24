"""Tokenize SQL, parse statements, and execute them against the relational state."""

import operator
import re
from dataclasses import dataclass, field
from itertools import islice
from sys import maxsize


class SQLError(ValueError):
    pass


TOKEN = re.compile(
    r"(?P<space>\s+)|(?P<text>'(?:[^']|'')*')|(?P<number>-?[0-9]+)"
    r"|(?P<word>[a-zA-Z_][a-zA-Z_0-9]*)|(?P<symbol><=|>=|<>|!=|[(),;*=<>])"
)
RESERVED = set(
    "CREATE TABLE PRIMARY KEY NOT NULL INSERT INTO VALUES SELECT FROM WHERE "
    "AND IS ORDER BY ASC DESC LIMIT UPDATE SET DELETE INTEGER TEXT BOOLEAN "
    "TRUE FALSE COUNT".split()
)
TYPES = {"INTEGER": int, "TEXT": str, "BOOLEAN": bool}
COMPARISONS = {
    "=": operator.eq, "!=": operator.ne, "<>": operator.ne, "<": operator.lt,
    ">": operator.gt, "<=": operator.le, ">=": operator.ge,
}
Value = int | str | bool | None


@dataclass(frozen=True)
class Column:
    name: str
    kind: str
    nullable: bool
    primary: bool


@dataclass(frozen=True)
class Predicate:
    column: str
    operator: str
    value: Value


@dataclass(frozen=True)
class Create:
    table: str
    columns: list[Column]


@dataclass(frozen=True)
class Insert:
    table: str
    columns: list[str] | None
    rows: list[list[Value]]


@dataclass(frozen=True)
class Select:
    table: str
    projection: list[str] | None
    count: bool
    filters: list[Predicate]
    order: tuple[str, bool] | None
    limit: int | None


@dataclass(frozen=True)
class Update:
    table: str
    assignments: list[tuple[str, Value]]
    filters: list[Predicate]


@dataclass(frozen=True)
class Delete:
    table: str
    filters: list[Predicate]


class Parser:
    def __init__(self, sql: str):
        self.tokens = []
        position = 0
        while position < len(sql):
            match = TOKEN.match(sql, position)
            if match is None:
                raise SQLError(f"Unexpected character at position {position + 1}")
            kind = match.lastgroup
            if kind != "space":
                self.tokens.append((kind, match.group()))
            position = match.end()
        self.tokens.append(("end", ""))
        self.position = 0

    @property
    def token(self):
        return self.tokens[self.position]

    def accept(self, value):
        kind, raw = self.token
        if kind in ("word", "symbol") and raw.upper() == value:
            self.position += 1
            return True
        return False

    def require(self, value):
        if not self.accept(value):
            raise SQLError(f"Expected {value}, found {self.token[1] or 'end of statement'}")

    def name(self):
        kind, raw = self.token
        if kind != "word" or raw.upper() in RESERVED:
            raise SQLError("Expected an unquoted, non-reserved identifier")
        self.position += 1
        return raw.lower()

    def integer(self):
        kind, raw = self.token
        if kind != "number":
            raise SQLError("Expected an integer")
        self.position += 1
        digits = raw.removeprefix("-").lstrip("0") or "0"
        if len(digits) > 19:
            raise SQLError("Integer is outside the signed 64-bit range")
        value = int(digits)
        if raw.startswith("-"):
            value = -value
        if not -(2**63) <= value < 2**63:
            raise SQLError("Integer is outside the signed 64-bit range")
        return value

    def literal(self):
        kind, raw = self.token
        if kind == "number":
            return self.integer()
        if kind == "text":
            self.position += 1
            return raw[1:-1].replace("''", "'")
        if self.accept("NULL"):
            return None
        if self.accept("TRUE"):
            return True
        if self.accept("FALSE"):
            return False
        raise SQLError("Expected a literal value")

    def comma_list(self, parse_item):
        items = [parse_item()]
        while self.accept(","):
            items.append(parse_item())
        return items

    def grouped(self, parse_item):
        self.require("(")
        items = self.comma_list(parse_item)
        self.require(")")
        return items

    def column(self):
        name = self.name()
        kind = self.token[1].upper()
        if kind not in TYPES or not self.accept(kind):
            raise SQLError("Expected INTEGER, TEXT or BOOLEAN")
        nullable, primary = True, False
        if self.accept("NOT"):
            self.require("NULL")
            nullable = False
        if self.accept("PRIMARY"):
            self.require("KEY")
            primary = True
            nullable = False
        return Column(name, kind, nullable, primary)

    def predicate(self):
        column = self.name()
        if self.accept("IS"):
            operation = "IS NOT" if self.accept("NOT") else "IS"
            self.require("NULL")
            return Predicate(column, operation, None)
        operation = self.token[1]
        if operation not in COMPARISONS or not self.accept(operation):
            raise SQLError("Expected a comparison or IS [NOT] NULL")
        return Predicate(column, operation, self.literal())

    def filters(self):
        if not self.accept("WHERE"):
            return []
        predicates = [self.predicate()]
        while self.accept("AND"):
            predicates.append(self.predicate())
        return predicates

    def assignment(self):
        column = self.name()
        self.require("=")
        return column, self.literal()

    def create(self):
        self.require("TABLE")
        return Create(self.name(), self.grouped(self.column))

    def insert(self):
        self.require("INTO")
        table = self.name()
        columns = self.grouped(self.name) if self.token == ("symbol", "(") else None
        self.require("VALUES")
        return Insert(table, columns, self.comma_list(self.value_row))

    def value_row(self):
        return self.grouped(self.literal)

    def select(self):
        count = self.accept("COUNT")
        projection = None
        if count:
            self.require("(")
            self.require("*")
            self.require(")")
        elif not self.accept("*"):
            projection = self.comma_list(self.name)
        self.require("FROM")
        table = self.name()
        filters = self.filters()
        order = None
        if self.accept("ORDER"):
            if count:
                raise SQLError("ORDER BY is unsupported with COUNT(*)")
            self.require("BY")
            column = self.name()
            descending = self.accept("DESC")
            if not descending:
                self.accept("ASC")
            order = (column, descending)
        limit = None
        if self.accept("LIMIT"):
            limit = self.integer()
            if limit < 0:
                raise SQLError("LIMIT must be non-negative")
        return Select(table, projection, count, filters, order, limit)

    def update(self):
        table = self.name()
        self.require("SET")
        return Update(table, self.comma_list(self.assignment), self.filters())

    def delete(self):
        self.require("FROM")
        return Delete(self.name(), self.filters())

    def parse(self):
        statements = {
            "CREATE": self.create, "INSERT": self.insert, "SELECT": self.select,
            "UPDATE": self.update, "DELETE": self.delete,
        }
        for keyword, parse_statement in statements.items():
            if self.accept(keyword):
                statement = parse_statement()
                self.accept(";")
                if self.token[0] != "end":
                    raise SQLError(f"Unexpected token {self.token[1]}")
                return statement
        raise SQLError("Expected CREATE, INSERT, SELECT, UPDATE or DELETE")


@dataclass
class Table:
    columns: dict[str, Column]
    primary: str
    rows: dict[Value, dict[str, Value]] = field(default_factory=dict)

    def column(self, name):
        if name not in self.columns:
            raise SQLError(f"Unknown column {name}")
        return self.columns[name]

    def validate(self, column, value, *, predicate=False):
        spec = self.column(column)
        if value is None:
            if not predicate and not spec.nullable:
                raise SQLError(f"{column} cannot be NULL")
        elif type(value) is not TYPES[spec.kind]:
            raise SQLError(f"{column} requires {spec.kind}")

    def matching(self, filters):
        for expression in filters:
            self.validate(expression.column, expression.value, predicate=True)
        candidates = self.rows.values()
        for expression in filters:
            if expression.column == self.primary and expression.operator == "=":
                row = self.rows.get(expression.value)
                candidates = () if row is None else (row,)
                break
        return (row for row in candidates if all(matches(row, expr) for expr in filters))


def matches(row, expression):
    left = row[expression.column]
    right = expression.value
    if expression.operator == "IS":
        return left is None
    if expression.operator == "IS NOT":
        return left is not None
    if left is None or right is None:
        return False
    return COMPARISONS[expression.operator](left, right)


def unique(names):
    if len(set(names)) != len(names):
        raise SQLError("Column names must be unique in this clause")


class Database:
    def __init__(self):
        self.tables: dict[str, Table] = {}

    def execute(self, sql: str) -> dict:
        statement = Parser(sql).parse()
        if isinstance(statement, Create):
            return self.create(statement)
        table = self.tables.get(statement.table)
        if table is None:
            raise SQLError(f"Unknown table {statement.table}")
        if isinstance(statement, Insert):
            return self.insert(table, statement)
        if isinstance(statement, Select):
            return self.select(table, statement)
        if isinstance(statement, Update):
            return self.update(table, statement)
        return self.delete(table, statement)

    def create(self, statement):
        if statement.table in self.tables:
            raise SQLError(f"Table {statement.table} already exists")
        unique([column.name for column in statement.columns])
        primary = [column.name for column in statement.columns if column.primary]
        if len(primary) != 1:
            raise SQLError("Each table must have one PRIMARY KEY")
        columns = {column.name: column for column in statement.columns}
        self.tables[statement.table] = Table(columns, primary[0])
        return {"affected": 0}

    def insert(self, table, statement):
        names = list(table.columns) if statement.columns is None else statement.columns
        unique(names)
        for name in names:
            table.column(name)
        additions = {}
        for values in statement.rows:
            if len(values) != len(names):
                raise SQLError("Column and value counts differ")
            row = dict.fromkeys(table.columns)
            row.update(zip(names, values))
            for name, value in row.items():
                table.validate(name, value)
            key = row[table.primary]
            if key in table.rows or key in additions:
                raise SQLError("Duplicate primary key")
            additions[key] = row
        table.rows.update(additions)
        return {"affected": len(additions)}

    def select(self, table, statement):
        rows = table.matching(statement.filters)
        if statement.count:
            count = sum(1 for _ in rows) if statement.limit != 0 else None
            return {"columns": ["count"], "rows": [] if count is None else [[count]]}
        projection = list(table.columns) if statement.projection is None else statement.projection
        for name in projection:
            table.column(name)
        if statement.order is not None:
            name, descending = statement.order
            table.column(name)
            # SQL NULLs sort first in ASC and last in DESC in this dialect.
            rows = sorted(rows, key=lambda row: (row[name] is not None, row[name]), reverse=descending)
        if statement.limit is not None:
            rows = islice(rows, min(statement.limit, maxsize))
        return {"columns": projection, "rows": [[row[name] for name in projection] for row in rows]}

    def update(self, table, statement):
        unique([name for name, _ in statement.assignments])
        changes = dict(statement.assignments)
        for name, value in changes.items():
            table.validate(name, value)
        replacements = {}
        matched = list(table.matching(statement.filters))
        original_keys = {row[table.primary] for row in matched}
        for row in matched:
            updated = row | changes
            key = updated[table.primary]
            if key in replacements or (key in table.rows and key not in original_keys):
                raise SQLError("Duplicate primary key")
            replacements[key] = updated
        for key in original_keys - replacements.keys():
            del table.rows[key]
        table.rows.update(replacements)
        return {"affected": len(replacements)}

    def delete(self, table, statement):
        keys = [row[table.primary] for row in table.matching(statement.filters)]
        for key in keys:
            del table.rows[key]
        return {"affected": len(keys)}
