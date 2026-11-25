"""A small replicated relational database built with the Python standard library."""

from .client import Client, QueryError

__version__ = "0.2.0"
__all__ = ["Client", "QueryError"]
