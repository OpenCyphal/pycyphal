# Copyright (C) 2021  OpenCyphal  <opencyphal.org>
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
from typing import Union, Optional, Iterator, Any
from pathlib import Path
import logging
import sqlite3
import pycyphal
from . import Entry, BackendError, Backend, Value


__all__ = ["StaticBackend"]


_TIMEOUT = 0.5
_LOCATION_VOLATILE = ":memory:"


# noinspection SqlNoDataSourceInspection,SqlResolve
class StaticBackend(Backend):
    """
    Register storage backend implementation based on SQLite.
    Supports either persistent on-disk single-file storage or volatile in-memory storage.

    >>> b = StaticBackend("my_register_file.db")
    >>> b.persistent    # If a file is specified, the storage is persistent.
    True
    >>> b.location
    'my_register_file.db'
    >>> b.close()
    >>> b = StaticBackend()
    >>> b.persistent    # If no file is specified, the data is kept in-memory.
    False
    >>> from pycyphal.application.register import Bit
    >>> b["foo"] = Value(bit=Bit([True, False, True]))   # Create new register.
    >>> b["foo"].mutable
    True
    >>> list(b["foo"].value.bit.value)
    [True, False, True]
    >>> b["foo"] = Value(bit=Bit([False, True, True]))  # Set new value.
    >>> list(b["foo"].value.bit.value)
    [False, True, True]
    >>> list(b)
    ['foo']
    >>> del b["foo"]
    >>> list(b)
    []
    """

    def __init__(self, location: Union[None, str, Path] = None):
        """
        :param location: Either a path to the database file, or None. If None, the data will be stored in memory.

        The database is always initialized with ``check_same_thread=False`` to enable delegating its initialization
        to a thread pool from an async context.
        This is important for this library because if one needs to initialize a new node from an async function,
        calling the factories directly may be unacceptable due to their blocking behavior,
        so one is likely to rely on :meth:`asyncio.loop.run_in_executor`.
        The executor will initialize the instance in a worker thread and then hand it over to the main thread,
        which is perfectly safe, but it would trigger a false error from the SQLite engine complaining about
        the possibility of concurrency-related bugs.
        """
        self._loc = str(location or _LOCATION_VOLATILE).strip()
        self._db = sqlite3.connect(self._loc, timeout=_TIMEOUT, check_same_thread=False)
        self._execute(
            r"""
            create table if not exists `register` (
                `name`      varchar(255) not null unique primary key,
                `value`     blob not null,
                `mutable`   boolean not null,
                `ts`        time not null default current_timestamp
            )
            """,
            commit=True,
        )
        _logger.debug("%r: Initialized with registers: %r", self, self.keys())
        super().__init__()

    @property
    def location(self) -> str:
        return self._loc

    @property
    def persistent(self) -> bool:
        return self._loc.lower() != _LOCATION_VOLATILE

    def close(self) -> None:
        self._db.close()

    def index(self, index: int) -> Optional[str]:
        res = self._execute(r"select name from register order by name limit 1 offset ?", index).fetchone()
        return res[0] if res else None

    def setdefault(self, key: str, default: Optional[Union[Entry, Value]] = None) -> Entry:
        # This override is necessary to support assignment of Value along with Entry.
        if key not in self:
            if default is None:
                raise TypeError  # pragma: no cover
            self[key] = default
        return self[key]

    def __getitem__(self, key: str) -> Entry:
        res = self._execute(r"select mutable, value from register where name = ?", key).fetchone()
        if res is None:
            raise KeyError(key)
        mutable, value = res
        assert isinstance(value, bytes)
        obj = pycyphal.dsdl.deserialize(Value, [memoryview(value)])
        if obj is None:  # pragma: no cover
            _logger.warning("%r: Value of %r is not a valid serialization of %s: %r", self, key, Value, value)
            raise KeyError(key)
        e = Entry(value=obj, mutable=bool(mutable))
        _logger.debug("%r: Get %r -> %r", self, key, e)
        return e

    def __setitem__(self, key: str, value: Union[Entry, Value]) -> None:
        """
        If the register does not exist, it will be implicitly created.
        If the value is an instance of :class:`Value`, the mutability flag defaults to the old value or True if none.
        """
        if isinstance(value, Value):
            try:
                mutable = self[key].mutable
            except KeyError:
                mutable = True
            e = Entry(value, mutable=mutable)
        elif isinstance(value, Entry):
            e = value
        else:  # pragma: no cover
            raise TypeError(f"Unexpected argument: {value!r}")
        _logger.debug("%r: Set %r <- %r", self, key, e)
        # language=SQLite
        self._execute(
            r"insert or replace into register (name, value, mutable) values (?, ?, ?)",
            key,
            b"".join(pycyphal.dsdl.serialize(e.value)),
            e.mutable,
            commit=True,
        )

    def __delitem__(self, key: str) -> None:
        _logger.debug("%r: Delete %r", self, key)
        self._execute(r"delete from register where name = ?", key, commit=True)

    def __iter__(self) -> Iterator[str]:
        return iter(x for x, in self._execute(r"select name from register order by name").fetchall())

    def __len__(self) -> int:
        return int(self._execute(r"select count(*) from register").fetchone()[0])

    def _execute(self, statement: str, *params: Any, commit: bool = False) -> sqlite3.Cursor:
        try:
            cur = self._db.execute(statement, params)
            if commit:
                self._db.commit()
            return cur
        except sqlite3.OperationalError as ex:
            raise BackendError(f"Database transaction has failed: {ex}") from ex


_logger = logging.getLogger(__name__)


def _unittest_memory() -> None:
    from uavcan.primitive import String_1 as String, Unstructured_1 as Unstructured

    st = StaticBackend()
    print(st)
    assert not st.keys()
    assert not st.index(0)
    assert None is st.get("foo")
    assert len(st) == 0
    del st["foo"]

    st["foo"] = Value(string=String("Hello world!"))
    e = st.get("foo")
    assert e
    assert e.value.string
    assert e.value.string.value.tobytes().decode() == "Hello world!"
    assert e.mutable
    assert len(st) == 1

    # Override the same register.
    st["foo"] = Value(unstructured=Unstructured([1, 2, 3]))
    e = st.get("foo")
    assert e
    assert e.value.unstructured
    assert e.value.unstructured.value.tobytes() == b"\x01\x02\x03"
    assert e.mutable
    assert len(st) == 1

    assert ["foo"] == list(st.keys())
    assert "foo" == st.index(0)
    assert None is st.index(1)
    assert ["foo"] == list(st.keys())
    del st["foo"]
    assert [] == list(st.keys())
    assert len(st) == 0

    st.close()


def _unittest_file() -> None:
    import tempfile
    from uavcan.primitive import Unstructured_1 as Unstructured

    # First, populate the database with registers.
    db_file = tempfile.mktemp(".db")
    print("DB file:", db_file)
    st = StaticBackend(db_file)
    print(st)
    st["a"] = Value(unstructured=Unstructured([1, 2, 3]))
    st["b"] = Value(unstructured=Unstructured([4, 5, 6]))
    assert len(st) == 2
    st.close()

    # Then re-open it in writeable mode and ensure correctness.
    st = StaticBackend(db_file)
    print(st)
    assert len(st) == 2
    e = st.get("a")
    assert e
    assert e.value.unstructured
    assert e.value.unstructured.value.tobytes() == b"\x01\x02\x03"
    assert e.mutable

    e = st.get("b")
    assert e
    assert e.value.unstructured
    assert e.value.unstructured.value.tobytes() == b"\x04\x05\x06"
    assert e.mutable
    st.close()
