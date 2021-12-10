# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import os
import asyncio
import logging
from typing import Awaitable, TypeVar, Any
from . import dsdl as dsdl
from .dsdl import DEMO_DIR as DEMO_DIR

assert ("PYTHONASYNCIODEBUG" in os.environ) or (
    os.environ.get("IGNORE_PYTHONASYNCIODEBUG", False)
), "PYTHONASYNCIODEBUG should be set while running the tests"


_logger = logging.getLogger(__name__)

_T = TypeVar("_T")

_PATCH_RESTORE_PREFIX = "_pyuavcan_orig_"


def asyncio_allow_event_loop_access_from_top_level() -> None:
    """
    This monkeypatch is needed to make doctests behave as if they were executed from inside an event loop.
    It is often required to access the current event loop from a non-async function invoked from the regular
    doctest context.
    One could use ``asyncio.get_event_loop`` for that until Python 3.10, where this behavior has been deprecated.

    Ideally, we should be able to run the entire doctest suite with an event loop available and ``await`` being
    enabled at the top level; however, as of right now this is not possible yet.
    You will find more info on this here: https://github.com/Erotemic/xdoctest/issues/115
    Until a proper solution is available, this hack will have to stay here.

    This function shall be invoked per test, because the test suite undoes its effect before starting the next test.
    """
    _logger.info("asyncio_allow_event_loop_access_from_top_level()")

    def swap(mod: Any, name: str, new: Any) -> None:
        restore = _PATCH_RESTORE_PREFIX + name
        if not hasattr(mod, restore):
            setattr(mod, restore, getattr(mod, name))
        setattr(mod, name, new)

    swap(asyncio, "get_event_loop", asyncio.get_event_loop_policy().get_event_loop)

    def events_get_event_loop(stacklevel: int = 0) -> asyncio.AbstractEventLoop:  # pragma: no cover
        _ = stacklevel
        return asyncio.get_event_loop_policy().get_event_loop()

    try:
        swap(asyncio.events, "_get_event_loop", events_get_event_loop)
    except AttributeError:  # pragma: no cover
        pass  # Python <3.10


def asyncio_restore() -> None:
    count = 0
    for mod in [asyncio, asyncio.events]:
        for k, v in mod.__dict__.items():
            if k.startswith(_PATCH_RESTORE_PREFIX):
                count += 1
                setattr(mod, k[len(_PATCH_RESTORE_PREFIX) :], v)
    _logger.info("asyncio_restore() %r", count)


def doctest_await(future: Awaitable[_T]) -> _T:
    """
    This is a helper for writing doctests of async functions. Behaves just like ``await``.
    This is a hack; when the proper solution is available it should be removed:
    https://github.com/Erotemic/xdoctest/issues/115
    """
    return asyncio.get_event_loop().run_until_complete(future)
