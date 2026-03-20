import logging
import typing

from pycyphal.util.error_reporting import handle_internal_error, set_internal_error_handler


def _unittest_handle_internal_error(caplog: typing.Any) -> None:
    received: list[BaseException] = []
    set_internal_error_handler(received.append)

    exc = RuntimeError("boom")
    handle_internal_error(logging.getLogger("test"), exc, "context: %s", "details")

    assert len(received) == 1
    assert received[0] is exc
    assert "context: details" in caplog.text

    set_internal_error_handler(None)


def _unittest_handle_internal_error_bad_repr(caplog: typing.Any) -> None:
    class BadRepr:
        def __repr__(self) -> str:
            raise ValueError("repr exploded")

        def __str__(self) -> str:
            raise ValueError("str exploded")

    received: list[BaseException] = []
    set_internal_error_handler(received.append)

    exc = RuntimeError("boom")
    handle_internal_error(logging.getLogger("test"), exc, "obj: %s", BadRepr())

    assert len(received) == 1
    assert received[0] is exc
    assert "Failed to format message" in caplog.text

    set_internal_error_handler(None)
