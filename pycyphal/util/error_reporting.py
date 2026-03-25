from __future__ import annotations

import logging
import sys
import typing

ErrorHandler = typing.Callable[[BaseException], None]

_error_handler: ErrorHandler | None = None


def set_internal_error_handler(handler: ErrorHandler | None) -> None:
    """
    Register a callback that will be invoked whenever an internal pycyphal component encounters
    an exception somewhere in background asyncio tasks.

    This is useful to be notified when something goes wrong while receiving messages in the background etc.

    """
    global _error_handler  # noqa: PLW0603
    _error_handler = handler


def handle_internal_error(
    logger: logging.Logger,
    e: BaseException,
    msg: str = "",
    *args: object,
) -> None:
    """
    Report an internal error: log it via the provided *logger* and invoke the registered error handler.

    :param logger: The logger to use for ``logger.exception``.
    :param e: The exception to report.
    :param msg: Optional context message describing where/why the error occurred.
                Defer any formatting for this functions, to also properly handle cases where you print
                something and its __repr__/__str__ raises an exception.
    :param args: Optional arguments for the context message.
    """
    if msg:
        try:
            msg = msg % args
        except Exception:
            # if formatting fails (due to a bad __str__/__repr__), suppress the exception and use a fallback message
            msg = f"Failed to format message '{msg}'"
    else:
        msg = "Unhandled internal error"

    logger.error(msg, exc_info=e)

    if _error_handler is not None:
        if sys.version_info >= (3, 11):
            e.add_note(msg)
        try:
            _error_handler(e)
        except Exception:
            logger.exception("Error in the registered internal error handler")
