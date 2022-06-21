# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

"""
This module implements forwarding between the standard subject ``uavcan.diagnostic.Record``
and Python's standard logging facilities (:mod:`logging`).
"""

from __future__ import annotations
import sys
import asyncio
import logging
from typing import Optional
from uavcan.diagnostic import Record_1 as Record
from uavcan.diagnostic import Severity_1 as Severity
import pycyphal
import pycyphal.application


__all__ = ["DiagnosticSubscriber", "DiagnosticPublisher", "Record", "Severity"]


_logger = logging.getLogger(__name__)


class DiagnosticSubscriber:
    """
    Subscribes to ``uavcan.diagnostic.Record`` and forwards every received message into Python's :mod:`logging`.
    The logger name is that of the current module.
    The log level mapping is defined by :attr:`SEVERITY_CYPHAL_TO_PYTHON`.

    This class is convenient for various CLI tools and automation scripts where the user will not
    need to implement additional logic to see log messages from the network.
    """

    SEVERITY_CYPHAL_TO_PYTHON = {
        Severity.TRACE: logging.INFO,
        Severity.DEBUG: logging.INFO,
        Severity.INFO: logging.INFO,
        Severity.NOTICE: logging.INFO,
        Severity.WARNING: logging.WARNING,
        Severity.ERROR: logging.ERROR,
        Severity.CRITICAL: logging.CRITICAL,
        Severity.ALERT: logging.CRITICAL,
    }

    def __init__(self, node: pycyphal.application.Node):
        sub_record = node.make_subscriber(Record)
        node.add_lifetime_hooks(
            lambda: sub_record.receive_in_background(self._on_message),
            sub_record.close,
        )

    async def _on_message(self, msg: Record, meta: pycyphal.transport.TransferFrom) -> None:
        node_id = meta.source_node_id if meta.source_node_id is not None else "anonymous"
        diag_text = msg.text.tobytes().decode("utf8", errors="replace")
        log_text = (
            f"uavcan.diagnostic.Record: node={node_id} severity={msg.severity.value} "
            + f"ts_sync={msg.timestamp.microsecond * 1e-6:0.6f} ts_local={meta.timestamp}:\n"
            + diag_text
        )
        level = self.SEVERITY_CYPHAL_TO_PYTHON.get(msg.severity.value, logging.CRITICAL)
        _logger.log(level, log_text)


class DiagnosticPublisher(logging.Handler):
    # noinspection PyTypeChecker,PyUnresolvedReferences
    """
    Implementation of :class:`logging.Handler` that forwards all log messages via the standard
    diagnostics subject of Cyphal.
    Log messages that are too long to fit into a Cyphal Record object are truncated.
    Log messages emitted by PyCyphal itself may be dropped to avoid infinite recursion.
    No messages will be published if the local node is anonymous.

    Here's a usage example. Set up test rigging:

    ..  doctest::
        :hide:

        >>> import tests
        >>> _ = tests.dsdl.compile()
        >>> tests.asyncio_allow_event_loop_access_from_top_level()
        >>> from tests import doctest_await

    >>> from pycyphal.transport.loopback import LoopbackTransport
    >>> from pycyphal.application import make_node, NodeInfo, make_registry
    >>> node = make_node(NodeInfo(), transport=LoopbackTransport(1))
    >>> node.start()

    Instantiate publisher and install it with the logging system:

    >>> diagnostic_pub = DiagnosticPublisher(node, level=logging.INFO)
    >>> logging.root.addHandler(diagnostic_pub)
    >>> diagnostic_pub.timestamping_enabled = True  # This is only allowed if the Cyphal network uses the wall clock.
    >>> diagnostic_pub.timestamping_enabled
    True

    Test it:

    >>> sub = node.make_subscriber(Record)
    >>> logging.info('Test message')
    >>> msg, _ = doctest_await(sub.receive_for(1.0))
    >>> msg.text.tobytes().decode()
    'root: Test message'
    >>> msg.severity.value == Severity.INFO     # The log level is mapped automatically.
    True

    Don't forget to remove it afterwards:

    >>> logging.root.removeHandler(diagnostic_pub)
    >>> node.close()

    The node factory :func:`pycyphal.application.make_node` actually allows you to do this automatically,
    so that you don't have to hard-code behaviors in the application sources:

    >>> registry = make_registry(None, {"UAVCAN__DIAGNOSTIC__SEVERITY": "2", "UAVCAN__DIAGNOSTIC__TIMESTAMP": "1"})
    >>> node = make_node(NodeInfo(), registry, transport=LoopbackTransport(1))
    >>> node.start()
    >>> sub = node.make_subscriber(Record)
    >>> logging.info('Test message')
    >>> msg, _ = doctest_await(sub.receive_for(1.0))
    >>> msg.text.tobytes().decode()
    'root: Test message'
    >>> msg.severity.value == Severity.INFO
    True
    >>> node.close()
    """

    def __init__(self, node: pycyphal.application.Node, level: int = logging.WARNING) -> None:
        self._pub: Optional[pycyphal.presentation.Publisher[Record]] = None
        self._fut: Optional[asyncio.Future[None]] = None
        self._forward_timestamp = False
        self._started = False
        super().__init__(level)

        def start() -> None:
            self._started = True
            if node.id is not None:
                self._pub = node.make_publisher(Record)
                self._pub.priority = pycyphal.transport.Priority.OPTIONAL
                self._pub.send_timeout = 10.0
            else:
                _logger.info("DiagnosticPublisher not initialized because the local node is anonymous")

        def close() -> None:
            self._started = False
            if self._pub:
                self._pub.close()
            if self._fut is not None:
                try:
                    self._fut.result()
                except asyncio.InvalidStateError:
                    pass  # May be unset https://github.com/OpenCyphal/pycyphal/issues/192

        node.add_lifetime_hooks(start, close)

    @property
    def timestamping_enabled(self) -> bool:
        """
        If True, the publisher will be setting the field ``timestamp`` of the published log messages to
        :attr:`logging.LogRecord.created` (with the appropriate unit conversion).
        If False (default), published messages will not be timestamped at all.
        """
        return self._forward_timestamp

    @timestamping_enabled.setter
    def timestamping_enabled(self, value: bool) -> None:
        self._forward_timestamp = bool(value)

    def emit(self, record: logging.LogRecord) -> None:
        """
        This method intentionally drops all low-severity messages originating from within PyCyphal itself
        to prevent infinite recursion through the logging system.
        """
        if not self._started or (record.module.startswith(pycyphal.__name__) and record.levelno < logging.ERROR):
            return

        # Further, unconditionally drop all messages while publishing is in progress for the same reason.
        # This logic may need to be reviewed later.
        if self._fut is not None and self._fut.done():
            self._fut.result()
            self._fut = None

        dcs_rec = DiagnosticPublisher.log_record_to_diagnostic_message(record, self._forward_timestamp)
        if self._fut is None:
            self._fut = asyncio.ensure_future(self._publish(dcs_rec))
        else:
            # DROPPED
            pass

    async def _publish(self, record: Record) -> None:
        try:
            if self._pub is not None and not await self._pub.publish(record):
                print(self, "TIMEOUT", record, file=sys.stderr)  # pragma: no cover
        except pycyphal.transport.TransportError:
            pass
        except Exception as ex:
            print(self, "ERROR", ex.__class__.__name__, ex, file=sys.stderr)  # pragma: no cover

    @staticmethod
    def log_record_to_diagnostic_message(record: logging.LogRecord, use_timestamp: bool) -> Record:
        from uavcan.time import SynchronizedTimestamp_1 as SynchronizedTimestamp

        ts: Optional[SynchronizedTimestamp] = None
        if use_timestamp:
            ts = SynchronizedTimestamp(microsecond=int(record.created * 1e6))

        # The magic severity conversion formula is found by a trivial linear regression:
        #   Fit[data, {1, x}, {{0, 0}, {10, 1}, {20, 2}, {30, 4}, {40, 5}, {50, 6}}]
        sev = min(7, round(-0.14285714285714374 + 0.12571428571428572 * record.levelno))

        text = f"{record.name}: {record.getMessage()}"
        text = text[:255]  # TODO: this is crude; expose array lengths from DSDL.
        return Record(timestamp=ts, severity=Severity(sev), text=text)

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(self, self._pub)
