# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

"""
Publishes ``uavcan.node.Heartbeat`` periodically and provides a couple of basic auxiliary services;
see :class:`pycyphal.application.heartbeat_publisher.HeartbeatPublisher`.
"""

from __future__ import annotations
import enum
import time
import typing
import logging
import asyncio
import uavcan.node
from uavcan.node import Heartbeat_1 as Heartbeat
import pycyphal
import pycyphal.application


class Health(enum.IntEnum):
    """
    Mirrors the health enumeration defined in ``uavcan.node.Heartbeat``.
    When enumerations are natively supported in DSDL, this will be replaced with an alias.
    """

    NOMINAL = uavcan.node.Health_1.NOMINAL
    ADVISORY = uavcan.node.Health_1.ADVISORY
    CAUTION = uavcan.node.Health_1.CAUTION
    WARNING = uavcan.node.Health_1.WARNING


class Mode(enum.IntEnum):
    """
    Mirrors the mode enumeration defined in ``uavcan.node.Heartbeat``.
    When enumerations are natively supported in DSDL, this will be replaced with an alias.
    """

    OPERATIONAL = uavcan.node.Mode_1.OPERATIONAL
    INITIALIZATION = uavcan.node.Mode_1.INITIALIZATION
    MAINTENANCE = uavcan.node.Mode_1.MAINTENANCE
    SOFTWARE_UPDATE = uavcan.node.Mode_1.SOFTWARE_UPDATE


VENDOR_SPECIFIC_STATUS_CODE_MASK = (
    2 ** pycyphal.dsdl.get_model(Heartbeat)["vendor_specific_status_code"].data_type.bit_length_set.max - 1
)


_logger = logging.getLogger(__name__)


class HeartbeatPublisher:
    """
    This class manages periodic publication of the node heartbeat message.
    Also it subscribes to heartbeat messages from other nodes and logs cautionary messages
    if a node-ID conflict is detected on the bus.

    The default states are as follows:

    - Health is NOMINAL.
    - Mode is OPERATIONAL.
    - Vendor-specific status code is zero.
    - Period is MAX_PUBLICATION_PERIOD (see the DSDL definition).
    - Priority is default (i.e., NOMINAL).
    """

    def __init__(self, node: pycyphal.application.Node):
        self._node = node
        self._health = Health.NOMINAL
        self._mode = Mode.OPERATIONAL
        self._vendor_specific_status_code = 0
        self._pre_heartbeat_handlers: typing.List[typing.Callable[[], None]] = []
        self._maybe_task: typing.Optional[asyncio.Task[None]] = None
        self._priority = pycyphal.presentation.DEFAULT_PRIORITY
        self._period = float(Heartbeat.MAX_PUBLICATION_PERIOD)
        self._subscriber = self._node.make_subscriber(Heartbeat)
        self._started_at = time.monotonic()

        def start() -> None:
            if not self._maybe_task:
                self._started_at = time.monotonic()
                self._subscriber.receive_in_background(self._handle_received_heartbeat)
                self._maybe_task = asyncio.get_event_loop().create_task(self._task_function())

        def close() -> None:
            if self._maybe_task:
                self._maybe_task.cancel()  # Cancel first to avoid exceptions from being logged from the task.
                self._maybe_task = None
                self._subscriber.close()

        node.add_lifetime_hooks(start, close)

    @property
    def node(self) -> pycyphal.application.Node:
        return self._node

    @property
    def uptime(self) -> float:
        """The current amount of time, in seconds, elapsed since the object was instantiated."""
        out = time.monotonic() - self._started_at
        assert out >= 0
        return out

    @property
    def health(self) -> Health:
        """The health value to report with Heartbeat; see :class:`Health`."""
        return self._health

    @health.setter
    def health(self, value: typing.Union[Health, int]) -> None:
        self._health = Health(value)

    @property
    def mode(self) -> Mode:
        """The mode value to report with Heartbeat; see :class:`Mode`."""
        return self._mode

    @mode.setter
    def mode(self, value: typing.Union[Mode, int]) -> None:
        self._mode = Mode(value)

    @property
    def vendor_specific_status_code(self) -> int:
        """The vendor-specific status code (VSSC) value to report with Heartbeat."""
        return self._vendor_specific_status_code

    @vendor_specific_status_code.setter
    def vendor_specific_status_code(self, value: int) -> None:
        value = int(value)
        if 0 <= value <= VENDOR_SPECIFIC_STATUS_CODE_MASK:
            self._vendor_specific_status_code = value
        else:
            raise ValueError(f"Invalid vendor-specific status code: {value}")

    @property
    def period(self) -> float:
        """
        How often the Heartbeat messages should be published. The upper limit (i.e., the lowest frequency)
        is constrained by the Cyphal specification; please see the DSDL source of ``uavcan.node.Heartbeat``.
        """
        return self._period

    @period.setter
    def period(self, value: float) -> None:
        value = float(value)
        if 0 < value <= Heartbeat.MAX_PUBLICATION_PERIOD:
            self._period = value
        else:
            raise ValueError(f"Invalid heartbeat period: {value}")

    @property
    def priority(self) -> pycyphal.transport.Priority:
        """
        The transfer priority level to use when publishing Heartbeat messages.
        """
        return self._priority

    @priority.setter
    def priority(self, value: pycyphal.transport.Priority) -> None:
        # noinspection PyArgumentList
        self._priority = pycyphal.transport.Priority(value)

    def add_pre_heartbeat_handler(self, handler: typing.Callable[[], None]) -> None:
        """
        Adds a new handler to be invoked immediately before a heartbeat message is published.
        The number of such handlers is unlimited.
        The handler invocation order follows the order of their registration.
        Handlers are invoked from a task running on the node's event loop.
        Handlers are not invoked until the instance is started.

        The handler can be used to synchronize the heartbeat message data (health, mode, vendor-specific status code)
        with external states. Observe that the handler will be invoked even if the heartbeat is not to be published,
        e.g., if the node is anonymous (does not have a node ID). If the handler throws an exception, it will be
        suppressed and logged. Note that the handler is to be not a coroutine but a regular function.

        This is a good method of scheduling periodic status checks on the node.
        """
        self._pre_heartbeat_handlers.append(handler)

    def make_message(self) -> Heartbeat:
        """Constructs a new heartbeat message from the object's state."""
        return Heartbeat(
            uptime=int(self.uptime),  # must floor
            health=uavcan.node.Health_1(self.health),
            mode=uavcan.node.Mode_1(self.mode),
            vendor_specific_status_code=self.vendor_specific_status_code,
        )

    async def _task_function(self) -> None:
        next_heartbeat_at = time.monotonic()
        pub: typing.Optional[pycyphal.presentation.Publisher[Heartbeat]] = None
        try:
            while self._maybe_task:
                try:
                    pycyphal.util.broadcast(self._pre_heartbeat_handlers)()
                    if self.node.id is not None:
                        if pub is None:
                            pub = self.node.make_publisher(Heartbeat)
                        assert pub is not None
                        pub.priority = self._priority
                        if not await pub.publish(self.make_message()):
                            _logger.warning("%s heartbeat send timed out", self)
                except Exception as ex:  # pragma: no cover
                    if (
                        isinstance(ex, (asyncio.CancelledError, pycyphal.transport.ResourceClosedError))
                        or not self._maybe_task
                    ):
                        _logger.debug("%s publisher task will exit: %s", self, ex)
                        break
                    _logger.exception("%s publisher task exception: %s", self, ex)

                next_heartbeat_at += self._period
                await asyncio.sleep(next_heartbeat_at - time.monotonic())
        finally:
            _logger.debug("%s publisher task is stopping", self)
            if pub is not None:
                pub.close()

    async def _handle_received_heartbeat(self, msg: Heartbeat, metadata: pycyphal.transport.TransferFrom) -> None:
        local_node_id = self.node.id
        remote_node_id = metadata.source_node_id
        if local_node_id is not None and remote_node_id is not None and local_node_id == remote_node_id:
            _logger.info(
                "NODE-ID CONFLICT: There is another node on the network that uses the same node-ID %d. "
                "Its latest heartbeat is %s with transfer metadata %s",
                remote_node_id,
                msg,
                metadata,
            )

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(
            self,
            heartbeat=self.make_message(),
            priority=self._priority.name,
            period=self._period,
        )
