# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

"""
Publishes ``uavcan.node.Heartbeat`` periodically and provides a couple of basic auxiliary services;
see :class:`HeartbeatPublisher`.
"""

import enum
import time
import typing
import logging
import asyncio
import uavcan.node
from uavcan.node import Heartbeat_1_0 as Heartbeat
import pyuavcan


class Health(enum.IntEnum):
    """
    Mirrors the health enumeration defined in ``uavcan.node.Heartbeat``.
    When enumerations are natively supported in DSDL, this will be replaced with an alias.
    """

    NOMINAL = uavcan.node.Health_1_0.NOMINAL
    ADVISORY = uavcan.node.Health_1_0.ADVISORY
    CAUTION = uavcan.node.Health_1_0.CAUTION
    WARNING = uavcan.node.Health_1_0.WARNING


class Mode(enum.IntEnum):
    """
    Mirrors the mode enumeration defined in ``uavcan.node.Heartbeat``.
    When enumerations are natively supported in DSDL, this will be replaced with an alias.
    """

    OPERATIONAL = uavcan.node.Mode_1_0.OPERATIONAL
    INITIALIZATION = uavcan.node.Mode_1_0.INITIALIZATION
    MAINTENANCE = uavcan.node.Mode_1_0.MAINTENANCE
    SOFTWARE_UPDATE = uavcan.node.Mode_1_0.SOFTWARE_UPDATE


VENDOR_SPECIFIC_STATUS_CODE_MASK = (
    2 ** list(pyuavcan.dsdl.get_model(Heartbeat)["vendor_specific_status_code"].data_type.bit_length_set)[0] - 1
)


_logger = logging.getLogger(__name__)


class HeartbeatPublisher:
    """
    This class manages periodic publication of the node heartbeat message.
    Also it subscribes to heartbeat messages from other nodes and logs cautionary messages
    if a node-ID conflict is detected on the bus.

    Instances must be manually started when initialization is finished by invoking :meth:`start`.

    The default states are as follows:

    - Health is NOMINAL.
    - Mode is OPERATIONAL.
    - Vendor-specific status code is zero.
    - Period is MAX_PUBLICATION_PERIOD (see the DSDL definition).
    - Priority is default as defined by the presentation layer (i.e., NOMINAL).
    """

    def __init__(self, presentation: pyuavcan.presentation.Presentation):
        self._presentation = presentation
        self._instantiated_at = time.monotonic()
        self._health = Health.NOMINAL
        self._mode = Mode.OPERATIONAL
        self._vendor_specific_status_code = 0
        self._pre_heartbeat_handlers: typing.List[typing.Callable[[], None]] = []
        self._maybe_task: typing.Optional[asyncio.Task[None]] = None

        self._publisher = self._presentation.make_publisher_with_fixed_subject_id(Heartbeat)
        self._publisher.send_timeout = float(Heartbeat.MAX_PUBLICATION_PERIOD)

        self._subscriber = self._presentation.make_subscriber_with_fixed_subject_id(Heartbeat)

    def start(self) -> None:
        """
        Starts the background publishing task on the presentation's event loop.
        It will be stopped automatically when closed. Does nothing if already started.
        """
        if not self._maybe_task:
            self._subscriber.receive_in_background(self._handle_received_heartbeat)
            self._maybe_task = self._presentation.loop.create_task(self._task_function())

    @property
    def uptime(self) -> float:
        """The current amount of time, in seconds, elapsed since the object was instantiated."""
        out = time.monotonic() - self._instantiated_at
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
        is constrained by the UAVCAN specification; please see the DSDL source of ``uavcan.node.Heartbeat``.
        The send timeout equals the period.
        """
        return self._publisher.send_timeout

    @period.setter
    def period(self, value: float) -> None:
        value = float(value)
        if 0 < value <= Heartbeat.MAX_PUBLICATION_PERIOD:
            self._publisher.send_timeout = value  # This is not a typo! Send timeout equals period here.
        else:
            raise ValueError(f"Invalid heartbeat period: {value}")

    @property
    def priority(self) -> pyuavcan.transport.Priority:
        """
        The transfer priority level to use when publishing Heartbeat messages.
        """
        return self._publisher.priority

    @priority.setter
    def priority(self, value: pyuavcan.transport.Priority) -> None:
        self._publisher.priority = pyuavcan.transport.Priority(value)

    @property
    def publisher(self) -> pyuavcan.presentation.Publisher[Heartbeat]:
        """
        Provides access to the underlying presentation layer publisher instance (see constructor).
        """
        return self._publisher

    def add_pre_heartbeat_handler(self, handler: typing.Callable[[], None]) -> None:
        """
        Adds a new handler to be invoked immediately before a heartbeat message is published.
        The number of such handlers is unlimited.
        The handler invocation order follows the order of their registration.
        Handlers are invoked from a task running on the presentation's event loop.
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
            health=uavcan.node.Health_1_0(self.health),
            mode=uavcan.node.Mode_1_0(self.mode),
            vendor_specific_status_code=self.vendor_specific_status_code,
        )

    def close(self) -> None:
        """
        Closes the publisher, the subscriber, and stops the internal task.
        Subsequent invocations have no effect.
        """
        if self._maybe_task:
            self._subscriber.close()
            self._publisher.close()
            self._maybe_task.cancel()
            self._maybe_task = None

    async def _task_function(self) -> None:
        next_heartbeat_at = time.monotonic()
        while self._maybe_task:
            try:
                self._call_pre_heartbeat_handlers()
                if self._presentation.transport.local_node_id is not None:
                    if not await self._publisher.publish(self.make_message()):
                        _logger.warning("%s heartbeat send timed out", self)

                next_heartbeat_at += self._publisher.send_timeout
                await asyncio.sleep(next_heartbeat_at - time.monotonic())
            except asyncio.CancelledError:
                _logger.debug("%s publisher task cancelled", self)
                break
            except pyuavcan.transport.ResourceClosedError as ex:
                _logger.debug("%s transport closed, publisher task will exit: %s", self, ex)
                break
            except Exception as ex:
                _logger.exception("%s publisher task exception: %s", self, ex)
        try:
            self._publisher.close()
        except pyuavcan.transport.TransportError:
            pass

    def _call_pre_heartbeat_handlers(self) -> None:
        pyuavcan.util.broadcast(self._pre_heartbeat_handlers)()

    async def _handle_received_heartbeat(self, msg: Heartbeat, metadata: pyuavcan.transport.TransferFrom) -> None:
        local_node_id = self._presentation.transport.local_node_id
        remote_node_id = metadata.source_node_id
        if local_node_id is not None and remote_node_id is not None and local_node_id == remote_node_id:
            _logger.warning(
                "NODE-ID CONFLICT: There is another node on the network that uses the same node-ID %d. "
                "Its latest heartbeat is %s with transfer metadata %s",
                remote_node_id,
                msg,
                metadata,
            )

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, heartbeat=self.make_message(), publisher=self._publisher)
