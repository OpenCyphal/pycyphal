#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import enum
import time
import typing
import logging
import asyncio
import pyuavcan
from uavcan.node import Heartbeat_1_0 as Heartbeat


DEFAULT_PRIORITY = pyuavcan.transport.Priority.SLOW


class Health(enum.IntEnum):
    NOMINAL  = Heartbeat.HEALTH_NOMINAL
    ADVISORY = Heartbeat.HEALTH_ADVISORY
    CAUTION  = Heartbeat.HEALTH_CAUTION
    WARNING  = Heartbeat.HEALTH_WARNING


class Mode(enum.IntEnum):
    OPERATIONAL     = Heartbeat.MODE_OPERATIONAL
    INITIALIZATION  = Heartbeat.MODE_INITIALIZATION
    MAINTENANCE     = Heartbeat.MODE_MAINTENANCE
    SOFTWARE_UPDATE = Heartbeat.MODE_SOFTWARE_UPDATE
    OFFLINE         = Heartbeat.MODE_OFFLINE


VENDOR_SPECIFIC_STATUS_CODE_MASK = \
    2 ** list(pyuavcan.dsdl.get_model(Heartbeat)['vendor_specific_status_code'].data_type.bit_length_set)[0] - 1


_logger = logging.getLogger(__name__)


class HeartbeatPublisher:
    """
    This class manages periodic publication of the node heartbeat message. The default states are as follows:
        - health NOMINAL
        - mode INITIALIZATION
        - vendor-specific status code is zero
        - period MAX_PUBLICATION_PERIOD
        - priority DEFAULT_PRIORITY
    """

    def __init__(self, presentation: pyuavcan.presentation.Presentation):
        self._presentation = presentation
        self._instantiated_at = time.monotonic()
        self._health = Health.NOMINAL
        self._mode = Mode.INITIALIZATION
        self._vendor_specific_status_code = 0
        self._publisher = self._presentation.make_publisher_with_fixed_subject_id(Heartbeat)
        self._pre_heartbeat_handlers: typing.List[typing.Callable[[], None]] = []
        self._period = float(Heartbeat.MAX_PUBLICATION_PERIOD)
        self._closed = False
        self._task = presentation.transport.loop.create_task(self._task_function())

    @property
    def uptime(self) -> float:
        out = time.monotonic() - self._instantiated_at
        assert out >= 0
        return out

    @property
    def health(self) -> Health:
        return self._health

    @health.setter
    def health(self, value: typing.Union[Health, int]) -> None:
        self._health = Health(value)

    @property
    def mode(self) -> Mode:
        return self._mode

    @mode.setter
    def mode(self, value: typing.Union[Mode, int]) -> None:
        self._mode = Mode(value)

    @property
    def vendor_specific_status_code(self) -> int:
        return self._vendor_specific_status_code

    @vendor_specific_status_code.setter
    def vendor_specific_status_code(self, value: int) -> None:
        value = int(value)
        if 0 <= value <= VENDOR_SPECIFIC_STATUS_CODE_MASK:
            self._vendor_specific_status_code = value
        else:
            raise ValueError(f'Invalid vendor-specific status code: {value}')

    @property
    def period(self) -> float:
        return self._period

    @period.setter
    def period(self, value: float) -> None:
        value = float(value)
        if 0 < value <= Heartbeat.MAX_PUBLICATION_PERIOD:
            self._period = value
        else:
            raise ValueError(f'Invalid heartbeat period: {value}')

    @property
    def priority(self) -> pyuavcan.transport.Priority:
        return self._publisher.priority

    @priority.setter
    def priority(self, value: pyuavcan.transport.Priority) -> None:
        self._publisher.priority = pyuavcan.transport.Priority(value)

    @property
    def publisher(self) -> pyuavcan.presentation.Publisher[Heartbeat]:
        """
        Provides access to the underlying presentation layer publisher instance.
        """
        return self._publisher

    def add_pre_heartbeat_handler(self, handler: typing.Callable[[], None]) -> None:
        """
        Adds a new handler to be invoked immediately before a heartbeat message is published.
        The handler can be used to synchronize the heartbeat message data (health, mode, vendor-specific status code)
        with external states. Observe that the handler will be invoked even if the heartbeat is not to be published,
        e.g., if the node is anonymous (does not have a node ID). If the handler throws an exception, it will be
        suppressed and logged. Note that the handler is to be not a coroutine but a regular function.
        This is also a good method of scheduling periodic status checks on the node.
        """
        self._pre_heartbeat_handlers.append(handler)

    def make_message(self) -> Heartbeat:
        """
        Constructs the current heartbeat message.
        """
        return Heartbeat(uptime=int(self.uptime),  # must floor
                         health=self.health,
                         mode=self.mode,
                         vendor_specific_status_code=self.vendor_specific_status_code)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._publisher.close()
            self._task.cancel()

    async def _task_function(self) -> None:
        next_heartbeat_at = time.monotonic()
        while not self._closed:
            try:
                next_heartbeat_at += self._period
                await asyncio.sleep(next_heartbeat_at - time.monotonic())
                self._call_pre_heartbeat_handlers()
                if self._presentation.transport.local_node_id is not None:
                    await self._publisher.publish(self.make_message())
            except asyncio.CancelledError:
                _logger.debug('%s publisher task cancelled', self)
                break
            except pyuavcan.transport.ResourceClosedError as ex:
                _logger.info('%s transport closed, publisher task will exit: %s', self, ex)
                break
            except Exception as ex:
                _logger.exception('%s publisher task exception: %s', self, ex)

        try:
            self._publisher.close()
        except pyuavcan.transport.TransportError:
            pass

    def _call_pre_heartbeat_handlers(self) -> None:
        for fun in self._pre_heartbeat_handlers:
            try:
                fun()
            except Exception as ex:
                _logger.exception('%s got an unhandled exception from the pre-heartbeat handler: %s', self, ex)

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, heartbeat=self.make_message(), publisher=self._publisher)
