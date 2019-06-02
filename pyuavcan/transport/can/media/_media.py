#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
from . import _frame, _filter


class Media(abc.ABC):
    # The frames handler is non-blocking and non-yielding; returns immediately.
    ReceivedFramesHandler = typing.Callable[[typing.Iterable[_frame.TimestampedDataFrame]], None]

    VALID_MAX_DATA_FIELD_LENGTH_SET = {8, 12, 16, 20, 24, 32, 48, 64}

    @property
    @abc.abstractmethod
    def max_data_field_length(self) -> int:
        """
        Must belong to VALID_MAX_DATA_FIELD_LENGTH_SET.

        Observe that the media interface doesn't care whether we're using CAN FD or CAN 2.0 because the UAVCAN
        CAN transport protocol itself doesn't care. The transport simply does not distinguish them.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def number_of_acceptance_filters(self) -> int:
        """
        The number of hardware acceptance filters supported by the underlying CAN controller. Some media drivers,
        such as SocketCAN, may implement acceptance filtering in software instead of hardware. In such case, the
        number of available filters may be unlimited (since they're all virtual), so this method should return the
        optimal number of filters which can be used without degrading the performance of the media driver. It is
        safe to err towards a smaller number (this may result in an increased processing load for the library);
        however, it is best to ensure that the underlying controller supports not less than four filters.

        If the underlying CAN protocol implementation does not support acceptance filtering (neither in software
        nor in hardware), its media driver must emulate it in software.

        The returned value not be less than one.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def set_received_frames_handler(self, handler: ReceivedFramesHandler) -> None:
        """
        Every received frame must be timestamped. Both monotonic and system timestamps are required.
        There are no timestamping accuracy requirements. An empty set of frames should never be reported.

        The media implementation must drop all non-data frames (RTR frames, error frames, etc.).

        If the set contains more than one frame, all frames must be ordered by the time of their arrival,
        which also should be reflected in their timestamps; that is, the timestamp of a frame at index N
        generally should not be higher than the timestamp of a frame at index N+1. The timestamp ordering,
        however, is not a strict requirement because it is recognized that due to error variations in the
        timestamping algorithms timestamp values may not be monotonically increasing.

        The implementation should strive to return as many frames per call as possible as long as that
        does not increase the worst case latency.

        The handler shall be invoked on the same event loop.

        The transport is guaranteed to invoke this method at least once during initialization; it can be used
        to perform a lazy start of the receive loop task.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def configure_acceptance_filters(self, configuration: typing.Sequence[_filter.FilterConfiguration]) -> None:
        """
        This method is invoked whenever the subscription set is changed in order to communicate to the underlying
        CAN controller hardware which CAN frames should be picked up and which ones should be ignored.

        An empty set of configurations means that the transport is not interested in any frames, i.e., all frames
        should be rejected by the controller. That is also the recommended default configuration (ignore all frames
        until explicitly requested otherwise).
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def enable_automatic_retransmission(self) -> None:
        """
        By default, automatic retransmission should be disabled to facilitate PnP node ID allocation. This method can
        be invoked at most once to disable it, which is usually done when the local node obtains a node ID.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, frames: typing.Iterable[_frame.DataFrame]) -> None:
        """
        All frames are guaranteed to share the same CAN ID. This guarantee may enable some optimizations.
        The frames MUST be delivered to the bus in the same order. The iterable is guaranteed to be non-empty.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        """
        After the media instance is closed, none of its methods can be used anymore. The behavior or methods after
        close() is undefined.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __str__(self) -> str:
        """
        Should print the basic media information.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__str__()
