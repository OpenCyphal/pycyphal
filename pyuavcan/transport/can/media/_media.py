#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import pyuavcan.util
from ._frame import DataFrame, TimestampedDataFrame
from ._filter import FilterConfiguration


class Media(abc.ABC):
    """
    It is recognized that the availability of some of the media implementations may be conditional on the type of
    platform (e.g., SocketCAN is Linux-only) and the availability of third-party software (e.g., PySerial may be
    needed for SLCAN). The media protocol requires that the Python packages containing such media implementations
    must be always importable. Whether all necessary dependencies are satisfied and requirements are met should be
    checked during class instantiation, not at the time of the import.
    """

    #: The frames handler is non-blocking and non-yielding; returns immediately.
    ReceivedFramesHandler = typing.Callable[[typing.Iterable[TimestampedDataFrame]], None]

    #: Valid MTU values for CAN 2.0 and CAN FD.
    VALID_MTU_SET = {8, 12, 16, 20, 24, 32, 48, 64}

    @property
    @abc.abstractmethod
    def interface_name(self) -> str:
        """
        The name of the interface on the local system. For example:

        - ``can0`` for SocketCAN
        - ``/dev/serial/by-id/usb-Zubax_Robotics_Zubax_Babel_28002E0001514D593833302000000000-if00`` for SLCAN
        - ``COM9`` for SLCAN
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def mtu(self) -> int:
        """
        Must belong to :attr:`VALID_MTU_SET`.
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
    def configure_acceptance_filters(self, configuration: typing.Sequence[FilterConfiguration]) -> None:
        """
        This method is invoked whenever the subscription set is changed in order to communicate to the underlying
        CAN controller hardware which CAN frames should be picked up and which ones should be ignored.

        An empty set of configurations means that the transport is not interested in any frames, i.e., all frames
        should be rejected by the controller. That is also the recommended default configuration (ignore all frames
        until explicitly requested otherwise).
        """
        raise NotImplementedError

    @abc.abstractmethod
    def enable_automatic_retransmission(self) -> None:
        """
        By default, automatic retransmission should be disabled to facilitate PnP node ID allocation. This method can
        be invoked at most once to enable it, which is usually done when the local node obtains a node ID.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, frames: typing.Iterable[DataFrame]) -> None:
        """
        All frames are guaranteed to share the same CAN ID. This guarantee may enable some optimizations.
        The frames MUST be delivered to the bus in the same order. The iterable is guaranteed to be non-empty.
        The method should avoid yielding the execution flow; instead, it is recommended to unload the frames
        into an internal transmission queue and return ASAP, as that minimizes the likelihood of inner
        priority inversion.
        The amount of time allocated on execution of this method is limited per the transport configuration.
        If the function does not complete in time, it will be cancelled and the transport will report an error.
        This allows the transport to detect when the interface is stuck in the bus-off state.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        """
        After the media instance is closed, none of its methods can be used anymore. The behavior or methods after
        :meth:`close` is undefined.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        """
        Prints the basic media information. Can be overridden if there is more relevant info to display.
        """
        return pyuavcan.util.repr_attributes(self,
                                             interface_name=self.interface_name,
                                             mtu=self.mtu)

    @staticmethod
    def list_available_interface_names() -> typing.Iterable[str]:
        """
        This static method returns the list of interface names that can be used with the media class implementing it.
        For example, for the SocketCAN media class it would return the SocketCAN interface names such as "vcan0";
        for SLCAN it would return the list of serial ports. Implementations should strive to sort the output so that
        the interfaces that are most likely to be used are listed first -- this helps GUI applications.
        If the media implementation cannot be used on the local platform (e.g., if this method is invoked on the
        SocketCAN media class on Windows), the method must return an empty set instead of raising an error.
        This guarantee supports an important use case where the caller would just iterate over all inheritors
        of this Media interface and ask each one to yield the list of available interfaces, and then just present
        that to the user.
        """
        raise NotImplementedError
