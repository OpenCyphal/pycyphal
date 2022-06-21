# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import abc
import typing
import asyncio
import warnings
import pycyphal.util
from pycyphal.transport import Timestamp
from ._frame import Envelope
from ._filter import FilterConfiguration


class Media(abc.ABC):
    """
    CAN hardware abstraction interface.

    It is recognized that the availability of some of the media implementations may be conditional on the type of
    platform (e.g., SocketCAN is Linux-only) and the availability of third-party software (e.g., PySerial may be
    needed for SLCAN). Python packages containing such media implementations shall be always importable.
    """

    ReceivedFramesHandler = typing.Callable[[typing.Sequence[typing.Tuple[Timestamp, Envelope]]], None]
    """
    The frames handler is non-blocking and non-yielding; returns immediately.
    The timestamp is provided individually per frame.
    """

    VALID_MTU_SET = {8, 12, 16, 20, 24, 32, 48, 64}
    """Valid MTU values for Classic CAN and CAN FD."""

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """
        Deprecated.
        """
        warnings.warn("The loop property is deprecated; use asyncio.get_running_loop() instead.", DeprecationWarning)
        return asyncio.get_event_loop()

    @property
    @abc.abstractmethod
    def interface_name(self) -> str:
        """
        The name of the interface on the local system. For example:

        - ``can0`` for SocketCAN;
        - ``/dev/serial/by-id/usb-Zubax_Robotics_Zubax_Babel_28002E0001514D593833302000000000-if00`` for SLCAN;
        - ``COM9`` for SLCAN.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def mtu(self) -> int:
        """
        The value belongs to :attr:`VALID_MTU_SET`.
        Observe that the media interface doesn't care whether we're using CAN FD or CAN 2.0 because the Cyphal
        CAN transport protocol itself doesn't care. The transport simply does not distinguish them.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def number_of_acceptance_filters(self) -> int:
        """
        The number of hardware acceptance filters supported by the underlying CAN controller.
        Some media drivers, such as SocketCAN, may implement acceptance filtering in software instead of hardware.
        The returned value shall be a positive integer. If the hardware does not support filtering at all,
        the media driver shall emulate at least one filter in software.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def start(self, handler: ReceivedFramesHandler, no_automatic_retransmission: bool) -> None:
        """
        Every received frame shall be timestamped. Both monotonic and system timestamps are required.
        There are no timestamping accuracy requirements. An empty set of frames should never be reported.

        The media implementation shall drop all non-data frames (RTR frames, error frames, etc.).

        If the set contains more than one frame, all frames must be ordered by the time of their arrival,
        which also should be reflected in their timestamps; that is, the timestamp of a frame at index N
        generally should not be higher than the timestamp of a frame at index N+1. The timestamp ordering,
        however, is not a strict requirement because it is recognized that due to error variations in the
        timestamping algorithms timestamp values may not be monotonically increasing.

        The implementation should strive to return as many frames per call as possible as long as that
        does not increase the worst case latency.

        The handler shall be invoked on the event loop returned by :attr:`loop`.

        The transport is guaranteed to invoke this method exactly once during (or shortly after) initialization;
        it can be used to perform a lazy start of the receive loop task/thread/whatever.
        It is undefined behavior to invoke this method more than once on the same instance.

        :param handler: Behold my transformation. You are empowered to do as you please.

        :param no_automatic_retransmission: If True, the CAN controller should be configured to abort transmission
            of CAN frames after first error or arbitration loss (time-triggered transmission mode).
            This mode is used by Cyphal to facilitate the PnP node-ID allocation process on the client side.
            Its support is not mandatory but highly recommended to avoid excessive disturbance of the bus
            while PnP allocations are in progress.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def configure_acceptance_filters(self, configuration: typing.Sequence[FilterConfiguration]) -> None:
        """
        This method is invoked whenever the subscription set is changed in order to communicate to the underlying
        CAN controller hardware which CAN frames should be accepted and which ones should be ignored.

        An empty set of configurations means that the transport is not interested in any frames, i.e., all frames
        should be rejected by the controller. That is also the recommended default configuration (ignore all frames
        until explicitly requested otherwise).
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, frames: typing.Iterable[Envelope], monotonic_deadline: float) -> int:
        """
        All passed frames are guaranteed to share the same CAN-ID. This guarantee may enable some optimizations.
        The frames shall be delivered to the bus in the same order. The iterable is guaranteed to be non-empty.

        The method returns when the deadline is reached even if some of the frames could not be transmitted.
        The returned value is the number of frames that have been sent. If the returned number is lower than
        the number of supplied frames, the outer transport logic will register an error, which is then propagated
        upwards all the way to the application level.

        The method should avoid yielding the execution flow; instead, it is recommended to unload the frames
        into an internal transmission queue and return ASAP, as that minimizes the likelihood of inner
        priority inversion. If that approach is used, implementations are advised to keep track of transmission
        deadline on a per-frame basis to meet the timing requirements imposed by the application.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        """
        After the media instance is closed, none of its methods can be used anymore.
        If a method is invoked after close, :class:`pycyphal.transport.ResourceClosedError` should be raised.
        This method is an exception to that rule: if invoked on a closed instance, it shall do nothing.
        """
        raise NotImplementedError

    @staticmethod
    def list_available_interface_names() -> typing.Iterable[str]:
        """
        Returns the list of interface names that can be used with the media class implementing it.
        For example, for the SocketCAN media class it would return the SocketCAN interface names such as "vcan0";
        for SLCAN it would return the list of serial ports.

        Implementations should strive to sort the output so that the interfaces that are most likely to be used
        are listed first -- this helps GUI applications.

        If the media implementation cannot be used on the local platform,
        the method shall return an empty set instead of raising an error.
        This guarantee supports an important use case where the caller would just iterate over all inheritors
        of this Media interface and ask each one to yield the list of available interfaces,
        and then just present that to the user.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(self, repr(self.interface_name), mtu=self.mtu)
