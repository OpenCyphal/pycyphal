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
    VALID_MTU = {8, 12, 16, 20, 24, 32, 48, 64}

    @property
    @abc.abstractmethod
    def mtu(self) -> int:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def number_of_acceptance_filters(self) -> int:
        """
        The number of hardware acceptance filters supported by the underlying CAN controller. Some media drivers,
        such as SocketCAN, may implement acceptance filtering in software instead of hardware. In such case, the
        number of available filters may be unlimited (since they're all virtual), so this method should return the
        optimal number of filters which can be used without degrading the performance of the media driver. It is
        safe to err towards a smaller number (this may result in an increased processing load for the library).
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def configure_acceptance_filters(self, configuration: typing.Sequence[_filter.FilterConfiguration]) -> None:
        """
        The initial configuration is unspecified (can be arbitrary). The transport is guaranteed to invoke this method
        during the initialization. This method may also be invoked whenever the subscription set is changed in order
        to communicate to the underlying CAN controller hardware which CAN frames should be picked up and which ones
        should be ignored. An empty set of configurations means that the transport is not interested in any frames,
        i.e., all frames should be rejected by the controller.
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
    async def send(self, frame: _frame.Frame) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[_frame.ReceivedFrame]:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError
