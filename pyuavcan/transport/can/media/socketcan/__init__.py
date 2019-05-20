#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from .. import _media, _frame


class SocketCAN(_media.Media):
    def __init__(self, mtu: int = max(_media.Media.VALID_MTU)) -> None:
        mtu = int(mtu)
        if mtu not in self.VALID_MTU:
            raise ValueError(f'Invalid MTU: {mtu} not in {self.VALID_MTU}')

        self._mtu = int(mtu)
        super(SocketCAN, self).__init__()

    @property
    def mtu(self) -> int:
        return self._mtu

    async def send(self, frame: _frame.Frame) -> None:
        raise NotImplementedError

    async def receive(self, monotonic_deadline: float) -> _frame.ReceivedFrame:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError
