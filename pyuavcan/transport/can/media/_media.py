#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
from . import _frame


class Media(abc.ABC):
    VALID_MTU = {8, 12, 16, 20, 24, 32, 48, 64}

    @property
    @abc.abstractmethod
    def mtu(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, frame: _frame.Frame) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def receive(self, monotonic_deadline: float) -> _frame.ReceivedFrame:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError
