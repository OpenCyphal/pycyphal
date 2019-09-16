#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan


class UDPOutputSession(pyuavcan.transport.OutputSession):
    def __init__(self):
        pass

    async def send_until(self, transfer: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
        pass

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        pass

    def disable_feedback(self) -> None:
        pass

    @property
    def specifier(self) -> pyuavcan.transport.SessionSpecifier:
        pass

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        pass

    def sample_statistics(self) -> pyuavcan.transport.SessionStatistics:
        pass

    def close(self) -> None:
        pass
