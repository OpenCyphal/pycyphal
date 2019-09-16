#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan


class UDPInputSession(pyuavcan.transport.InputSession):
    def __init__(self):
        pass

    async def receive_until(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        pass

    @property
    def transfer_id_timeout(self) -> float:
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
