#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan


class UDPInputSession(pyuavcan.transport.InputSession):
    def __init__(self) -> None:
        raise NotImplementedError

    async def receive_until(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        raise NotImplementedError

    @property
    def transfer_id_timeout(self) -> float:
        raise NotImplementedError

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        raise NotImplementedError

    @property
    def specifier(self) -> pyuavcan.transport.InputSessionSpecifier:
        raise NotImplementedError

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        raise NotImplementedError

    def sample_statistics(self) -> pyuavcan.transport.SessionStatistics:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError
