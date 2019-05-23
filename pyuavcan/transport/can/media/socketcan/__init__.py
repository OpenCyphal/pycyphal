#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan.transport.can.media as _media


class SocketCAN(_media.Media):
    def __init__(self, max_data_field_length: int = 64) -> None:
        max_data_field_length = int(max_data_field_length)
        if max_data_field_length not in self.VALID_MAX_DATA_FIELD_LENGTH_SET:
            raise ValueError(f'Invalid MTU: {max_data_field_length} not in {self.VALID_MAX_DATA_FIELD_LENGTH_SET}')

        self._max_data_field_length = int(max_data_field_length)
        super(SocketCAN, self).__init__()

    @property
    def max_data_field_length(self) -> int:
        return self._max_data_field_length

    @property
    def number_of_acceptance_filters(self) -> int:
        """
        https://www.kernel.org/doc/Documentation/networking/can.txt
        https://github.com/torvalds/linux/blob/9c7db5004280767566e91a33445bf93aa479ef02/net/can/af_can.c#L327-L348
        https://github.com/torvalds/linux/blob/54dee406374ce8adb352c48e175176247cb8db7c/include/uapi/linux/can.h#L200
        """
        return 512

    async def configure_acceptance_filters(self, configuration: typing.Sequence[_media.FilterConfiguration]) -> None:
        raise NotImplementedError

    async def enable_automatic_retransmission(self) -> None:
        raise NotImplementedError

    async def send(self, frames: typing.Iterable[_media.Frame]) -> None:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> _media.TimestampedFrame:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError
