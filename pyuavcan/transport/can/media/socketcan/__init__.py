#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio
import pyuavcan.transport.can.media as _media


class SocketCAN(_media.Media):
    def __init__(self,
                 iface_name:            str,
                 max_data_field_length: int,
                 loop:                  typing.Optional[asyncio.AbstractEventLoop] = None) -> None:
        max_data_field_length = int(max_data_field_length)
        if max_data_field_length not in self.VALID_MAX_DATA_FIELD_LENGTH_SET:
            raise ValueError(f'Invalid MTU: {max_data_field_length} not in {self.VALID_MAX_DATA_FIELD_LENGTH_SET}')

        self._iface_name = str(iface_name)
        self._max_data_field_length = int(max_data_field_length)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        super(SocketCAN, self).__init__()

    @property
    def interface_name(self) -> str:
        return self._iface_name

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

    def set_received_frames_handler(self, handler: _media.Media.ReceivedFramesHandler) -> None:
        # TODO: start RX loop task
        raise NotImplementedError

    async def configure_acceptance_filters(self, configuration: typing.Sequence[_media.FilterConfiguration]) -> None:
        raise NotImplementedError

    async def enable_automatic_retransmission(self) -> None:
        raise NotImplementedError

    async def send(self, frames: typing.Iterable[_media.DataFrame]) -> None:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> typing.Iterable[_media.TimestampedDataFrame]:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError
