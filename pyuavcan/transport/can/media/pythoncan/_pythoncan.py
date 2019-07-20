#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import logging
import pyuavcan.transport.can.media as _media


_logger = logging.getLogger(__name__)


class PythonCANMedia(_media.Media):
    """
    A media interface adapter for `python-can <https://github.com/hardbyte/python-can>`_.
    This is a stub, the implementation is missing. Please submit patches!
    """

    def __init__(self) -> None:
        raise NotImplementedError

    @property
    def interface_name(self) -> str:
        raise NotImplementedError

    @property
    def mtu(self) -> int:
        raise NotImplementedError

    @property
    def number_of_acceptance_filters(self) -> int:
        raise NotImplementedError

    def set_received_frames_handler(self, handler: _media.Media.ReceivedFramesHandler) -> None:
        raise NotImplementedError

    def configure_acceptance_filters(self, configuration: typing.Sequence[_media.FilterConfiguration]) -> None:
        _logger.warning('%s FIXME: acceptance filter configuration is not yet implemented; please submit patches! '
                        'Requested configuration: %s',
                        self, ', '.join(map(str, configuration)))

    def enable_automatic_retransmission(self) -> None:
        raise NotImplementedError

    async def send_until(self, frames: typing.Iterable[_media.DataFrame], monotonic_deadline: float) -> int:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    @staticmethod
    def list_available_interface_names() -> typing.Iterable[str]:
        raise NotImplementedError
