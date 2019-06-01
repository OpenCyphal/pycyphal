#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import logging
import pyuavcan.transport


Finalizer = typing.Callable[[], None]


_logger = logging.getLogger(__name__)


class CANSession:
    def __init__(self, finalizer: Finalizer):
        def finalizer_proxy() -> None:
            if not self._closed:
                self._closed = True
                finalizer()

        self._closed = False
        self._finalizer = finalizer_proxy

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(
                f'The requested action cannot be performed because the session object {self} is closed')
