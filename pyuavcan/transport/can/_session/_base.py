# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import logging
import pyuavcan.transport


SessionFinalizer = typing.Callable[[], None]


_logger = logging.getLogger(__name__)


class CANSession:
    def __init__(self, finalizer: SessionFinalizer):
        self._close_finalizer: typing.Optional[SessionFinalizer] = finalizer

    def _raise_if_closed(self) -> None:
        if self._close_finalizer is None:
            raise pyuavcan.transport.ResourceClosedError(
                f"The requested action cannot be performed because the session object {self} is closed"
            )

    def close(self) -> None:
        fin = self._close_finalizer
        if fin is not None:
            self._close_finalizer = None
            fin()
