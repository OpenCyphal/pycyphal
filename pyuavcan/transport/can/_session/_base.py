#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import logging


Finalizer = typing.Callable[[], None]


_logger = logging.getLogger(__name__)


class Session:
    def __init__(self, finalizer: Finalizer):
        self._finalizer = finalizer

    def __del__(self) -> None:
        try:
            self._finalizer()
        except Exception as ex:
            _logger.info(f'Finalizer for {self!r} has failed: {ex}', exc_info=True)
