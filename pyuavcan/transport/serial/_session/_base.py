#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing


class SerialSession:
    def __init__(self, finalizer: typing.Callable[[], None]):
        self._close_finalizer: typing.Optional[typing.Callable[[], None]] = finalizer

    def close(self) -> None:
        fin = self._close_finalizer
        if fin is not None:
            self._close_finalizer = None
            fin()
