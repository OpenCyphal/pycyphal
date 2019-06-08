#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import time
import typing
import pyuavcan
import uavcan.node


NodeInfo = uavcan.node.GetInfo_0_1.Response


class Node:
    def __init__(self,
                 transport: pyuavcan.transport.Transport,
                 info:      NodeInfo):
        self._instantiated_at = pyuavcan.transport.Timestamp.now()
        self._presentation = pyuavcan.presentation.Presentation(transport)
        self._info = info
        self._closed = False

    @property
    def presentation(self) -> pyuavcan.presentation.Presentation:
        return self._presentation

    @property
    def info(self) -> NodeInfo:
        return self._info

    @property
    def uptime(self) -> float:
        out = time.monotonic() - float(self._instantiated_at.monotonic)
        assert out >= 0
        return out

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._presentation.transport.local_node_id

    async def set_local_node_id(self, node_id: int) -> None:
        await self._presentation.transport.set_local_node_id(node_id)

    async def close(self) -> None:
        self._closed = True
        await self._presentation.close()
