# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import dataclasses
import pyuavcan.transport
from ._base import Deduplicator


class CyclicDeduplicator(Deduplicator):
    def __init__(self, transfer_id_modulo: int) -> None:
        self._tid_modulo = int(transfer_id_modulo)
        assert self._tid_modulo > 0
        self._remote_states: typing.List[typing.Optional[_RemoteState]] = []

    def should_accept_transfer(
        self,
        *,
        iface_id: int,
        transfer_id_timeout: float,
        timestamp: pyuavcan.transport.Timestamp,
        source_node_id: typing.Optional[int],
        transfer_id: int,
    ) -> bool:
        if source_node_id is None:
            # Anonymous transfers are fully stateless, so always accepted.
            # This may lead to duplications and reordering but this is a design limitation.
            return True

        # If a similar architecture is used on an embedded system, this normally would be a static array.
        if len(self._remote_states) <= source_node_id:
            self._remote_states += [None] * (source_node_id - len(self._remote_states) + 1)
            assert len(self._remote_states) == source_node_id + 1

        if self._remote_states[source_node_id] is None:
            # First transfer from this node, create new state and accept unconditionally.
            self._remote_states[source_node_id] = _RemoteState(iface_id=iface_id, last_timestamp=timestamp)
            return True

        # We have seen transfers from this node before, so we need to perform actual deduplication.
        state = self._remote_states[source_node_id]
        assert state is not None

        # If the current interface was seen working recently, reject traffic from other interfaces.
        # Note that the time delta may be negative due to timestamping variations and inner latency variations.
        time_delta = timestamp.monotonic - state.last_timestamp.monotonic
        iface_switch_allowed = time_delta > transfer_id_timeout
        if not iface_switch_allowed and state.iface_id != iface_id:
            return False

        # TODO: The TID modulo setting is not currently used yet.
        # TODO: It may be utilized later to implement faster iface fallback.

        # Either we're on the same interface or (the interface is new and the current one seems to be down).
        state.iface_id = iface_id
        state.last_timestamp = timestamp
        return True


@dataclasses.dataclass
class _RemoteState:
    iface_id: int
    last_timestamp: pyuavcan.transport.Timestamp
