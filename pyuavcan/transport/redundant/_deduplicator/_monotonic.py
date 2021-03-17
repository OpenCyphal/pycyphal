# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import dataclasses
import pyuavcan.transport
from ._base import Deduplicator


class MonotonicDeduplicator(Deduplicator):
    def __init__(self) -> None:
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
        del iface_id  # Not used in monotonic deduplicator.
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
            self._remote_states[source_node_id] = _RemoteState(last_transfer_id=transfer_id, last_timestamp=timestamp)
            return True

        # We have seen transfers from this node before, so we need to perform actual deduplication.
        state = self._remote_states[source_node_id]
        assert state is not None

        # If we have seen transfers with higher TID values recently, reject this one as duplicate.
        tid_timeout = (timestamp.monotonic - state.last_timestamp.monotonic) > transfer_id_timeout
        if not tid_timeout and transfer_id <= state.last_transfer_id:
            return False

        # Otherwise, this is either a new transfer or a TID timeout condition has occurred.
        state.last_transfer_id = transfer_id
        state.last_timestamp = timestamp
        return True


@dataclasses.dataclass
class _RemoteState:
    last_transfer_id: int
    last_timestamp: pyuavcan.transport.Timestamp
