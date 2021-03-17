# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import asyncio
import logging
import dataclasses
from typing import Optional, Set, Any
import pydsdl
import pyuavcan.application
from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier
from uavcan.node.port import List_0_1 as List
from uavcan.node.port import SubjectIDList_0_1 as SubjectIDList
from uavcan.node.port import ServiceIDList_0_1 as ServiceIDList
from uavcan.node.port import SubjectID_1_0 as SubjectID


@dataclasses.dataclass(frozen=True)
class _State:
    pub: Set[int]
    sub: Set[int]
    cln: Set[int]
    srv: Set[int]


class PortListPublisher:
    """
    This class is to be automatically instantiated by :class:`pyuavcan.application.Node`.
    Publishing will be suspended while the local node-ID is anonymous.
    The status is updated every second, publications happen every MAX_PUBLICATION_PERIOD seconds or on change.
    """

    _UPDATE_PERIOD = 1.0
    _MAX_UPDATES_BETWEEN_PUBLICATIONS = int(List.MAX_PUBLICATION_PERIOD / _UPDATE_PERIOD)

    def __init__(self, node: pyuavcan.application.Node) -> None:
        self._node = node
        self._pub: Optional[pyuavcan.presentation.Publisher[List]] = None
        self._updates_since_pub = 0
        self._next_update_at = 0.0
        self._timer: Optional[asyncio.TimerHandle] = None
        self._state = _State(set(), set(), set(), set())

        def start() -> None:
            self._next_update_at = self.node.loop.time() + PortListPublisher._UPDATE_PERIOD
            self._timer = self.node.loop.call_at(self._next_update_at, self._update)

        def close() -> None:
            if self._pub is not None:
                self._pub.close()
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None

        self.node.add_lifetime_hooks(start, close)

    @property
    def node(self) -> pyuavcan.application.Node:
        return self._node

    def _get_publisher(self) -> Optional[pyuavcan.presentation.Publisher[List]]:
        if self._pub is None:
            try:
                self._pub = self.node.make_publisher(List)
                self._pub.priority = pyuavcan.transport.Priority.OPTIONAL
            except Exception as ex:  # pragma: no cover
                _logger.exception("%r: Could not initialize the publisher: %s", self, ex)
            else:
                _logger.debug("%r: Publisher initialized: %r", self, self._pub)
        return self._pub

    def _update(self) -> None:
        self._updates_since_pub += 1
        self._next_update_at += PortListPublisher._UPDATE_PERIOD
        self._timer = self.node.loop.call_at(self._next_update_at, self._update)

        if self.node.id is None:
            return
        publisher = self._get_publisher()
        if publisher is None:
            return

        trans = self.node.presentation.transport
        input_ds = [x.specifier.data_specifier for x in trans.input_sessions]
        srv_in_ds = [x for x in input_ds if isinstance(x, ServiceDataSpecifier)]
        state = _State(
            pub={
                x.specifier.data_specifier.subject_id
                for x in trans.output_sessions
                if isinstance(x.specifier.data_specifier, MessageDataSpecifier)
            },
            sub={x.subject_id for x in input_ds if isinstance(x, MessageDataSpecifier)},
            cln={x.service_id for x in srv_in_ds if x.role == ServiceDataSpecifier.Role.RESPONSE},
            srv={x.service_id for x in srv_in_ds if x.role == ServiceDataSpecifier.Role.REQUEST},
        )

        state_changed = state != self._state
        time_expired = self._updates_since_pub >= PortListPublisher._MAX_UPDATES_BETWEEN_PUBLICATIONS
        if state_changed or time_expired:
            _logger.debug("%r: Publishing: state_changed=%r, state=%r", self, state_changed, state)
            self._state = state
            self._updates_since_pub = 0  # Should we handle ResourceClosedError here?
            publisher.publish_soon(_make_port_list(self._state, trans.capture_active))

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, self.node)


_logger = logging.getLogger(__name__)


def _make_port_list(state: _State, packet_capture_mode: bool) -> List:
    from uavcan.primitive import Empty_1_0 as Empty

    return List(
        publishers=_make_subject_id_list(state.pub),
        subscribers=_make_subject_id_list(state.sub) if not packet_capture_mode else SubjectIDList(total=Empty()),
        clients=_make_service_id_list(state.cln),
        servers=_make_service_id_list(state.srv),
    )


def _make_subject_id_list(ports: Set[int]) -> SubjectIDList:
    sparse_list_type = pyuavcan.dsdl.get_model(SubjectIDList)["sparse_list"].data_type
    assert isinstance(sparse_list_type, pydsdl.ArrayType)

    if len(ports) <= sparse_list_type.capacity:
        return SubjectIDList(sparse_list=[SubjectID(x) for x in sorted(ports)])

    out = SubjectIDList()
    assert out.mask is not None
    _populate_mask(ports, out.mask)
    return out


def _make_service_id_list(ports: Set[int]) -> ServiceIDList:
    out = ServiceIDList()
    _populate_mask(ports, out.mask)
    return out


def _populate_mask(ports: Set[int], output: Any) -> None:
    for idx in range(len(output)):  # pylint: disable=consider-using-enumerate
        output[idx] = idx in ports


def _unittest_make_port_list() -> None:
    state = _State(
        pub={1, 8191, 0},
        sub=set(range(257)),
        cln=set(),
        srv=set(range(512)),
    )

    msg = _make_port_list(state, False)

    assert msg.publishers.sparse_list is not None
    pubs = [x.value for x in msg.publishers.sparse_list]
    assert pubs == [0, 1, 8191]  # Sorted!

    assert msg.subscribers.mask is not None
    assert msg.subscribers.mask.sum() == 257
    for idx in range(SubjectIDList.CAPACITY):
        assert msg.subscribers.mask[idx] == (idx < 257)

    assert msg.clients.mask.sum() == 0
    assert msg.servers.mask.sum() == 512


def _unittest_populate_mask() -> None:
    srv = SubjectIDList()
    mask = srv.mask
    assert mask is not None
    _populate_mask({1, 2, 8191}, mask)
    for idx in range(SubjectIDList.CAPACITY):
        assert mask[idx] == (idx in {1, 2, 8191})
