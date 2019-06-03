#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
from pyuavcan.transport import DataSpecifier, MessageDataSpecifier, ServiceDataSpecifier
from ._session import CANInputSession, PromiscuousCANInput, SelectiveCANInput
from ._identifier import CANID


class InputDispatchTable:
    """
    Time-memory trade-off: the input dispatch table is tens of megabytes large, but the lookup is very fast and O(1).
    This is necessary to ensure scalability for high-load applications such as real-time network monitoring.
    """
    _NUM_SUBJECTS = MessageDataSpecifier.SUBJECT_ID_MASK + 1
    _NUM_SERVICES = ServiceDataSpecifier.SERVICE_ID_MASK + 1
    _NUM_NODE_IDS = CANID.NODE_ID_MASK + 1

    # Services multiplied by two to account for requests and responses.
    # One added to nodes to allow promiscuous inputs which don't care about source node ID.
    _TABLE_SIZE = (_NUM_SUBJECTS + _NUM_SERVICES * 2) * (_NUM_NODE_IDS + 1)

    def __init__(self) -> None:
        # This method of construction is an order of magnitude faster than range-based. It matters here. A lot.
        self._table: typing.List[typing.Optional[CANInputSession]] = [None] * (self._TABLE_SIZE + 1)

        # A parallel dict is necessary for constant-complexity element listing. Traversing the table takes forever.
        self._dict: typing.Dict[typing.Tuple[DataSpecifier, typing.Optional[int]], CANInputSession] = {}

    @property
    def items(self) -> typing.Iterable[CANInputSession]:
        return self._dict.values()

    def add(self, session: CANInputSession) -> None:
        """
        This method is used only when a new input session is created; performance is not a priority.
        """
        key = self._key(session)
        self._table[self._compute_index(*key)] = session
        self._dict[key] = session

    def get(self,
            data_specifier: DataSpecifier,
            source_node_id: typing.Optional[int]) -> typing.Optional[CANInputSession]:
        """
        Constant-time lookup. Invoked for every received frame.
        """
        return self._table[self._compute_index(data_specifier, source_node_id)]

    def remove(self,
               data_specifier: DataSpecifier,
               source_node_id: typing.Optional[int]) -> None:
        """
        This method is used only when an input session is destroyed; performance is not a priority.
        """
        key = data_specifier, source_node_id
        self._table[self._compute_index(*key)] = None
        del self._dict[key]

    @staticmethod
    def _key(session: CANInputSession) -> typing.Tuple[DataSpecifier, typing.Optional[int]]:
        ds = session.metadata.data_specifier
        if isinstance(session, PromiscuousCANInput):
            return ds, None
        elif isinstance(session, SelectiveCANInput):
            return ds, session.source_node_id
        else:
            assert False

    @staticmethod
    def _compute_index(data_specifier: DataSpecifier, source_node_id: typing.Optional[int]) -> int:
        if isinstance(data_specifier, MessageDataSpecifier):
            dim1 = data_specifier.subject_id
        elif isinstance(data_specifier, ServiceDataSpecifier):
            if data_specifier.role == data_specifier.Role.CLIENT:
                dim1 = data_specifier.service_id + InputDispatchTable._NUM_SUBJECTS
            elif data_specifier.role == data_specifier.Role.SERVER:
                dim1 = data_specifier.service_id + InputDispatchTable._NUM_SUBJECTS + InputDispatchTable._NUM_SERVICES
            else:
                assert False
        else:
            assert False

        dim2_cardinality = InputDispatchTable._NUM_NODE_IDS + 1
        dim2 = source_node_id if source_node_id is not None else InputDispatchTable._NUM_NODE_IDS

        point = dim1 * dim2_cardinality + dim2

        assert 0 <= point < InputDispatchTable._TABLE_SIZE
        return point


def _unittest_input_dispatch_table() -> None:
    from pytest import raises
    from pyuavcan.transport import SessionMetadata, PayloadMetadata
    from ._session import PromiscuousCANInput

    t = InputDispatchTable()
    assert len(list(t.items)) == 0
    assert t.get(MessageDataSpecifier(1234), None) is None
    with raises(LookupError):
        t.remove(MessageDataSpecifier(1234), 123)

    async def finalizer() -> None:
        pass    # pragma: no cover

    a = PromiscuousCANInput(SessionMetadata(MessageDataSpecifier(1234), PayloadMetadata(456, 789)), None, finalizer)
    t.add(a)
    t.add(a)
    assert list(t.items) == [a]
    assert t.get(MessageDataSpecifier(1234), None) == a
    t.remove(MessageDataSpecifier(1234), None)
    assert len(list(t.items)) == 0


# noinspection PyProtectedMember
def _unittest_slow_input_dispatch_table_index() -> None:
    values: typing.Set[int] = set()
    for node_id in (*range(InputDispatchTable._NUM_NODE_IDS), None):
        for subj in range(InputDispatchTable._NUM_SUBJECTS):
            out = InputDispatchTable._compute_index(MessageDataSpecifier(subj), node_id)
            assert out not in values
            values.add(out)
            assert out < InputDispatchTable._TABLE_SIZE

        for serv in range(InputDispatchTable._NUM_SERVICES):
            for role in ServiceDataSpecifier.Role:
                out = InputDispatchTable._compute_index(ServiceDataSpecifier(serv, role), node_id)
                assert out not in values
                values.add(out)
                assert out < InputDispatchTable._TABLE_SIZE

    assert len(values) == InputDispatchTable._TABLE_SIZE
