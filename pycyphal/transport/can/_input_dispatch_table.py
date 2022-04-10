# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
from pycyphal.transport import MessageDataSpecifier, ServiceDataSpecifier, InputSessionSpecifier
from ._session import CANInputSession
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
        self._dict: typing.Dict[InputSessionSpecifier, CANInputSession] = {}

    @property
    def items(self) -> typing.Iterable[CANInputSession]:
        return self._dict.values()

    def add(self, session: CANInputSession) -> None:
        """
        This method is used only when a new input session is created; performance is not a priority.
        """
        key = session.specifier
        self._table[self._compute_index(key)] = session
        self._dict[key] = session

    def get(self, specifier: InputSessionSpecifier) -> typing.Optional[CANInputSession]:
        """
        Constant-time lookup. Invoked for every received frame.
        """
        return self._table[self._compute_index(specifier)]

    def remove(self, specifier: InputSessionSpecifier) -> None:
        """
        This method is used only when an input session is destroyed; performance is not a priority.
        """
        self._table[self._compute_index(specifier)] = None
        del self._dict[specifier]

    @staticmethod
    def _compute_index(specifier: InputSessionSpecifier) -> int:
        ds, nid = specifier.data_specifier, specifier.remote_node_id
        if isinstance(ds, MessageDataSpecifier):
            dim1 = ds.subject_id
        elif isinstance(ds, ServiceDataSpecifier):
            if ds.role == ds.Role.REQUEST:
                dim1 = ds.service_id + InputDispatchTable._NUM_SUBJECTS
            elif ds.role == ds.Role.RESPONSE:
                dim1 = ds.service_id + InputDispatchTable._NUM_SUBJECTS + InputDispatchTable._NUM_SERVICES
            else:
                assert False
        else:
            assert False

        dim2_cardinality = InputDispatchTable._NUM_NODE_IDS + 1
        dim2 = nid if nid is not None else InputDispatchTable._NUM_NODE_IDS

        point = dim1 * dim2_cardinality + dim2

        assert 0 <= point < InputDispatchTable._TABLE_SIZE
        return point


def _unittest_input_dispatch_table() -> None:
    from pytest import raises
    from pycyphal.transport import PayloadMetadata

    t = InputDispatchTable()
    assert len(list(t.items)) == 0
    assert t.get(InputSessionSpecifier(MessageDataSpecifier(1234), None)) is None
    with raises(LookupError):
        t.remove(InputSessionSpecifier(MessageDataSpecifier(1234), 123))

    a = CANInputSession(
        InputSessionSpecifier(MessageDataSpecifier(1234), None),
        PayloadMetadata(456),
        lambda: None,
    )
    t.add(a)
    t.add(a)
    assert list(t.items) == [a]
    assert t.get(InputSessionSpecifier(MessageDataSpecifier(1234), None)) == a
    t.remove(InputSessionSpecifier(MessageDataSpecifier(1234), None))
    assert len(list(t.items)) == 0


def _unittest_slow_input_dispatch_table_index() -> None:
    table_size = InputDispatchTable._TABLE_SIZE  # pylint: disable=protected-access
    values: typing.Set[int] = set()
    for node_id in (*range(InputDispatchTable._NUM_NODE_IDS), None):  # pylint: disable=protected-access
        for subj in range(InputDispatchTable._NUM_SUBJECTS):  # pylint: disable=protected-access
            out = InputDispatchTable._compute_index(  # pylint: disable=protected-access
                InputSessionSpecifier(MessageDataSpecifier(subj), node_id)
            )
            assert out not in values
            values.add(out)
            assert out < table_size

        for serv in range(InputDispatchTable._NUM_SERVICES):  # pylint: disable=protected-access
            for role in ServiceDataSpecifier.Role:
                out = InputDispatchTable._compute_index(  # pylint: disable=protected-access
                    InputSessionSpecifier(ServiceDataSpecifier(serv, role), node_id)
                )
                assert out not in values
                values.add(out)
                assert out < table_size

    assert len(values) == table_size
