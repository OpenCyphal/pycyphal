# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

"""
Plug-and-play node-ID allocation logic. See the class documentation for usage info.

Remember that a network that contains static nodes alongside PnP nodes may encounter node-ID conflicts
when a static node appears online after its node-ID is already granted to a PnP node.
To avoid this, the Specification recommends that PnP nodes and static nodes are not to be mixed on the same network
(excepting the allocators themselves -- they are always static, naturally).
"""

from __future__ import annotations
import abc
from typing import Optional, Union, Any
import random
import asyncio
import pathlib
import logging
import sqlite3
from uavcan.pnp import NodeIDAllocationData_1_0 as NodeIDAllocationData_1
from uavcan.pnp import NodeIDAllocationData_2_0 as NodeIDAllocationData_2
from uavcan.node import ID_1_0 as ID
import pyuavcan
import pyuavcan.application


_PSEUDO_UNIQUE_ID_MASK = (
    2 ** list(pyuavcan.dsdl.get_model(NodeIDAllocationData_1)["unique_id_hash"].data_type.bit_length_set)[0] - 1
)

_NODE_ID_MASK = 2 ** max(pyuavcan.dsdl.get_model(ID)["value"].data_type.bit_length_set) - 1

_UNIQUE_ID_SIZE_BYTES = pyuavcan.application.NodeInfo().unique_id.size

_NUM_RESERVED_TOP_NODE_IDS = 2

_DB_DEFAULT_LOCATION = ":memory:"
_DB_TIMEOUT = 1.0


_logger = logging.getLogger(__name__)


class Allocatee:
    """
    Plug-and-play node-ID protocol client.

    This class represents a node that requires an allocated node-ID.
    Once started, the client will keep issuing node-ID allocation requests until either a node-ID is granted
    or until the node-ID of the specified transport instance ceases to be anonymous
    (that could happen if the transport is re-configured by the application locally).
    The status (whether the allocation is finished or still in progress) is to be queried periodically
    via :meth:`get_result`.

    Uses v1 allocation messages if the transport MTU is small (like if the transport is Classic CAN).
    Switches between v1 and v2 as necessary on the fly if the transport is reconfigured at runtime.

    Unlike other application-layer function implementations, this class takes a transport instance directly
    instead of a node because it is expected to be used before the node object is constructed.
    """

    DEFAULT_PRIORITY = pyuavcan.transport.Priority.SLOW

    _MTU_THRESHOLD = max(pyuavcan.dsdl.get_model(NodeIDAllocationData_2).bit_length_set) // 8

    def __init__(
        self,
        transport_or_presentation: Union[pyuavcan.transport.Transport, pyuavcan.presentation.Presentation],
        local_unique_id: bytes,
        preferred_node_id: Optional[int] = None,
    ):
        """
        :param transport_or_presentation:
            The transport to run the allocation client on, or the presentation instance constructed on it.
            If the transport is not anonymous (i.e., a node-ID is already set),
            the allocatee will simply return the existing node-ID and do nothing.

        :param local_unique_id:
            The 128-bit globally unique-ID of the local node; the same value is also contained
            in ``uavcan.node.GetInfo.Response``.
            Beware that random generation of the unique-ID at every launch is a bad idea because it will
            exhaust the allocation table quickly.
            Refer to the Specification for details.

        :param preferred_node_id:
            If the application prefers to obtain a particular node-ID, it can specify it here.
            If provided, the PnP allocator will try to find a node-ID that is close to the stated preference.
            If not provided, the PnP allocator will pick a node-ID at its own discretion.
        """
        if isinstance(transport_or_presentation, pyuavcan.transport.Transport):
            self._transport = transport_or_presentation
            self._presentation = pyuavcan.presentation.Presentation(self._transport)
        elif isinstance(transport_or_presentation, pyuavcan.presentation.Presentation):
            self._transport = transport_or_presentation.transport
            self._presentation = transport_or_presentation
        else:  # pragma: no cover
            raise TypeError(f"Expected transport or presentation controller, found {type(transport_or_presentation)}")

        self._local_unique_id = local_unique_id
        self._preferred_node_id = int(preferred_node_id if preferred_node_id is not None else _NODE_ID_MASK)
        if not isinstance(self._local_unique_id, bytes) or len(self._local_unique_id) != _UNIQUE_ID_SIZE_BYTES:
            raise ValueError(f"Invalid unique-ID: {self._local_unique_id!r}")
        if not (0 <= self._preferred_node_id <= _NODE_ID_MASK):
            raise ValueError(f"Invalid preferred node-ID: {self._preferred_node_id}")

        self._result: Optional[int] = None
        self._sub_1 = self._presentation.make_subscriber_with_fixed_subject_id(NodeIDAllocationData_1)
        self._sub_2 = self._presentation.make_subscriber_with_fixed_subject_id(NodeIDAllocationData_2)
        self._pub: Union[
            None,
            pyuavcan.presentation.Publisher[NodeIDAllocationData_1],
            pyuavcan.presentation.Publisher[NodeIDAllocationData_2],
        ] = None
        self._timer: Optional[asyncio.TimerHandle] = None

        self._sub_1.receive_in_background(self._on_response)
        self._sub_2.receive_in_background(self._on_response)
        self._restart_timer()

    @property
    def presentation(self) -> pyuavcan.presentation.Presentation:
        return self._presentation

    def get_result(self) -> Optional[int]:
        """
        None if the allocation is still in progress. If the allocation is finished, this is the allocated node-ID.
        """
        res = self.presentation.transport.local_node_id
        return res if res is not None else self._result

    def close(self) -> None:
        """
        Stop the allocation process. The allocatee automatically closes itself shortly after the allocation is finished,
        so it's not necessary to invoke this method after a successful allocation.
        **The underlying transport is NOT closed.** The method is idempotent.
        """
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._sub_1.close()
        self._sub_2.close()
        if self._pub is not None:
            self._pub.close()
            self._pub = None

    def _on_timer(self) -> None:
        self._restart_timer()
        if self.get_result() is not None:
            self.close()
            return

        msg: Any = None
        try:
            if self.presentation.transport.protocol_parameters.mtu > self._MTU_THRESHOLD:
                msg = NodeIDAllocationData_2(node_id=ID(self._preferred_node_id), unique_id=self._local_unique_id)
            else:
                msg = NodeIDAllocationData_1(unique_id_hash=_make_pseudo_unique_id(self._local_unique_id))

            if self._pub is None or self._pub.dtype != type(msg):
                if self._pub is not None:
                    self._pub.close()
                self._pub = self.presentation.make_publisher_with_fixed_subject_id(type(msg))
                self._pub.priority = self.DEFAULT_PRIORITY

            _logger.debug("Publishing allocation request %s", msg)
            self._pub.publish_soon(msg)
        except Exception as ex:
            _logger.exception("Could not send allocation request %s: %s", msg, ex)

    def _restart_timer(self) -> None:
        t_request = random.random()
        self._timer = self.presentation.loop.call_later(t_request, self._on_timer)

    async def _on_response(
        self, msg: Union[NodeIDAllocationData_1, NodeIDAllocationData_2], meta: pyuavcan.transport.TransferFrom
    ) -> None:
        if self.get_result() is not None:  # Allocation already done, nothing else to do.
            return

        if meta.source_node_id is None:  # Another request, ignore.
            return

        allocated: Optional[int] = None
        if isinstance(msg, NodeIDAllocationData_1):
            if msg.unique_id_hash == _make_pseudo_unique_id(self._local_unique_id) and len(msg.allocated_node_id) > 0:
                allocated = msg.allocated_node_id[0].value
        elif isinstance(msg, NodeIDAllocationData_2):
            if msg.unique_id.tobytes() == self._local_unique_id:
                allocated = msg.node_id.value
        else:
            assert False, "Internal logic error"

        if allocated is None:
            return  # UID mismatch.

        assert isinstance(allocated, int)
        protocol_params = self.presentation.transport.protocol_parameters
        max_node_id = min(protocol_params.max_nodes - 1, _NODE_ID_MASK)
        if not (0 <= allocated <= max_node_id):
            _logger.warning(
                "Allocated node-ID %s ignored because it is incompatible with the transport: %s",
                allocated,
                protocol_params,
            )
            return

        _logger.info("Plug-and-play allocation done: got node-ID %s from server %s", allocated, meta.source_node_id)
        self._result = allocated


class Allocator:
    """
    An abstract PnP allocator interface. See derived classes.

    If an existing allocation table is reused with a least capable transport where the maximum node-ID is smaller,
    the allocator may create redundant allocations in order to avoid granting node-ID values that exceed the valid
    node-ID range for the transport.
    """

    DEFAULT_PUBLICATION_TIMEOUT = 5.0
    """
    The allocation message publication timeout is chosen to be large because the data is constant
    (does not lose relevance over time) and the priority level is usually low.
    """

    @abc.abstractmethod
    def register_node(self, node_id: int, unique_id: Optional[bytes]) -> None:
        """
        This method shall be invoked whenever a new node appears online and/or whenever its unique-ID is obtained.
        The recommended usage pattern is to subscribe to the update events from
        :class:`pyuavcan.application.node_tracker.NodeTracker`, where the necessary update logic is already implemented.
        """
        raise NotImplementedError


class CentralizedAllocator(Allocator):
    """
    The centralized plug-and-play node-ID allocator. See Specification for details.
    """

    def __init__(
        self,
        node: pyuavcan.application.Node,
        database_file: Optional[Union[str, pathlib.Path]] = None,
    ):
        """
        :param node:
            The node instance to run the allocator on.
            The 128-bit globally unique-ID of the local node will be sourced from this instance.
            Refer to the Specification for details.

        :param database_file:
            If provided, shall specify the path to the database file containing an allocation table.
            If the file does not exist, it will be automatically created. If None (default), the allocation table
            will be created in memory (therefore the allocation data will be lost after the instance is disposed).
        """
        self._node = node
        local_node_id = self.node.id
        if local_node_id is None:
            raise ValueError("The allocator cannot run on an anonymous node")
        # The database is initialized with ``check_same_thread=False`` to enable delegating its initialization
        # to a thread pool from an async context. This is important for this library because if one needs to
        # initialize a new instance from an async function, running the initialization directly may be unacceptable
        # due to its blocking behavior, so one is likely to rely on :meth:`asyncio.loop.run_in_executor`.
        # The executor will initialize the instance in a worker thread and then hand it over to the main thread,
        # which is perfectly safe, but it would trigger a false error from the SQLite engine complaining about
        # the possibility of concurrency-related bugs.
        self._alloc = _AllocationTable(
            sqlite3.connect(str(database_file or _DB_DEFAULT_LOCATION), timeout=_DB_TIMEOUT, check_same_thread=False)
        )
        self._alloc.register(local_node_id, self.node.info.unique_id.tobytes())
        self._sub1 = self.node.make_subscriber(NodeIDAllocationData_1)
        self._sub2 = self.node.make_subscriber(NodeIDAllocationData_2)
        self._pub1 = self.node.make_publisher(NodeIDAllocationData_1)
        self._pub2 = self.node.make_publisher(NodeIDAllocationData_2)
        self._pub1.send_timeout = self.DEFAULT_PUBLICATION_TIMEOUT
        self._pub2.send_timeout = self.DEFAULT_PUBLICATION_TIMEOUT

        def start() -> None:
            _logger.debug("Centralized allocator starting with the following allocation table:\n%s", self._alloc)
            self._sub1.receive_in_background(self._on_message)
            self._sub2.receive_in_background(self._on_message)

        def close() -> None:
            for port in [self._sub1, self._sub2, self._pub1, self._pub2]:
                assert isinstance(port, pyuavcan.presentation.Port)
                port.close()
            self._alloc.close()

        node.add_lifetime_hooks(start, close)

    @property
    def node(self) -> pyuavcan.application.Node:
        return self._node

    def register_node(self, node_id: int, unique_id: Optional[bytes]) -> None:
        self._alloc.register(node_id, unique_id)

    async def _on_message(
        self, msg: Union[NodeIDAllocationData_1, NodeIDAllocationData_2], meta: pyuavcan.transport.TransferFrom
    ) -> None:
        if meta.source_node_id is not None:
            _logger.error(  # pylint: disable=logging-fstring-interpolation
                f"Invalid network configuration: another node-ID allocator detected at node-ID {meta.source_node_id}. "
                f"There shall be exactly one allocator on the network. If modular redundancy is desired, "
                f"use a distributed allocator (currently, a centralized allocator is running). "
                f"The detected allocation response message is {msg} with metadata {meta}."
            )
            return

        _logger.debug("Received allocation request %s with metadata %s", msg, meta)
        max_node_id = self.node.presentation.transport.protocol_parameters.max_nodes - 1 - _NUM_RESERVED_TOP_NODE_IDS
        assert max_node_id > 0

        if isinstance(msg, NodeIDAllocationData_1):
            allocated = self._alloc.allocate(max_node_id, max_node_id, pseudo_unique_id=msg.unique_id_hash)
            if allocated is not None:
                self._respond_v1(meta.priority, msg.unique_id_hash, allocated)
                return
        elif isinstance(msg, NodeIDAllocationData_2):
            uid = msg.unique_id.tobytes()
            allocated = self._alloc.allocate(msg.node_id.value, max_node_id, unique_id=uid)
            if allocated is not None:
                self._respond_v2(meta.priority, uid, allocated)
                return
        else:
            assert False, "Internal logic error"
        _logger.warning("Allocation table is full, ignoring request %s with %s. Please purge the table.", msg, meta)

    def _respond_v1(self, priority: pyuavcan.transport.Priority, unique_id_hash: int, allocated_node_id: int) -> None:
        msg = NodeIDAllocationData_1(unique_id_hash=unique_id_hash, allocated_node_id=[ID(allocated_node_id)])
        _logger.info("Publishing allocation response v1: %s", msg)
        self._pub1.priority = priority
        self._pub1.publish_soon(msg)

    def _respond_v2(self, priority: pyuavcan.transport.Priority, unique_id: bytes, allocated_node_id: int) -> None:
        msg = NodeIDAllocationData_2(
            node_id=ID(allocated_node_id),
            unique_id=unique_id,
        )
        _logger.info("Publishing allocation response v2: %s", msg)
        self._pub2.priority = priority
        self._pub2.publish_soon(msg)


class DistributedAllocator(Allocator):
    """
    This class is a placeholder. The implementation is missing (could use help here).
    The implementation can be based on the existing distributed allocator from Libuavcan v0,
    although the new PnP protocol is much simpler because it lacks multi-stage exchanges.
    """

    def __init__(self, node: pyuavcan.application.Node):
        assert node
        raise NotImplementedError((self.__doc__ or "").strip())

    def register_node(self, node_id: int, unique_id: Optional[bytes]) -> None:
        raise NotImplementedError


class _AllocationTable:
    _SCHEMA = """
    create table if not exists `allocation` (
        `node_id`          int not null unique check(node_id >= 0),
        `unique_id_hex`    varchar(32) not null,  -- all zeros if unique-ID is unknown.
        `pseudo_unique_id` bigint not null check(pseudo_unique_id >= 0), -- 48 LSB of CRC64WE(unique-ID); v1 compat.
        `ts`               time not null default current_timestamp,
        primary key(node_id)
    );
    """

    def __init__(self, db_connection: sqlite3.Connection):
        self._con = db_connection
        self._con.execute(self._SCHEMA)
        self._con.commit()

    def register(self, node_id: int, unique_id: Optional[bytes]) -> None:
        unique_id_defined = unique_id is not None
        if unique_id is None:
            unique_id = bytes(_UNIQUE_ID_SIZE_BYTES)
        if not isinstance(unique_id, bytes) or len(unique_id) != _UNIQUE_ID_SIZE_BYTES:
            raise ValueError(f"Invalid unique-ID: {unique_id!r}")
        if not isinstance(node_id, int) or not (0 <= node_id <= _NODE_ID_MASK):
            raise ValueError(f"Invalid node-ID: {node_id!r}")

        def execute() -> None:
            assert isinstance(unique_id, bytes)
            self._con.execute(
                "insert or replace into allocation (node_id, unique_id_hex, pseudo_unique_id) values (?, ?, ?);",
                (node_id, unique_id.hex(), _make_pseudo_unique_id(unique_id)),
            )
            self._con.commit()

        res = self._con.execute("select unique_id_hex from allocation where node_id = ?", (node_id,)).fetchone()
        existing_uid = bytes.fromhex(res[0]) if res is not None else None
        if existing_uid is None:
            _logger.debug("Original node registration: NID % 5d, UID %s", node_id, unique_id.hex())
            execute()
        elif unique_id_defined and existing_uid != unique_id:
            _logger.debug(
                "Updated node registration:  NID % 5d, UID %s -> %s", node_id, existing_uid.hex(), unique_id.hex()
            )
            execute()

    def allocate(
        self,
        preferred_node_id: int,
        max_node_id: int,
        unique_id: Optional[bytes] = None,
        pseudo_unique_id: Optional[int] = None,
    ) -> Optional[int]:
        use_unique_id = unique_id is not None
        preferred_node_id = min(max(preferred_node_id, 0), max_node_id)
        _logger.debug(
            "Table alloc request: preferred_node_id=%s, max_node_id=%s, unique_id=%s, pseudo_unique_id=%s",
            preferred_node_id,
            max_node_id,
            unique_id.hex() if unique_id else None,
            pseudo_unique_id,
        )
        if unique_id is None:
            unique_id = bytes(_UNIQUE_ID_SIZE_BYTES)
        if pseudo_unique_id is None:
            pseudo_unique_id = _make_pseudo_unique_id(unique_id)
        assert isinstance(unique_id, bytes) and len(unique_id) == _UNIQUE_ID_SIZE_BYTES
        assert isinstance(pseudo_unique_id, int) and (0 <= pseudo_unique_id <= _PSEUDO_UNIQUE_ID_MASK)

        # Check if there is an existing allocation for this UID. If there are multiple matches, pick the newest.
        # Ignore existing allocations where the node-ID exceeds the maximum in case we're reusing an existing
        # allocation table with a less capable transport.
        if use_unique_id:
            res = self._con.execute(
                "select node_id from allocation where unique_id_hex = ? and node_id <= ? order by ts desc limit 1",
                (unique_id.hex(), max_node_id),
            ).fetchone()
        else:
            res = self._con.execute(
                "select node_id from allocation where pseudo_unique_id = ? and node_id <= ? order by ts desc limit 1",
                (pseudo_unique_id, max_node_id),
            ).fetchone()
        if res is not None:
            candidate = int(res[0])
            assert 0 <= candidate <= max_node_id, "Internal logic error"
            _logger.debug(
                "Serving existing allocation: NID %s, (pseudo-)UID %s",
                candidate,
                unique_id.hex() if use_unique_id else hex(pseudo_unique_id),
            )
            return candidate

        # Do a new allocation. Consider re-implementing this in pure SQL -- should be possible with SQLite.
        result: Optional[int] = None
        candidate = preferred_node_id
        while result is None and candidate <= max_node_id:
            if self._try_allocate(candidate, unique_id, pseudo_unique_id):
                result = candidate
            candidate += 1
        candidate = preferred_node_id
        while result is None and candidate >= 0:
            if self._try_allocate(candidate, unique_id, pseudo_unique_id):
                result = candidate
            candidate -= 1

        # Final report.
        if result is not None:
            _logger.debug(
                "New allocation: allocated NID %s, (pseudo-)UID %s, preferred NID %s",
                result,
                unique_id.hex() if use_unique_id else hex(pseudo_unique_id),
                preferred_node_id,
            )
        return result

    def close(self) -> None:
        self._con.close()

    def _try_allocate(self, node_id: int, unique_id: bytes, pseudo_unique_id: int) -> bool:
        try:
            self._con.execute(
                "insert into allocation (node_id, unique_id_hex, pseudo_unique_id) values (?, ?, ?);",
                (node_id, unique_id.hex(), pseudo_unique_id),
            )
            self._con.commit()
        except sqlite3.IntegrityError:  # Such entry already exists.
            return False
        return True

    def __str__(self) -> str:
        """Displays the table as a multi-line string in TSV format with one header line."""
        lines = ["Node-ID\t" + "Unique-ID/hash (hex)".ljust(32 + 1 + 12) + "\tUpdate timestamp"]
        for nid, uid_hex, pseudo, ts in self._con.execute(
            "select node_id, unique_id_hex, pseudo_unique_id, ts from allocation order by ts desc"
        ).fetchall():
            lines.append(f"{nid: 5d}  \t{uid_hex:32s}/{pseudo:012x}\t{ts}")
        return "\n".join(lines) + "\n"


def _make_pseudo_unique_id(unique_id: bytes) -> int:
    """
    The recommended mapping function from unique-ID to pseudo unique-ID.
    """
    from pyuavcan.transport.commons.crc import CRC64WE

    assert isinstance(unique_id, bytes) and len(unique_id) == _UNIQUE_ID_SIZE_BYTES
    return int(CRC64WE.new(unique_id).value & _PSEUDO_UNIQUE_ID_MASK)
