from __future__ import division, absolute_import, print_function, unicode_literals
import time
import collections
from logging import getLogger

import uavcan
import uavcan.node


logger = getLogger(__name__)


class NodeStatusMonitor(object):
    TIMEOUT = uavcan.protocol.NodeStatus().OFFLINE_TIMEOUT_MS / 1000  # @UndefinedVariable
    TRANSFER_PRIORITY = uavcan.TRANSFER_PRIORITY_LOWEST - 1
    RETRY_INTERVAL = 1

    class Entry:
        def __init__(self):
            self.node_id = None
            self.status = None
            self.info = None
            self.monotonic_timestamp = None

        def _update_from_status(self, e):
            self.monotonic_timestamp = time.monotonic()
            self.node_id = e.transfer.source_node_id
            if self.status and e.message.uptime_sec < self.status.uptime_sec:
                self.info = None
            self.status = e.message
            if self.info:
                #self.info.status = self.status
                for fld, _ in self.status.fields.items():  # TODO: This is temporary, until assignment is implemented
                    self.info.status.fields[fld] = self.status.fields[fld]

        def _update_from_info(self, e):
            self.monotonic_timestamp = time.monotonic()
            self.node_id = e.transfer.source_node_id
            self.status = e.response.status
            self.info = e.response

        def __str__(self):
            return '%d:%s' % (self.node_id, self.info if self.info else self.status)

        __repr__ = __str__

    def __init__(self, node, on_info_update_callback=None):
        self.on_info_update_callback = on_info_update_callback
        self._handle = node.add_handler(uavcan.protocol.NodeStatus, self._on_node_status)  # @UndefinedVariable
        self._registry = {}  # {node_id: Entry}

    def exists(self, node_id):
        """Returns True if the given node ID exists, false otherwise
        """
        return node_id in self._registry

    def get(self, node_id):
        """Returns an Entry instance for the given node ID.
        If the requested node ID does not exist, throws KeyError.
        """
        if (self._registry[node_id].monotonic_timestamp + self.TIMEOUT) < time.monotonic():
            del self._registry[node_id]
        return self._registry[node_id]

    def get_all_node_id(self):
        """Returns a generator or an iterable containing all currently active node ID.
        """
        return self._registry.keys()

    def find_all(self, predicate):
        """Returns a generator that produces a sequence of Entry objects for which the predicate returned True.
        """
        for _nid, entry in self._registry.items():
            if predicate(entry):
                yield entry

    def stop(self):
        """Stops the instance. The registry will not be cleared.
        """
        self._handle.remove()

    def _on_node_status(self, e):
        node_id = e.transfer.source_node_id

        try:
            entry = self.get(node_id)
        except KeyError:
            entry = self.Entry()
            entry._info_requested_at = 0
            self._registry[node_id] = entry

        entry._update_from_status(e)

        if not entry.info and entry.monotonic_timestamp - entry._info_requested_at > self.RETRY_INTERVAL:
            entry._info_requested_at = entry.monotonic_timestamp
            e.node.request(uavcan.protocol.GetNodeInfo.Request(), node_id,  # @UndefinedVariable
                           priority=self.TRANSFER_PRIORITY, callback=self._on_info_response)

    def _on_info_response(self, e):
        if not e:
            return

        try:
            entry = self.get(e.transfer.source_node_id)
        except KeyError:
            entry = self.Entry()
            self._registry[e.transfer.source_node_id] = entry

        entry._update_from_info(e)

        hw_unique_id = "".join(format(c, "02X") for c in e.response.hardware_version.unique_id)
        msg = (
            "[#{0:03d}:uavcan.protocol.GetNodeInfo] " +
            "software_version.major={1:d} " +
            "software_version.minor={2:d} " +
            "software_version.vcs_commit={3:08x} " +
            "software_version.image_crc={4:016X} " +
            "hardware_version.major={5:d} " +
            "hardware_version.minor={6:d} " +
            "hardware_version.unique_id={7!s} " +
            "name={8!r}"
        ).format(
            e.transfer.source_node_id,
            e.response.software_version.major,
            e.response.software_version.minor,
            e.response.software_version.vcs_commit,
            e.response.software_version.image_crc,
            e.response.hardware_version.major,
            e.response.hardware_version.minor,
            hw_unique_id,
            e.response.name.decode()
        )
        logger.info(msg)

        if self.on_info_update_callback:
            self.on_info_update_callback(entry)


class DynamicNodeIDServer(object):
    QUERY_TIMEOUT = uavcan.protocol.dynamic_node_id.Allocation().FOLLOWUP_TIMEOUT_MS / 1000  # @UndefinedVariable
    DEFAULT_NODE_ID_RANGE = 1, 125

    def __init__(self, node, node_status_monitor, dynamic_node_id_range=None):
        """
        :param node: Node instance
        :param node_status_monitor: Instance of NodeStatusMonitor
        :param dynamic_node_id_range: Range of node ID available for dynamic allocation; defaults to [1, 125]
        """
        self._allocation_table = {}  # {unique_id: node_id}
        self._query = bytes()
        self._query_timestamp = 0
        self._node_monitor = node_status_monitor

        self._dynamic_node_id_range = dynamic_node_id_range or DynamicNodeIDServer.DEFAULT_NODE_ID_RANGE
        self._handle = node.add_handler(uavcan.protocol.dynamic_node_id.Allocation,  # @UndefinedVariable
                                        self._on_allocation_message)

    def stop(self):
        """Stops the instance.
        """
        self._handle.remove()

    def get_allocated_node_id(self):
        """Returns a generator or an iterable containing all node ID that were allocated by this allocator.
        """
        return self._allocation_table.values()

    def _on_allocation_message(self, e):
        # TODO: request validation
        if e.message.first_part_of_unique_id:
            # First-phase messages trigger a second-phase query
            self._query = e.message.unique_id.to_bytes()
            self._query_timestamp = time.monotonic()

            response = uavcan.protocol.dynamic_node_id.Allocation()  # @UndefinedVariable
            response.first_part_of_unique_id = 0
            response.node_id = 0
            response.unique_id.from_bytes(self._query)
            e.node.broadcast(response)

            logger.debug("[DynamicNodeIDServer] Got first-stage dynamic ID request for {0!r}".format(self._query))

        elif len(e.message.unique_id) == 6 and len(self._query) == 6:
            # Second-phase messages trigger a third-phase query
            self._query += e.message.unique_id.to_bytes()
            self._query_timestamp = time.monotonic()

            response = uavcan.protocol.dynamic_node_id.Allocation()  # @UndefinedVariable
            response.first_part_of_unique_id = 0
            response.node_id = 0
            response.unique_id.from_bytes(self._query)
            e.node.broadcast(response)
            logger.debug("[DynamicNodeIDServer] Got second-stage dynamic ID request for {0!r}".format(self._query))

        elif len(e.message.unique_id) == 4 and len(self._query) == 12:
            # Third-phase messages trigger an allocation
            self._query += e.message.unique_id.to_bytes()
            self._query_timestamp = time.monotonic()

            logger.debug("[DynamicNodeIDServer] Got third-stage dynamic ID request for {0!r}".format(self._query))

            node_requested_id = e.message.node_id
            node_allocated_id = None

            allocated_node_ids = set(self._allocation_table.values()) | set(self._node_monitor.get_all_node_id())
            allocated_node_ids.add(e.node.node_id)

            # If we've already allocated a node ID to this device, return the same one
            if self._query in self._allocation_table:
                node_allocated_id = self._allocation_table[self._query]

            # If an ID was requested but not allocated yet, allocate the first
            # ID equal to or higher than the one that was requested
            if node_requested_id and not node_allocated_id:
                for node_id in range(node_requested_id, self._dynamic_node_id_range[1]):
                    if node_id not in allocated_node_ids:
                        node_allocated_id = node_id
                        break

            # If no ID was allocated in the above step (also if the requested
            # ID was zero), allocate the highest unallocated node ID
            if not node_allocated_id:
                for node_id in range(self._dynamic_node_id_range[1], self._dynamic_node_id_range[0], -1):
                    if node_id not in allocated_node_ids:
                        node_allocated_id = node_id
                        break

            self._allocation_table[self._query] = node_allocated_id

            if node_allocated_id:
                response = uavcan.protocol.dynamic_node_id.Allocation()  # @UndefinedVariable
                response.first_part_of_unique_id = 0
                response.node_id = node_allocated_id
                response.unique_id.from_bytes(self._query)
                e.node.broadcast(response)

                self._query = bytes()   # Resetting the state

                logger.info("[DynamicNodeIDServer] Allocated node ID #{0:03d} to node with unique ID {1!r}"
                            .format(node_allocated_id, self._query))
            else:
                logger.error("[DynamicNodeIDServer] Couldn't allocate dynamic node ID")

        elif time.monotonic() - self._query_timestamp > DynamicNodeIDServer.QUERY_TIMEOUT:
            # Mis-sequenced reply and no good replies during the timeout period -- reset the query now.
            self._query = bytes()
            logger.error("[DynamicNodeIDServer] Query timeout, resetting query")


class DebugLogMessageMonitor(object):
    def __init__(self, node):
        self._handle = node.add_handler(uavcan.protocol.debug.LogMessage, self._on_message)  # @UndefinedVariable

    def stop(self):
        self._handle.remove()

    def _on_message(self, e):
        logmsg = "DebugLogMessageMonitor [#{0:03d}:{1}] {2}"\
            .format(e.transfer.source_node_id, e.message.source.decode(), e.message.text.decode())
        (logger.debug, logger.info, logger.warning, logger.error)[e.message.level.value](logmsg)
