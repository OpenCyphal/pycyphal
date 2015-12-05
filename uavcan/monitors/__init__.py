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
        self._registry = {}

    def exists(self, node_id):
        return node_id in self._registry

    def get(self, node_id):
        if (self._registry[node_id].monotonic_timestamp + self.TIMEOUT) < time.monotonic():
            del self._registry[node_id]
        return self._registry[node_id]

    def stop(self):
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
    ALLOCATION = {}
    QUERY = ""
    QUERY_TIME = 0.0
    QUERY_TIMEOUT = 3.0

    def __init__(self, *args, **kwargs):
        super(DynamicNodeIDServer, self).__init__(*args, **kwargs)
        self.dynamic_id_range = kwargs.get("dynamic_id_range", (1, 127))

    def on_message(self, e):
        if e.message.first_part_of_unique_id:
            # First-phase messages trigger a second-phase query
            DynamicNodeIDServer.QUERY = e.message.unique_id.to_bytes()
            DynamicNodeIDServer.QUERY_TIME = time.monotonic()

            response = uavcan.protocol.dynamic_node_id.Allocation()  # @UndefinedVariable
            response.first_part_of_unique_id = 0
            response.node_id = 0
            response.unique_id.from_bytes(DynamicNodeIDServer.QUERY)
            e.node.broadcast(response)

            logger.debug("[MASTER] Got first-stage dynamic ID request for {0!r}".format(DynamicNodeIDServer.QUERY))
        elif len(e.message.unique_id) == 6 and len(DynamicNodeIDServer.QUERY) == 6:
            # Second-phase messages trigger a third-phase query
            DynamicNodeIDServer.QUERY += e.message.unique_id.to_bytes()
            DynamicNodeIDServer.QUERY_TIME = time.monotonic()

            response = uavcan.protocol.dynamic_node_id.Allocation()  # @UndefinedVariable
            response.first_part_of_unique_id = 0
            response.node_id = 0
            response.unique_id.from_bytes(DynamicNodeIDServer.QUERY)
            e.node.broadcast(response)
            logger.debug("[MASTER] Got second-stage dynamic ID request for {0!r}".format(DynamicNodeIDServer.QUERY))
        elif len(e.message.unique_id) == 4 and len(DynamicNodeIDServer.QUERY) == 12:
            # Third-phase messages trigger an allocation
            DynamicNodeIDServer.QUERY += e.message.unique_id.to_bytes()
            DynamicNodeIDServer.QUERY_TIME = time.monotonic()

            logger.debug("[MASTER] Got third-stage dynamic ID request for {0!r}".format(DynamicNodeIDServer.QUERY))

            node_requested_id = e.message.node_id
            node_allocated_id = None

            allocated_node_ids = \
                set(DynamicNodeIDServer.ALLOCATION.itervalues()) | set(NodeStatusMonitor.NODE_STATUS.iterkeys())
            allocated_node_ids.add(e.node.node_id)

            # If we've already allocated a node ID to this device, return the
            # same one
            if DynamicNodeIDServer.QUERY in DynamicNodeIDServer.ALLOCATION:
                node_allocated_id = DynamicNodeIDServer.ALLOCATION[DynamicNodeIDServer.QUERY]

            # If an ID was requested but not allocated yet, allocate the first
            # ID equal to or higher than the one that was requested
            if node_requested_id and not node_allocated_id:
                for node_id in range(node_requested_id, self.dynamic_id_range[1]):
                    if node_id not in allocated_node_ids:
                        node_allocated_id = node_id
                        break

            # If no ID was allocated in the above step (also if the requested
            # ID was zero), allocate the highest unallocated node ID
            if not node_allocated_id:
                for node_id in range(self.dynamic_id_range[1], self.dynamic_id_range[0], -1):
                    if node_id not in allocated_node_ids:
                        node_allocated_id = node_id
                        break

            DynamicNodeIDServer.ALLOCATION[DynamicNodeIDServer.QUERY] = node_allocated_id

            if node_allocated_id:
                response = uavcan.protocol.dynamic_node_id.Allocation()  # @UndefinedVariable
                response.first_part_of_unique_id = 0
                response.node_id = node_allocated_id
                response.unique_id.from_bytes(DynamicNodeIDServer.QUERY)
                e.node.broadcast(response)
                logger.info("[MASTER] Allocated node ID #{0:03d} to node with unique ID {1!r}"
                            .format(node_allocated_id, DynamicNodeIDServer.QUERY))
            else:
                logger.error("[MASTER] Couldn't allocate dynamic node ID")
        elif time.monotonic() - DynamicNodeIDServer.QUERY_TIME > DynamicNodeIDServer.QUERY_TIMEOUT:
            # Mis-sequenced reply and no good replies during the timeout
            # period -- reset the query now.
            DynamicNodeIDServer.QUERY = ""
            logger.error("[MASTER] Query timeout, resetting query")


class DebugLogMessageMonitor(object):
    def __init__(self, node):
        self._handle = node.add_handler(uavcan.protocol.debug.LogMessage, self._on_message)  # @UndefinedVariable

    def stop(self):
        self._handle.remove()

    def _on_message(self, e):
        logmsg = "DebugLogMessageMonitor [#{0:03d}:{1}] {2}"\
            .format(e.transfer.source_node_id, e.message.source.decode(), e.message.text.decode())
        (logger.debug, logger.info, logger.warning, logger.error)[e.message.level.value](logmsg)
