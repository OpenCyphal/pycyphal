from __future__ import division, absolute_import, print_function, unicode_literals
import time
import collections
import sqlite3
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

    class UpdateEvent:
        EVENT_ID_NEW = 'new'
        EVENT_ID_INFO_UPDATE = 'info_update'
        EVENT_ID_OFFLINE = 'offline'

        def __init__(self, entry, event_id):
            self.entry = entry
            self.event_id = event_id

        def __str__(self):
            return self.event_id + ':' + str(self.entry)

    class UpdateHandlerRemover:
        def __init__(self, remover):
            self._remover = remover

        def remove(self):
            self._remover()

        def try_remove(self):
            try:
                self._remover()
            except ValueError:
                pass

    def __init__(self, node):
        self._update_callbacks = []
        self._handle = node.add_handler(uavcan.protocol.NodeStatus, self._on_node_status)  # @UndefinedVariable
        self._registry = {}  # {node_id: Entry}
        self._timer = node.periodic(1, self._remove_stale)

    def add_update_handler(self, callback):
        """The specified callback will be invoked when:
        - A new node appears
        - Node info for an existing node gets updated
        - Node goes offline
        Call remove() or try_remove() on the returned object to unregister the handler.
        """
        self._update_callbacks.append(callback)
        return self.UpdateHandlerRemover(lambda: self._update_callbacks.remove(callback))

    def _call_event_handlers(self, event):
        for cb in self._update_callbacks:
            cb(event)

    def exists(self, node_id):
        """Returns True if the given node ID exists, false otherwise
        """
        return node_id in self._registry

    def get(self, node_id):
        """Returns an Entry instance for the given node ID.
        If the requested node ID does not exist, throws KeyError.
        """
        if (self._registry[node_id].monotonic_timestamp + self.TIMEOUT) < time.monotonic():
            self._call_event_handlers(self.UpdateEvent(self._registry[node_id],
                                                       self.UpdateEvent.EVENT_ID_OFFLINE))
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
        self._timer.remove()

    def _remove_stale(self):
        for nid, e in list(self._registry.items())[:]:
            if (e.monotonic_timestamp + self.TIMEOUT) < time.monotonic():
                del self._registry[nid]
                self._call_event_handlers(self.UpdateEvent(e, self.UpdateEvent.EVENT_ID_OFFLINE))

    def _on_node_status(self, e):
        node_id = e.transfer.source_node_id

        try:
            entry = self.get(node_id)
            new_entry = False
        except KeyError:
            entry = self.Entry()
            entry._info_requested_at = 0
            self._registry[node_id] = entry
            new_entry = True

        entry._update_from_status(e)
        if new_entry:
            self._call_event_handlers(self.UpdateEvent(entry, self.UpdateEvent.EVENT_ID_NEW))

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

        self._call_event_handlers(self.UpdateEvent(entry, self.UpdateEvent.EVENT_ID_INFO_UPDATE))


class DynamicNodeIDServer(object):
    QUERY_TIMEOUT = uavcan.protocol.dynamic_node_id.Allocation().FOLLOWUP_TIMEOUT_MS / 1000  # @UndefinedVariable
    DEFAULT_NODE_ID_RANGE = 1, 125
    DATABASE_STORAGE_MEMORY = ':memory:'

    class AllocationTable(object):
        def __init__(self, path):
            assert isinstance(path, str)
            self.db = sqlite3.connect(path)  # @UndefinedVariable

            self._modify('''CREATE TABLE IF NOT EXISTS `allocation` (
            `node_id`   INTEGER NOT NULL UNIQUE,
            `unique_id` blob,
            `ts`        time NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(node_id));''')

        def _modify(self, what, *args):
            c = self.db.cursor()
            c.execute(what, args)   # Tuple!
            self.db.commit()

        def close(self):
            self.db.close()

        def set(self, unique_id, node_id):
            if unique_id is not None and unique_id == bytes([0] * len(unique_id)):
                unique_id = None
            logger.debug('[DynamicNodeIDServer] AllocationTable update: #{0:03d} {1!r}'.format(node_id, unique_id))
            self._modify('''insert or replace into allocation (node_id, unique_id) values (?, ?);''',
                         node_id, unique_id)

        def get_node_id(self, unique_id):
            assert isinstance(unique_id, bytes)
            c = self.db.cursor()
            c.execute('''select node_id from allocation where unique_id = ? order by ts desc limit 1''',
                      (unique_id,))
            res = c.fetchone()
            return res[0] if res else None

        def get_unique_id(self, node_id):
            assert isinstance(node_id, int)
            c = self.db.cursor()
            c.execute('''select unique_id from allocation where node_id = ?''', (node_id,))
            res = c.fetchone()
            return res[0] if res else None

        def get_all_node_id(self):
            c = self.db.cursor()
            c.execute('''select node_id from allocation order by ts desc''')
            return [x for x, in c.fetchall()]

    def __init__(self, node, node_status_monitor, database_storage=None, dynamic_node_id_range=None):
        """
        :param node: Node instance.

        :param node_status_monitor: Instance of NodeStatusMonitor.

        :param database_storage: Path to the file where the instance will keep the allocation table.
                                 If not provided, the allocation table will be kept in memory.

        :param dynamic_node_id_range: Range of node ID available for dynamic allocation; defaults to [1, 125].
        """
        self._allocation_table = DynamicNodeIDServer.AllocationTable(database_storage or self.DATABASE_STORAGE_MEMORY)
        self._query = bytes()
        self._query_timestamp = 0
        self._node_monitor_event_handle = node_status_monitor.add_update_handler(self._handle_monitor_event)

        self._dynamic_node_id_range = dynamic_node_id_range or DynamicNodeIDServer.DEFAULT_NODE_ID_RANGE
        self._handle = node.add_handler(uavcan.protocol.dynamic_node_id.Allocation,  # @UndefinedVariable
                                        self._on_allocation_message)

        self._allocation_table.set(node.node_info.hardware_version.unique_id.to_bytes(), node.node_id)

    def _handle_monitor_event(self, event):
        unique_id = event.entry.info.hardware_version.unique_id.to_bytes() if event.entry.info else None
        self._allocation_table.set(unique_id, event.entry.node_id)

    def stop(self):
        """Stops the instance and closes the allocation table storage.
        """
        self._handle.remove()
        self._node_monitor_event_handle.remove()
        self._allocation_table.close()

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
            node_allocated_id = self._allocation_table.get_node_id(self._query)

            # If an ID was requested but not allocated yet, allocate the first
            # ID equal to or higher than the one that was requested
            if node_requested_id and not node_allocated_id:
                for node_id in range(node_requested_id, self._dynamic_node_id_range[1]):
                    if not self._allocation_table.get_unique_id(node_id):
                        node_allocated_id = node_id
                        break

            # If no ID was allocated in the above step (also if the requested
            # ID was zero), allocate the highest unallocated node ID
            if not node_allocated_id:
                for node_id in range(self._dynamic_node_id_range[1], self._dynamic_node_id_range[0], -1):
                    if not self._allocation_table.get_unique_id(node_id):
                        node_allocated_id = node_id
                        break

            self._allocation_table.set(self._query, node_allocated_id)

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
