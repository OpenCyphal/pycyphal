#
# Copyright (c) 2020 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
Keeps track of online nodes by subscribing to ``uavcan.node.Heartbeat`` and requesting ``uavcan.node.GetInfo``
when necessary; see :class:`NodeTracker`.
"""

import typing
import asyncio
import logging
from uavcan.node import Heartbeat_1_0 as Heartbeat
from uavcan.node import GetInfo_1_0 as GetInfo
import pyuavcan


Entry = typing.NamedTuple('Entry', [
    ('heartbeat', Heartbeat),
    ('info', typing.Optional[GetInfo.Response]),
])
"""
The data kept per online node.
The heartbeat is the latest received one.
The info is None until the node responds to the GetInfo request.
"""


UpdateHandler = typing.Callable[[int, typing.Optional[Entry], typing.Optional[Entry]], None]
"""
Arguments: node-ID, old entry, new entry. See :meth:`NodeTracker.add_update_handler` for details.
"""


_logger = logging.getLogger(__name__)


class NodeTracker:
    """
    This class is designed for tracking the list of online nodes in real time.
    It subscribes to ``uavcan.node.Heartbeat`` to keep a list of online nodes.
    Whenever a new node appears online or an existing node is restarted
    (restart is detected via the uptime counter),
    the tracker invokes ``uavcan.node.GetInfo`` on it and keeps the response until the node is restarted again
    or until it goes offline (offline nodes detected via heartbeat timeout).
    If the node did not reply to ``uavcan.node.GetInfo``, the request will be retried later.

    If the local node is anonymous, the info request functionality will be automatically disabled;
    it will be re-enabled automatically if the local node is assigned a node-ID later.

    The tracked node registry *does not include the local node*.
    If the local node-ID is N, the registry will not contain an entry at key N unless there is a node-ID conflict
    in the network.

    The class provides IoC events which are triggered on change.
    The collected data can also be accessed by direct polling synchronously.
    """

    GET_INFO_PRIORITY = pyuavcan.transport.Priority.OPTIONAL
    """
    The logic tolerates the loss of responses, hence the optional priority level.
    This way, we can retry without affecting high-priority communications.
    """

    DEFAULT_GET_INFO_TIMEOUT = 5.0
    """
    The default request timeout is larger than the recommended default because the data is immutable
    (does not lose validity over time) and the priority level is low which may cause delays.
    """

    DEFAULT_GET_INFO_ATTEMPTS = 10
    """
    Abandon efforts if the remote node did not respond to GetInfo this many times.
    The counter will resume from scratch if the node is restarted or a new node under that node-ID is detected.
    """

    def __init__(self, presentation: pyuavcan.presentation.Presentation):
        self._presentation = presentation
        self._sub_heartbeat = self._presentation.make_subscriber_with_fixed_subject_id(Heartbeat)

        self._registry: typing.Dict[int, Entry] = {}
        self._offline_timers: typing.Dict[int, asyncio.TimerHandle] = {}
        self._info_tasks: typing.Dict[int, asyncio.Task[None]] = {}

        self._update_handlers: typing.List[UpdateHandler] = []

        self._get_info_timeout = self.DEFAULT_GET_INFO_TIMEOUT
        self._get_info_attempts = self.DEFAULT_GET_INFO_ATTEMPTS

    @property
    def get_info_timeout(self) -> float:
        """
        Allows the user to override the default ``uavcan.node.GetInfo`` request timeout.
        The value shall be a finite positive number.
        """
        return self._get_info_timeout

    @get_info_timeout.setter
    def get_info_timeout(self, value: float) -> None:
        value = float(value)
        if 0 < value < float('+inf'):
            self._get_info_timeout = value
        else:
            raise ValueError(f'Invalid response timeout value: {value}')

    @property
    def get_info_attempts(self) -> int:
        """
        Allows the user to override the default ``uavcan.node.GetInfo`` request retry limit.
        The value shall be a non-negative integer number.
        The value of zero disables GetInfo requests completely.
        """
        return self._get_info_attempts

    @get_info_attempts.setter
    def get_info_attempts(self, value: int) -> None:
        value = int(value)
        if 0 <= value:
            self._get_info_attempts = value
        else:
            raise ValueError(f'Invalid attempt limit: {value}')

    @property
    def registry(self) -> typing.Dict[int, Entry]:
        """
        Access the live online node registry. Keys are node-ID, values are :class:`Entry`.
        The returned value is a copy of the actual registry to prevent accidental mutation.
        Elements are ordered by node-ID.
        """
        return {k: v for k, v in sorted(self._registry.items(), key=lambda item: item[0])}

    def start(self) -> None:
        """
        The registry is empty and hooks are not invoked until the instance is started.
        """
        _logger.debug('Starting %s', self)
        self._sub_heartbeat.receive_in_background(self._on_heartbeat)  # Idempotent

    def close(self) -> None:
        """
        When closed the registry is emptied and all handlers are removed.
        This is to avoid accidental reliance on obsolete data.
        """
        _logger.debug('Closing %s', self)
        self._sub_heartbeat.close()
        self._registry.clear()
        self._update_handlers.clear()

        for tm in self._offline_timers.values():
            tm.cancel()
        self._offline_timers.clear()

        for tsk in self._info_tasks.values():
            tsk.cancel()
        self._info_tasks.clear()

    def add_update_handler(self, handler: UpdateHandler) -> None:
        """
        Register a callable that will be invoked whenever the node registry is changed.
        The arguments are: node-ID, old entry, new entry.
        The handler is invoked in the following cases with the specified arguments:

        - New node appeared online. The old-entry is None. The new-entry info is None.
        - A known node went offline. The new-entry is None.
        - A known node restarted. Neither entry is None. The new-entry info is None.
        - A node responds to a ``uavcan.node.GetInfo`` request. Neither entry is None. The new-entry info is not None.

        Received Heartbeat messages change the registry as well, but they do not trigger the hook.
        Handlers can be added and removed at any moment regardless of whether the instance is started.
        """
        if not callable(handler):  # pragma: no cover
            raise ValueError(f'Bad handler: {handler}')
        self._update_handlers.append(handler)

    def remove_update_handler(self, handler: UpdateHandler) -> None:
        """
        Remove a previously added hook identified by referential equivalence. Behaves like :meth:`list.remove`.
        """
        self._update_handlers.remove(handler)

    async def _on_heartbeat(self, msg: Heartbeat, metadata: pyuavcan.transport.TransferFrom) -> None:
        node_id = metadata.source_node_id
        if node_id is None:
            _logger.warning(f'Anonymous nodes shall not publish Heartbeat. Message: {msg}. Metadata: {metadata}.')
            return

        # Construct the new entry and decide if we need to issue another GetInfo request.
        update = True
        old = self._registry.get(node_id)
        if old is None:
            new = Entry(msg, None)
            _logger.debug('New node %s heartbeat %s', node_id, msg)
        elif old[0].uptime > msg.uptime:
            new = Entry(msg, None)
            _logger.debug('Known node %s restarted. New heartbeat: %s. Old entry: %s', node_id, msg, old)
        else:
            new = Entry(msg, old[1])
            update = False

        # Set up the offline timer that will fire when the Heartbeat messages were not seen for long enough.
        self._registry[node_id] = new
        try:
            self._offline_timers[node_id].cancel()
        except LookupError:
            pass
        self._offline_timers[node_id] = self._presentation.loop.call_later(Heartbeat.OFFLINE_TIMEOUT,
                                                                           self._on_offline,
                                                                           node_id)

        # Do the update unless this is just a regular heartbeat (no restart, known node).
        if update:
            self._request_info(node_id)
            self._notify(node_id, old, new)

    def _on_offline(self, node_id: int) -> None:
        try:
            old = self._registry[node_id]
            _logger.debug('Offline timeout expired for node %s. Old entry: %s', node_id, old)
            self._notify(node_id, old, None)
            del self._registry[node_id]
            self._cancel_task(node_id)
            del self._offline_timers[node_id]
        except Exception as ex:
            _logger.exception(f'Offline timeout handler error for node {node_id}: {ex}')

    def _cancel_task(self, node_id: int) -> None:
        try:
            task = self._info_tasks[node_id]
        except LookupError:
            pass
        else:
            task.cancel()
            del self._info_tasks[node_id]
            _logger.debug('GetInfo task for node %s canceled', node_id)

    def _request_info(self, node_id: int) -> None:
        async def attempt() -> bool:
            client = self._presentation.make_client_with_fixed_service_id(GetInfo, node_id)
            try:
                client.priority = self.GET_INFO_PRIORITY
                client.response_timeout = self._get_info_timeout
                response = await client.call(GetInfo.Request())
                if response is not None:
                    _logger.debug('GetInfo response: %s', response)
                    obj, _meta = response
                    assert isinstance(obj, GetInfo.Response)
                    old = self._registry[node_id]
                    new = Entry(old[0], obj)
                    self._registry[node_id] = new
                    self._notify(node_id, old, new)
                    return True
                return False
            finally:
                client.close()

        async def worker() -> None:
            try:
                _logger.debug('GetInfo task for node %s started', node_id)
                remaining_attempts = self._get_info_attempts
                while remaining_attempts > 0:
                    try:
                        if await attempt():
                            break
                    except (pyuavcan.transport.OperationNotDefinedForAnonymousNodeError,
                            pyuavcan.presentation.RequestTransferIDVariabilityExhaustedError):
                        await asyncio.sleep(self._get_info_timeout)  # Keep retrying forever.
                    else:
                        remaining_attempts -= 1
                    _logger.debug('GetInfo task for node %s will retry; remaining attempts: %s',
                                  node_id, remaining_attempts)
                _logger.debug('GetInfo task for node %s is exiting', node_id)
            except asyncio.CancelledError:
                raise
            except pyuavcan.transport.ResourceClosedError:
                _logger.debug(f'GetInfo task for node {node_id} is stopping because the transport is closed.')
            except Exception as ex:
                _logger.exception(f'GetInfo task for node {node_id} has crashed: {ex}')
            del self._info_tasks[node_id]

        self._cancel_task(node_id)
        self._info_tasks[node_id] = self._presentation.loop.create_task(worker())

    def _notify(self, node_id: int, old_entry: typing.Optional[Entry], new_entry: typing.Optional[Entry]) -> None:
        assert isinstance(old_entry, Entry) or old_entry is None
        assert isinstance(new_entry, Entry) or new_entry is None
        for eh in self._update_handlers:
            try:
                eh(node_id, old_entry, new_entry)
            except Exception as ex:
                _logger.exception(f'Unhandled exception suppressed in handler {eh}: {ex}')
