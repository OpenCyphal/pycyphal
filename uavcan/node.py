# encoding=utf-8

from __future__ import division, absolute_import, print_function, unicode_literals
import time
import collections
import sched
import sys
import inspect
from logging import getLogger

import uavcan
import uavcan.driver as driver
import uavcan.transport as transport
from uavcan import UAVCANException


DEFAULT_NODE_STATUS_INTERVAL = 0.5
DEFAULT_SERVICE_TIMEOUT = 0.5
DEFAULT_TRANSFER_PRIORITY = 20


logger = getLogger(__name__)


class Scheduler(object):
    """This class implements a simple non-blocking event scheduler.
    It supports one-shot and periodic events.
    """

    def __init__(self):
        if sys.version_info[0] > 2:
            # Nice and easy.
            self._scheduler = sched.scheduler()
            self._run_scheduler = lambda: self._scheduler.run(blocking=False)
        else:
            # Nightmare inducing hacks
            class SayNoToBlockingSchedulingException(uavcan.UAVCANException):
                pass

            def delayfunc_impostor(duration):
                if duration > 0:
                    raise SayNoToBlockingSchedulingException('No!')

            self._scheduler = sched.scheduler(time.monotonic, delayfunc_impostor)

            def run_scheduler():
                try:
                    return self._scheduler.run()
                except SayNoToBlockingSchedulingException:
                    q = self._scheduler.queue
                    return q[0][0] if q else None

            self._run_scheduler = run_scheduler

    def _make_sched_handle(self, get_event):
        class EventHandle(object):
            @staticmethod
            def cancel():
                self._scheduler.cancel(get_event())

            @staticmethod
            def try_cancel():
                try:
                    self._scheduler.cancel(get_event())
                    return True
                except ValueError:
                    return False

        return EventHandle()

    def _poll_scheduler_and_get_remaining_time_to_next_deadline(self):
        next_deadline_at = self._run_scheduler()
        return None if next_deadline_at is None else (next_deadline_at - self._scheduler.timefunc())

    def defer(self, timeout_seconds, callback):
        """This method allows to invoke the callback with specified arguments once the specified amount of time.
        :returns: EventHandle object. Call .cancel() on it to cancel the event.
        """
        priority = 1
        event = self._scheduler.enter(timeout_seconds, priority, callback, ())
        return self._make_sched_handle(lambda: event)

    def periodic(self, period_seconds, callback):
        """This method allows to invoke the callback periodically, with specified time intervals.
        Note that the scheduler features zero phase drift.
        :returns: EventHandle object. Call .cancel() on it to cancel the event.
        """
        priority = 0

        def caller(scheduled_deadline):
            # Event MUST be re-registered first in order to ensure that it can be cancelled from the callback
            scheduled_deadline += period_seconds
            event_holder[0] = self._scheduler.enterabs(scheduled_deadline, priority, caller, (scheduled_deadline,))
            callback()

        first_deadline = self._scheduler.timefunc() + period_seconds
        event_holder = [self._scheduler.enterabs(first_deadline, priority, caller, (first_deadline,))]
        return self._make_sched_handle(lambda: event_holder[0])

    def has_pending_events(self):
        """Returns true if there is at least one pending event in the queue.
        """
        return not self._scheduler.empty()


class TransferEvent(object):
    def __init__(self, transfer, node, payload_attr_name):
        setattr(self, payload_attr_name, transfer.payload)
        self.transfer = transfer
        self.node = node

    def __str__(self):
        return str(self.transfer)

    def __repr__(self):
        return repr(self.transfer)


class HandlerDispatcher(object):
    class Remover:
        def __init__(self, remover):
            self._remover = remover

        def remove(self):
            self._remover()

        def try_remove(self):
            try:
                self._remover()
                return True
            except ValueError:
                return False

    def __init__(self, node):
        self._handlers = []  # type, callable
        self._node = node

    def add_handler(self, uavcan_type, handler, **kwargs):
        service = {
            uavcan_type.KIND_SERVICE: True,
            uavcan_type.KIND_MESSAGE: False
        }[uavcan_type.kind]

        # If handler is a class, create a wrapper function and register it as a regular callback
        if inspect.isclass(handler):
            def class_handler_adapter(event):
                h = handler(event, **kwargs)
                if service:
                    h.on_request()
                    return h.response
                else:
                    h.on_message()

            return self.add_handler(uavcan_type, class_handler_adapter)

        # At this point process the handler as a regular callback
        def call(transfer):
            event = TransferEvent(transfer, self._node, 'request' if service else 'message')
            result = handler(event, **kwargs)
            if service:
                if result is None:
                    raise UAVCANException('Service request handler did not return a response [%r, %r]' %
                                          (uavcan_type, handler))
                self._node.respond(result,
                                   transfer.source_node_id,
                                   transfer.transfer_id,
                                   transfer.transfer_priority)
            else:
                if result is not None:
                    raise UAVCANException('Message request handler did not return None [%r, %r]' %
                                          (uavcan_type, handler))

        entry = uavcan_type, call
        self._handlers.append(entry)
        return self.Remover(lambda: self._handlers.remove(entry))

    def remove_handlers(self, uavcan_type):
        self._handlers = list(filter(lambda x: x[0] != uavcan_type, self._handlers))

    def call_handlers(self, transfer):
        for uavcan_type, wrapper in self._handlers:
            if uavcan_type == transfer.payload.type:
                wrapper(transfer)


class Node(Scheduler):
    def __init__(self, can_driver, node_id=None, node_status_interval=None,
                 mode=None, node_info=None, **_extras):
        """It is recommended to use make_node() rather than instantiating this type directly.

        :param can_driver: CAN bus driver object. Calling close() on a node object closes its driver instance.

        :param node_id: Node ID of the current instance. Defaults to None, which enables passive mode.

        :param node_status_interval: NodeStatus broadcasting interval. Defaults to DEFAULT_NODE_STATUS_INTERVAL.

        :param mode: Initial operating mode (INITIALIZATION, OPERATIONAL, etc.); defaults to INITIALIZATION.

        :param node_info: Structure of type uavcan.protocol.GetNodeInfo.Response, responsed with when the local
                          node is queried for its node info.
        """
        super(Node, self).__init__()

        self._handler_dispatcher = HandlerDispatcher(self)

        self._can_driver = can_driver
        self.node_id = node_id

        self._transfer_manager = transport.TransferManager()
        self._outstanding_requests = {}
        self._outstanding_request_callbacks = {}
        self._next_transfer_ids = collections.defaultdict(int)

        self.start_time_monotonic = time.monotonic()

        # NodeStatus publisher
        self.health = uavcan.protocol.NodeStatus().HEALTH_OK                     # @UndefinedVariable
        self.mode = mode or uavcan.protocol.NodeStatus().MODE_INITIALIZATION     # @UndefinedVariable
        self.vendor_specific_status_code = 0

        node_status_interval = node_status_interval or DEFAULT_NODE_STATUS_INTERVAL
        self.periodic(node_status_interval, self._send_node_status)

        # GetNodeInfo server
        self.node_info = node_info or uavcan.protocol.GetNodeInfo.Response()     # @UndefinedVariable
        self.add_handler(uavcan.protocol.GetNodeInfo, lambda _: self.node_info)  # @UndefinedVariable

    def _recv_frame(self, message):
        frame_id, frame_data, ext_id = message
        if not ext_id:
            return

        frame = transport.Frame(frame_id, frame_data)
        # logger.debug("Node._recv_frame(): got {0!s}".format(frame))

        transfer_frames = self._transfer_manager.receive_frame(frame)
        if not transfer_frames:
            return

        transfer = transport.Transfer()
        transfer.from_frames(transfer_frames)

        logger.debug("Node._recv_frame(): received {0!r}".format(transfer))

        if (transfer.service_not_message and not transfer.request_not_response) and \
                transfer.dest_node_id == self.node_id:
            # This is a reply to a request we sent. Look up the original request and call the appropriate callback
            requests = self._outstanding_requests.keys()
            for key in requests:
                if transfer.is_response_to(self._outstanding_requests[key]):
                    # Call the request's callback and remove it from the active list
                    event = TransferEvent(transfer, self, 'response')
                    self._outstanding_request_callbacks[key](event)
                    del self._outstanding_requests[key]
                    del self._outstanding_request_callbacks[key]
                    break
        elif not transfer.service_not_message or transfer.dest_node_id == self.node_id:
            # This is a request or a broadcast; look up the appropriate handler by data type ID
            self._handler_dispatcher.call_handlers(transfer)

    def _next_transfer_id(self, key):
        transfer_id = self._next_transfer_ids[key]
        self._next_transfer_ids[key] = (transfer_id + 1) & 0x1F
        return transfer_id

    def _throw_if_anonymous(self):
        if not self.node_id:
            raise uavcan.UAVCANException('The node is configured in anonymous mode')

    def _send_node_status(self):
        if self.node_id:
            uptime_sec = int(time.monotonic() - self.start_time_monotonic + 0.5)
            self.broadcast(uavcan.protocol.NodeStatus(uptime_sec=uptime_sec,  # @UndefinedVariable
                                                      health=self.health,
                                                      mode=self.mode,
                                                      vendor_specific_status_code=self.vendor_specific_status_code))

    def add_handler(self, uavcan_type, handler, **kwargs):
        """Adds a handler for the specified data type.
        :param uavcan_type: DSDL data type. Only transfers of this type will be accepted for this handler.
        :param handler:     The handler. This must be either a callable or a class.
        :param **kwargs:    Extra arguments for the handler.
        :return: A remover object that can be used to unregister the handler as follows:
            x = node.add_handler(...)
            # Remove the handler like this:
            x.remove()
            # Or like this:
            if x.try_remove():
                print('The handler has been removed successfully')
            else:
                print('There is no such handler')
        """
        return self._handler_dispatcher.add_handler(uavcan_type, handler, **kwargs)

    def remove_handlers(self, uavcan_type):
        """Removes all handlers for the specified DSDL data type.
        """
        self._handler_dispatcher.remove_handlers(uavcan_type)

    def spin(self, timeout=None):
        """Runs background processes until timeout expires.
        Note that all processing is implemented in one thread.
        :param timeout: The method will return once this amount of time expires.
                        If None, the method will never return.
                        If zero, the method will handle only those events that are ready, then return immediately.
        """
        deadline = (time.monotonic() + timeout) if timeout is not None else sys.float_info.max

        def execute_once():
            next_event_at = self._poll_scheduler_and_get_remaining_time_to_next_deadline() or sys.float_info.max

            read_timeout = min(next_event_at, deadline) - time.monotonic()
            read_timeout = max(read_timeout, 0)
            read_timeout = min(read_timeout, 1)

            frame = self._can_driver.receive(read_timeout)
            if frame:
                self._recv_frame(frame)

        execute_once()
        while time.monotonic() < deadline:
            execute_once()

    def request(self, payload, dest_node_id, callback, priority=None, timeout=None):
        self._throw_if_anonymous()

        # Preparing the transfer
        transfer_id = self._next_transfer_id((payload.type.default_dtid, dest_node_id))
        transfer = transport.Transfer(payload=payload,
                                      source_node_id=self.node_id,
                                      dest_node_id=dest_node_id,
                                      transfer_id=transfer_id,
                                      transfer_priority=priority or DEFAULT_TRANSFER_PRIORITY,
                                      service_not_message=True,
                                      request_not_response=True)

        # Sending the transfer
        for frame in transfer.to_frames():
            self._can_driver.send(frame.message_id, frame.bytes, extended=True)

        # Registering a callback that will be invoked if there was no response after 'timeout' seconds
        def on_timeout():
            del self._outstanding_requests[transfer.key]
            del self._outstanding_request_callbacks[transfer.key]
            callback(None)

        timeout = timeout or DEFAULT_SERVICE_TIMEOUT
        timeout_caller_handle = self.defer(timeout, on_timeout)

        # This wrapper will automatically cancel the timeout callback if there was a response
        def timeout_cancelling_wrapper(event):
            timeout_caller_handle.try_cancel()
            callback(event)

        # Registering the pending request using the wrapper above instead of the callback
        self._outstanding_requests[transfer.key] = transfer
        self._outstanding_request_callbacks[transfer.key] = timeout_cancelling_wrapper

        logger.debug("Node.request(dest_node_id={0:d}): sent {1!r}".format(dest_node_id, payload))

    def respond(self, payload, dest_node_id, transfer_id, priority):
        self._throw_if_anonymous()

        transfer = transport.Transfer(
            payload=payload,
            source_node_id=self.node_id,
            dest_node_id=dest_node_id,
            transfer_id=transfer_id,
            transfer_priority=priority,
            service_not_message=True,
            request_not_response=False
        )
        for frame in transfer.to_frames():
            self._can_driver.send(frame.message_id, frame.bytes, extended=True)

        logger.debug("Node.respond(dest_node_id={0:d}, transfer_id={0:d}, priority={0:d}): sent {1!r}"
                     .format(dest_node_id, transfer_id, priority, payload))

    def broadcast(self, payload, priority=None):
        self._throw_if_anonymous()

        transfer_id = self._next_transfer_id(payload.type.default_dtid)
        transfer = transport.Transfer(payload=payload,
                                      source_node_id=self.node_id,
                                      transfer_id=transfer_id,
                                      transfer_priority=priority or DEFAULT_TRANSFER_PRIORITY,
                                      service_not_message=False)

        for frame in transfer.to_frames():
            self._can_driver.send(frame.message_id, frame.bytes, extended=True)

        logger.debug("Node.broadcast(): sent {0!r}".format(payload))

    def close(self):
        self._can_driver.close()


def make_node(can_device_name, **kwargs):
    """Constructs a node instance with specified CAN device.
    :param can_device_name: CAN device name, e.g. "/dev/ttyACM0", "COM9", "can0".
    :param kwargs: These arguments will be supplied to the CAN driver factory and to the node constructor.
    """
    can = driver.make_driver(can_device_name, **kwargs)
    return Node(can, **kwargs)


class Monitor(object):
    def __init__(self, event):
        self.message = event.message
        self.transfer = event.transfer
        self.node = event.node

    def on_message(self):
        pass


class Service(object):
    def __init__(self, event):
        self.request = event.request
        self.transfer = event.transfer
        self.node = event.node
        self.response = self.request.type.Response()

    def on_request(self):
        pass
