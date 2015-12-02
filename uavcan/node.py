#encoding=utf-8

from __future__ import division, absolute_import, print_function, unicode_literals
import time
import logging
import collections
import sched
import sys

import uavcan
import uavcan.driver as driver
import uavcan.transport as transport


DEFAULT_NODE_STATUS_INTERVAL = 0.5


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
            class SayNoToBlockingSchedulingException(Exception):
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

    def defer(self, timeout_seconds, callback, *args, **kwargs):
        """This method allows to invoke the callback with specified arguments once the specified amount of time.
        :returns: EventHandle object. Call .cancel() on it to cancel the event.
        """
        priority = 1
        event = self._scheduler.enter(timeout_seconds, priority, callback, args, kwargs)
        return self._make_sched_handle(lambda: event)

    def periodic(self, period_seconds, callback, *args, **kwargs):
        """This method allows to invoke the callback periodically, with specified time intervals.
        Note that the scheduler features zero phase drift.
        :returns: EventHandle object. Call .cancel() on it to cancel the event.
        """
        priority = 0

        def caller(scheduled_deadline):
            # Event MUST be re-registered first in order to ensure that it can be cancelled from the callback
            scheduled_deadline += period_seconds
            event_holder[0] = self._scheduler.enterabs(scheduled_deadline, priority, caller, (scheduled_deadline,))
            callback(*args, **kwargs)

        first_deadline = self._scheduler.timefunc() + period_seconds
        event_holder = [self._scheduler.enterabs(first_deadline, priority, caller, (first_deadline,))]
        return self._make_sched_handle(lambda: event_holder[0])

    def has_pending_events(self):
        """Returns true if there is at least one pending event in the queue.
        """
        return not self._scheduler.empty()


class Node(Scheduler):
    def __init__(self, can_driver, node_id=None, node_status_interval=None, **_extras):
        """It is recommended to use make_node() rather than instantiating this type directly.
        :param can_driver: CAN bus driver object. Calling close() on a node object closes its driver instance.
        :param node_id: Node ID of the current instance. Defaults to None, which enables passive mode.
        :param node_status_interval: NodeStatus broadcasting interval. Defaults to DEFAULT_NODE_STATUS_INTERVAL.
        """
        super(Node, self).__init__()

        self._can_driver = can_driver
        self.node_id = node_id

        self._handlers = []
        self._transfer_manager = transport.TransferManager()
        self._outstanding_requests = {}
        self._outstanding_request_callbacks = {}
        self._outstanding_request_timestamps = {}
        self._next_transfer_ids = collections.defaultdict(int)

        self.start_time_monotonic = time.monotonic()

        self.health = uavcan.protocol.NodeStatus().HEALTH_OK
        self.mode = uavcan.protocol.NodeStatus().MODE_INITIALIZATION
        self.vendor_specific_status_code = 0

        node_status_interval = node_status_interval or DEFAULT_NODE_STATUS_INTERVAL
        self.periodic(node_status_interval, self._send_node_status)

    def _recv_frame(self, message):
        frame_id, frame_data, ext_id = message
        if not ext_id:
            return

        frame = transport.Frame(frame_id, frame_data)
        # logging.debug("Node._recv_frame(): got {0!s}".format(frame))

        transfer_frames = self._transfer_manager.receive_frame(frame)
        if not transfer_frames:
            return

        transfer = transport.Transfer()
        transfer.from_frames(transfer_frames)

        logging.debug("Node._recv_frame(): received {0!r}".format(transfer))

        if (transfer.service_not_message and not transfer.request_not_response) and \
                transfer.dest_node_id == self.node_id:
            # This is a reply to a request we sent. Look up the original request and call the appropriate callback
            requests = self._outstanding_requests.keys()
            for key in requests:
                if transfer.is_response_to(self._outstanding_requests[key]):
                    # Call the request's callback and remove it from the active list
                    self._outstanding_request_callbacks[key](transfer.payload, transfer)
                    del self._outstanding_requests[key]
                    del self._outstanding_request_callbacks[key]
                    del self._outstanding_request_timestamps[key]
                    break
        elif not transfer.service_not_message or transfer.dest_node_id == self.node_id:
            # This is a request, a unicast or a broadcast; look up the appropriate handler by data type ID
            for handler in self._handlers:
                if handler[0] == transfer.payload.type:
                    kwargs = handler[2] if len(handler) == 3 else {}
                    h = handler[1](transfer.payload, transfer, self, **kwargs)
                    h._execute()

    def _next_transfer_id(self, key):
        transfer_id = self._next_transfer_ids[key]
        self._next_transfer_ids[key] = (transfer_id + 1) & 0x1F
        return transfer_id

    def _send_node_status(self):
        if self.node_id:
            msg = uavcan.protocol.NodeStatus()
            msg.uptime_sec = int(time.monotonic() - self.start_time_monotonic + 0.5)
            msg.health = self.health
            msg.mode = self.mode
            msg.sub_mode = 0
            msg.vendor_specific_status_code = self.vendor_specific_status_code
            self.send_message(msg)

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

            frame = self._can_driver.receive(read_timeout)
            if frame:
                self._recv_frame(frame)

        execute_once()
        while time.monotonic() < deadline:
            execute_once()

    def send_request(self, payload, dest_node_id=None, callback=None):
        transfer_id = self._next_transfer_id((payload.type.default_dtid, dest_node_id))
        transfer = transport.Transfer(payload=payload,
                                      source_node_id=self.node_id,
                                      dest_node_id=dest_node_id,
                                      transfer_id=transfer_id,
                                      service_not_message=True,
                                      request_not_response=True)

        for frame in transfer.to_frames():
            self._can_driver.send(frame.message_id, frame.bytes, extended=True)

        if callback:
            self._outstanding_requests[transfer.key] = transfer
            self._outstanding_request_callbacks[transfer.key] = callback
            self._outstanding_request_timestamps[transfer.key] = time.monotonic()

        logging.debug("Node.send_request(dest_node_id={0:d}): sent {1!r}".format(dest_node_id, payload))

    def send_message(self, payload):
        if not self.node_id:
            raise Exception('The node is configured in anonymous mode')  # TODO: use custom exception class

        transfer_id = self._next_transfer_id(payload.type.default_dtid)
        transfer = transport.Transfer(payload=payload,
                                      source_node_id=self.node_id,
                                      transfer_id=transfer_id,
                                      service_not_message=False)

        for frame in transfer.to_frames():
            self._can_driver.send(frame.message_id, frame.bytes, extended=True)

        logging.debug("Node.send_message(): sent {0!r}".format(payload))

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
    def __init__(self, payload, transfer, node, *args, **kwargs):
        self.message = payload
        self.transfer = transfer
        self.node = node

    def _execute(self):
        self.on_message(self.message)

    def on_message(self, message):
        pass


class Service(Monitor):
    def __init__(self, *args, **kwargs):
        super(Service, self).__init__(*args, **kwargs)
        self.request = self.message
        self.response = transport.CompoundValue(self.request.type, tao=True, mode="response")

    def _execute(self):
        result = self.on_request()

        # Send the response transfer
        transfer = transport.Transfer(
            payload=self.response,
            source_node_id=self.node.node_id,
            dest_node_id=self.transfer.source_node_id,
            transfer_id=self.transfer.transfer_id,
            transfer_priority=self.transfer.transfer_priority,
            service_not_message=True,
            request_not_response=False
        )
        for frame in transfer.to_frames():
            self.node.can.send(frame.message_id, frame.bytes,
                               extended=True)

        logging.debug("ServiceHandler._execute(dest_node_id={0:d}): sent {1!r}"
                      .format(self.transfer.source_node_id, self.response))

    def on_request(self):
        pass
