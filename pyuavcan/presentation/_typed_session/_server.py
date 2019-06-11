#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import time
import typing
import asyncio
import logging
import dataclasses
import pyuavcan.dsdl
import pyuavcan.transport
from ._base import ServiceClass, ServiceTypedSession, TypedSessionFinalizer
from ._error import TypedSessionClosedError


_LISTEN_FOREVER_TIMEOUT = 1


OutputSessionFactory = typing.Callable[[int], typing.Awaitable[pyuavcan.transport.OutputSession]]
ServiceRequestClass = typing.TypeVar('ServiceRequestClass', bound=pyuavcan.dsdl.CompositeObject)
ServiceResponseClass = typing.TypeVar('ServiceResponseClass', bound=pyuavcan.dsdl.CompositeObject)


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ServerStatistics:
    request_transfer:         pyuavcan.transport.Statistics
    response_transfers:       typing.Dict[int, pyuavcan.transport.Statistics]
    served_requests:          int
    deserialization_failures: int
    malformed_requests:       int


@dataclasses.dataclass
class ServiceRequestMetadata:
    timestamp:      pyuavcan.transport.Timestamp
    priority:       pyuavcan.transport.Priority
    transfer_id:    int
    client_node_id: int


ServiceRequestHandler = typing.Callable[[ServiceRequestClass, ServiceRequestMetadata],
                                        typing.Awaitable[typing.Optional[ServiceResponseClass]]]


class Server(ServiceTypedSession[ServiceClass]):
    """
    At most one task can use the server at any given time. The public API entries are serialized with a lock to
    enforce this. There can be at most one server instance per data specifier.
    """
    def __init__(self,
                 dtype:                            typing.Type[ServiceClass],
                 input_transport_session:          pyuavcan.transport.InputSession,
                 output_transport_session_factory: OutputSessionFactory,
                 finalizer:                        TypedSessionFinalizer,
                 loop:                             asyncio.AbstractEventLoop):
        self._dtype = dtype
        self._input_transport_session = input_transport_session
        self._output_transport_session_factory = output_transport_session_factory
        self._finalizer = finalizer
        self._loop = loop

        self._output_transport_sessions: typing.Dict[int, pyuavcan.transport.OutputSession] = {}
        self._lock = asyncio.Lock(loop=loop)
        self._closed = False

        self._served_request_count = 0
        self._deserialization_failure_count = 0
        self._malformed_request_count = 0

    async def listen_forever(self, handler: ServiceRequestHandler[ServiceClass.Request, ServiceClass.Response]) -> None:
        """
        Listen for requests forever. When a request is received, the handler will be invoked. If the handler throws
        an exception, it will be propagated outside.
        """
        while True:
            await self.listen_for(handler, _LISTEN_FOREVER_TIMEOUT)

    async def listen_for(self,
                         handler: ServiceRequestHandler[ServiceClass.Request, ServiceClass.Response],
                         timeout: float) -> None:
        """
        This is like listen_forever() except that we exit normally after the specified timeout has expired.
        """
        return await self.listen_until(handler, monotonic_deadline=time.monotonic() + timeout)

    async def listen_until(self,
                           handler:            ServiceRequestHandler[ServiceClass.Request, ServiceClass.Response],
                           monotonic_deadline: float) -> None:
        """
        This is like listen_forever() except that we exit normally after the specified monotonic deadline (i.e., the
        deadline value is compared against time.monotonic()).
        """
        # Observe that if we aggregate redundant transports with different non-monotonic transfer ID modulo values,
        # it might be that the transfer ID that we obtained from the request may be invalid for some of the transports.
        # This is why we can't reliably aggregate redundant transports with different transfer ID overflow parameters.
        while True:
            async with self._lock:
                self._raise_if_closed()
                out = await self._try_receive_until(monotonic_deadline)
                if out is None:
                    break           # Timed out.

                # Launch a concurrent task to retrieve the response session while the application's handler is running.
                # This allows us to minimize the request processing time.
                self._served_request_count += 1
                request, meta = out
                response_transport_session_creation_task = \
                    self._loop.create_task(self._get_output_transport_session(meta.client_node_id))
                try:
                    # Invoke the application. The handler may throw, we don't care, let the caller sort this out.
                    assert isinstance(request, self._dtype.Request), 'Internal protocol violation'
                    response = await handler(request, meta)  # type: ignore
                    if response is not None and not isinstance(response, self._dtype.Response):
                        raise ValueError(
                            f'The application request handler has returned an invalid response: '
                            f'expected an instance of {self._dtype.Response} or None, found {type(response)} instead. '
                            f'The corresponding request was {request} with metadata {meta}')
                finally:
                    # Can't leave a task pending. Never.
                    response_transport_session = await response_transport_session_creation_task

                # Send the response unless the application has opted out, in which case do nothing.
                if response is not None:
                    await self._do_send(response, meta, response_transport_session)  # type: ignore

    def sample_statistics(self) -> ServerStatistics:
        """
        Returns the statistical counters of this server, including the statistical metrics of the underlying
        transport sessions.
        """
        return ServerStatistics(request_transfer=self._input_transport_session.sample_statistics(),
                                response_transfers={nid: ts.sample_statistics()
                                                    for nid, ts in self._output_transport_sessions.items()},
                                served_requests=self._served_request_count,
                                deserialization_failures=self._deserialization_failure_count,
                                malformed_requests=self._malformed_request_count)

    @property
    def dtype(self) -> typing.Type[ServiceClass]:
        return self._dtype

    @property
    def input_transport_session(self) -> pyuavcan.transport.InputSession:
        return self._input_transport_session

    async def close(self) -> None:
        async with self._lock:
            self._raise_if_closed()
            await self._finalizer((self._input_transport_session, *self._output_transport_sessions.values()))

    async def _try_receive_until(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[ServiceClass.Request, ServiceRequestMetadata]]:
        while True:
            transfer = await self._input_transport_session.try_receive(monotonic_deadline)
            if transfer is None:
                return None
            if transfer.source_node_id is not None:
                meta = ServiceRequestMetadata(timestamp=transfer.timestamp,
                                              priority=transfer.priority,
                                              transfer_id=transfer.transfer_id,
                                              client_node_id=transfer.source_node_id)
                request = pyuavcan.dsdl.try_deserialize(self._dtype.Request, transfer.fragmented_payload)
                if request is not None:
                    return request, meta  # type: ignore
                else:
                    self._deserialization_failure_count += 1
            else:
                self._malformed_request_count += 1

    @staticmethod
    async def _do_send(response: ServiceClass.Response,
                       metadata: ServiceRequestMetadata,
                       session:  pyuavcan.transport.OutputSession) -> None:
        timestamp = pyuavcan.transport.Timestamp.now()
        fragmented_payload = list(pyuavcan.dsdl.serialize(response))
        transfer = pyuavcan.transport.Transfer(timestamp=timestamp,
                                               priority=metadata.priority,
                                               transfer_id=metadata.transfer_id,
                                               fragmented_payload=fragmented_payload)
        await session.send(transfer)

    async def _get_output_transport_session(self, client_node_id: int) -> pyuavcan.transport.OutputSession:
        try:
            return self._output_transport_sessions[client_node_id]
        except LookupError:
            out = await self._output_transport_session_factory(client_node_id)
            self._output_transport_sessions[client_node_id] = out
            return out

    def _raise_if_closed(self) -> None:
        assert self._lock.locked(), 'Internal protocol violation: the lock is not acquired'
        if self._closed:
            raise TypedSessionClosedError(repr(self))
