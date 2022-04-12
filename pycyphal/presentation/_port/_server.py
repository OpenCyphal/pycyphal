# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import asyncio
import logging
import dataclasses
import pycyphal.dsdl
import pycyphal.transport
import pycyphal.util
from ._base import T, ServicePort, PortFinalizer, DEFAULT_SERVICE_REQUEST_TIMEOUT
from ._error import PortClosedError


# Shouldn't be too large as this value defines how quickly the serving task will detect that the underlying
# transport is closed.
_LISTEN_FOREVER_TIMEOUT = 1


OutputTransportSessionFactory = typing.Callable[[int], pycyphal.transport.OutputSession]


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ServerStatistics:
    request_transport_session: pycyphal.transport.SessionStatistics
    """There is only one input transport session per server."""

    response_transport_sessions: typing.Dict[int, pycyphal.transport.SessionStatistics]
    """This is a mapping keyed by the remote client node-ID value. One transport session per client."""

    served_requests: int

    deserialization_failures: int
    """Requests that could not be received because of bad input transfers."""

    malformed_requests: int
    """Problems at the transport layer."""


@dataclasses.dataclass(frozen=True)
class ServiceRequestMetadata:
    """
    This structure is supplied with every received request for informational purposes.
    The application is not required to do anything with it.
    """

    timestamp: pycyphal.transport.Timestamp
    """Timestamp of the first frame of the request transfer."""

    priority: pycyphal.transport.Priority
    """Same priority will be used for the response (see Specification)."""

    transfer_id: int
    """Same transfer-ID will be used for the response (see Specification)."""

    client_node_id: int
    """The response will be sent back to this node."""

    def __repr__(self) -> str:
        kwargs = {f.name: getattr(self, f.name) for f in dataclasses.fields(self)}
        kwargs["priority"] = self.priority.name
        del kwargs["timestamp"]
        return pycyphal.util.repr_attributes(self, str(self.timestamp), **kwargs)


ServiceRequestHandler = typing.Callable[
    [typing.Any, ServiceRequestMetadata],
    typing.Awaitable[typing.Optional[typing.Any]],
]
"""
Type of the async request handler callable.
This should be parameterized by T.Request and T.Response, but it is currently not possible due to limitations of MyPy:
https://github.com/python/mypy/issues/7121
"""


class Server(ServicePort[T]):
    """
    At most one task can use the server at any given time.
    The instance must be closed manually to stop the server.
    """

    def __init__(
        self,
        dtype: typing.Type[T],
        input_transport_session: pycyphal.transport.InputSession,
        output_transport_session_factory: OutputTransportSessionFactory,
        finalizer: PortFinalizer,
    ):
        """
        Do not call this directly! Use :meth:`Presentation.get_server`.
        """
        if not pycyphal.dsdl.is_service_type(dtype):
            raise TypeError(f"Not a service type: {dtype}")

        self._dtype = dtype
        self._request_dtype = self._dtype.Request  # type: ignore
        self._response_dtype = self._dtype.Response  # type: ignore
        self._input_transport_session = input_transport_session
        self._output_transport_session_factory = output_transport_session_factory
        self._finalizer = finalizer

        self._output_transport_sessions: typing.Dict[int, pycyphal.transport.OutputSession] = {}
        self._maybe_task: typing.Optional[asyncio.Task[None]] = None
        self._closed = False
        self._send_timeout = DEFAULT_SERVICE_REQUEST_TIMEOUT

        self._served_request_count = 0
        self._deserialization_failure_count = 0
        self._malformed_request_count = 0

        assert pycyphal.dsdl.is_serializable(self._request_dtype)
        assert pycyphal.dsdl.is_serializable(self._response_dtype)

    # ----------------------------------------  MAIN API  ----------------------------------------

    async def serve(
        self,
        handler: ServiceRequestHandler,
        monotonic_deadline: typing.Optional[float] = None,
    ) -> None:
        """
        This is like :meth:`serve_for` except that it exits normally after the specified monotonic deadline is reached.
        The deadline value is compared against :meth:`asyncio.AbstractEventLoop.time`.
        If no deadline is provided, it is assumed to be infinite.
        """
        loop = asyncio.get_running_loop()
        # Observe that if we aggregate redundant transports with different non-monotonic transfer ID modulo values,
        # it might be that the transfer ID that we obtained from the request may be invalid for some of the transports.
        # This is why we can't reliably aggregate redundant transports with different transfer-ID overflow parameters.
        while not self._closed:
            out: typing.Optional[typing.Tuple[object, ServiceRequestMetadata]]
            if monotonic_deadline is None:
                out = await self._receive(loop.time() + _LISTEN_FOREVER_TIMEOUT)
                if out is None:
                    continue
            else:
                out = await self._receive(monotonic_deadline)
                if out is None:
                    break  # Timed out.

            self._served_request_count += 1
            request, meta = out
            response: typing.Optional[object] = None  # Fallback state
            assert isinstance(request, self._request_dtype), "Internal protocol violation"
            try:
                response = await handler(request, meta)
                if response is not None and not isinstance(response, self._response_dtype):
                    raise TypeError(
                        f"The application request handler has returned an invalid response: "
                        f"expected an instance of {self._response_dtype} or None, "
                        f"found {type(response)} instead. "
                        f"The corresponding request was {request} with metadata {meta}."
                    )
            except Exception as ex:
                if isinstance(ex, asyncio.CancelledError):
                    raise
                _logger.exception("%s unhandled exception in the handler: %s", self, ex)

            response_transport_session = self._get_output_transport_session(meta.client_node_id)

            # Send the response unless the application has opted out, in which case do nothing.
            if response is not None:
                # TODO: make the send timeout configurable.
                await self._do_send(response, meta, response_transport_session, loop.time() + self._send_timeout)

    async def serve_for(self, handler: ServiceRequestHandler, timeout: float) -> None:
        """
        Listen for requests for the specified time or until the instance is closed, then exit.

        When a request is received, the supplied handler callable will be invoked with the request object
        and the associated metadata object (which contains auxiliary information such as the client's node-ID).
        The handler shall return the response or None. If None is returned, the server will not send any response back
        (this practice is discouraged). If the handler throws an exception, it will be suppressed and logged.
        """
        loop = asyncio.get_running_loop()
        return await self.serve(handler, monotonic_deadline=loop.time() + timeout)

    def serve_in_background(self, handler: ServiceRequestHandler) -> None:
        """
        Start a new task and use it to run the server in the background.
        The task will be stopped when the server is closed.

        When a request is received, the supplied handler callable will be invoked with the request object
        and the associated metadata object (which contains auxiliary information such as the client's node-ID).
        The handler shall return the response or None. If None is returned, the server will not send any response back
        (this practice is discouraged). If the handler throws an exception, it will be suppressed and logged.

        If the background task is already running, it will be cancelled and a new one will be started instead.
        This method of serving requests shall not be used concurrently with other methods.
        """

        async def task_function() -> None:
            while not self._closed:
                try:
                    await self.serve_for(handler, _LISTEN_FOREVER_TIMEOUT)
                except asyncio.CancelledError:
                    _logger.debug("%s task cancelled", self)
                    break
                except pycyphal.transport.ResourceClosedError as ex:
                    _logger.debug("%s task got a resource closed error and will exit: %s", self, ex)
                    break
                except Exception as ex:
                    _logger.exception("%s task failure: %s", self, ex)
                    await asyncio.sleep(1)  # TODO is this an adequate failure management strategy?

        if self._maybe_task is not None:
            self._maybe_task.cancel()

        self._raise_if_closed()
        self._maybe_task = asyncio.get_event_loop().create_task(task_function())

    # ----------------------------------------  AUXILIARY  ----------------------------------------

    @property
    def send_timeout(self) -> float:
        """
        Every response transfer will have to be sent in this amount of time.
        If the time is exceeded, the attempt is aborted and a warning is logged.
        The default value is :data:`DEFAULT_SERVICE_REQUEST_TIMEOUT`.
        """
        return self._send_timeout

    @send_timeout.setter
    def send_timeout(self, value: float) -> None:
        value = float(value)
        if 0 < value < float("+inf"):
            self._send_timeout = value
        else:
            raise ValueError(f"Invalid send timeout value: {value}")

    def sample_statistics(self) -> ServerStatistics:
        """
        Returns the statistical counters of this server instance,
        including the statistical metrics of the underlying transport sessions.
        """
        return ServerStatistics(
            request_transport_session=self._input_transport_session.sample_statistics(),
            response_transport_sessions={
                nid: ts.sample_statistics() for nid, ts in self._output_transport_sessions.items()
            },
            served_requests=self._served_request_count,
            deserialization_failures=self._deserialization_failure_count,
            malformed_requests=self._malformed_request_count,
        )

    @property
    def dtype(self) -> typing.Type[T]:
        return self._dtype

    @property
    def input_transport_session(self) -> pycyphal.transport.InputSession:
        return self._input_transport_session

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            if self._maybe_task is not None:  # The task may be holding the lock.
                try:
                    self._maybe_task.cancel()  # We don't wait for it to exit because it's pointless.
                except Exception as ex:
                    _logger.exception("%s task could not be cancelled: %s", self, ex)
                self._maybe_task = None

            self._finalizer((self._input_transport_session, *self._output_transport_sessions.values()))

    async def _receive(
        self, monotonic_deadline: float
    ) -> typing.Optional[typing.Tuple[object, ServiceRequestMetadata]]:
        while True:
            transfer = await self._input_transport_session.receive(monotonic_deadline)
            if transfer is None:
                return None
            if transfer.source_node_id is not None:
                meta = ServiceRequestMetadata(
                    timestamp=transfer.timestamp,
                    priority=transfer.priority,
                    transfer_id=transfer.transfer_id,
                    client_node_id=transfer.source_node_id,
                )
                request = pycyphal.dsdl.deserialize(self._request_dtype, transfer.fragmented_payload)
                _logger.debug("%r received request: %r", self, request)
                if request is not None:
                    return request, meta
                self._deserialization_failure_count += 1
            else:
                self._malformed_request_count += 1

    @staticmethod
    async def _do_send(
        response: object,
        metadata: ServiceRequestMetadata,
        session: pycyphal.transport.OutputSession,
        monotonic_deadline: float,
    ) -> bool:
        timestamp = pycyphal.transport.Timestamp.now()
        fragmented_payload = list(pycyphal.dsdl.serialize(response))
        transfer = pycyphal.transport.Transfer(
            timestamp=timestamp,
            priority=metadata.priority,
            transfer_id=metadata.transfer_id,
            fragmented_payload=fragmented_payload,
        )
        return await session.send(transfer, monotonic_deadline)

    def _get_output_transport_session(self, client_node_id: int) -> pycyphal.transport.OutputSession:
        try:
            return self._output_transport_sessions[client_node_id]
        except LookupError:
            out = self._output_transport_session_factory(client_node_id)
            self._output_transport_sessions[client_node_id] = out
            return out

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise PortClosedError(repr(self))
