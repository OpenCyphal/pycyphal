# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import asyncio
import logging
import dataclasses
import pyuavcan.dsdl
import pyuavcan.transport
from ._base import ServiceClass, ServicePort, PortFinalizer, OutgoingTransferIDCounter, Closable
from ._base import DEFAULT_PRIORITY, DEFAULT_SERVICE_REQUEST_TIMEOUT
from ._error import PortClosedError, RequestTransferIDVariabilityExhaustedError


# Shouldn't be too large as this value defines how quickly the task will detect that the underlying transport is closed.
_RECEIVE_TIMEOUT = 1


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ClientStatistics:
    """
    The counters are maintained at the hidden client instance which is not accessible to the user.
    As such, clients with the same session specifier will share the same set of statistical counters.
    """

    request_transport_session: pyuavcan.transport.SessionStatistics
    response_transport_session: pyuavcan.transport.SessionStatistics
    sent_requests: int
    deserialization_failures: int  #: Response transfers that could not be deserialized into a response object.
    unexpected_responses: int  #: Response transfers that could not be matched with a request state.


class Client(ServicePort[ServiceClass]):
    """
    A task should request its own client instance from the presentation layer controller.
    Do not share the same client instance across different tasks. This class implements the RAII pattern.

    Implementation info: all client instances sharing the same session specifier also share the same
    underlying implementation object containing the transport sessions which is reference counted and
    destroyed automatically when the last client instance is closed;
    the user code cannot access it and generally shouldn't care.
    None of the settings of a client instance, such as timeout or priority, can affect other client instances;
    this does not apply to the transfer-ID counter objects though because they are transport-layer entities
    and therefore are shared per session specifier.

    .. note::
        Normally we should use correct generic types ``ServiceClass.Request`` and ``ServiceClass.Response`` in the API;
        however, MyPy does not support that yet. Please find the context at
        https://github.com/python/mypy/issues/7121 (please upvote!) and https://github.com/UAVCAN/pyuavcan/issues/61.
        We use a tentative workaround for now to silence bogus type errors. When the missing logic is implemented
        in MyPy, this should be switched back to proper implementation.
    """

    def __init__(self, impl: ClientImpl[ServiceClass], loop: asyncio.AbstractEventLoop):
        """
        Do not call this directly! Use :meth:`Presentation.make_client`.
        """
        assert not impl.is_closed, "Internal logic error"
        self._maybe_impl: typing.Optional[ClientImpl[ServiceClass]] = impl
        impl.register_proxy()  # Register ASAP to ensure correct finalization.

        self._loop = loop
        self._dtype = impl.dtype  # Permit usage after close()
        self._input_transport_session = impl.input_transport_session  # Same
        self._output_transport_session = impl.output_transport_session  # Same
        self._transfer_id_counter = impl.transfer_id_counter  # Same
        self._response_timeout = DEFAULT_SERVICE_REQUEST_TIMEOUT
        self._priority = DEFAULT_PRIORITY

    async def call(
        self, request: pyuavcan.dsdl.CompositeObject
    ) -> typing.Optional[typing.Tuple[pyuavcan.dsdl.CompositeObject, pyuavcan.transport.TransferFrom]]:
        """
        Sends the request to the remote server using the pre-configured priority and response timeout parameters.
        Returns the response along with its transfer info in the case of successful completion.
        If the server did not provide a valid response on time, returns None.

        On certain feature-limited transfers (such as CAN) the call may raise
        :class:`pyuavcan.presentation.RequestTransferIDVariabilityExhaustedError`
        if there are too many concurrent requests.
        """
        if self._maybe_impl is None:
            raise PortClosedError(repr(self))
        return await self._maybe_impl.call(
            request=request, priority=self._priority, response_timeout=self._response_timeout
        )

    @property
    def response_timeout(self) -> float:
        """
        The response timeout value used for requests emitted via this proxy instance.
        This parameter is configured separately per proxy instance; i.e., it is not shared across different client
        instances under the same session specifier, so that, for example, different tasks invoking the same service
        on the same server node can have different timeout settings.
        The same value is also used as send timeout for the underlying call to
        :meth:`pyuavcan.transport.OutputSession.send`.
        The default value is set according to the recommendations provided in the Specification,
        which is :data:`DEFAULT_SERVICE_REQUEST_TIMEOUT`.
        """
        return self._response_timeout

    @response_timeout.setter
    def response_timeout(self, value: float) -> None:
        value = float(value)
        if 0 < value < float("+inf"):
            self._response_timeout = float(value)
        else:
            raise ValueError(f"Invalid response timeout value: {value}")

    @property
    def priority(self) -> pyuavcan.transport.Priority:
        """
        The priority level used for requests emitted via this proxy instance.
        This parameter is configured separately per proxy instance; i.e., it is not shared across different client
        instances under the same session specifier.
        """
        return self._priority

    @priority.setter
    def priority(self, value: pyuavcan.transport.Priority) -> None:
        self._priority = pyuavcan.transport.Priority(value)

    @property
    def dtype(self) -> typing.Type[ServiceClass]:
        return self._dtype

    @property
    def transfer_id_counter(self) -> OutgoingTransferIDCounter:
        """
        Allows the caller to reach the transfer-ID counter instance.
        The instance is shared for clients under the same session.
        I.e., if there are two clients that use the same service-ID and same server node-ID,
        they will share the same transfer-ID counter.
        """
        return self._transfer_id_counter

    @property
    def input_transport_session(self) -> pyuavcan.transport.InputSession:
        return self._input_transport_session

    @property
    def output_transport_session(self) -> pyuavcan.transport.OutputSession:
        """
        The transport session used for request transfers.
        """
        return self._output_transport_session

    def sample_statistics(self) -> ClientStatistics:
        """
        The statistics are counted at the hidden implementation instance.
        Clients that use the same session specifier will have the same set of statistical counters.
        """
        if self._maybe_impl is None:
            raise PortClosedError(repr(self))
        return ClientStatistics(
            request_transport_session=self.output_transport_session.sample_statistics(),
            response_transport_session=self.input_transport_session.sample_statistics(),
            sent_requests=self._maybe_impl.sent_request_count,
            deserialization_failures=self._maybe_impl.deserialization_failure_count,
            unexpected_responses=self._maybe_impl.unexpected_response_count,
        )

    def close(self) -> None:
        impl, self._maybe_impl = self._maybe_impl, None
        if impl is not None:
            impl.remove_proxy()

    def __del__(self) -> None:
        if self._maybe_impl is not None:
            # https://docs.python.org/3/reference/datamodel.html#object.__del__
            # DO NOT invoke logging from the finalizer because it may resurrect the object!
            # Once it is resurrected, we may run into resource management issue if __del__() is invoked again.
            # Whether it is invoked the second time is an implementation detail.
            # If it is invoked again, then we may terminate the client implementation prematurely, leaving existing
            # client proxy instances with a dead reference to a finalized implementation.
            # RAII is difficult in Python. Maybe we should require the user to manage resources manually?
            self._maybe_impl.remove_proxy()
            self._maybe_impl = None


class ClientImpl(Closable, typing.Generic[ServiceClass]):
    """
    The client implementation. There is at most one such implementation per session specifier. It may be shared
    across multiple users with the help of the proxy class. When the last proxy is closed or garbage collected,
    the implementation will also be closed and removed. This is not a part of the library API.
    """

    def __init__(
        self,
        dtype: typing.Type[ServiceClass],
        input_transport_session: pyuavcan.transport.InputSession,
        output_transport_session: pyuavcan.transport.OutputSession,
        transfer_id_counter: OutgoingTransferIDCounter,
        transfer_id_modulo_factory: typing.Callable[[], int],
        finalizer: PortFinalizer,
        loop: asyncio.AbstractEventLoop,
    ):
        self.dtype = dtype
        self.input_transport_session = input_transport_session
        self.output_transport_session = output_transport_session

        self.sent_request_count = 0
        self.unsent_request_count = 0
        self.deserialization_failure_count = 0
        self.unexpected_response_count = 0

        self.transfer_id_counter = transfer_id_counter
        # The transfer ID modulo may change if the transport is reconfigured at runtime. This is certainly not a
        # common use case, but it makes sense supporting it in this library since it's supposed to be usable with
        # diagnostic and inspection tools.
        self._transfer_id_modulo_factory = transfer_id_modulo_factory
        self._maybe_finalizer: typing.Optional[PortFinalizer] = finalizer
        self._loop = loop

        self._lock = asyncio.Lock()
        self._proxy_count = 0
        self._response_futures_by_transfer_id: typing.Dict[
            int, asyncio.Future[typing.Tuple[pyuavcan.dsdl.CompositeObject, pyuavcan.transport.TransferFrom]]
        ] = {}

        self._task = loop.create_task(self._task_function())

    @property
    def is_closed(self) -> bool:
        return self._maybe_finalizer is None

    async def call(
        self, request: pyuavcan.dsdl.CompositeObject, priority: pyuavcan.transport.Priority, response_timeout: float
    ) -> typing.Optional[typing.Tuple[pyuavcan.dsdl.CompositeObject, pyuavcan.transport.TransferFrom]]:
        async with self._lock:
            if self.is_closed:
                raise PortClosedError(repr(self))

            # We have to compute the modulus here manually instead of just letting the transport do that because
            # the response will use the modulus instead of the full TID and we have to match it with the request.
            transfer_id = self.transfer_id_counter.get_then_increment() % self._transfer_id_modulo_factory()
            if transfer_id in self._response_futures_by_transfer_id:
                raise RequestTransferIDVariabilityExhaustedError(repr(self))

            try:
                future = self._loop.create_future()
                self._response_futures_by_transfer_id[transfer_id] = future
                # The lock is still taken, this is intentional. Serialize access to the transport.
                send_result = await self._do_send(
                    request=request,
                    transfer_id=transfer_id,
                    priority=priority,
                    monotonic_deadline=self._loop.time() + response_timeout,
                )
            except BaseException:
                self._forget_future(transfer_id)
                raise

        # Wait for the response with the lock released.
        # We have to make sure that no matter what happens, we remove the future from the table upon exit;
        # otherwise the user will get a false exception when the same transfer ID is reused (which only happens
        # with some low-capability transports such as CAN bus though).
        try:
            if send_result:
                self.sent_request_count += 1
                response, transfer = await asyncio.wait_for(future, timeout=response_timeout)
                assert isinstance(response, self.dtype.Response)
                assert isinstance(transfer, pyuavcan.transport.TransferFrom)
                return response, transfer
            self.unsent_request_count += 1
            return None
        except asyncio.TimeoutError:
            return None
        finally:
            self._forget_future(transfer_id)

    def register_proxy(self) -> None:  # Proxy (de-)registration is always possible even if closed.
        assert not self.is_closed, "Internal logic error: cannot register a new proxy on a closed instance"
        assert self._proxy_count >= 0
        self._proxy_count += 1
        _logger.debug("%s got a new proxy, new count %s", self, self._proxy_count)

    def remove_proxy(self) -> None:
        self._proxy_count -= 1
        _logger.debug("%s has lost a proxy, new count %s", self, self._proxy_count)
        assert self._proxy_count >= 0
        if self._proxy_count <= 0:
            self.close()  # RAII auto-close

    @property
    def proxy_count(self) -> int:
        """Testing facilitation."""
        assert self._proxy_count >= 0
        return self._proxy_count

    def close(self) -> None:
        try:
            self._task.cancel()
        except Exception as ex:
            _logger.debug("Could not cancel the task %r: %s", self._task, ex, exc_info=True)
        self._finalize()

    async def _do_send(
        self,
        request: pyuavcan.dsdl.CompositeObject,
        transfer_id: int,
        priority: pyuavcan.transport.Priority,
        monotonic_deadline: float,
    ) -> bool:
        if not isinstance(request, self.dtype.Request):
            raise TypeError(
                f"Invalid request object: expected an instance of {self.dtype.Request}, "
                f"got {type(request)} instead."
            )

        timestamp = pyuavcan.transport.Timestamp.now()
        fragmented_payload = list(pyuavcan.dsdl.serialize(request))
        transfer = pyuavcan.transport.Transfer(
            timestamp=timestamp, priority=priority, transfer_id=transfer_id, fragmented_payload=fragmented_payload
        )
        return await self.output_transport_session.send(transfer, monotonic_deadline)

    async def _task_function(self) -> None:
        exception: typing.Optional[Exception] = None
        try:
            while not self.is_closed:
                transfer = await self.input_transport_session.receive(self._loop.time() + _RECEIVE_TIMEOUT)
                if transfer is None:
                    continue

                response = pyuavcan.dsdl.deserialize(self.dtype.Response, transfer.fragmented_payload)
                if response is None:
                    self.deserialization_failure_count += 1
                    continue

                try:
                    fut = self._response_futures_by_transfer_id.pop(transfer.transfer_id)
                except LookupError:
                    _logger.info(
                        "Unexpected response %s with transfer %s; TID values of pending requests: %r",
                        response,
                        transfer,
                        list(self._response_futures_by_transfer_id.keys()),
                    )
                    self.unexpected_response_count += 1
                else:
                    fut.set_result((response, transfer))
        except asyncio.CancelledError:
            _logger.debug("Cancelling the task of %s", self)
        except Exception as ex:
            exception = ex
            # Do not use f-string because it can throw, unlike the built-in formatting facility of the logger
            _logger.exception("Fatal error in the task of %s: %s", self, ex)
        finally:
            self._finalize(exception)
            assert self.is_closed

    def _forget_future(self, transfer_id: int) -> None:
        try:
            del self._response_futures_by_transfer_id[transfer_id]
        except LookupError:
            pass

    def _finalize(self, exception: typing.Optional[Exception] = None) -> None:
        exception = exception if exception is not None else PortClosedError(repr(self))
        try:
            if self._maybe_finalizer is not None:
                self._maybe_finalizer([self.input_transport_session, self.output_transport_session])
                self._maybe_finalizer = None
        except Exception as ex:
            _logger.exception("%s failed to finalize: %s", self, ex)
        for fut in self._response_futures_by_transfer_id.values():
            try:
                fut.set_exception(exception)
            except asyncio.InvalidStateError:
                pass

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes_noexcept(
            self,
            dtype=str(pyuavcan.dsdl.get_model(self.dtype)),
            input_transport_session=self.input_transport_session,
            output_transport_session=self.output_transport_session,
            proxy_count=self._proxy_count,
        )
