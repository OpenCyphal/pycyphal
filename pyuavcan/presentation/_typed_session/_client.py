#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

# TODO: UNDISABLE WHEN THIS IS RESOLVED: https://github.com/python/mypy/issues/7121
# TODO: SEE ALSO https://github.com/UAVCAN/pyuavcan/issues/61
# mypy: ignore-errors

from __future__ import annotations
import time
import typing
import asyncio
import logging
import dataclasses
import pyuavcan.dsdl
import pyuavcan.transport
from ._base import ServiceClass, ServiceTypedSession, TypedSessionFinalizer, OutgoingTransferIDCounter
from ._base import DEFAULT_PRIORITY
from ._error import TypedSessionClosedError, RequestTransferIDVariabilityExhaustedError


_RECEIVE_TIMEOUT = 10


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ClientStatistics:
    request_transport_session:  pyuavcan.transport.Statistics
    response_transport_session: pyuavcan.transport.Statistics
    sent_requests:              int
    deserialization_failures:   int
    unexpected_responses:       int


class Client(ServiceTypedSession[ServiceClass]):
    """
    Each task should request its own independent client instance from the presentation layer controller. Do not
    share the same client instance across different tasks. ALl client instances sharing the same session specifier
    also share the same underlying implementation object which is reference counted and destroyed automatically when
    the last client instance is closed.
    """

    DEFAULT_RESPONSE_TIMEOUT = 1.0       # Default timeout per the Specification

    def __init__(self,
                 impl: ClientImpl[ServiceClass],
                 loop: asyncio.AbstractEventLoop):
        self._maybe_impl: typing.Optional[ClientImpl[ServiceClass]] = impl
        self._loop = loop
        self._dtype = impl.dtype  # Permit usage after close()
        impl.register_proxy()
        self._response_timeout = self.DEFAULT_RESPONSE_TIMEOUT
        self._priority = DEFAULT_PRIORITY

    async def try_call(self, request: ServiceClass.Request) -> typing.Optional[ServiceClass.Response]:
        """
        A simplified version of try_call_with_transfer() that simply returns the response object without any metadata.
        """
        out = await self.try_call_with_transfer(request=request)
        return out[0] if out is not None else None

    async def try_call_with_transfer(self, request: ServiceClass.Request) \
            -> typing.Optional[typing.Tuple[ServiceClass.Response, pyuavcan.transport.TransferFrom]]:
        """
        Sends the request to the remote server using the pre-configured priority and response timeout parameters.
        Returns the response along with its transfer in the case of successful completion; if the server did not
        provide a valid response on time, returns None.
        """
        return await self._impl.try_call_with_transfer(request=request,
                                                       priority=self._priority,
                                                       response_timeout=self._response_timeout)

    @property
    def response_timeout(self) -> float:
        """
        The response timeout value used for requests emitted via this proxy instance.
        This parameter is configured separately per proxy instance; i.e., it is not shared across different client
        instances under the same session specifier.
        The default value is set according to the recommendations provided in the Specification.
        """
        return self._response_timeout

    @response_timeout.setter
    def response_timeout(self, value: float) -> None:
        self._response_timeout = float(value)

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
        assert value in pyuavcan.transport.Priority
        self._priority = value

    @property
    def dtype(self) -> typing.Type[ServiceClass]:
        return self._dtype

    @property
    def input_transport_session(self) -> pyuavcan.transport.InputSession:
        return self._impl.input_transport_session

    @property
    def output_transport_session(self) -> pyuavcan.transport.OutputSession:
        """
        The transport session used for request transfers.
        """
        return self._impl.output_transport_session

    def sample_statistics(self) -> ClientStatistics:
        return ClientStatistics(request_transport_session=self.output_transport_session.sample_statistics(),
                                response_transport_session=self.input_transport_session.sample_statistics(),
                                sent_requests=self._impl.sent_request_count,
                                deserialization_failures=self._impl.deserialization_failure_count,
                                unexpected_responses=self._impl.unexpected_response_count)

    def close(self) -> None:
        impl = self._impl
        self._maybe_impl = None
        impl.remove_proxy()

    @property
    def _impl(self) -> ClientImpl[ServiceClass]:
        if self._maybe_impl is None:
            raise TypedSessionClosedError(repr(self))
        else:
            return self._maybe_impl

    def __del__(self) -> None:
        if self._maybe_impl is not None:
            _logger.info('%s has not been disposed of properly; fixing', self)
            # We can't just call close() here because the object is being deleted
            self._maybe_impl.remove_proxy()


class ClientImpl(typing.Generic[ServiceClass]):
    """
    The client implementation. There is at most one such implementation per session specifier. It may be shared
    across multiple users with the help of the proxy class. When the last proxy is closed or garbage collected,
    the implementation will also be closed and removed. This is not a part of the library API.
    """
    def __init__(self,
                 dtype:                      typing.Type[ServiceClass],
                 input_transport_session:    pyuavcan.transport.InputSession,
                 output_transport_session:   pyuavcan.transport.OutputSession,
                 transfer_id_counter:        OutgoingTransferIDCounter,
                 transfer_id_modulo_factory: typing.Callable[[], int],
                 finalizer:                  TypedSessionFinalizer,
                 loop:                       asyncio.AbstractEventLoop):
        self.dtype = dtype
        self.input_transport_session = input_transport_session
        self.output_transport_session = output_transport_session

        self.sent_request_count = 0
        self.deserialization_failure_count = 0
        self.unexpected_response_count = 0

        self._transfer_id_counter = transfer_id_counter
        # The transfer ID modulo may change if the transport is reconfigured at runtime. This is certainly not a
        # common use case, but it makes sense supporting it in this library since it's supposed to be usable with
        # diagnostic and inspection tools.
        self._transfer_id_modulo_factory = transfer_id_modulo_factory
        self._finalizer = finalizer
        self._loop = loop

        self._lock = asyncio.Lock(loop=loop)
        self._closed = False
        self._proxy_count = 0
        self._response_futures_by_transfer_id: \
            typing.Dict[int, asyncio.Future[typing.Tuple[ServiceClass.Response, pyuavcan.transport.TransferFrom]]] = {}

        self._task = loop.create_task(self._task_function())

    async def try_call_with_transfer(self,
                                     request:          ServiceClass.Request,
                                     priority:         pyuavcan.transport.Priority,
                                     response_timeout: float) \
            -> typing.Optional[typing.Tuple[ServiceClass.Response, pyuavcan.transport.TransferFrom]]:
        async with self._lock:
            self._raise_if_closed()

            # We have to compute the modulus here manually instead of just letting the transport do that because
            # the response will use the modulus instead of the full TID and we have to match it with the request.
            transfer_id = self._transfer_id_counter.get_then_increment() % self._transfer_id_modulo_factory()
            if transfer_id in self._response_futures_by_transfer_id:
                raise RequestTransferIDVariabilityExhaustedError(repr(self))

            try:
                future = self._loop.create_future()
                self._response_futures_by_transfer_id[transfer_id] = future
                # The lock is still taken, this is intentional. Serialize access to the transport.
                await self._do_send(request=request,
                                    transfer_id=transfer_id,
                                    priority=priority)
                self.sent_request_count += 1
            except BaseException:
                self._forget_future(transfer_id)
                raise

        # Wait for the response with the lock released.
        # We have to make sure that no matter what happens, we remove the future from the table upon exit;
        # otherwise the user will get a false exception when the same transfer ID is reused (which only happens
        # with some low-capability transports such as CAN bus though).
        try:
            response, transfer = await asyncio.wait_for(future, timeout=response_timeout, loop=self._loop)
            assert isinstance(response, self.dtype.Response)
            assert isinstance(transfer, pyuavcan.transport.TransferFrom)
            return response, transfer  # type: ignore
        except asyncio.TimeoutError:
            return None
        finally:
            self._forget_future(transfer_id)

    def register_proxy(self) -> None:
        self._raise_if_closed()
        assert self._proxy_count >= 0
        self._proxy_count += 1
        _logger.debug('%s got a new proxy, new count %s', self, self._proxy_count)

    def remove_proxy(self) -> None:
        self._raise_if_closed()
        self._proxy_count -= 1
        _logger.debug('%s has lost a proxy, new count %s', self, self._proxy_count)
        assert self._proxy_count >= 0
        if self._proxy_count <= 0:
            _logger.info('%s is being closed', self)
            self._closed = True
            self._task.cancel()

    @property
    def proxy_count(self) -> int:
        """Testing facilitation."""
        assert self._proxy_count >= 0
        return self._proxy_count

    async def _do_send(self,
                       request:     ServiceClass.Request,
                       transfer_id: int,
                       priority:    pyuavcan.transport.Priority) -> None:
        timestamp = pyuavcan.transport.Timestamp.now()
        fragmented_payload = list(pyuavcan.dsdl.serialize(request))
        transfer = pyuavcan.transport.Transfer(timestamp=timestamp,
                                               priority=priority,
                                               transfer_id=transfer_id,
                                               fragmented_payload=fragmented_payload)
        await self.output_transport_session.send(transfer)

    async def _task_function(self) -> None:
        exception: typing.Optional[Exception] = None
        try:
            while not self._closed:
                transfer = await self.input_transport_session.try_receive(time.monotonic() + _RECEIVE_TIMEOUT)
                if transfer is None:
                    continue

                response = pyuavcan.dsdl.try_deserialize(self.dtype.Response, transfer.fragmented_payload)
                if response is None:
                    self.deserialization_failure_count += 1
                    continue

                try:
                    fut = self._response_futures_by_transfer_id.pop(transfer.transfer_id)
                except LookupError:
                    _logger.info('Unexpected response %s with transfer %s; TID values of pending requests: %r',
                                 response, transfer, list(self._response_futures_by_transfer_id.keys()))
                    self.unexpected_response_count += 1
                else:
                    fut.set_result((response, transfer))  # type: ignore
        except asyncio.CancelledError:
            _logger.info('Cancelling the task of %s', self)
        except Exception as ex:
            exception = ex
            # Do not use f-string because it can throw, unlike the built-in formatting facility of the logger
            _logger.exception('Fatal error in the task of %s: %s', self, ex)

        try:
            self._closed = True
            self._finalizer([self.input_transport_session, self.output_transport_session])
        except Exception as ex:
            exception = ex
            # Do not use f-string because it can throw, unlike the built-in formatting facility of the logger
            _logger.exception(f'Failed to finalize %s: %s', self, ex)

        exception = exception if exception is not None else TypedSessionClosedError(repr(self))
        for fut in self._response_futures_by_transfer_id.values():
            try:
                fut.set_exception(exception)
            except asyncio.InvalidStateError:
                pass
        assert self._closed

    def _forget_future(self, transfer_id: int) -> None:
        try:
            del self._response_futures_by_transfer_id[transfer_id]
        except LookupError:
            pass

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise TypedSessionClosedError(repr(self))

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes_noexcept(self,
                                                      dtype=str(pyuavcan.dsdl.get_model(self.dtype)),
                                                      input_transport_session=self.input_transport_session,
                                                      output_transport_session=self.output_transport_session,
                                                      proxy_count=self._proxy_count)
