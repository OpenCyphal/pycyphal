# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio
import dataclasses
import pyuavcan.transport
import pyuavcan.util
from ._input_session import LoopbackInputSession
from ._output_session import LoopbackOutputSession
from ._tracer import LoopbackCapture, LoopbackTracer


@dataclasses.dataclass
class LoopbackTransportStatistics(pyuavcan.transport.TransportStatistics):
    pass


class LoopbackTransport(pyuavcan.transport.Transport):
    """
    The loopback transport is intended for basic testing and API usage demonstrations.
    It works by short-circuiting input and output sessions together as if there was an underlying network.

    It is not possible to exchange data between different nodes using this transport.
    The only valid usage is sending and receiving same data on the same node.
    """

    def __init__(
        self,
        local_node_id: typing.Optional[int],
        *,
        allow_anonymous_transfers: bool = True,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._local_node_id = int(local_node_id) if local_node_id is not None else None
        self._allow_anonymous_transfers = allow_anonymous_transfers
        self._input_sessions: typing.Dict[pyuavcan.transport.InputSessionSpecifier, LoopbackInputSession] = {}
        self._output_sessions: typing.Dict[pyuavcan.transport.OutputSessionSpecifier, LoopbackOutputSession] = {}
        self._capture_handlers: typing.List[pyuavcan.transport.CaptureCallback] = []
        self._spoof_result: typing.Union[bool, Exception] = True
        # Unlimited protocol capabilities by default.
        self._protocol_parameters = pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=2 ** 64,
            max_nodes=2 ** 64,
            mtu=2 ** 64 - 1,
        )

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        return self._protocol_parameters

    @protocol_parameters.setter
    def protocol_parameters(self, value: pyuavcan.transport.ProtocolParameters) -> None:
        if isinstance(value, pyuavcan.transport.ProtocolParameters):
            self._protocol_parameters = value
        else:  # pragma: no cover
            raise ValueError(f"Unexpected value: {value}")

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._local_node_id

    @property
    def spoof_result(self) -> typing.Union[bool, Exception]:
        """
        Test rigging. If True, :meth:`spoof` will always succeed (this is the default).
        If False, it will always time out. If :class:`Exception`, it will be raised.
        """
        return self._spoof_result

    @spoof_result.setter
    def spoof_result(self, value: typing.Union[bool, Exception]) -> None:
        self._spoof_result = value

    def close(self) -> None:
        sessions = (*self._input_sessions.values(), *self._output_sessions.values())
        self._input_sessions.clear()
        self._output_sessions.clear()
        for s in sessions:
            s.close()
        self.spoof_result = pyuavcan.transport.ResourceClosedError(f"The transport is closed: {self}")

    def get_input_session(
        self, specifier: pyuavcan.transport.InputSessionSpecifier, payload_metadata: pyuavcan.transport.PayloadMetadata
    ) -> LoopbackInputSession:
        def do_close() -> None:
            try:
                del self._input_sessions[specifier]
            except LookupError:
                pass

        try:
            sess = self._input_sessions[specifier]
        except KeyError:
            sess = LoopbackInputSession(
                specifier=specifier, payload_metadata=payload_metadata, loop=self.loop, closer=do_close
            )
            self._input_sessions[specifier] = sess
        return sess

    def get_output_session(
        self, specifier: pyuavcan.transport.OutputSessionSpecifier, payload_metadata: pyuavcan.transport.PayloadMetadata
    ) -> LoopbackOutputSession:
        def do_close() -> None:
            try:
                del self._output_sessions[specifier]
            except LookupError:
                pass

        async def do_route(tr: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
            del monotonic_deadline  # Unused, all operations always successful and instantaneous.
            if specifier.remote_node_id in {self.local_node_id, None}:  # Otherwise drop the transfer.
                tr_from = pyuavcan.transport.TransferFrom(
                    timestamp=tr.timestamp,
                    priority=tr.priority,
                    transfer_id=tr.transfer_id % self.protocol_parameters.transfer_id_modulo,
                    fragmented_payload=list(tr.fragmented_payload),
                    source_node_id=self.local_node_id,
                )
                del tr
                pyuavcan.util.broadcast(self._capture_handlers)(
                    LoopbackCapture(
                        tr_from.timestamp,
                        pyuavcan.transport.AlienTransfer(
                            pyuavcan.transport.AlienTransferMetadata(
                                tr_from.priority,
                                tr_from.transfer_id,
                                pyuavcan.transport.AlienSessionSpecifier(
                                    self.local_node_id, specifier.remote_node_id, specifier.data_specifier
                                ),
                            ),
                            list(tr_from.fragmented_payload),
                        ),
                    )
                )
                for remote_node_id in {self.local_node_id, None}:  # Multicast to both: selective and promiscuous.
                    try:
                        destination_session = self._input_sessions[
                            pyuavcan.transport.InputSessionSpecifier(specifier.data_specifier, remote_node_id)
                        ]
                    except LookupError:
                        pass
                    else:
                        await destination_session.push(tr_from)
            return True

        try:
            sess = self._output_sessions[specifier]
        except KeyError:
            if self.local_node_id is None and not self._allow_anonymous_transfers:
                raise pyuavcan.transport.OperationNotDefinedForAnonymousNodeError(
                    f"Anonymous transfers are not enabled for {self}"
                ) from None
            sess = LoopbackOutputSession(
                specifier=specifier, payload_metadata=payload_metadata, loop=self.loop, closer=do_close, router=do_route
            )
            self._output_sessions[specifier] = sess
        return sess

    def sample_statistics(self) -> LoopbackTransportStatistics:
        return LoopbackTransportStatistics()

    @property
    def input_sessions(self) -> typing.Sequence[LoopbackInputSession]:
        return list(self._input_sessions.values())

    @property
    def output_sessions(self) -> typing.Sequence[LoopbackOutputSession]:
        return list(self._output_sessions.values())

    def begin_capture(self, handler: pyuavcan.transport.CaptureCallback) -> None:
        self._capture_handlers.append(handler)

    @property
    def capture_active(self) -> bool:
        return len(self._capture_handlers) > 0

    @staticmethod
    def make_tracer() -> LoopbackTracer:
        """
        See :class:`LoopbackTracer`.
        """
        return LoopbackTracer()

    async def spoof(self, transfer: pyuavcan.transport.AlienTransfer, monotonic_deadline: float) -> bool:
        """
        Spoofed transfers can be observed using :meth:`begin_capture`. Also see :attr:`spoof_result`.
        """
        if isinstance(self._spoof_result, Exception):
            raise self._spoof_result
        if self._spoof_result:
            pyuavcan.util.broadcast(self._capture_handlers)(
                LoopbackCapture(pyuavcan.transport.Timestamp.now(), transfer)
            )
        else:
            await asyncio.sleep(monotonic_deadline - asyncio.get_running_loop().time())
        return self._spoof_result

    @property
    def capture_handlers(self) -> typing.Sequence[pyuavcan.transport.CaptureCallback]:
        return self._capture_handlers[:]

    def _get_repr_fields(self) -> typing.Tuple[typing.List[typing.Any], typing.Dict[str, typing.Any]]:
        return [], {
            "local_node_id": self.local_node_id,
            "allow_anonymous_transfers": self._allow_anonymous_transfers,
        }
