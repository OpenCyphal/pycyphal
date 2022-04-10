# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import abc
import typing
import dataclasses
import pycyphal


@dataclasses.dataclass(frozen=True)
class Capture:
    """
    This is the abstract data class for all events reported via the capture API.

    If a transport implementation defines multiple event types, it is recommended to define a common superclass
    for them such that it is always possible to determine which transport an event has arrived from using a single
    instance check.
    """

    timestamp: pycyphal.transport.Timestamp

    @staticmethod
    def get_transport_type() -> typing.Type[pycyphal.transport.Transport]:
        """
        Static reference to the type of transport that can emit captures of this type.
        For example, for Cyphal/serial it would be :class:`pycyphal.transport.serial.SerialTransport`.
        Although the method is static, it shall be overridden by all inheritors.
        """
        raise NotImplementedError


CaptureCallback = typing.Callable[[Capture], None]


@dataclasses.dataclass(frozen=True)
class AlienSessionSpecifier:
    """
    See :class:`AlienTransfer` and the abstract transport model.
    """

    source_node_id: typing.Optional[int]
    """None represents an anonymous transfer."""

    destination_node_id: typing.Optional[int]
    """None represents a broadcast transfer."""

    data_specifier: pycyphal.transport.DataSpecifier

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(
            self, self.data_specifier, source_node_id=self.source_node_id, destination_node_id=self.destination_node_id
        )


@dataclasses.dataclass(frozen=True)
class AlienTransferMetadata:
    priority: pycyphal.transport.Priority

    transfer_id: int
    """
    For outgoing transfers over transports with cyclic transfer-ID the modulo is computed automatically.
    The user does not have to bother; although, if it is desired to match the spoofed transfer with some
    follow-up activity (like a service response), the user needs to compute the modulo manually for obvious reasons.
    """

    session_specifier: AlienSessionSpecifier

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(
            self, self.session_specifier, priority=self.priority.name, transfer_id=self.transfer_id
        )


@dataclasses.dataclass(frozen=True)
class AlienTransfer:
    """
    This type models a captured (sniffed) decoded transfer exchanged between a local node and a remote node,
    between *remote nodes*, misaddressed transfer, or a spoofed transfer.

    It is different from :class:`pycyphal.transport.Transfer` because the latter is intended for normal communication,
    whereas this type is designed for advanced network diagnostics, which is a very different use case.
    You may notice that the regular transfer model does not include some information such as, say, the route specifier,
    because the respective behaviors are managed by the transport configuration.
    """

    metadata: AlienTransferMetadata

    fragmented_payload: pycyphal.transport.FragmentedPayload
    """
    For reconstructed transfers the number of fragments equals the number of frames in the transfer.
    For outgoing transfers the number of fragments may be arbitrary, the payload is always rearranged correctly.
    """

    def __eq__(self, other: object) -> bool:
        """
        Transfers whose payload is fragmented differently but content-wise is identical compare equal.

        >>> from pycyphal.transport import MessageDataSpecifier, Priority
        >>> meta = AlienTransferMetadata(Priority.LOW, 999, AlienSessionSpecifier(123, None, MessageDataSpecifier(888)))
        >>> a =  AlienTransfer(meta, fragmented_payload=[memoryview(b'abc'), memoryview(b'def')])
        >>> a == AlienTransfer(meta, fragmented_payload=[memoryview(b'abcd'), memoryview(b''), memoryview(b'ef')])
        True
        >>> a == AlienTransfer(meta, fragmented_payload=[memoryview(b'abcdef')])
        True
        >>> a == AlienTransfer(meta, fragmented_payload=[])
        False
        """
        if isinstance(other, AlienTransfer):

            def cat(fp: pycyphal.transport.FragmentedPayload) -> memoryview:
                return fp[0] if len(fp) == 1 else memoryview(b"".join(fp))

            return self.metadata == other.metadata and cat(self.fragmented_payload) == cat(other.fragmented_payload)
        return NotImplemented

    def __repr__(self) -> str:
        fragmented_payload = "+".join(f"{len(x)}B" for x in self.fragmented_payload)
        return pycyphal.util.repr_attributes(self, self.metadata, fragmented_payload=f"[{fragmented_payload}]")


@dataclasses.dataclass(frozen=True)
class Trace:
    """
    Base event reconstructed by :class:`Tracer`.
    Transport-specific implementations may define custom subclasses.
    """

    timestamp: pycyphal.transport.Timestamp
    """
    The local time when the traced event took place or was commenced.
    For transfers, this is the timestamp of the first frame.
    """


@dataclasses.dataclass(frozen=True)
class ErrorTrace(Trace):
    """
    This trace is yielded when the tracer has determined that it is unable to reconstruct a transfer.
    It may be further specialized by transport implementations.
    """


@dataclasses.dataclass(frozen=True)
class TransferTrace(Trace):
    """
    Reconstructed network data transfer (possibly exchanged between remote nodes) along with metadata.
    """

    transfer: AlienTransfer

    transfer_id_timeout: float
    """
    The tracer uses heuristics to automatically deduce the optimal transfer-ID timeout value per session
    based on the supplied captures.
    Whenever a new transfer is reassembled, the auto-deduced transfer-ID timeout that is currently used
    for its session is reported for informational purposes.
    This value may be used later to perform transfer deduplication if redundant tracers are used;
    for that, see :mod:`pycyphal.transport.redundant`.
    """


class Tracer(abc.ABC):
    """
    The tracer takes single instances of :class:`Capture` at the input and delivers a reconstructed high-level
    view of network events (modeled by :class:`Trace`) at the output.
    It keeps massive internal state that is modified whenever :meth:`update` is invoked.
    The class may be used either for real-time analysis on a live network, or for post-mortem analysis with capture
    events read from a black box recorder or a log file.

    Instances of this class are entirely isolated from the outside world; they do not perform any IO and do not hold
    any resources, they are purely computing entities.
    To reset the state (e.g., in order to start analyzing a new log) simply discard the old instance and use a new one.

    The user should never attempt to instantiate implementations manually; instead, the factory method
    :meth:`pycyphal.transport.Transport.make_tracer` should be used.

    Each transport implementation typically implements its own tracer.
    """

    @abc.abstractmethod
    def update(self, cap: Capture) -> typing.Optional[Trace]:
        """
        Takes a captured low-level network event at the input, returns a reconstructed high-level event at the output.
        If the event is considered irrelevant or did not update the internal state significantly
        (i.e., this is a non-last frame of a multi-frame transfer), the output is None.
        Reconstructed multi-frame transfers are reported as a single event when the last frame is received.

        Capture instances that are not supported by the current transport are silently ignored and None is returned.
        """
        raise NotImplementedError
