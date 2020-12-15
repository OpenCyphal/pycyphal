# Copyright (c) UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import abc
import typing
import dataclasses
import pyuavcan


@dataclasses.dataclass(frozen=True)
class Capture:
    """
    This is the abstract data class for all events reported via the capture API.

    If a transport implementation defines multiple event types, it is recommended to define a common superclass
    for them such that it is always possible to determine which transport an event has arrived from using a single
    instance check.
    """
    timestamp: pyuavcan.transport.Timestamp


CaptureCallback = typing.Callable[[Capture], None]


@dataclasses.dataclass(frozen=True)
class AlienTransfer:
    """
    This type models a captured (sniffed) decoded transfer exchanged between a local node and a remote node,
    between *remote nodes*, misaddressed transfer, or a spoofed transfer.

    It is different from :class:`pyuavcan.transport.Transfer` because the latter is intended for normal communication,
    whereas this type is designed for advanced network diagnostics, which is a very different use case.
    You may notice that the regular transfer model does not include some information such as, say, the route specifier,
    because the respective behaviors are managed by the transport configuration.
    """

    priority: pyuavcan.transport.Priority

    source_node_id: typing.Optional[int]
    """
    None represents an anonymous transfer.
    """

    destination_node_id: typing.Optional[int]
    """
    None represents a broadcast transfer.
    """

    data_specifier: pyuavcan.transport.DataSpecifier

    transfer_id: int
    """
    For outgoing transfers over transports with cyclic transfer-ID the modulo is computed automatically.
    The user does not have to bother; although, if it is desired to match the spoofed transfer with some
    follow-up activity (like a service response), the user needs to compute the modulo manually for obvious reasons.
    """

    fragmented_payload: pyuavcan.transport.FragmentedPayload
    """
    For reconstructed transfers the number of fragments equals the number of frames in the transfer.
    For outgoing transfers the number of fragments may be arbitrary, the payload is always rearranged correctly.
    """

    def __repr__(self) -> str:
        fragmented_payload = '+'.join(f'{len(x)}B' for x in self.fragmented_payload)
        kwargs = {
            f.name: getattr(self, f.name) for f in dataclasses.fields(self)
        }
        kwargs['priority'] = str(self.priority).split('.')[-1]
        kwargs['fragmented_payload'] = f'[{fragmented_payload}]'
        return pyuavcan.util.repr_attributes(self, **kwargs)


@dataclasses.dataclass(frozen=True)
class Trace:
    """
    Base event reconstructed by :class:`Tracer`.
    Transport-specific implementations may define custom subclasses.
    """
    timestamp: pyuavcan.transport.Timestamp
    """
    The local time when the traced event took place or was commenced.
    For transfers, this is the timestamp of the first frame.
    """


@dataclasses.dataclass(frozen=True)
class TransferTrace(Trace):
    """
    Reconstructed network data transfer along with references to all its frames.
    """
    transfer: AlienTransfer

    frames: typing.List[Capture]
    """
    The order of the frames matches the order of their reception.
    """


class Tracer(abc.ABC):
    """
    The tracer takes single instances of :class:`Capture` at the input and delivers a reconstructed high-level
    view of the network at the output.
    It keeps massive internal state that is modified whenever :meth:`update` is invoked.
    The class may be used either for real-time analysis on a live network, or for post-mortem analysis with capture
    events read from a black box recorder or a log file.

    Instances of this class are entirely isolated from the outside world; they do not perform any IO and do not hold
    any resources, they are purely computing entities.
    To reset the state (e.g., in order to start analyzing a new log) simply discard the old instance and use a new one.

    The user should never attempt to instantiate implementations manually; instead, the factory method
    :meth:`pyuavcan.transport.Transport.make_tracer` should be used.
    """

    def update(self, event: Capture) -> typing.Optional[Trace]:
        """
        Captured low-level network event at the input, reconstructed high-level event at the output.
        If the event is considered irrelevant or did not update the internal state significantly
        (i.e., this is a non-last frame of a multi-frame transfer), the output is None.
        Reconstructed multi-frame transfers are reported as a single event when the last frame is received.
        """
        raise NotImplementedError
