# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import copy
import typing
import asyncio
import logging
import warnings
import ipaddress
import dataclasses
import pycyphal
from ._session import UDPInputSession, SelectiveUDPInputSession, PromiscuousUDPInputSession
from ._session import PromiscuousUDPInputSessionStatistics, SelectiveUDPInputSessionStatistics
from ._session import UDPOutputSession, UDPInputSessionStatistics
from ._frame import UDPFrame
from ._ip import SocketFactory, Sniffer, LinkLayerCapture, IPAddress
from ._tracer import UDPTracer, UDPCapture
from .. import OperationNotDefinedForAnonymousNodeError


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class UDPTransportStatistics(pycyphal.transport.TransportStatistics):
    received_datagrams: typing.Dict[
        pycyphal.transport.InputSessionSpecifier, UDPInputSessionStatistics
    ] = dataclasses.field(default_factory=dict)
    """
    Basic input session statistics: instances of :class:`UDPInputSessionStatistics` keyed by their data specifier.
    """


class UDPTransport(pycyphal.transport.Transport):
    """
    The Cyphal/UDP (IP v4/v6) transport is designed for low-latency, high-throughput, high-reliability
    vehicular networks based on Ethernet.
    Please read the module documentation for details.
    """

    TRANSFER_ID_MODULO = UDPFrame.TRANSFER_ID_MASK + 1

    VALID_MTU_RANGE = 1200, 9000
    """
    The minimum is based on the IPv6 specification, which guarantees that the path MTU is at least 1280 bytes large.
    This value is also acceptable for virtually all IPv4 local or real-time networks.
    Lower MTU values shall not be used because they may lead to multi-frame transfer fragmentation where this is
    not expected by the designer, possibly violating the real-time constraints.

    A conventional Ethernet jumbo frame can carry up to 9 KiB (9216 bytes).
    These are the application-level MTU values, so we take overheads into account.
    """

    VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE = (1, 5)

    def __init__(
        self,
        local_ip_address: IPAddress | str,
        local_node_id: typing.Optional[int] = 0,
        *,  # The following parameters are keyword-only.
        mtu: int = min(VALID_MTU_RANGE),
        service_transfer_multiplier: int = 1,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
        anonymous: bool = False,
    ):
        """
        :param local_ip_address: Specifies which local network interface to use for this transport.

            Using ``INADDR_ANY`` here (i.e., ``0.0.0.0`` for IPv4) is not expected to work reliably or be portable
            because this configuration is, generally, incompatible with multicast sockets (even in the anonymous mode).
            In order to set up even a listening multicast socket, it is necessary to specify the correct local
            address such that the underlying IP stack is aware of which interface to receive multicast packets from.

            When the anonymous mode is enabled, it is quite possible to snoop on the network even if there is
            another node running locally on the same interface
            (because sockets are initialized with ``SO_REUSEADDR`` and ``SO_REUSEPORT``, when available).

        :param local_node_id: As explained previously, the node-ID is part of the UDP Frame.

            - If the value is None, an anonymous instance will be constructed.
              Emitted UDP frames will then report its :attr:`source_node_id` as None.

            - If the value is a non-negative integer, then we can setup both input and output sessions.

        :param mtu: The application-level MTU for outgoing packets.
            In other words, this is the maximum number of serialized bytes per Cyphal/UDP frame.
            Transfers where the number of payload bytes does not exceed this value will be single-frame transfers,
            otherwise, multi-frame transfers will be used.
            This setting affects only outgoing frames;
            the MTU of incoming frames is fixed at a sufficiently large value to accept any meaningful UDP frame.

            The default value is the smallest valid value for reasons of compatibility.

        :param service_transfer_multiplier: Deterministic data loss mitigation is disabled by default.
            This parameter specifies the number of times each outgoing service transfer will be repeated.
            This setting does not affect message transfers.

        :param loop: Deprecated.

        :param anonymous: DEPRECATED and scheduled for removal; replace with ``local_node_id=None``.
        """
        if anonymous:  # Backward compatibility. Will be removed.
            local_node_id = None
            warnings.warn("Parameter 'anonymous' is deprecated. Use 'local_node_id=None' instead.", DeprecationWarning)
        if loop:
            warnings.warn("The loop parameter is deprecated.", DeprecationWarning)

        if isinstance(local_ip_address, str):
            local_ip_address = ipaddress.ip_address(local_ip_address)

        assert (local_node_id is None) or (0 <= local_node_id < 0xFFFF)

        self._sock_factory = SocketFactory.new(local_ip_address)
        self._anonymous = local_node_id is None
        self._local_ip_address = local_ip_address
        self._local_node_id = local_node_id
        self._mtu = int(mtu)
        self._srv_multiplier = int(service_transfer_multiplier)

        low, high = self.VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE
        if not (low <= self._srv_multiplier <= high):
            raise ValueError(f"Invalid service transfer multiplier: {self._srv_multiplier}")

        low, high = self.VALID_MTU_RANGE
        if not (low <= self._mtu <= high):
            raise ValueError(f"Invalid MTU: {self._mtu} bytes")

        self._input_registry: typing.Dict[pycyphal.transport.InputSessionSpecifier, UDPInputSession] = {}
        self._output_registry: typing.Dict[pycyphal.transport.OutputSessionSpecifier, UDPOutputSession] = {}

        self._sniffer: typing.Optional[Sniffer] = None
        self._capture_handlers: typing.List[pycyphal.transport.CaptureCallback] = []

        self._closed = False
        self._statistics = UDPTransportStatistics()

        _logger.debug("%s: Initialized with local node-ID %s", self, self._local_node_id)

    @property
    def protocol_parameters(self) -> pycyphal.transport.ProtocolParameters:
        return pycyphal.transport.ProtocolParameters(
            transfer_id_modulo=self.TRANSFER_ID_MODULO,
            max_nodes=self._sock_factory.max_nodes,
            mtu=self._mtu,
        )

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return None if self._anonymous else self._local_node_id

    def close(self) -> None:
        self._closed = True
        for s in (*self.input_sessions, *self.output_sessions):
            try:
                s.close()
            except Exception as ex:  # pragma: no cover
                _logger.exception("%s: Failed to close %r: %s", self, s, ex)
        if self._sniffer is not None:
            self._sniffer.close()
            self._sniffer = None

    def get_input_session(
        self, specifier: pycyphal.transport.InputSessionSpecifier, payload_metadata: pycyphal.transport.PayloadMetadata
    ) -> UDPInputSession:
        self._ensure_not_closed()
        if specifier not in self._input_registry:

            def finalizer() -> None:
                del self._input_registry[specifier]

            sock = self._sock_factory.make_input_socket(self.local_node_id, specifier.data_specifier)
            if specifier.is_promiscuous:
                prom_stats = PromiscuousUDPInputSessionStatistics()
                self._statistics.received_datagrams[specifier] = prom_stats
                self._input_registry[specifier] = PromiscuousUDPInputSession(
                    specifier,
                    payload_metadata,
                    sock,
                    finalizer,
                    self._local_node_id,
                    prom_stats,
                )
            elif self.local_node_id is not None:
                sel_stats = SelectiveUDPInputSessionStatistics()
                self._statistics.received_datagrams[specifier] = sel_stats
                self._input_registry[specifier] = SelectiveUDPInputSession(
                    specifier,
                    payload_metadata,
                    sock,
                    finalizer,
                    self._local_node_id,
                    sel_stats,
                )
            else:
                raise OperationNotDefinedForAnonymousNodeError(
                    "Anonymous UDP Transport cannot create non-promiscuous input session"
                )
        out = self._input_registry[specifier]
        assert isinstance(out, UDPInputSession)
        assert out.specifier == specifier
        return out

    def get_output_session(
        self, specifier: pycyphal.transport.OutputSessionSpecifier, payload_metadata: pycyphal.transport.PayloadMetadata
    ) -> UDPOutputSession:
        self._ensure_not_closed()
        if specifier not in self._output_registry:

            # check if anonymous, in that case no service transfers are allowed
            if self._anonymous and isinstance(specifier.data_specifier, pycyphal.transport.ServiceDataSpecifier):
                raise OperationNotDefinedForAnonymousNodeError(
                    "Anonymous UDP Transport cannot create service output session"
                )

            def finalizer() -> None:
                del self._output_registry[specifier]

            multiplier = (
                self._srv_multiplier
                if isinstance(specifier.data_specifier, pycyphal.transport.ServiceDataSpecifier)
                else 1
            )
            sock = self._sock_factory.make_output_socket(specifier.remote_node_id, specifier.data_specifier)
            self._output_registry[specifier] = UDPOutputSession(
                specifier=specifier,
                payload_metadata=payload_metadata,
                mtu=self._mtu,
                multiplier=multiplier,
                sock=sock,
                source_node_id=self._local_node_id,
                finalizer=finalizer,
            )

        out = self._output_registry[specifier]
        assert isinstance(out, UDPOutputSession)
        assert out.specifier == specifier
        return out

    def sample_statistics(self) -> UDPTransportStatistics:
        return copy.copy(self._statistics)

    @property
    def input_sessions(self) -> typing.Sequence[UDPInputSession]:
        return list(self._input_registry.values())

    @property
    def output_sessions(self) -> typing.Sequence[UDPOutputSession]:
        return list(self._output_registry.values())

    @property
    def local_ip_address(self) -> IPAddress:
        assert isinstance(self._sock_factory, SocketFactory)
        return self._sock_factory.local_ip_address

    def begin_capture(self, handler: pycyphal.transport.CaptureCallback) -> None:
        """
        Reported events are of type :class:`UDPCapture`.

        In order for the network capture to work, the local machine should be connected to a SPAN port of the switch.
        See https://en.wikipedia.org/wiki/Port_mirroring and read the documentation for your networking hardware.
        Additional preconditions must be met depending on the platform:

        - On GNU/Linux, network capture requires that either the process is executed by root,
          or the raw packet capture capability ``CAP_NET_RAW`` is enabled.
          For more info read ``man 7 capabilities`` and consider checking the docs for Wireshark/libpcap.

        - On Windows, Npcap needs to be installed and configured; see https://nmap.org/npcap/.

        Packets that do not originate from the current Cyphal/UDP subnet (configured on this transport instance)
        are not reported via this interface.
        This restriction is critical because there may be other Cyphal/UDP networks running on the same physical
        L2 network segregated by different subnets, so that if foreign packets were not dropped,
        conflicts would occur.
        """
        self._ensure_not_closed()
        if self._sniffer is None:
            _logger.debug("%s: Starting UDP/IP packet capture (hope you have permissions)", self)
            self._sniffer = self._sock_factory.make_sniffer(self._process_capture)
        self._capture_handlers.append(handler)

    @property
    def capture_active(self) -> bool:
        return self._sniffer is not None

    @staticmethod
    def make_tracer() -> UDPTracer:
        """
        See :class:`UDPTracer`.
        """
        return UDPTracer()

    async def spoof(self, transfer: pycyphal.transport.AlienTransfer, monotonic_deadline: float) -> bool:
        """
        Not implemented yet. Always raises :class:`NotImplementedError`.
        When implemented, this method will rely on libpcap to emit spoofed link-layer packets.
        """
        raise NotImplementedError

    def _process_capture(self, capture: LinkLayerCapture) -> None:
        """This handler may be invoked from a different thread (the capture thread)."""
        pycyphal.util.broadcast(self._capture_handlers)(UDPCapture(capture.timestamp, capture.packet))

    def _ensure_not_closed(self) -> None:
        if self._closed:
            raise pycyphal.transport.ResourceClosedError(f"{self} is closed")

    def _get_repr_fields(self) -> typing.Tuple[typing.List[typing.Any], typing.Dict[str, typing.Any]]:
        return [repr(str(self.local_ip_address))], {
            "local_node_id": self.local_node_id,
            "service_transfer_multiplier": self._srv_multiplier,
            "mtu": self._mtu,
        }
