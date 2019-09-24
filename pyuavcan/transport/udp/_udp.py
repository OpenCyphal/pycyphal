#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import copy
import typing
import asyncio
import logging
import dataclasses
import pyuavcan
from ._session import UDPInputSession, SelectiveUDPInputSession, PromiscuousUDPInputSession
from ._session import UDPOutputSession
from ._frame import UDPFrame
from ._network_map import NetworkMap
from ._port_mapping import map_data_specifier_to_udp_port
from ._demultiplexer import Demultiplexer, DemultiplexerStatistics


# This is for internal use only: the maximum possible payload per UDP frame.
# We assume that it equals the maximum size of an Ethernet jumbo frame.
# We subtract the size of the L2/L3/L4 overhead here, and add one byte to enable packet truncation detection.
_MAX_UDP_MTU = 9 * 1024 - 20 - 8 + 1


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class UDPTransportStatistics(pyuavcan.transport.TransportStatistics):
    demultiplexer_statistics: typing.Dict[pyuavcan.transport.DataSpecifier, DemultiplexerStatistics] = \
        dataclasses.field(default_factory=dict)


class UDPTransport(pyuavcan.transport.Transport):
    """
    The UDP transport is experimental and is not yet part of the UAVCAN specification.
    Future revisions may break wire compatibility until the transport is formally specified.
    Context: https://forum.uavcan.org/t/alternative-transport-protocols/324.

    Incoming traffic from IP addresses that cannot be mapped to a valid node-ID value is rejected.

    If IPv6 is used, the flow-ID of UAVCAN packets shall be zero.

    UAVCAN uses a wide range of UDP ports [15360, 49151].
    Operating systems that comply with the IANA ephemeral port range recommendations are expected to be
    compatible with this; otherwise there may be port assignment conflicts.
    All new versions of MS Windows starting with Vista and Server 2008 are compatible with the IANA recommendations.
    Many versions of GNU/Linux, however, are not, but it can be fixed by manual reconfiguration:
    https://stackoverflow.com/questions/28573390/how-to-view-and-edit-the-ephemeral-port-range-on-linux.

    The concept of anonymous node is not defined for UDP/IP; in this transport, every node always has a node-ID.
    If address auto-configuration is desired, lower-level solutions should be used, such as DHCP.

    The UDP transport supports all transfer categories:

    +--------------------+--------------------------+---------------------------+
    | Supported transfers| Unicast                  | Broadcast                 |
    +====================+==========================+===========================+
    |**Message**         | Yes                      | Yes                       |
    +-----------+--------+--------------------------+---------------------------+
    |           |Request | Yes                      | Yes                       |
    |**Service**+--------+--------------------------+---------------------------+
    |           |Response| Yes                      | Banned by Specification   |
    +-----------+--------+--------------------------+---------------------------+
    """

    #: By default, service transfer multiplication is disabled for UDP.
    #: This option is only justified for extremely unreliable experimental networks, not in production.
    DEFAULT_SERVICE_TRANSFER_MULTIPLIER = 1
    VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE = (1, 5)

    #: The recommended application-level MTU is one kibibyte. Lower values should not be used.
    #: This is compatible with the IPv6 minimum MTU requirement, which is 1280 bytes.
    #: The IPv4 has a lower MTU requirement of 576 bytes, but for local networks the MTU is normally much higher.
    #: The transport can always accept any MTU regardless of its configuration.
    DEFAULT_MTU = 1024

    #: A conventional Ethernet jumbo frame can carry up to 9 KiB (9216 bytes).
    #: These are the application-level MTU values, so we take overheads into account.
    #: An attempt to transmit a larger frame than supported by L2 will lead to IP fragmentation.
    VALID_MTU_RANGE = (1024, 9000)

    #: The maximum theoretical number of nodes on the network is determined by raising 2 into this power.
    #: A node-ID is the set of this many least significant bits of the IP address of the node.
    NODE_ID_BIT_LENGTH = NetworkMap.NODE_ID_BIT_LENGTH

    def __init__(self,
                 ip_address:                  str,
                 mtu:                         int = DEFAULT_MTU,
                 service_transfer_multiplier: int = DEFAULT_SERVICE_TRANSFER_MULTIPLIER,
                 loop:                        typing.Optional[asyncio.AbstractEventLoop] = None):
        """
        :param ip_address: Specifies which local IP address to use for this transport.
            This setting also implicitly specifies the network interface to use.
            All sockets will be bound (see ``bind()``) to the specified local address.
            If the specified address is not available locally, or if the specified address cannot be mapped to
            a valid local node-ID, the initialization will fail with
            :class:`pyuavcan.transport.InvalidMediaConfigurationError`.

            IPv4 addresses shall have the network mask specified, this is necessary for the transport to
            determine the subnet's broadcast address (for broadcast UAVCAN transfers).
            The mask will also be used to derive the range of node-ID values for the subnet,
            capped by two raised to the power of the node-ID bit length.
            For example:

            - ``192.168.1.200/24`` -- a subnet with up to 255 UAVCAN nodes; for example:

                - ``192.168.1.0`` -- node-ID of zero (may be unusable depending on the network configuration).
                - ``192.168.1.254`` -- the maximum available node-ID in this subnet is 254.
                - ``192.168.1.255`` -- the broadcast address, not a valid node.

            - ``127.100.0.42/16`` -- a subnet with the maximum possible number of nodes ``2**NODE_ID_BIT_LENGTH``.

                - ``127.100.0.1`` -- node-ID 1.
                - ``127.100.0.255`` -- node-ID 255.
                - ``127.100.15.255`` -- node-ID 4095.
                - ``127.100.255.123`` -- not a valid node-ID because it exceeds ``2**NODE_ID_BIT_LENGTH``.
                  All traffic from this address will be rejected as non-UAVCAN.
                - ``127.100.255.255`` -- the broadcast address; notice that this address lies outside of the
                  node-ID-mapped space, no conflicts.

            IPv6 addresses may be specified without the mask, in which case it will be assumed to be
            equal ``128 - NODE_ID_BIT_LENGTH``.
            Don't forget to specify the scope-ID for link-local IPv6 addresses.

        :param mtu: The application-level MTU for outgoing packets. In other words, this is the maximum
            number of payload bytes per UDP frame. Transfers with a fewer number of payload bytes will be
            single-frame transfers, otherwise multi-frame transfers will be used.
            This setting affects only outgoing frames; the MTU of incoming frames may be arbitrary.

        :param service_transfer_multiplier: Specifies the number of times each outgoing service transfer will be
            repeated. The duplicates are emitted subsequently immediately following the original. This feature
            can be used to reduce the likelihood of service transfer loss over unreliable networks. Assuming that
            the probability of transfer loss ``P`` is time-invariant, the influence of the multiplier ``M`` can
            be approximately modeled as ``P' = P^M``. For example, given a network that successfully delivers 90%
            of transfers, and the probabilities of adjacent transfer loss are uncorrelated, the multiplication
            factor of 2 can increase the link reliability up to ``100% - (100% - 90%)^2 = 99%``. Removal of
            duplicate transfers at the opposite end of the link is natively guaranteed by the UAVCAN protocol;
            no special activities are needed there (read the UAVCAN Specification for background). This setting
            does not affect message transfers.

        :param loop: The event loop to use. Defaults to :func:`asyncio.get_event_loop`.
        """
        self._network_map = NetworkMap.new(ip_address)
        self._mtu = int(mtu)
        self._srv_multiplier = int(service_transfer_multiplier)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        low, high = self.VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE
        if not (low <= self._srv_multiplier <= high):
            raise ValueError(f'Invalid service transfer multiplier: {self._srv_multiplier}')

        low, high = self.VALID_MTU_RANGE
        if not (low <= self._mtu <= high):
            raise ValueError(f'Invalid MTU: {self._mtu} bytes')

        _logger.debug(f'IP: {self._network_map}; max nodes: {self._network_map.max_nodes}; '
                      f'local node-ID: {self.local_node_id}')

        self._demultiplexer_registry: typing.Dict[pyuavcan.transport.DataSpecifier, Demultiplexer] = {}
        self._input_registry: typing.Dict[pyuavcan.transport.InputSessionSpecifier, UDPInputSession] = {}
        self._output_registry: typing.Dict[pyuavcan.transport.OutputSessionSpecifier, UDPOutputSession] = {}

        self._closed = False
        self._statistics = UDPTransportStatistics()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        return pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=UDPFrame.TRANSFER_ID_MASK + 1,
            max_nodes=self._network_map.max_nodes,
            mtu=self._mtu,
        )

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._network_map.local_node_id

    def set_local_node_id(self, node_id: int) -> None:
        _ = node_id
        raise pyuavcan.transport.InvalidTransportConfigurationError(
            f'Cannot assign the node-ID of a UDP transport. '
            f'Configure the local IP address via the operating system or use DHCP.'
        )

    def close(self) -> None:
        self._closed = True
        for s in (*self.input_sessions, *self.output_sessions):
            try:
                s.close()
            except Exception as ex:  # pragma: no cover
                _logger.exception('%s: Failed to close %r: %s', self, s, ex)

    def get_input_session(self,
                          specifier:        pyuavcan.transport.InputSessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> UDPInputSession:
        self._ensure_not_closed()
        if specifier not in self._input_registry:
            self._setup_input_session(specifier, payload_metadata)
        assert specifier.data_specifier in self._demultiplexer_registry
        out = self._input_registry[specifier]
        assert isinstance(out, UDPInputSession)
        assert out.specifier == specifier
        return out

    def get_output_session(self,
                           specifier:        pyuavcan.transport.OutputSessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> UDPOutputSession:
        self._ensure_not_closed()
        if specifier not in self._output_registry:
            def finalizer() -> None:
                del self._output_registry[specifier]

            multiplier = \
                self._srv_multiplier if isinstance(specifier.data_specifier, pyuavcan.transport.ServiceDataSpecifier) \
                else 1
            sock = self._network_map.make_output_socket(
                specifier.remote_node_id,
                map_data_specifier_to_udp_port(specifier.data_specifier)
            )
            self._output_registry[specifier] = UDPOutputSession(
                specifier=specifier,
                payload_metadata=payload_metadata,
                mtu=self._mtu,
                multiplier=multiplier,
                sock=sock,
                loop=self._loop,
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
    def descriptor(self) -> str:
        return f'<udp mtu="{self._mtu}" srv_mult="{self._srv_multiplier}">{self._network_map}</udp>'

    def _setup_input_session(self,
                             specifier:        pyuavcan.transport.InputSessionSpecifier,
                             payload_metadata: pyuavcan.transport.PayloadMetadata) -> None:
        """
        In order to set up a new input session, we have to link together a lot of objects. Tricky.
        So we extract this into a separate method, where the precondition is that the session does not
        exist and the post-condition is that it does exist.
        """
        assert specifier not in self._input_registry

        if specifier.data_specifier not in self._demultiplexer_registry:
            self._demultiplexer_registry[specifier.data_specifier] = Demultiplexer(
                sock=self._network_map.make_input_socket(map_data_specifier_to_udp_port(specifier.data_specifier)),
                udp_mtu=_MAX_UDP_MTU,
                node_id_mapper=self._network_map.map_ip_address_to_node_id,
                statistics=self._statistics.demultiplexer_statistics.setdefault(specifier.data_specifier,
                                                                                DemultiplexerStatistics()),
                loop=self.loop,
            )
            _logger.debug('%r: New %r for %s',
                          self, self._demultiplexer_registry[specifier.data_specifier], specifier.data_specifier)

        demux = self._demultiplexer_registry[specifier.data_specifier]

        def finalizer() -> None:
            del self._input_registry[specifier]
            try:
                demux.remove_listener(specifier.remote_node_id)
            finally:
                if not demux.has_listeners:
                    try:
                        _logger.debug('%r: Destroying %r for %s', self, demux, specifier.data_specifier)
                        demux.close()
                    finally:
                        assert self._demultiplexer_registry[specifier.data_specifier] is demux
                        del self._demultiplexer_registry[specifier.data_specifier]

        cls: typing.Union[typing.Type[PromiscuousUDPInputSession], typing.Type[SelectiveUDPInputSession]] = \
            PromiscuousUDPInputSession if specifier.is_promiscuous else SelectiveUDPInputSession
        session = cls(
            specifier=specifier,
            payload_metadata=payload_metadata,
            loop=self.loop,
            finalizer=finalizer,
        )
        # noinspection PyProtectedMember
        demux.add_listener(specifier.remote_node_id, session._process_frame)
        self._input_registry[specifier] = session

    def _ensure_not_closed(self) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(f'{self} is closed')
