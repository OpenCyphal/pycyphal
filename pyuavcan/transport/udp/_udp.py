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
from ._port_mapping import udp_port_from_data_specifier
from ._demultiplexer import UDPDemultiplexer, UDPDemultiplexerStatistics


# This is for internal use only: the maximum possible payload per UDP frame.
# We assume that it equals the maximum size of an Ethernet jumbo frame.
# We subtract the size of the L2/L3/L4 overhead here, and add one byte to enable packet truncation detection.
_MAX_UDP_MTU = 9 * 1024 - 20 - 8 + 1


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class UDPTransportStatistics(pyuavcan.transport.TransportStatistics):
    #: Basic input session statistics: instances of :class:`UDPDemultiplexerStatistics` keyed by data specifier.
    demultiplexer: typing.Dict[pyuavcan.transport.DataSpecifier, UDPDemultiplexerStatistics] = \
        dataclasses.field(default_factory=dict)


class UDPTransport(pyuavcan.transport.Transport):
    r"""
    The UDP transport is experimental and is not yet part of the UAVCAN specification.
    Future revisions may break wire compatibility until the transport is formally specified.
    Context: https://forum.uavcan.org/t/alternative-transport-protocols/324.

    The UDP transport is essentially a trivial stateless UDP blaster.
    In the spirit of UAVCAN, it is designed to be simple and robust.
    Much of the data handling work is offloaded to the standard underlying UDP/IP stack.

    The data specifier is manifested on the wire as the destination UDP port number;
    the mapping function is implemented in :func:`udp_port_from_data_specifier`.
    The source port number can be arbitrary (ephemeral), its value is ignored.

    UAVCAN uses a wide range of UDP ports: [15360, 49151].
    UDP/IP stacks that comply with the IANA ephemeral port range recommendations are expected to be
    compatible with this; otherwise, there may be port assignment conflicts.
    All new versions of MS Windows starting with Vista and Server 2008 are compatible with the IANA recommendations.
    Many versions of GNU/Linux, however, are not, but it can be fixed by manual reconfiguration:
    https://stackoverflow.com/questions/28573390/how-to-view-and-edit-the-ephemeral-port-range-on-linux.

    The node-ID of a node is the value of its host address (i.e., IP address with the subnet bits zeroed out);
    the bits above the :attr:`NODE_ID_BIT_LENGTH`-th bit shall be zero::

        IPv4 address:   127.000.012.123/8
        Subnet mask:    255.000.000.000
        Host mask:      000.255.255.255
                        \_/ \_________/
                      subnet    host
                     address   address
                                 \____/
                               node-ID=3195

        IPv6 address:   fe80:0000:0000:0000:0000:0000:0000:0c7b%enp6s0/64
        Subnet mask:    ffff:ffff:ffff:ffff:0000:0000:0000:0000
        Host mask:      0000:0000:0000:0000:ffff:ffff:ffff:ffff
                        \_________________/ \_________________/
                          subnet address        host address
                                                           \__/
                                                        node-ID=3195

    An IP address that does not match the above requirement cannot be mapped to a node-ID value.
    Nodes that are configured with such IP addresses are considered anonymous.
    Incoming traffic from IP addresses that cannot be mapped to a valid node-ID value is rejected;
    this behavior enables co-existence of UAVCAN/UDP with other UDP protocols on the same network.

    The concept of anonymous transfer is not defined for UDP/IP;
    in this transport, in order to be able to emit a transfer, the node shall have a valid node-ID value.
    This means that an anonymous UAVCAN/UDP node can only listen to broadcast
    network traffic (i.e., can subscribe to subjects) but cannot transmit anything.
    If address auto-configuration is desired, lower-level solutions should be used, such as DHCP.

    Both IPv4 and IPv6 are supported with minimal differences, although IPv6 is not expected to be useful in
    a vehicular network because virtually none of its advantages are relevant there,
    and the increased overhead is detrimental to the network's latency and throughput.
    If IPv6 is used, the flow-ID of UAVCAN packets is set to zero.

    Applications relying on this particular transport implementation will be unable to detect a node-ID conflict on
    the bus because the implementation discards all broadcast traffic originating from its own IP address.
    This is a very environment-specific edge case resulting from certain peculiarities of the Berkeley socket API.
    Other implementations of UAVCAN/UDP (particularly those for embedded systems) may not have this limitation.

    The datagram payload format is documented in :class:`UDPFrame`.
    Again, it is designed to be simple and low-overhead, which is not difficult considering that
    the entirety of the session specifier is reified through the UDP/IP stack:

    +---------------------------------------+---------------------------------------+
    | Parameter                             | Manifested in                         |
    +=======================================+=======================================+
    | Transfer priority                     |                                       |
    +---------------------------------------+                                       |
    | Transfer-ID                           | UDP datagram payload (frame header)   |
    +---------------------------------------+                                       |
    | Data type hash                        |                                       |
    +-------------------+-------------------+---------------------------------------+
    |                   | Route specifier   | IP address (least significant bits)   |
    | Session specifier +-------------------+---------------------------------------+
    |                   | Data specifier    | UDP destination port number           |
    +-------------------+-------------------+---------------------------------------+

    For unreliable networks, deterministic data loss mitigation is supported.
    This measure is only available for service transfers, not for message transfers due to their different semantics.
    If the probability of a frame loss exceeds the desired reliability threshold,
    the transport can be configured to repeat every outgoing service transfer a specified number of times,
    on the assumption that the probability of losing any given frame is uncorrelated (or weakly correlated)
    with that of its neighbors.
    For instance, suppose that a service transfer contains three frames, F0 to F2,
    and the service transfer multiplication factor is two,
    then the resulting frame sequence would be as follows::

        F0      F1      F2      F0      F1      F2
        \_______________/       \_______________/
           main copy             redundant copy
         (TX timestamp)      (never TX-timestamped)

        ------------------ time ------------------>

    As shown on the diagram, if the transmission timestamping is requested, only the first copy is timestamped.
    Further, any errors occurring during the transmission of redundant copies
    may be silently ignored by the stack, provided that the main copy is transmitted successfully.

    The resulting behavior in the provided example is that the transport network may
    lose up to three unique frames without affecting the application.
    In the following example, the frames F0 and F2 of the main copy are lost, but the transfer survives::

        F0 F1 F2 F0 F1 F2
        |  |  |  |  |  |
        x  |  x  |  |  \_____ F2 __________________________
           |     |  \________ F1 (redundant, discarded) x  \
           |     \___________ F0 ________________________  |
           \_________________ F1 ______________________  \ |
                                                       \ | |
        ----- time ----->                              v v v
                                                    reassembled
                                                    multi-frame
                                                     transfer

    For time-deterministic (real-time) networks this strategy is preferred over the conventional
    confirmation-retry approach (e.g., the TCP model) because it results in more predictable
    network load, lower worst-case latency, and is stateless (participants do not make assumptions
    about the state of other agents involved in data exchange).

    The UDP transport supports all transfer categories:

    +--------------------+--------------------------+---------------------------+
    | Supported transfers| Unicast                  | Broadcast                 |
    +====================+==========================+===========================+
    |**Message**         | Yes                      | Yes                       |
    +--------------------+--------------------------+---------------------------+
    |**Service**         | Yes                      | Banned by Specification   |
    +--------------------+--------------------------+---------------------------+
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
    #: An attempt to transmit a larger frame than supported by L2 may lead to IP fragmentation,
    #: which is undesirable for time-deterministic networks.
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
            All output sockets will be bound (see ``bind()``) to the specified local address.
            If the specified address is not available locally, initialization will fail with
            :class:`pyuavcan.transport.InvalidMediaConfigurationError`.

            If the specified IP address cannot be mapped to a valid node-ID, the local node will be anonymous.
            An IP address will be impossible to map to a valid node-ID if the address happens to be
            the broadcast address for the subnet (e.g., ``192.168.0.255/24``),
            or if the value of the host address exceeds the valid node-ID range (e.g.,
            given IP address ``127.123.123.123/8``, the host address is 8092539,
            which exceeds the range of valid node-ID values).

            If the local node is anonymous, any attempt to create an output session will fail with
            :class:`pyuavcan.transport.OperationNotDefinedForAnonymousNodeError`.

            For use on localhost, any IP address from the localhost range can be used;
            for example, ``127.0.0.123``.
            This generally does not work with physical interfaces;
            for example, if a host has one physical interface at ``192.168.1.200``,
            an attempt to run a node at ``192.168.1.201`` will trigger the media configuration error
            because ``bind()`` will fail with ``EADDRNOTAVAIL``.
            One can change the node-ID of a physical transport by altering the network
            interface configuration in the underlying operating system itself.

            IPv4 addresses shall have the network mask specified, this is necessary for the transport to
            determine the subnet's broadcast address (for broadcast UAVCAN transfers).
            The mask will also be used to derive the range of node-ID values for the subnet,
            capped by two raised to the power of the node-ID bit length.
            For example:

            - ``192.168.1.200/24`` -- a subnet with up to 255 UAVCAN nodes; for example:

                - ``192.168.1.0`` -- node-ID of zero (may be unusable depending on the network configuration).
                - ``192.168.1.254`` -- the maximum available node-ID in this subnet is 254.
                - ``192.168.1.255`` -- the broadcast address, not a valid node. If you specify this address,
                  the local node will be anonymous.

            - ``127.0.0.42/8`` -- a subnet with the maximum possible number of nodes ``2**NODE_ID_BIT_LENGTH``.
              The local loopback subnet is useful for testing.

                - ``127.0.0.1`` -- node-ID 1.
                - ``127.0.0.255`` -- node-ID 255.
                - ``127.0.15.255`` -- node-ID 4095.
                - ``127.123.123.123`` -- not a valid node-ID because it exceeds ``2**NODE_ID_BIT_LENGTH``.
                  All traffic from this address will be rejected as non-UAVCAN.
                  If used for local node, the local node will be anonymous.
                - ``127.255.255.255`` -- the broadcast address; notice that this address lies outside of the
                  node-ID-mapped space, no conflicts. If used for local node, the local node will be anonymous.

            IPv6 addresses may be specified without the mask, in which case it will be assumed to be
            equal ``128 - NODE_ID_BIT_LENGTH``.
            Don't forget to specify the scope-ID for link-local IPv6 addresses.

        :param mtu: The application-level MTU for outgoing packets. In other words, this is the maximum
            number of payload bytes per UDP frame. Transfers with a fewer number of payload bytes will be
            single-frame transfers, otherwise multi-frame transfers will be used.
            This setting affects only outgoing frames; the MTU of incoming frames is fixed at a sufficiently
            large value to accept any meaningful UDP frame.

        :param service_transfer_multiplier: Deterministic data loss mitigation is disabled by default.
            This parameter specifies the number of times each outgoing service transfer will be
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

        self._demultiplexer_registry: typing.Dict[pyuavcan.transport.DataSpecifier, UDPDemultiplexer] = {}
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
                udp_port_from_data_specifier(specifier.data_specifier)
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
        return f'<udp srv_mult="{self._srv_multiplier}">{self._network_map}</udp>'

    @property
    def local_ip_address_with_netmask(self) -> str:
        """
        The configured IP address of the local node with network mask.
        For example: ``192.168.1.200/24``.
        """
        return str(self._network_map)

    def _setup_input_session(self,
                             specifier:        pyuavcan.transport.InputSessionSpecifier,
                             payload_metadata: pyuavcan.transport.PayloadMetadata) -> None:
        """
        In order to set up a new input session, we have to link together a lot of objects. Tricky.
        Also, the setup and teardown actions shall be atomic. Hence the separate method.
        """
        assert specifier not in self._input_registry

        try:
            if specifier.data_specifier not in self._demultiplexer_registry:
                _logger.debug('%r: Setting up new demultiplexer for %s', self, specifier.data_specifier)
                # Service transfers cannot be broadcast.
                expect_broadcast = not isinstance(specifier.data_specifier, pyuavcan.transport.ServiceDataSpecifier)
                udp_port = udp_port_from_data_specifier(specifier.data_specifier)
                self._demultiplexer_registry[specifier.data_specifier] = UDPDemultiplexer(
                    sock=self._network_map.make_input_socket(udp_port, expect_broadcast),
                    udp_mtu=_MAX_UDP_MTU,
                    node_id_mapper=self._network_map.map_ip_address_to_node_id,
                    local_node_id=self.local_node_id,
                    statistics=self._statistics.demultiplexer.setdefault(specifier.data_specifier,
                                                                         UDPDemultiplexerStatistics()),
                    loop=self.loop,
                )

            cls: typing.Union[typing.Type[PromiscuousUDPInputSession], typing.Type[SelectiveUDPInputSession]] = \
                PromiscuousUDPInputSession if specifier.is_promiscuous else SelectiveUDPInputSession

            session = cls(specifier=specifier,
                          payload_metadata=payload_metadata,
                          loop=self.loop,
                          finalizer=lambda: self._teardown_input_session(specifier))

            # noinspection PyProtectedMember
            self._demultiplexer_registry[specifier.data_specifier].add_listener(specifier.remote_node_id,
                                                                                session._process_frame)
        except Exception:
            self._teardown_input_session(specifier)  # Rollback to ensure atomicity.
            raise

        self._input_registry[specifier] = session

    def _teardown_input_session(self, specifier: pyuavcan.transport.InputSessionSpecifier) -> None:
        """
        The finalizer may be invoked at any point during the setup process,
        so it must be able to deconstruct the pipeline even if it is not fully set up.
        This is why we have these try-except everywhere. Who knew that atomic transactions can be so messy?
        """
        # Unregister the session first.
        try:
            del self._input_registry[specifier]
        except LookupError:
            pass

        # Remove the session from the list of demultiplexer listeners.
        try:
            demux = self._demultiplexer_registry[specifier.data_specifier]
        except LookupError:
            pass    # The demultiplexer has not been set up yet, nothing to do.
        else:
            try:
                demux.remove_listener(specifier.remote_node_id)
            except LookupError:
                pass

            # Destroy the demultiplexer if there are no listeners left.
            if not demux.has_listeners:
                try:
                    _logger.debug('%r: Destroying %r for %s', self, demux, specifier.data_specifier)
                    demux.close()
                finally:
                    del self._demultiplexer_registry[specifier.data_specifier]

    def _ensure_not_closed(self) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(f'{self} is closed')
