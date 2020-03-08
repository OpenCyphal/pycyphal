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
    demultiplexer: typing.Dict[pyuavcan.transport.DataSpecifier, UDPDemultiplexerStatistics] = \
        dataclasses.field(default_factory=dict)
    """
    Basic input session statistics: instances of :class:`UDPDemultiplexerStatistics` keyed by data specifier.
    """


class UDPTransport(pyuavcan.transport.Transport):
    """
    The UAVCAN/UDP (IP v4/v6) transport is designed for low-latency, high-throughput, high-reliability
    vehicular networks based on Ethernet.
    Please read the module documentation for details.
    """

    DEFAULT_SERVICE_TRANSFER_MULTIPLIER = 1
    """
    By default, service transfer multiplication is disabled for UDP.
    This option may be justified for extremely unreliable experimental networks.
    """

    VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE = (1, 5)

    DEFAULT_MTU = 1024
    """
    The recommended application-level MTU is one kibibyte. Lower values should not be used.
    This is compatible with the IPv6 minimum MTU requirement, which is 1280 bytes.
    The IPv4 has a lower MTU requirement of 576 bytes, but for local networks the MTU is normally much higher.
    The transport can always accept any MTU regardless of its configuration.
    """

    VALID_MTU_RANGE = (1024, 9000)
    """
    A conventional Ethernet jumbo frame can carry up to 9 KiB (9216 bytes).
    These are the application-level MTU values, so we take overheads into account.
    An attempt to transmit a larger frame than supported by L2 may lead to IP fragmentation,
    which is undesirable for time-deterministic networks.
    """

    NODE_ID_BIT_LENGTH = NetworkMap.NODE_ID_BIT_LENGTH
    """
    The maximum theoretical number of nodes on the network is determined by raising 2 into this power.
    A node-ID is the set of this many least significant bits of the IP address of the node.
    """

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

        :param mtu: The application-level MTU for outgoing packets.
            In other words, this is the maximum number of payload bytes per UDP frame.
            Transfers where the number of payload bytes does not exceed this value will be single-frame transfers,
            otherwise, multi-frame transfers will be used.
            This setting affects only outgoing frames;
            the MTU of incoming frames is fixed at a sufficiently large value to accept any meaningful UDP frame.

        :param service_transfer_multiplier: Deterministic data loss mitigation is disabled by default.
            This parameter specifies the number of times each outgoing service transfer will be repeated.
            This setting does not affect message transfers.

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
