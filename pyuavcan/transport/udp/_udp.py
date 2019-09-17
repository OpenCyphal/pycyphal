#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import copy
import typing
import asyncio
import logging
import ipaddress
import dataclasses
import pyuavcan
from ._session import UDPInputSession, UDPOutputSession
from ._frame import UDPFrame


_logger = logging.getLogger(__name__)


IPNetwork = typing.Union[ipaddress.IPv4Network,
                         ipaddress.IPv6Network]

IPAddress = typing.Union[ipaddress.IPv4Address,
                         ipaddress.IPv6Address]

IPAddressSequence = typing.Union[typing.Sequence[ipaddress.IPv4Address],
                                 typing.Sequence[ipaddress.IPv6Address]]


@dataclasses.dataclass
class UDPTransportStatistics(pyuavcan.transport.TransportStatistics):
    pass


class UDPTransport(pyuavcan.transport.Transport):
    """
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

    def __init__(self,
                 network:                     typing.Union[str, IPNetwork],
                 mtu:                         int = DEFAULT_MTU,
                 service_transfer_multiplier: int = DEFAULT_SERVICE_TRANSFER_MULTIPLIER,
                 loop:                        typing.Optional[asyncio.AbstractEventLoop] = None):
        self._network = ipaddress.ip_network(network)
        self._mtu = int(mtu)
        self._srv_multiplier = int(service_transfer_multiplier)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        low, high = self.VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE
        if not (low <= self._srv_multiplier <= high):
            raise ValueError(f'Invalid service transfer multiplier: {self._srv_multiplier}')

        low, high = self.VALID_MTU_RANGE
        if not (low <= self._mtu <= high):
            raise ValueError(f'Invalid MTU: {self._mtu} bytes')

        self._input_registry: typing.Dict[pyuavcan.transport.SessionSpecifier, UDPInputSession] = {}
        self._output_registry: typing.Dict[pyuavcan.transport.SessionSpecifier, UDPOutputSession] = {}

        self._hosts: IPAddressSequence = tuple(sorted(self._network.hosts))
        self._closed = False
        self._statistics = UDPTransportStatistics()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        return pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=UDPFrame.TRANSFER_ID_MASK + 1,
            max_nodes=len(self._hosts),
            mtu=self._mtu,
        )

    @property
    def hosts(self) -> IPAddressSequence:
        """
        The ordered list of hosts on the network.
        The index of a host on this list equals its node-ID.
        """
        return self._hosts

    @property
    def local_node_id(self) -> typing.Optional[int]:
        pass

    def set_local_node_id(self, node_id: int) -> None:
        pass

    def close(self) -> None:
        self._closed = True
        raise NotImplementedError

    def get_input_session(self,
                          specifier:        pyuavcan.transport.SessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> UDPInputSession:
        raise NotImplementedError

    def get_output_session(self,
                           specifier:        pyuavcan.transport.SessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> UDPOutputSession:
        raise NotImplementedError

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
        return f'<udp mtu="{self._mtu}" srv_mult="{self._srv_multiplier}">{self._network}</udp>'
