# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
from typing import Mapping, Iterator, Type, Optional, TypeVar, Union, Sequence, Callable
import itertools
import pyuavcan
from .register import ValueProxy, Value


def make_transport(
    registers: Mapping[str, Union[ValueProxy, Value]],
    *,
    reconfigurable: bool = False,
) -> Optional[pyuavcan.transport.Transport]:
    """
    Construct a transport instance based on the configuration encoded in the supplied registers.
    If more than one transport is defined, a redundant instance will be constructed.

    The register schema is documented below per transport class
    (refer to the transport class documentation to find the defaults for optional registers).
    All transports also accept the following standard regsiters:

    +-------------------+-------------------+-----------------------------------------------------------------------+
    | Register name     | Register type     | Semantics                                                             |
    +===================+===================+=======================================================================+
    | ``uavcan.node.id``| ``natural16[1]``  | The node-ID to use. If not provided or the value exceeds the valid    |
    |                   |                   | range, the constructed node will be anonymous.                        |
    +-------------------+-------------------+-----------------------------------------------------------------------+

    ..  list-table:: :mod:`pyuavcan.transport.udp`
        :widths: 1 1 9
        :header-rows: 1

        * - Register name
          - Register type
          - Register semantics

        * - ``uavcan.udp.ip``
          - ``string``
          - Whitespace-separated list of /16 IP subnet addresses.
            16 least significant bits are replaced with the node-ID if configured, otherwise left unchanged.
            E.g.: ``127.42.0.42``, node-ID 257, result ``127.42.1.1``.
            E.g.: ``127.42.0.42``, anonymous, result ``127.42.0.42``.

        * - ``uavcan.udp.duplicate_service_transfers``
          - ``bit[1]``
          - Apply deterministic data loss mitigation to RPC-service transfers by setting multiplication factor = 2.

        * - ``uavcan.udp.mtu``
          - ``natural16[1]``
          - The MTU for all constructed transport instances.

    ..  list-table:: :mod:`pyuavcan.transport.serial`
        :widths: 1 1 9
        :header-rows: 1

        * - Register name
          - Register type
          - Register semantics

        * - ``uavcan.serial.port``
          - ``string``
          - Whitespace-separated list of serial port names.
            E.g.: ``/dev/ttyACM0``, ``COM9``, ``socket://localhost:50905``.

        * - ``uavcan.serial.duplicate_service_transfers``
          - ``bit[1]``
          - Apply deterministic data loss mitigation to RPC-service transfers by setting multiplication factor = 2.

        * - ``uavcan.serial.baudrate``
          - ``natural16[1]``
          - The baudrate to set for all specified serial ports. Leave unchanged if not set.

    ..  list-table:: :mod:`pyuavcan.transport.can`
        :widths: 1 1 9
        :header-rows: 1

        * - Register name
          - Register type
          - Register semantics

        * - ``uavcan.can.iface``
          - ``string``
          - Whitespace-separated list of CAN iface names.
            Each iface name shall follow the format defined in :class:`pyuavcan.transport.can.media.pythoncan`.
            E.g.: ``socketcan:vcan0``.

        * - ``uavcan.can.mtu``
          - ``bit[1]``
          - The MTU value to use with all constructed CAN transports.
            If not provided, a sensible default is deduced using heuristics.

        * - ``uavcan.can.bitrate``
          - ``natural32[2]``
          - The bitrates to use for all constructed CAN transports
            for arbitration (first value) and data (second value) segments.
            To use Classic CAN, set both to the same value and set MTU = 8.
            If not provided, a sensible default is deduced using heuristics.

    ..  list-table:: :mod:`pyuavcan.transport.loopback`
        :widths: 1 1 9
        :header-rows: 1

        * - Register name
          - Register type
          - Register semantics

        * - ``uavcan.loopback``
          - ``bit[1]``
          - If True, a loopback transport will be constructed. This is intended for testing only.

    :param registers:
        A mapping of :class:`str` to :class:`pyuavcan.application.register.ValueProxy`
        (e.g., an instance of :class:`pyuavcan.application.register.Registry` or a regular dict).

    :param reconfigurable:
        If False (default), the return value is:

        - None if the registers do not encode a valid transport configuration.
        - A single transport instance if a non-redundant configuration is defined.
        - An instance of :class:`pyuavcan.transport.RedundantTransport` if more than one transport
          configuration is defined.

        If True, then the returned instance is always of type :class:`pyuavcan.transport.RedundantTransport`,
        where the set of inferiors is empty if no transport configuration is defined.
        This case is intended for applications that may want to change the transport configuration afterwards.

    :return:
        None if no transport is configured AND ``reconfigurable`` is False.
        Otherwise, a functional transport instance is returned.

    >>> from pyuavcan.application.register import Value, String, Natural16
    >>> reg = {
    ...     "uavcan.udp.ip": Value(string=String("127.99.0.0")),
    ...     "uavcan.node.id": Value(natural16=Natural16([257])),
    ... }
    >>> tr = make_transport(reg)
    >>> tr
    UDPTransport('127.99.1.1', local_node_id=257, ...)
    >>> tr.close()
    >>> tr = make_transport(reg, reconfigurable=True)    # Same but reconfigurable.
    >>> tr                                                              # Wrapped into RedundantTransport.
    RedundantTransport(UDPTransport('127.99.1.1', local_node_id=257, ...))
    >>> tr.close()

    >>> reg = {                                                         # Triply-redundant heterogeneous transport:
    ...     "uavcan.udp.ip":      Value(string=String("127.99.0.15 127.111.0.15")),     # Double UDP transport
    ...     "uavcan.serial.port": Value(string=String("socket://localhost:50905")),     # Single serial transport
    ... }
    >>> tr = make_transport(reg)     # The node-ID was not set, so the transport is anonymous.
    >>> tr                                          # doctest: +NORMALIZE_WHITESPACE
    RedundantTransport(UDPTransport('127.99.0.15',  local_node_id=None, ...),
                       UDPTransport('127.111.0.15', local_node_id=None, ...),
                       SerialTransport('socket://localhost:50905', local_node_id=None, ...))
    >>> tr.close()

    >>> reg = {
    ...     "uavcan.can.iface":   Value(string=String("virtual: virtual:")),
    ...     "uavcan.can.mtu":     Value(natural16=Natural16([64])),
    ...     "uavcan.can.bitrate": Value(natural16=Natural16([1_000_000, 4_000_000])),
    ...     "uavcan.node.id":     Value(natural16=Natural16([123])),
    ... }
    >>> tr = make_transport(reg)
    >>> tr                                          # doctest: +NORMALIZE_WHITESPACE
    RedundantTransport(CANTransport(PythonCANMedia('virtual:', mtu=64), local_node_id=123),
                       CANTransport(PythonCANMedia('virtual:', mtu=64), local_node_id=123))
    >>> tr.close()

    >>> reg = {
    ...     "uavcan.udp.ip": Value(string=String("127.99.1.1")),    # Per the standard register specification,
    ...     "uavcan.node.id": Value(natural16=Natural16([0xFFFF])), # value 0xFFFF also means unset/anonymous.
    ... }
    >>> tr = make_transport(reg)
    >>> tr
    UDPTransport('127.99.1.1', local_node_id=None, ...)
    >>> tr.close()

    >>> tr = make_transport({})
    >>> tr is None
    True
    >>> tr = make_transport({}, reconfigurable=True)
    >>> tr                  # Redundant transport with no inferiors.
    RedundantTransport()
    """

    reg = _Adapter(registers)

    node_id = reg.cast("uavcan.node.id", int)
    # Per Specification, if uavcan.node.id = 65535, the node-ID is unspecified.
    # TODO: currently, we raise an error if the node-ID setting exceeds the maximum allowed value for the current
    # transport, but the spec recommends that we should handle this as if the node-ID was not set at all.
    if node_id is not None and not (0 <= node_id < 0xFFFF):
        node_id = None

    transports = list(itertools.chain(*(f(reg, node_id) for f in _SPECIALIZATIONS)))
    assert all(isinstance(t, pyuavcan.transport.Transport) for t in transports)

    if not reconfigurable:
        if not transports:
            return None
        if len(transports) == 1:
            return transports[0]

    from pyuavcan.transport.redundant import RedundantTransport

    red = RedundantTransport()
    for tr in transports:
        red.attach_inferior(tr)
    return red


class _Adapter(Mapping[str, ValueProxy]):
    _RegisterType = TypeVar("_RegisterType", int, float, bool, str, bytes)

    def __init__(self, inner: Mapping[str, Union[ValueProxy, Value]]) -> None:
        self._inner = inner

    def cast(self, name: str, ty: Type[_RegisterType]) -> Optional[_RegisterType]:
        try:
            return ty(self[name])
        except KeyError:
            return None

    def __getitem__(self, key: str) -> ValueProxy:
        return ValueProxy(self._inner[key])

    def __iter__(self) -> Iterator[str]:
        return self._inner.__iter__()

    def __len__(self) -> int:
        return len(self._inner)


def _make_udp(reg: _Adapter, node_id: Optional[int]) -> Iterator[pyuavcan.transport.Transport]:
    try:
        ip_list = str(ValueProxy(reg["uavcan.udp.ip"])).split()
    except KeyError:
        return

    from pyuavcan.transport.udp import UDPTransport

    mtu = reg.cast("uavcan.udp.mtu", int) or min(UDPTransport.VALID_MTU_RANGE)
    srv_mult = int(reg.cast("uavcan.udp.duplicate_service_transfers", bool) or False) + 1
    for ip in ip_list:
        yield UDPTransport(ip, node_id, mtu=mtu, service_transfer_multiplier=srv_mult)


def _make_serial(reg: _Adapter, node_id: Optional[int]) -> Iterator[pyuavcan.transport.Transport]:
    try:
        port_list = str(ValueProxy(reg["uavcan.serial.port"])).split()
    except KeyError:
        return

    from pyuavcan.transport.serial import SerialTransport

    srv_mult = int(reg.cast("uavcan.serial.duplicate_service_transfers", bool) or False) + 1
    baudrate = reg.cast("uavcan.serial.baudrate", int)
    for port in port_list:
        yield SerialTransport(str(port), node_id, service_transfer_multiplier=srv_mult, baudrate=baudrate)


def _make_can(reg: _Adapter, node_id: Optional[int]) -> Iterator[pyuavcan.transport.Transport]:
    try:
        iface_list = str(ValueProxy(reg["uavcan.can.iface"])).split()
    except KeyError:
        return

    from pyuavcan.transport.can import CANTransport

    mtu = reg.cast("uavcan.can.mtu", int)
    try:
        br_arb, br_data = ValueProxy(reg["uavcan.can.bitrate"]).ints
    except (KeyError, ValueError):
        br_arb = 1_000_000
        br_data = br_arb * (4 if mtu is None or mtu > 8 else 1)

    for iface in iface_list:
        media: pyuavcan.transport.can.media.Media
        if iface.lower().startswith("socketcan:"):
            from pyuavcan.transport.can.media.socketcan import SocketCANMedia

            mtu = mtu or (8 if br_arb == br_data else 64)
            media = SocketCANMedia(iface.split(":")[-1], mtu=mtu)
        else:
            from pyuavcan.transport.can.media.pythoncan import PythonCANMedia

            media = PythonCANMedia(iface, br_arb if br_arb == br_data else (br_arb, br_data), mtu)
        yield CANTransport(media, node_id)


def _make_loopback(reg: _Adapter, node_id: Optional[int]) -> Iterator[pyuavcan.transport.Transport]:
    if reg.cast("uavcan.loopback", bool):
        from pyuavcan.transport.loopback import LoopbackTransport

        yield LoopbackTransport(node_id)


_SPECIALIZATIONS: Sequence[Callable[[_Adapter, Optional[int]], Iterator[pyuavcan.transport.Transport]]] = [
    v for k, v in globals().items() if callable(v) and k.startswith("_make_")
]
assert len(_SPECIALIZATIONS) >= 4
