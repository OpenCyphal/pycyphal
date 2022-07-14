# Copyright (c) 2021 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import sys
from typing import Iterator, Optional, Sequence, Callable
import itertools
import pycyphal
from .register import ValueProxy, Natural16, Natural32, RelaxedValue

if sys.version_info >= (3, 9):
    from collections.abc import MutableMapping
else:  # pragma: no cover
    from typing import MutableMapping  # pylint: disable=ungrouped-imports


def make_transport(
    registers: MutableMapping[str, ValueProxy],
    *,
    reconfigurable: bool = False,
) -> Optional[pycyphal.transport.Transport]:
    """
    Constructs a transport instance based on the configuration encoded in the supplied registers.
    If more than one transport is defined, a redundant instance will be constructed.

    The register schema is documented below per transport class
    (refer to the transport class documentation to find the defaults for optional registers).
    All transports also accept the following standard regsiters:

    +-------------------+-------------------+-----------------------------------------------------------------------+
    | Register name     | Register type     | Semantics                                                             |
    +===================+===================+=======================================================================+
    | ``uavcan.node.id``| ``natural16[1]``  | The node-ID to use. If the value exceeds the valid                    |
    |                   |                   | range, the constructed node will be anonymous.                        |
    +-------------------+-------------------+-----------------------------------------------------------------------+

    ..  list-table:: :mod:`pycyphal.transport.udp`
        :widths: 1 1 9
        :header-rows: 1

        * - Register name
          - Register type
          - Register semantics

        * - ``uavcan.udp.iface``
          - ``string``
          - Whitespace-separated list of /16 IP subnet addresses.
            16 least significant bits are replaced with the node-ID if configured, otherwise left unchanged.
            E.g.: ``127.42.0.42``: node-ID 257, result ``127.42.1.1``;
            ``127.42.0.42``: anonymous, result ``127.42.0.42``.

        * - ``uavcan.udp.duplicate_service_transfers``
          - ``bit[1]``
          - Apply deterministic data loss mitigation to RPC-service transfers by setting multiplication factor = 2.

        * - ``uavcan.udp.mtu``
          - ``natural16[1]``
          - The MTU for all constructed transport instances.

    ..  list-table:: :mod:`pycyphal.transport.serial`
        :widths: 1 1 9
        :header-rows: 1

        * - Register name
          - Register type
          - Register semantics

        * - ``uavcan.serial.iface``
          - ``string``
          - Whitespace-separated list of serial port names.
            E.g.: ``/dev/ttyACM0``, ``COM9``, ``socket://127.0.0.1:50905``.

        * - ``uavcan.serial.duplicate_service_transfers``
          - ``bit[1]``
          - Apply deterministic data loss mitigation to RPC-service transfers by setting multiplication factor = 2.

        * - ``uavcan.serial.baudrate``
          - ``natural32[1]``
          - The baudrate to set for all specified serial ports. Leave unchanged if zero.

    ..  list-table:: :mod:`pycyphal.transport.can`
        :widths: 1 1 9
        :header-rows: 1

        * - Register name
          - Register type
          - Register semantics

        * - ``uavcan.can.iface``
          - ``string``
          - Whitespace-separated list of CAN iface names.
            Each iface name shall follow the format defined in :mod:`pycyphal.transport.can.media.pythoncan`.
            E.g.: ``socketcan:vcan0``.
            On GNU/Linux, the ``socketcan:`` prefix selects :mod:`pycyphal.transport.can.media.socketcan`
            instead of PythonCAN.
            All platforms support the ``candump:`` prefix, which selects :mod:`pycyphal.transport.can.media.candump`;
            the text after colon is the path of the log file;
            e.g., ``candump:/home/pavel/candump-2022-07-14_150815.log``.

        * - ``uavcan.can.mtu``
          - ``natural16[1]``
          - The MTU value to use with all constructed CAN transports.
            Values other than 8 and 64 should not be used.

        * - ``uavcan.can.bitrate``
          - ``natural32[2]``
          - The bitrates to use for all constructed CAN transports
            for arbitration (first value) and data (second value) segments.
            To use Classic CAN, set both to the same value and set MTU = 8.

    ..  list-table:: :mod:`pycyphal.transport.loopback`
        :widths: 1 1 9
        :header-rows: 1

        * - Register name
          - Register type
          - Register semantics

        * - ``uavcan.loopback``
          - ``bit[1]``
          - If True, a loopback transport will be constructed. This is intended for testing only.

    :param registers:
        A mutable mapping of :class:`str` to :class:`pycyphal.application.register.ValueProxy`.
        Normally, it should be constructed by :func:`pycyphal.application.make_registry`.

    :param reconfigurable:
        If False (default), the return value is:

        - None if the registers do not encode a valid transport configuration.
        - A single transport instance if a non-redundant configuration is defined.
        - An instance of :class:`pycyphal.transport.RedundantTransport` if more than one transport
          configuration is defined.

        If True, then the returned instance is always of type :class:`pycyphal.transport.RedundantTransport`,
        where the set of inferiors is empty if no transport configuration is defined.
        This case is intended for applications that may want to change the transport configuration afterwards.

    :return:
        None if no transport is configured AND ``reconfigurable`` is False.
        Otherwise, a functional transport instance is returned.

    :raises:
        - :class:`pycyphal.application.register.MissingRegisterError` if a register is expected but cannot be found.
        - :class:`pycyphal.application.register.ValueConversionError` if a register is found but its value
          cannot be converted to the correct type.

    ..  doctest::
        :hide:

        >>> import tests
        >>> tests.asyncio_allow_event_loop_access_from_top_level()

    >>> from pycyphal.application.register import ValueProxy, Natural16, Natural32
    >>> reg = {
    ...     "uavcan.udp.iface": ValueProxy("127.99.0.0"),
    ...     "uavcan.node.id": ValueProxy(Natural16([257])),
    ... }
    >>> tr = make_transport(reg)
    >>> tr
    UDPTransport('127.99.1.1', local_node_id=257, ...)
    >>> tr.close()
    >>> tr = make_transport(reg, reconfigurable=True)                   # Same but reconfigurable.
    >>> tr                                                              # Wrapped into RedundantTransport.
    RedundantTransport(UDPTransport('127.99.1.1', local_node_id=257, ...))
    >>> tr.close()

    >>> int(reg["uavcan.udp.mtu"])      # Defaults created automatically to expose all configurables.
    1200
    >>> int(reg["uavcan.can.mtu"])
    64
    >>> reg["uavcan.can.bitrate"].ints
    [1000000, 4000000]

    >>> reg = {                                             # Triply-redundant heterogeneous transport:
    ...     "uavcan.udp.iface":    ValueProxy("127.99.0.15 127.111.0.15"),  # Double UDP transport
    ...     "uavcan.serial.iface": ValueProxy("socket://127.0.0.1:50905"),  # Serial transport
    ... }
    >>> tr = make_transport(reg)                            # The node-ID was not set, so the transport is anonymous.
    >>> tr                                          # doctest: +NORMALIZE_WHITESPACE
    RedundantTransport(UDPTransport('127.99.0.15',  local_node_id=None, ...),
                       UDPTransport('127.111.0.15', local_node_id=None, ...),
                       SerialTransport('socket://127.0.0.1:50905', local_node_id=None, ...))
    >>> tr.close()

    >>> reg = {
    ...     "uavcan.can.iface":   ValueProxy("virtual: virtual:"),    # Doubly-redundant CAN
    ...     "uavcan.can.mtu":     ValueProxy(Natural16([32])),
    ...     "uavcan.can.bitrate": ValueProxy(Natural32([500_000, 2_000_000])),
    ...     "uavcan.node.id":     ValueProxy(Natural16([123])),
    ... }
    >>> tr = make_transport(reg)
    >>> tr                                          # doctest: +NORMALIZE_WHITESPACE
    RedundantTransport(CANTransport(PythonCANMedia('virtual:', mtu=32), local_node_id=123),
                       CANTransport(PythonCANMedia('virtual:', mtu=32), local_node_id=123))
    >>> tr.close()

    >>> reg = {
    ...     "uavcan.udp.iface": ValueProxy("127.99.1.1"),       # Per the standard register specs,
    ...     "uavcan.node.id": ValueProxy(Natural16([0xFFFF])),  # 0xFFFF means unset/anonymous.
    ... }
    >>> tr = make_transport(reg)
    >>> tr
    UDPTransport('127.99.1.1', local_node_id=None, ...)
    >>> tr.close()

    >>> tr = make_transport({})
    >>> tr is None
    True
    >>> tr = make_transport({}, reconfigurable=True)
    >>> tr                                                          # Redundant transport with no inferiors.
    RedundantTransport()
    """

    def init(name: str, default: RelaxedValue) -> ValueProxy:
        return registers.setdefault("uavcan." + name, ValueProxy(default))

    # Per Specification, if uavcan.node.id = 65535, the node-ID is unspecified.
    node_id: Optional[int] = int(init("node.id", Natural16([0xFFFF])))
    # TODO: currently, we raise an error if the node-ID setting exceeds the maximum allowed value for the current
    # transport, but the spec recommends that we should handle this as if the node-ID was not set at all.
    if node_id is not None and not (0 <= node_id < 0xFFFF):
        node_id = None

    transports = list(itertools.chain(*(f(registers, node_id) for f in _SPECIALIZATIONS)))
    assert all(isinstance(t, pycyphal.transport.Transport) for t in transports)

    if not reconfigurable:
        if not transports:
            return None
        if len(transports) == 1:
            return transports[0]

    from pycyphal.transport.redundant import RedundantTransport

    red = RedundantTransport()
    for tr in transports:
        red.attach_inferior(tr)
    return red


def _make_udp(
    registers: MutableMapping[str, ValueProxy], node_id: Optional[int]
) -> Iterator[pycyphal.transport.Transport]:
    def init(name: str, default: RelaxedValue) -> ValueProxy:
        return registers.setdefault("uavcan.udp." + name, ValueProxy(default))

    ip_list = str(init("iface", "")).split()
    mtu = int(init("mtu", Natural16([1200])))
    srv_mult = int(init("duplicate_service_transfers", False)) + 1

    if ip_list:
        from pycyphal.transport.udp import UDPTransport

        for ip in ip_list:
            yield UDPTransport(ip, node_id, mtu=mtu, service_transfer_multiplier=srv_mult)


def _make_serial(
    registers: MutableMapping[str, ValueProxy], node_id: Optional[int]
) -> Iterator[pycyphal.transport.Transport]:
    def init(name: str, default: RelaxedValue) -> ValueProxy:
        return registers.setdefault("uavcan.serial." + name, ValueProxy(default))

    port_list = str(init("iface", "")).split()
    srv_mult = int(init("duplicate_service_transfers", False)) + 1
    baudrate = int(init("baudrate", Natural32([0]))) or None

    if port_list:
        from pycyphal.transport.serial import SerialTransport

        for port in port_list:
            yield SerialTransport(str(port), node_id, service_transfer_multiplier=srv_mult, baudrate=baudrate)


def _make_can(
    registers: MutableMapping[str, ValueProxy], node_id: Optional[int]
) -> Iterator[pycyphal.transport.Transport]:
    def init(name: str, default: RelaxedValue) -> ValueProxy:
        return registers.setdefault("uavcan.can." + name, ValueProxy(default))

    iface_list = str(init("iface", "")).split()
    mtu = int(init("mtu", Natural16([64])))
    br_arb, br_data = init("bitrate", Natural32([1_000_000, 4_000_000])).ints

    if iface_list:
        from pycyphal.transport.can import CANTransport

        for iface in iface_list:
            media: pycyphal.transport.can.media.Media
            if iface.lower().startswith("socketcan:"):
                from pycyphal.transport.can.media.socketcan import SocketCANMedia

                media = SocketCANMedia(iface.split(":", 1)[-1], mtu=mtu)
            elif iface.lower().startswith("candump:"):
                from pycyphal.transport.can.media.candump import CandumpMedia

                media = CandumpMedia(iface.split(":", 1)[-1])
            else:
                from pycyphal.transport.can.media.pythoncan import PythonCANMedia

                media = PythonCANMedia(iface, br_arb if br_arb == br_data else (br_arb, br_data), mtu)
            yield CANTransport(media, node_id)


def _make_loopback(
    registers: MutableMapping[str, ValueProxy], node_id: Optional[int]
) -> Iterator[pycyphal.transport.Transport]:
    # Not sure if exposing this is a good idea because the loopback transport is hardly useful outside of test envs.
    if registers.setdefault("uavcan.loopback", ValueProxy(False)):
        from pycyphal.transport.loopback import LoopbackTransport

        yield LoopbackTransport(node_id)


_SPECIALIZATIONS: Sequence[
    Callable[[MutableMapping[str, ValueProxy], Optional[int]], Iterator[pycyphal.transport.Transport]]
] = [v for k, v in globals().items() if callable(v) and k.startswith("_make_")]
assert len(_SPECIALIZATIONS) >= 4
