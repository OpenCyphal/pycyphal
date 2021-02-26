# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import sys
import random
from typing import Callable, Optional, Union, List
from pathlib import Path
import logging
import pyuavcan
from ._node import Node, NodeInfo
from . import register
from .register.backend.sqlite import SQLiteBackend
from ._transport_factory import make_transport


class MissingTransportConfigurationError(register.MissingRegisterError):
    pass


class DefaultRegistry(register.Registry):
    def __init__(self, sqlite: SQLiteBackend) -> None:
        from .register.backend.dynamic import DynamicBackend

        self._backend_sqlite = sqlite
        self._backend_dynamic = DynamicBackend()
        super().__init__()

    @property
    def backends(self) -> List[register.backend.Backend]:
        return [self._backend_sqlite, self._backend_dynamic]

    def _create_persistent(self, name: str, value: register.Value) -> None:
        _logger.debug("%r: Create persistent %r = %r", self, name, value)
        self._backend_sqlite[name] = value

    def _create_dynamic(
        self,
        name: str,
        get: Callable[[], register.Value],
        set: Optional[Callable[[register.Value], None]],
    ) -> None:
        _logger.debug("%r: Create dynamic %r from get=%r set=%r", self, name, get, set)
        self._backend_dynamic[name] = get if set is None else (get, set)


class DefaultNode(Node):
    def __init__(
        self,
        presentation: pyuavcan.presentation.Presentation,
        info: NodeInfo,
        registry: register.Registry,
    ) -> None:
        self._presentation = presentation
        self._info = info
        self._registry = registry
        super().__init__()

    @property
    def presentation(self) -> pyuavcan.presentation.Presentation:
        return self._presentation

    @property
    def info(self) -> NodeInfo:
        return self._info

    @property
    def registry(self) -> register.Registry:
        return self._registry


def make_node(
    info: NodeInfo,
    register_file: Union[None, str, Path] = None,
    *,
    use_environment_variables: bool = True,
    transport: Optional[pyuavcan.transport.Transport] = None,
    reconfigurable_transport: bool = False,
) -> Node:
    """
    Initialize a new node by parsing the configuration encoded in the UAVCAN registers.
    Missing standard registers will be automatically created.

    If ``transport`` is given, it will be used as-is (but see argument docs below).
    If not given, a new transport instance will be constructed using :func:`make_transport`.

    Prior to construction, the register file will be updated/extended based on the register values passed via the
    environment variables (if any) and the explicit ``schema``.
    Empty values in ``schema`` trigger removal of such registers from the register file
    (non-existent registers do not trigger an error).
    This is useful when the application needs to migrate its register file created by an earlier version.

    Aside from the registers that encode the transport configuration (which are documented in :func:`make_transport`),
    the following registers are considered (if they don't exist, they are automatically created).
    Generally, it is not possible to change their type --- automatic type conversion may take place to prevent that.
    They are split into groups by application-layer function they configure.

    ..  list-table:: General
        :widths: 1 1 9
        :header-rows: 1

        * - Register name
          - Register type
          - Register semantics

        * - ``uavcan.node.unique_id``
          - ``unstructured``
          - The unique-ID of the local node.
            This register is only used if the caller did not set ``unique_id`` in ``info``.
            If not defined, a new random value is generated and stored as immutable
            (therefore, if no persistent register file is used, a new unique-ID is generated at every launch, which
            may be undesirable in some applications, particularly those that require PnP node-ID allocation).

        * - ``uavcan.node.description``
          - ``string``
          - As defined by the UAVCAN Specification, this standard register is intended to store a human-friendly
            description of the node.

    ..  list-table:: :mod:`pyuavcan.application.diagnostic`
        :widths: 1 1 9
        :header-rows: 1

        * - Register name
          - Register type
          - Register semantics

        * - ``uavcan.diagnostic.severity``
          - ``natural8[1]``
          - If the value is a valid severity level as defined in ``uavcan.diagnostic.Severity``,
            the node will publish its application log records of matching severity level to the standard subject
            ``uavcan.diagnostic.Record`` using :class:`pyuavcan.application.diagnostic.DiagnosticPublisher`.
            This is done by installing a root handler in :mod:`logging`.

        * - ``uavcan.diagnostic.timestamp``
          - ``bit[1]``
          - If true, the published log messages will initialize the synchronized ``timestamp`` field
            from the log record timestamp provided by the :mod:`logging` library.
            This is only safe if the UAVCAN network is known to be synchronized on the same time system as the
            wall clock of the local computer.
            Otherwise, the timestamp is left at zero (which means "unknown" per Specification).

    Additional application-layer functions and their respective registers may be added later.

    :param info:
        Response object to ``uavcan.node.GetInfo``. The following fields will be populated automatically:

        - ``protocol_version`` from :data:`pyuavcan.UAVCAN_SPECIFICATION_VERSION`.

        - If not set by the caller: ``unique_id`` is read from register as specified above.

        - If not set by the caller: ``name`` is constructed from hex-encoded unique-ID like:
          ``anonymous.b0228a49c25ff23a3c39915f81294622``.

    :param register_file:
        Path to the SQLite file containing the register database; or, in other words,
        the configuration file of this application/node.
        If not provided (default), the registers of this instance will be stored in-memory (volatile configuration).
        If path is provided but the file does not exist, it will be created automatically.
        See :attr:`Node.registry`, :meth:`Node.new_register`.

    :param use_environment_variables:
        If True (default), the registers will be updated based on the environment variables.
        :attr:`register.Registry.use_defaults_from_environment` will be set to the same value
        (it can be changed after the node is constructed).

        False can be passed if the application receives its register configuration at launch from some other source
        (this is uncommon).

        See also: :meth:`register.Registry.update_from_environment` and standard RPC-service ``uavcan.register.Access``.

    :param transport:
        If not provided (default), a new transport instance will be initialized based on the available registers using
        :func:`make_transport`.
        If provided, the node will be constructed with this transport instance and take its ownership.
        In the latter case, transport-related registers will NOT be created, which may be undesirable.

    :param reconfigurable_transport:
        If True, the node will be constructed with :mod:`pyuavcan.transport.redundant`,
        which permits runtime reconfiguration.
        If the transport argument is given and it is not a redundant transport, it will be wrapped into one.
        Also see :func:`make_transport`.

    :raises:
        - :class:`pyuavcan.application.register.MissingRegisterError` if a register is expected but cannot be found,
          or if no transport is configured.
        - :class:`pyuavcan.application.register.ValueConversionError` if a register is found but its value
          cannot be converted to the correct type.
        - Also see :func:`make_transport`.

    ..  note::

        Consider extending this factory with a capability to automatically run the node-ID allocation client
        :class:`pyuavcan.application.plug_and_play.Allocatee` if the available registers do not encode a non-anonymous
        node-ID value.

        Until this is implemented, to run the allocator one needs to construct the transport manually using
        :func:`make_transport`, then run the allocation client, then invoke this factory again with something like
        ``schema={"uavcan.node.id": Value(natural16=Natural16([your_allocated_node_id]))}``.

        While tedious, this is not that much of a problem because the PnP protocol is mostly intended for
        hardware nodes rather than software ones.
        A typical software node would normally receive its node-ID at startup (see also Yakut Orchestrator).
    """
    from pyuavcan.transport.redundant import RedundantTransport

    def init_transport() -> pyuavcan.transport.Transport:
        if transport is None:
            out = make_transport(registry, reconfigurable=reconfigurable_transport)
            if out is not None:
                return out
            raise MissingTransportConfigurationError(
                f"Available registers do not encode a valid transport configuration: {list(registry)}"
            )
        if not isinstance(transport, RedundantTransport) and reconfigurable_transport:
            out = RedundantTransport()
            out.attach_inferior(transport)
            return out
        return transport

    registry = DefaultRegistry(SQLiteBackend(register_file or ""))
    try:
        # Update all currently existing registers from environment variables early.
        # New registers will be updated ad-hoc at creation time if "use_defaults_from_environment" is set.
        registry.use_defaults_from_environment = use_environment_variables
        if use_environment_variables:
            for name in registry:
                registry.update_from_environment(name)

        # Populate certain fields of the node info structure automatically and create standard registers.
        info.protocol_version.major, info.protocol_version.minor = pyuavcan.UAVCAN_SPECIFICATION_VERSION
        if info.unique_id.sum() == 0:
            info.unique_id = bytes(
                registry.setdefault(
                    "uavcan.node.unique_id",
                    register.Value(
                        unstructured=register.Unstructured(random.getrandbits(128).to_bytes(16, sys.byteorder))
                    ),
                )
            )
        registry.setdefault("uavcan.node.description", register.Value(string=register.String()))

        if len(info.name) == 0:  # Do our best to decently support lazy instantiations that don't even give a name.
            name = "anonymous." + info.unique_id.tobytes().hex()
            _logger.info("Automatic name: %r", name)
            info.name = name

        # Construct the node and its application-layer functions.
        node = DefaultNode(pyuavcan.presentation.Presentation(init_transport()), info, registry)
        _make_diagnostic_publisher(node)
    except Exception:
        registry.close()  # We do not close it at normal exit because it's handed over to the node.
        raise
    return node


def _make_diagnostic_publisher(node: Node) -> None:
    from .diagnostic import DiagnosticSubscriber, DiagnosticPublisher

    uavcan_severity = int(
        node.registry.setdefault("uavcan.diagnostic.severity", register.Value(natural8=register.Natural8([0xFF])))
    )
    timestamping_enabled = bool(
        node.registry.setdefault("uavcan.diagnostic.timestamp", register.Value(bit=register.Bit([False])))
    )

    try:
        level = DiagnosticSubscriber.SEVERITY_UAVCAN_TO_PYTHON[uavcan_severity]
    except LookupError:
        return

    diag_publisher = DiagnosticPublisher(node, level=level)
    diag_publisher.timestamping_enabled = timestamping_enabled

    logging.root.addHandler(diag_publisher)
    node.add_lifetime_hooks(None, lambda: logging.root.removeHandler(diag_publisher))


_logger = logging.getLogger(__name__)
