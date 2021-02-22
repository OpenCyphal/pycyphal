# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import sys
import random
from typing import Callable, Tuple, Optional, Union
from pathlib import Path
import logging
import pyuavcan
from ._node import Node, NodeInfo
from . import register
from .register.backend.sqlite import SQLiteBackend
from .register.backend.dynamic import DynamicBackend
from ._transport_factory import make_transport

if sys.version_info >= (3, 9):
    from collections.abc import Mapping
else:  # pragma: no cover
    from typing import Mapping  # pylint: disable=ungrouped-imports


# Update the initialization logic when adding new entries here:
REG_NODE_ID = "uavcan.node.id"
REG_UNIQUE_ID = "uavcan.node.unique_id"
REG_DESCRIPTION = "uavcan.node.description"
REG_DIAGNOSTIC_SEVERITY = "uavcan.diagnostic.severity"
REG_DIAGNOSTIC_TIMESTAMP = "uavcan.diagnostic.timestamp"


class MissingTransportConfigurationError(register.MissingRegisterError):
    pass


class DefaultNode(Node):
    """
    This is a Voldemort type, hence it doesn't need public docs.
    """

    def __init__(
        self,
        presentation: pyuavcan.presentation.Presentation,
        info: NodeInfo,
        backend_sqlite: SQLiteBackend,
        backend_dynamic: DynamicBackend,
    ) -> None:
        self._presentation = presentation
        self._info = info

        self._backend_sqlite = backend_sqlite
        self._backend_dynamic = backend_dynamic
        self._registry = register.Registry([self._backend_sqlite, self._backend_dynamic])

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

    def new_register(
        self,
        name: str,
        value_or_getter_or_getter_setter: Union[
            register.Value,
            register.ValueProxy,
            Callable[[], Union[register.Value, register.ValueProxy]],
            Tuple[
                Callable[[], Union[register.Value, register.ValueProxy]],
                Callable[[register.Value], None],
            ],
        ],
    ) -> None:
        def strictify(x: Union[register.Value, register.ValueProxy]) -> register.Value:
            if isinstance(x, register.ValueProxy):
                return x.value
            return x

        v = value_or_getter_or_getter_setter
        _logger.debug("%r: Create register %r = %r", self, name, v)
        if isinstance(v, (register.Value, register.ValueProxy)):
            self._backend_sqlite[name] = strictify(v)
        elif callable(v):
            self._backend_dynamic[name] = lambda: strictify(v())  # type: ignore
        elif isinstance(v, tuple) and len(v) == 2 and all(map(callable, v)):
            g, s = v
            self._backend_dynamic[name] = (lambda: strictify(g())), s
        else:  # pragma: no cover
            raise TypeError(f"Invalid type of register creation argument: {type(v).__name__}")


def make_node(
    info: NodeInfo,
    register_file: Union[None, str, Path] = None,
    schema: Optional[Mapping[str, Union[register.ValueProxy, register.Value]]] = None,
    *,
    ignore_environment_variables: bool = False,
    transport: Optional[pyuavcan.transport.Transport] = None,
    reconfigurable_transport: bool = False,
) -> Node:
    """
    Initialize a new node by parsing the configuration encoded in the UAVCAN registers.

    If ``transport`` is given, it will be used as-is (but see argument docs below).
    If not given, a new transport instance will be constructed using :func:`make_transport`.

    Prior to construction, the register file will be updated/extended based on the register values passed via the
    environment variables (if any) and the explicit ``schema``.
    Environment variables and ``schema`` that encode empty-valued registers trigger removal of such registers
    from the file (non-existent registers do not trigger an error).

    Register removal is useful, in particular, when the node needs to be switched from one transport type to another
    (e.g., from UDP to CAN):
    without the ability to remove registers, it would not be possible to tell the node to stop using a previously
    configured transport without editing or removing the register file.

    Aside from the registers that encode the transport configuration (which are documented in :func:`make_transport`),
    the following registers are considered.
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
            It is not used by the implementation itself but created automatically if not present.

    ..  list-table:: :mod:`pyuavcan.application.diagnostic`
        :widths: 1 1 9
        :header-rows: 1

        * - Register name
          - Register type
          - Register semantics

        * - ``uavcan.diagnostic.severity``
          - ``natural16[1]``
          - If defined and the value is a valid severity level as defined in ``uavcan.diagnostic.Severity``,
            the node will publish its application log records of matching severity level to the standard subject
            ``uavcan.diagnostic.Record`` using :class:`pyuavcan.application.diagnostic.DiagnosticPublisher`.
            This is done by installing a root handler in :mod:`logging`.

        * - ``uavcan.diagnostic.timestamp``
          - ``bit[1]``
          - If defined and true, the published log messages will initialize the synchronized ``timestamp`` field
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

    :param schema:
        These values will be checked before the environment variables are parsed (unless disabled)
        to make sure that every register specified here exists in the register file with the specified type.

        Existing registers of matching type will be kept unchanged (even if the value is different).
        Existing registers of a different type will be type-converted to the specified type.
        Missing registers will be created with the specified value.

        Empty values trigger removal of corresponding registers from the register file
        (but note that they may be re-created from environment variables afterward).

        Use this parameter to define the register schema of the node.
        Do not use it for setting default node-ID or port-IDs.

    :param ignore_environment_variables:
        If False (default), the register values passed via environment variables will be automatically parsed
        and for each register the respective entry in the register file will be updated/created.
        The details are specified in :func:`register.parse_environment_variables`.
        Empty values trigger removal of corresponding registers from the register file.

        True can be passed if the application receives its register configuration at launch from some other source.

    :param transport:
        If not provided (default), a new transport instance will be initialized based on the available registers using
        :func:`make_transport`.
        If provided, the node will be constructed with this transport instance and take its ownership.

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
            out = make_transport(register.Registry([db]), reconfigurable=reconfigurable_transport)
            if out is not None:
                return out
            raise MissingTransportConfigurationError(
                f"Available registers do not encode a valid transport configuration: {list(db)}"
            )
        if not isinstance(transport, RedundantTransport) and reconfigurable_transport:
            out = RedundantTransport()
            out.attach_inferior(transport)
            return out
        return transport

    db = SQLiteBackend(register_file or "")
    try:
        # Apply defaults and schema first to ensure that new registers are created with the correct types
        # before environment variables are applied.
        _apply_schema(db, schema or {})
        _apply_schema(
            db,
            {
                REG_NODE_ID: register.Value(natural16=register.Natural16([0xFFFF])),
                REG_DESCRIPTION: register.Value(string=register.String()),
                REG_DIAGNOSTIC_SEVERITY: register.Value(natural16=register.Natural16([8])),
                REG_DIAGNOSTIC_TIMESTAMP: register.Value(bit=register.Bit([False])),
            },
        )
        if not ignore_environment_variables:
            _apply_env_vars(db)

        # Populate certain fields of the node info structure automatically.
        info.protocol_version.major, info.protocol_version.minor = pyuavcan.UAVCAN_SPECIFICATION_VERSION
        if info.unique_id.sum() == 0:
            if REG_UNIQUE_ID not in db:
                uid_size_bytes = 16
                uid = random.getrandbits(8 * uid_size_bytes).to_bytes(uid_size_bytes, sys.byteorder)
                _logger.info("New unique-ID generated: %s", uid.hex())
                db[REG_UNIQUE_ID] = register.backend.Entry(
                    register.Value(unstructured=register.Unstructured(uid)),
                    mutable=False,
                )
            info.unique_id = bytes(register.ValueProxy(db[REG_UNIQUE_ID].value))

        if len(info.name) == 0:  # Do our best to decently support lazy instantiations that don't even give a name.
            name = "anonymous." + info.unique_id.tobytes().hex()
            _logger.info("Automatic name: %r", name)
            info.name = name

        # Construct the node.
        presentation = pyuavcan.presentation.Presentation(init_transport())
        node = DefaultNode(
            presentation,
            info,
            db,
            DynamicBackend(),
        )

        # Check if any application-layer functions require instantiation.
        _make_diagnostic_publisher(node)
    except Exception:
        db.close()  # We do not close the database at normal exit because it's handed over to the node.
        raise
    return node


def _apply_schema(db: SQLiteBackend, schema: Mapping[str, Union[register.ValueProxy, register.Value]]) -> None:
    for name, value in schema.items():
        _logger.debug("Register init from schema: %r <-- %r", name, value)
        value = register.ValueProxy(value)
        if value.value.empty:  # Remove register under this name.
            try:
                del db[name]
            except LookupError:
                pass
        else:
            existing = db.get(name)
            if existing is None or existing.value.empty:
                mutable = True
            else:
                value.assign(existing.value)  # Perform type conversion to match expectations of the application.
                mutable = existing.mutable
            db[name] = register.backend.Entry(value.value, mutable=mutable)


def _apply_env_vars(db: SQLiteBackend) -> None:
    for name, value in register.parse_environment_variables().items():
        _logger.debug("Register init from env var: %r <-- %r", name, value)
        if value.empty:  # Remove register under this name.
            try:
                del db[name]
            except LookupError:
                pass
        else:
            try:
                existing = db[name]
            except LookupError:
                db[name] = value
            else:  # Force to the correct type.
                converted = register.ValueProxy(existing.value)
                converted.assign(value)
                db[name] = register.backend.Entry(converted.value, mutable=existing.mutable)


def _make_diagnostic_publisher(node: Node) -> None:
    try:
        uavcan_severity = int(node.registry[REG_DIAGNOSTIC_SEVERITY])
    except KeyError:
        return

    from .diagnostic import DiagnosticSubscriber, DiagnosticPublisher

    try:
        level = DiagnosticSubscriber.SEVERITY_UAVCAN_TO_PYTHON[uavcan_severity]
    except KeyError:
        return

    diag_publisher = DiagnosticPublisher(node, level=level)
    try:
        diag_publisher.timestamping_enabled = bool(node.registry[REG_DIAGNOSTIC_TIMESTAMP])
    except KeyError:
        pass
    logging.root.addHandler(diag_publisher)
    node.add_lifetime_hooks(None, lambda: logging.root.removeHandler(diag_publisher))


_logger = logging.getLogger(__name__)
