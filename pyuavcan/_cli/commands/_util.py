# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import re
import typing
import logging
import decimal
import importlib
import pyuavcan.dsdl
from .dsdl_generate_packages import DSDLGeneratePackagesCommand


_NAME_COMPONENT_SEPARATOR = "."


_logger = logging.getLogger(__name__)


def construct_port_id_and_type(spec: str) -> typing.Tuple[int, typing.Type[pyuavcan.dsdl.CompositeObject]]:
    r"""
    Parses a data specifier string of the form ``[port_id.]full_data_type_name.major_version.minor_version``.
    Name separators may be replaced with ``/`` or ``\`` for compatibility with file system paths;
    the version number separators may also be underscores for convenience.
    Raises ValueError, possibly with suggestions, if such type is non-reachable.
    """
    port_id, full_name, major, minor = _parse_data_spec(spec)
    name_components = full_name.split(_NAME_COMPONENT_SEPARATOR)
    namespace_components, short_name = name_components[:-1], name_components[-1]
    _logger.debug(
        "Parsed data spec %r: port_id=%r, namespace_components=%r, short_name=%r, major=%r, minor=%r",
        spec,
        port_id,
        namespace_components,
        short_name,
        major,
        minor,
    )

    # Import the generated data type.
    try:
        mod = None
        for comp in namespace_components:
            name = (mod.__name__ + "." + comp) if mod else comp  # type: ignore
            try:
                mod = importlib.import_module(name)
            except ImportError:  # We seem to have hit a reserved word; try with an underscore.
                mod = importlib.import_module(name + "_")
    except ImportError:
        raise ValueError(
            f"The data spec string specifies a non-existent namespace: {spec!r}. "
            f"{DSDLGeneratePackagesCommand.make_usage_suggestion_text(namespace_components[0])}"
        ) from None

    try:
        dtype = getattr(mod, f"{short_name}_{major}_{minor}")
    except AttributeError:
        raise ValueError(f"The data spec string specifies a non-existent short type name: {spec!r}") from None

    if issubclass(dtype, pyuavcan.dsdl.CompositeObject):
        model = pyuavcan.dsdl.get_model(dtype)
        port_id = port_id if port_id is not None else model.fixed_port_id
        if port_id is None:
            raise ValueError(
                f"The data spec does not specify a port ID, "
                f"and a fixed port ID is not defined for the specified data type: {spec!r}"
            )
        return port_id, dtype
    else:
        raise ValueError(f"The data spec does not specify a valid type: {spec!r}")


def convert_transfer_metadata_to_builtin(
    transfer: pyuavcan.transport.TransferFrom, **extra_fields: typing.Dict[str, typing.Any]
) -> typing.Dict[str, typing.Any]:
    out = {
        "timestamp": {
            "system": transfer.timestamp.system.quantize(_MICRO),
            "monotonic": transfer.timestamp.monotonic.quantize(_MICRO),
        },
        "priority": transfer.priority.name.lower(),
        "transfer_id": transfer.transfer_id,
        "source_node_id": transfer.source_node_id,
    }
    out.update(extra_fields)
    return {"_metadata_": out}


_MICRO = decimal.Decimal("0.000001")

_RE_SPLIT = re.compile(r"^(?:(\d+)\.)?((?:[a-zA-Z_][a-zA-Z0-9_]*)(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)[_.](\d+)[_.](\d+)$")
"""
Splits ``123.ns.Type.123.45`` into ``('123', 'ns.Type', '123', '45')``.
Splits     ``ns.Type.123.45`` into ``(None, 'ns.Type', '123', '45')``.
The version separators (the last two) may be underscores.
"""


def _parse_data_spec(spec: str) -> typing.Tuple[typing.Optional[int], str, int, int]:
    r"""
    Transform the provided data spec into: [port-ID], full name, major version, minor version.
    Component separators may be ``/`` or ``\``. Version number separators (the last two) may also be underscores.
    Raises ValueError if non-compliant.
    """
    spec = spec.strip().replace("/", _NAME_COMPONENT_SEPARATOR).replace("\\", _NAME_COMPONENT_SEPARATOR)
    match = _RE_SPLIT.match(spec)
    if match is None:
        raise ValueError(f"Malformed data spec: {spec!r}")
    frag_port_id, frag_full_name, frag_major, frag_minor = match.groups()
    return (int(frag_port_id) if frag_port_id is not None else None), frag_full_name, int(frag_major), int(frag_minor)


def _unittest_parse_data_spec() -> None:
    import pytest

    assert (123, "ns.Type", 12, 34) == _parse_data_spec(" 123.ns.Type.12.34 ")
    assert (123, "ns.Type", 12, 34) == _parse_data_spec("123.ns.Type_12.34")
    assert (123, "ns.Type", 12, 34) == _parse_data_spec("123.ns/Type.12_34")
    assert (123, "ns.Type", 12, 34) == _parse_data_spec("123.ns.Type_12_34")
    assert (123, "ns.Type", 12, 34) == _parse_data_spec(r"123\ns\Type_12_34 ")

    assert (None, "ns.Type", 12, 34) == _parse_data_spec("ns.Type.12.34 ")
    assert (123, "Type", 12, 34) == _parse_data_spec("123.Type.12.34")
    assert (None, "Type", 12, 34) == _parse_data_spec("Type.12.34")
    assert (123, "ns0.sub.Type0", 0, 1) == _parse_data_spec("123.ns0.sub.Type0.0.1")
    assert (None, "ns0.sub.Type0", 255, 255) == _parse_data_spec(r"ns0/sub\Type0.255.255")

    with pytest.raises(ValueError):
        _parse_data_spec("123.ns.Type.12")
    with pytest.raises(ValueError):
        _parse_data_spec("123.ns.Type.12.43.56")
    with pytest.raises(ValueError):
        _parse_data_spec("ns.Type.12")
    with pytest.raises(ValueError):
        _parse_data_spec("ns.Type.12.43.56")
    with pytest.raises(ValueError):
        _parse_data_spec("123.ns.0Type.12.43")
