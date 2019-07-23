#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import logging
import decimal
import importlib
import pyuavcan.dsdl
from .dsdl_generate_packages import DSDLGeneratePackagesCommand


_logger = logging.getLogger(__name__)


def construct_port_id_and_type(spec: str) -> typing.Tuple[int, typing.Type[pyuavcan.dsdl.CompositeObject]]:
    """
    Parses a data specifier string of the form ``[port_id.]full_data_type_name.major_version.minor_version``.
    Raises ValueError, possibly with suggestions, if such type is non-reachable.
    """
    components = spec.strip().split('.')

    # If the first component is an integer, it's the port ID.
    try:
        port_id: typing.Optional[int] = int(components[0], 0)
        components = components[1:]
    except ValueError:
        port_id = None

    # Segment the rest of the string.
    try:
        major, minor = int(components[-2]), int(components[-1])
        name_components = components[:-2]
        if len(name_components) < 2:
            raise ValueError
        namespace_components, short_name = name_components[:-1], name_components[-1]
    except Exception:
        raise ValueError(f'Malformed data spec: {spec!r}') from None

    _logger.debug('Parsed data spec %r: port_id=%r, namespace_components=%r, short_name=%r, major=%r, minor=%r',
                  spec, port_id, namespace_components, short_name, major, minor)

    # Import the generated data type.
    try:
        mod = None
        for comp in namespace_components:
            name = (mod.__name__ + '.' + comp) if mod else comp  # type: ignore
            try:
                mod = importlib.import_module(name)
            except ImportError:  # We seem to have hit a reserved word; try with an underscore.
                mod = importlib.import_module(name + '_')
    except ImportError:
        raise ValueError(f'The data spec string specifies a non-existent namespace: {spec!r}. '
                         f'{DSDLGeneratePackagesCommand.make_usage_suggestion_text(namespace_components[0])}') from None

    try:
        dtype = getattr(mod, f'{short_name}_{major}_{minor}')
    except AttributeError:
        raise ValueError(f'The data spec string specifies a non-existent short type name: {spec!r}') from None

    if issubclass(dtype, pyuavcan.dsdl.CompositeObject):
        model = pyuavcan.dsdl.get_model(dtype)
        port_id = port_id if port_id is not None else model.fixed_port_id
        if port_id is None:
            raise ValueError(f'The data spec does not specify a port ID, '
                             f'and a fixed port ID is not defined for the specified data type: {spec!r}')
        return port_id, dtype
    else:
        raise ValueError(f'The data spec does not specify a valid type: {spec!r}')


def convert_transfer_metadata_to_builtin(transfer: pyuavcan.transport.TransferFrom,
                                         **extra_fields: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    out = {
        'timestamp': {
            'system':    transfer.timestamp.system.quantize(_MICRO),
            'monotonic': transfer.timestamp.monotonic.quantize(_MICRO),
        },
        'priority':       transfer.priority.name.lower(),
        'transfer_id':    transfer.transfer_id,
        'source_node_id': transfer.source_node_id,
    }
    out.update(extra_fields)
    return {
        '_metadata_': out
    }


_MICRO = decimal.Decimal('0.000001')
