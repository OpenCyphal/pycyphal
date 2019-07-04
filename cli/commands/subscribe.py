#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio
import logging
import argparse
import pyuavcan
from . import _base, _transport, _data_spec


INFO = _base.CommandInfo(
    help='''
Subscribe to the specified subject, receive and print messages into stdout.
'''.strip(),
    examples=f'''
pyuavcan sub uavcan.node.Heartbeat.1
'''.strip(),
    aliases=[
        'sub',
    ]
)


_logger = logging.getLogger(__name__)

_BuiltinRepresentation = typing.Dict[str, typing.Any]
_Formatter = typing.Callable[[typing.List[_BuiltinRepresentation]], str]


def register_arguments(parser: argparse.ArgumentParser) -> None:
    _transport.add_argument_transport(parser)

    parser.add_argument(
        'data_spec',
        metavar='[SUBJECT_ID.]FULL_MESSAGE_TYPE_NAME.MAJOR.MINOR',
        nargs='+',
        help='''
A set of full message type names with version and optional subject-ID for each.
The subject-ID can be omitted if a fixed one is defined for the data type.
Examples:
    1234.uavcan.node.Heartbeat.1.0 (using subject-ID 1234)
    uavcan.node.Heartbeat.1.0 (using the fixed subject-ID 32085)
'''.strip(),
    )

    parser.add_argument(
        '--format',
        choices=list(_FORMATTER_FACTORIES.keys()),
        default=list(_FORMATTER_FACTORIES.keys())[0],
        help='''
The format of the data printed into stdout.
Default: %(default)s
'''.strip(),
    )

    parser.add_argument(
        '--with-metadata', '-M',
        action='store_true',
        help='''
Emit metadata together with each message.
'''.strip(),
    )


def execute(args: argparse.Namespace) -> None:
    transport = _transport.construct_transport(args.transport)
    subjects = [_data_spec.construct_port_id_and_type(ds) for ds in args.data_spec]
    formatter = _FORMATTER_FACTORIES[args.format]()
    with_metadata = bool(args.with_metadata)

    asyncio.get_event_loop().run_until_complete(
        _run(transport=transport,
             subject_id_dtype_pairs=subjects,
             formatter=formatter,
             with_metadata=with_metadata)
    )


async def _run(transport:              pyuavcan.transport.Transport,
               subject_id_dtype_pairs: typing.List[typing.Tuple[int, typing.Type[pyuavcan.dsdl.CompositeObject]]],
               formatter:              _Formatter,
               with_metadata:          bool) -> None:
    if len(subject_id_dtype_pairs) < 1:
        raise ValueError('Nothing to do: no subjects specified')
    elif len(subject_id_dtype_pairs) == 1:
        subject_id, dtype = subject_id_dtype_pairs[0]
    else:
        # TODO: add support for multi-subject synchronous subscribers https://github.com/UAVCAN/pyuavcan/issues/65
        raise NotImplementedError('Multi-subject subscription is not yet implemented, sorry!')

    _logger.info(f'Starting the subscriber with transport={transport}, '
                 f'subject_id_dtype_pairs={subject_id_dtype_pairs}, '
                 f'formatter={formatter}, with_metadata={with_metadata}')

    model = pyuavcan.dsdl.get_model(dtype)
    pres = pyuavcan.presentation.Presentation(transport)
    sub = pres.make_subscriber(dtype=dtype, subject_id=subject_id)

    async for msg, transfer in sub:
        assert isinstance(transfer, pyuavcan.transport.TransferFrom)
        bi = pyuavcan.dsdl.to_builtin(msg)
        if with_metadata:
            bi['_transfer_'] = {
                'timestamp': {
                    'system':    transfer.timestamp.system,
                    'monotonic': transfer.timestamp.monotonic,
                },
                'priority':       transfer.priority,
                'transfer_id':    transfer.transfer_id,
                'source_node_id': transfer.source_node_id,  # None if anonymous
            }
            bi['_port_'] = {
                'type': [model.full_name, model.version.major, model.version.minor],
                'subject_id': subject_id,
            }

        print(formatter([bi]))


def _make_yaml_formatter() -> _Formatter:
    try:
        import yaml
    except ImportError:
        raise ImportError('Please install PyYAML to use this formatter: pip install pyyaml') from None

    return lambda bi: yaml.dump(bi, explicit_start=True)


def _make_json_formatter() -> _Formatter:
    import json
    return json.dumps


def _make_tsv_formatter() -> _Formatter:
    raise NotImplementedError('Sorry, the TSV formatter is not yet implemented')


_FORMATTER_FACTORIES = {
    'yaml': _make_yaml_formatter,
    'json': _make_json_formatter,
    'tsv': _make_tsv_formatter,
}
