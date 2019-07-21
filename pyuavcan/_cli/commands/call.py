#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import typing
import asyncio
import decimal
import logging
import argparse
import contextlib

import argparse_utils

import pyuavcan
from . import _util
from ._util.yaml import YAMLLoader


INFO = _util.base.CommandInfo(
    help='''
Invoke a service using a specified request object and print the response.
The local node will also publish heartbeat and respond to GetInfo.
'''.strip(),
    examples='''
pyuavcan call uavcan.node.GetInfo.1.0 '{}'
'''.strip(),
    aliases=[]
)


_logger = logging.getLogger(__name__)


def register_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        'service_spec',
        metavar='[SERVICE_ID.]FULL_SERVICE_TYPE_NAME.MAJOR.MINOR',
        help='''
The full service type name with version and optional service-ID.
The service-ID can be omitted if a fixed one is defined for the data type.
Examples:
    123.uavcan.node.ExecuteCommand.1.0 (using service-ID 123)
    uavcan.node.ExecuteCommand.1.0 (using the fixed service-ID 435)
'''.strip(),
    )

    parser.add_argument(
        'field_spec',
        metavar='YAML_FIELDS',
        type=YAMLLoader().load,
        help='''
The YAML (or JSON, which is a subset of YAML)-formatted contents of the
request object. Missing fields will be left at their default values.
Use empty dict as "{}" to construct a default-initialized request object.
For more info about the YAML representation, read the PyUAVCAN documentation
on builtin-based representations.
'''.strip(),
    )

    parser.add_argument(
        'server_node_id',
        metavar='SERVER_NODE_ID',
        type=int,
        help=f'''
The node ID of the server that the request will be sent to.
Valid values range from zero (inclusive) to a transport-specific upper limit.
'''.strip(),
    )

    _util.node.add_arguments(parser,
                             command_module_name=__name__,
                             allow_anonymous=False)
    _util.formatter.add_arguments(parser)

    parser.add_argument(
        '--timeout', '-T',
        metavar='REAL',
        type=float,
        default=pyuavcan.presentation.DEFAULT_SERVICE_REQUEST_TIMEOUT,
        help=f'''
Request timeout; i.e., how long to wait for the response before giving up.
Default: %(default)s
'''.strip(),
    )

    parser.add_argument(
        '--priority',
        default=pyuavcan.presentation.DEFAULT_PRIORITY,
        action=argparse_utils.enum_action(pyuavcan.transport.Priority),
        help='''
Priority of the request transfer. Applies to the heartbeat as well.
Default: %(default)s
'''.strip(),
    )

    parser.add_argument(
        '--transfer-id',
        default=0,
        type=int,
        help='''
The transfer-ID value to use for the request transfer.
The value will be also used as the initial value of heartbeat publications.
Default: %(default)s
'''.strip(),
    )

    parser.add_argument(
        '--with-metadata', '-M',
        action='store_true',
        help='''
Emit transfer metadata together with the response.
'''.strip(),
    )


def execute(args: argparse.Namespace) -> int:
    return asyncio.get_event_loop().run_until_complete(_do_execute(args))


async def _do_execute(args: argparse.Namespace) -> int:
    formatter = _util.formatter.construct_formatter(args)

    with contextlib.closing(_util.node.construct_node(args)) as node:
        import pyuavcan.application
        assert isinstance(node, pyuavcan.application.Node)

        node.heartbeat_publisher.priority = args.priority
        node.heartbeat_publisher.publisher.transfer_id_counter.override(args.transfer_id)

        # Construct the request object.
        service_id, dtype = _util.port_spec.construct_port_id_and_type(args.service_spec)
        if not issubclass(dtype, pyuavcan.dsdl.ServiceObject):
            raise ValueError(f'Expected a service type; got this: {dtype.__name__}')

        request = pyuavcan.dsdl.update_from_builtin(dtype.Request(), args.field_spec)
        _logger.info('Request object: %r', request)

        # Initialize the client instance.
        client = node.make_client(dtype, service_id, args.server_node_id)
        client.response_timeout = args.timeout
        client.priority = args.priority
        client.transfer_id_counter.override(args.transfer_id)

        request_ts_transport_layer: typing.Optional[pyuavcan.transport.Timestamp] = None

        def on_transfer_feedback(fb: pyuavcan.transport.Feedback) -> None:
            nonlocal request_ts_transport_layer
            request_ts_transport_layer = fb.first_frame_transmission_timestamp

        client.output_transport_session.enable_feedback(on_transfer_feedback)

        # Perform the call.
        request_ts_application_layer = pyuavcan.transport.Timestamp.now()
        result = await client.call_with_transfer(request)
        response_ts_application_layer = pyuavcan.transport.Timestamp.now()

        # Print the results.
        if result is None:
            print(f'Did not receive a response in {client.response_timeout:0.1f} seconds', file=sys.stderr)
            return 1
        else:
            if not request_ts_transport_layer:  # pragma: no cover
                request_ts_transport_layer = request_ts_application_layer
                _logger.error('The transport implementation is misbehaving: feedback was never emitted; '
                              'falling back to software timestamping. '
                              'Please submit a bug report. Involved instances: node=%r, client=%r, result=%r',
                              node, client, result)

            response, transfer = result

            transport_layer_duration = transfer.timestamp.monotonic - request_ts_transport_layer.monotonic
            application_layer_duration = \
                response_ts_application_layer.monotonic - request_ts_application_layer.monotonic

            bi: typing.Dict[str, typing.Any] = {}  # We use updates to ensure proper dict ordering: metadata before data
            if args.with_metadata:
                bi.update({
                    '_transfer_': {
                        'timestamp': {
                            'system':    transfer.timestamp.system.quantize(_1EM6),
                            'monotonic': transfer.timestamp.monotonic.quantize(_1EM6),
                        },
                        'priority':       transfer.priority.name.lower(),
                        'transfer_id':    transfer.transfer_id,
                        'source_node_id': transfer.source_node_id,
                    },
                    '_request_timing_': {
                        'transport_layer': {
                            'timestamp': {
                                'system':    request_ts_transport_layer.system.quantize(_1EM6),
                                'monotonic': request_ts_transport_layer.monotonic.quantize(_1EM6),
                            },
                            'duration': transport_layer_duration.quantize(_1EM6),
                        },
                        'application_layer': {
                            'timestamp': {
                                'system':    request_ts_application_layer.system.quantize(_1EM6),
                                'monotonic': request_ts_application_layer.monotonic.quantize(_1EM6),
                            },
                            'duration': application_layer_duration.quantize(_1EM6),
                            'local_overhead': (application_layer_duration - transport_layer_duration).quantize(_1EM6),
                        },
                    }
                })
            bi.update(pyuavcan.dsdl.to_builtin(response))

            print(formatter({
                service_id: bi,
            }))

    return 0


_1EM6 = decimal.Decimal('0.000001')
