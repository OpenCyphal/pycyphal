#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import re
import time
import atexit
import pickle
import typing
import logging
import pathlib
import argparse
import xml.etree.ElementTree
import pyuavcan
from .._yaml import YAMLLoader, YAMLDumper
from .._paths import OUTPUT_TRANSFER_ID_MAP_DIR, OUTPUT_TRANSFER_ID_MAP_MAX_AGE
from .transport import TransportFactory
from ._base import SubsystemFactory


_logger = logging.getLogger(__name__)


class NodeFactory(SubsystemFactory):
    """
    Constructs a node instance. The instance must be start()ed by the caller afterwards.
    """

    def __init__(self,
                 node_name_suffix: str,
                 allow_anonymous:  bool):
        self._node_name_suffix = str(node_name_suffix)
        self._allow_anonymous = bool(allow_anonymous)
        self._transport_factory = TransportFactory()
        assert re.match(r'[a-z][a-z0-9_]*[a-z0-9]', self._node_name_suffix), 'Poorly chosen name'

    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        self._transport_factory.register_arguments(parser)
        parser.add_argument(
            '--heartbeat-fields',
            default='{}',
            metavar='YAML_FIELDS',
            type=YAMLLoader().load,
            help='''
Value of the heartbeat message uavcan.node.Heartbeat published by the node. The uptime will be overridden so
specifying it here will have no effect. Has no effect if the node is anonymous (i.e., without a local node-ID) because
anonymous nodes do not publish their heartbeat.

For more info about the YAML representation, read the PyUAVCAN documentation on builtin-based representations.

Unless overridden, the following defaults are used:
- Mode operational.
- Health nominal.
- Vendor-specific status code equals (PID %% 100) of the command, where PID is its process-ID.
Default: %(default)s
'''.strip())
        node_info_fields = {
            'protocol_version': {
                'major': pyuavcan.UAVCAN_SPECIFICATION_VERSION[0],
                'minor': pyuavcan.UAVCAN_SPECIFICATION_VERSION[1],
            },
            'software_version': {
                'major': pyuavcan.__version_info__[0],
                'minor': pyuavcan.__version_info__[1],
            },
            'name': 'org.uavcan.pyuavcan.cli.' + self._node_name_suffix,
        }

        def construct_node_info_fields(text: str) -> typing.Dict[str, typing.Any]:
            out = node_info_fields.copy()
            out.update(YAMLLoader().load(text))
            return out

        parser.add_argument(
            '--node-info-fields',
            default='{}',
            type=construct_node_info_fields,
            metavar='YAML_FIELDS',
            help=f'''
Value of the node info response uavcan.node.GetInfo returned by the node. This argument overrides the following
defaults per-field:

{YAMLDumper().dumps(node_info_fields).strip()}

For more info about the YAML representation, read the PyUAVCAN documentation on builtin-based representations.
Default: %(default)s
'''.strip())

    def construct_subsystem(self, args: argparse.Namespace) -> object:
        """
        We use object instead of Node because the Node class requires generated code to be generated.
        """
        from pyuavcan import application
        from pyuavcan.application import heartbeat_publisher

        node_info = pyuavcan.dsdl.update_from_builtin(application.NodeInfo(), args.node_info_fields)
        _logger.debug('Node info: %r', node_info)

        transport = self._transport_factory.construct_subsystem(args)
        presentation = pyuavcan.presentation.Presentation(transport)
        node = application.Node(presentation, info=node_info)
        try:
            # Configure the heartbeat publisher.
            if args.heartbeat_fields.pop('uptime', None) is not None:
                _logger.warning('Specifying uptime has no effect because it will be overridden by the node.')
            node.heartbeat_publisher.health = \
                args.heartbeat_fields.pop('health', heartbeat_publisher.Health.NOMINAL)
            node.heartbeat_publisher.mode = \
                args.heartbeat_fields.pop('mode', heartbeat_publisher.Mode.OPERATIONAL)
            node.heartbeat_publisher.vendor_specific_status_code = args.heartbeat_fields.pop(
                'vendor_specific_status_code', os.getpid() % 100
            )
            _logger.debug('Node heartbeat: %r', node.heartbeat_publisher.make_message())
            if args.heartbeat_fields:
                raise ValueError(f'Unrecognized heartbeat fields: {args.heartbeat_fields}')

            # Check the node-ID configuration.
            if not self._allow_anonymous and node.presentation.transport.local_node_id is None:
                raise ValueError('The specified transport is configured in anonymous mode, '
                                 'which cannot be used with the selected command. '
                                 'Please specify the node-ID explicitly, or use a different transport.')

            # Configure the transfer-ID map.
            # Register save on exit even if we're anonymous because the local node-ID may be provided later.
            self._register_output_transfer_id_map_save_at_exit(node.presentation)
            # Restore if we have a node-ID. If we don't, no restoration will take place even if the node-ID is
            # provided later. This behavior is acceptable for CLI; a regular UAVCAN application will not need
            # to deal with saving/restoration at all since this use case is specific to CLI only.
            if node.presentation.transport.local_node_id is not None:
                tid_map_path = _get_output_transfer_id_file_path(node.presentation.transport.local_node_id,
                                                                 node.presentation.transport.descriptor)
                _logger.debug('Output TID map file: %s', tid_map_path)
                tid_map = self._restore_output_transfer_id_map(tid_map_path)
                _logger.debug('Output TID map with %d records from %s', len(tid_map), tid_map_path)
                _logger.debug('Output TID map dump: %r', tid_map)
                # noinspection PyTypeChecker
                presentation.output_transfer_id_map.update(tid_map)  # type: ignore
            else:
                _logger.debug('Output TID map not restored because the local node is anonymous.')

            return node
        except Exception:
            node.close()
            raise

    @staticmethod
    def _restore_output_transfer_id_map(file_path: pathlib.Path) \
            -> typing.Dict[object, pyuavcan.presentation.OutgoingTransferIDCounter]:
        try:
            with open(str(file_path), 'rb') as f:
                tid_map = pickle.load(f)
        except Exception as ex:
            _logger.info('Output TID map: Could not restore from file %s: %s: %s', file_path, type(ex).__name__, ex)
            return {}

        mtime_abs_diff = abs(file_path.stat().st_mtime - time.time())
        if mtime_abs_diff > OUTPUT_TRANSFER_ID_MAP_MAX_AGE:
            _logger.debug('Output TID map: File %s is valid but too old: mtime age diff %.0f s',
                          file_path, mtime_abs_diff)
            return {}

        if isinstance(tid_map, dict) and all(isinstance(v, pyuavcan.presentation.OutgoingTransferIDCounter)
                                             for v in tid_map.values()):
            return tid_map
        else:
            _logger.warning('Output TID map file %s contains invalid data of type %s',
                            file_path, type(tid_map).__name__)
            return {}

    @staticmethod
    def _register_output_transfer_id_map_save_at_exit(presentation: pyuavcan.presentation.Presentation) -> None:
        # We MUST sample the configuration early because if this is a redundant transport it may reset its
        # reported descriptor and local node-ID back to default after close().
        local_node_id = presentation.transport.local_node_id
        descriptor = presentation.transport.descriptor

        def do_save_at_exit() -> None:
            if local_node_id is not None:
                file_path = _get_output_transfer_id_file_path(local_node_id, descriptor)
                tmp_path = f'{file_path}.{os.getpid()}.{time.time_ns()}.tmp'
                _logger.debug('Output TID map save: %s --> %s', tmp_path, file_path)
                with open(tmp_path, 'wb') as f:
                    pickle.dump(presentation.output_transfer_id_map, f)
                # We use replace for compatibility reasons. On POSIX, a call to rename() will be made, which is
                # guaranteed to be atomic. On Windows this may fall back to non-atomic copy, which is still
                # acceptable for us here. If the file ends up being damaged, we'll simply ignore it at next startup.
                os.replace(tmp_path, str(file_path))
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            else:
                _logger.debug('Output TID map NOT saved because the transport instance is anonymous')

        atexit.register(do_save_at_exit)


def _get_output_transfer_id_file_path(local_node_id: int, transport_descriptor: str) -> pathlib.Path:
    replacement_char = '-'
    fname = ','.join(
        sorted(map(lambda s: re.sub(r'[\\/*?:"<>| ]+', replacement_char, s).strip(replacement_char),
                   xml.etree.ElementTree.fromstring(transport_descriptor).itertext()))
    ) or 'unnamed'
    directory = OUTPUT_TRANSFER_ID_MAP_DIR / f'node-id-{local_node_id}'
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f'{fname}.pickle'


def _unittest_output_tid_file_path() -> None:
    assert _get_output_transfer_id_file_path(
        123,
        '<redundant><can media="socketcan" mtu="64">can0</can><serial baudrate="115200">COM9</serial></redundant>'
    ).stem == 'COM9,can0'

    # It MUST be order-invariant
    assert _get_output_transfer_id_file_path(
        123,
        '<redundant>'
        '<serial baudrate="115200">/dev/ttyACM0</serial>'
        '<can media="socketcan" mtu="64">can0</can>'
        '</redundant>'
    ).stem == 'can0,dev-ttyACM0'

    assert _get_output_transfer_id_file_path(123, '<serial baudrate="115200">COM9</serial>').stem == 'COM9'

    assert _get_output_transfer_id_file_path(123456, '<loopback/>').stem == 'unnamed'
