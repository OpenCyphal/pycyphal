#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import random
import asyncio
import logging
import argparse
import contextlib

import pyuavcan
from . import _util


INFO = _util.base.CommandInfo(
    help='''
Automatically find a node-ID value that is not used by any other node that
is currently online. This is a simpler alternative to plug-and-play node-ID
allocation logic defined in Specification. Unlike the solution presented
there, this alternative is non-deterministic and collision-prone; it is
fundamentally unsafe and it should not be used in production. Instead, it is
intended for use in R&D and testing applications, either directly by humans
or from automation scripts. The operating principle is extremely simple and
can be viewed as a simplification of the node-ID claiming procedure defined
in J1939: listen to Heartbeat messages for a short while, build the list of
node-ID values that are currently in use, and then randomly pick a node-ID
from the unused ones. The listening duration is determined heuristically
at run time, but it is never less than one second; for most use cases it is
unlikely to exceed three seconds.
'''.strip(),
    aliases=[
        'pick-nid',
    ]
)


_logger = logging.getLogger(__name__)


def register_arguments(parser: argparse.ArgumentParser) -> None:
    _util.transport.add_arguments(parser)


def execute(args: argparse.Namespace) -> None:
    transport = _util.transport.construct_transport(args)
    asyncio.get_event_loop().run_until_complete(_run(transport=transport))


async def _run(transport: pyuavcan.transport.Transport) -> None:
    import uavcan.node
    node_id_set_cardinality = transport.protocol_parameters.node_id_set_cardinality
    candidates = set(range(node_id_set_cardinality))
    pres = pyuavcan.presentation.Presentation(transport)
    with contextlib.closing(pres):
        deadline = _get_new_deadline()
        sub = pres.make_subscriber_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
        while asyncio.get_event_loop().time() <= deadline:
            result = await sub.receive_with_transfer_until(deadline)
            if result is not None:
                msg, transfer = result
                assert isinstance(transfer, pyuavcan.transport.TransferFrom)
                _logger.debug('Received %r via %r', msg, transfer)
                if transfer.source_node_id is None:
                    _logger.warning('FYI, the network contains an anonymous node which is publishing Heartbeat. '
                                    'Please contact the vendor and inform them that this behavior is non-compliant. '
                                    'The offending heartbeat message is: %r, transfer: %r', msg, transfer)
                else:
                    try:
                        candidates.remove(int(transfer.source_node_id))
                    except LookupError:
                        pass
                    else:
                        _logger.info('Deadline extension; %d candidates left of %d possible',
                                     len(candidates), node_id_set_cardinality)
                        deadline = _get_new_deadline()
            else:
                break

    if not candidates:
        raise RuntimeError(f'All {node_id_set_cardinality} of the available node-ID values are occupied.')
    else:
        pick = random.choice(list(candidates))
        _logger.info('The set of unoccupied node-ID values contains %d elements; the randomly chosen value is %d',
                     len(candidates), pick)
        print(pick)


def _get_new_deadline() -> float:
    import uavcan.node
    return asyncio.get_event_loop().time() + uavcan.node.Heartbeat_1_0.MAX_PUBLICATION_PERIOD
