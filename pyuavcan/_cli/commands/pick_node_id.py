#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import typing
import random
import asyncio
import logging
import argparse
import contextlib
import pyuavcan
from . import _subsystems
from ._base import Command, SubsystemFactory


class PickNodeIDCommand(Command):
    @property
    def names(self) -> typing.Sequence[str]:
        return ['pick-node-id', 'pick-nid']

    @property
    def help(self) -> str:
        return '''
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
at run time; for most use cases it is unlikely to exceed three seconds.
'''.strip()

    @property
    def examples(self) -> typing.Optional[str]:
        return None

    @property
    def subsystem_factories(self) -> typing.Sequence[SubsystemFactory]:
        return [
            _subsystems.transport.TransportFactory(),
        ]

    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        del parser
        pass

    def execute(self, args: argparse.Namespace, subsystems: typing.Sequence[object]) -> int:
        transport, = subsystems
        assert isinstance(transport, pyuavcan.transport.Transport)
        return asyncio.get_event_loop().run_until_complete(_run(transport=transport))


_logger = logging.getLogger(__name__)


async def _run(transport: pyuavcan.transport.Transport) -> int:
    import uavcan.node
    node_id_set_cardinality = transport.protocol_parameters.node_id_set_cardinality
    if node_id_set_cardinality >= 2 ** 32:
        # Special case: for very large sets just pick a random number. Very large sets are only possible with test
        # transports such as loopback so it's acceptable. If necessary, later we could develop a more robust solution.
        print(random.randint(0, node_id_set_cardinality - 1))
        return 0

    candidates = set(range(node_id_set_cardinality))
    pres = pyuavcan.presentation.Presentation(transport)
    with contextlib.closing(pres):
        deadline = asyncio.get_event_loop().time() + uavcan.node.Heartbeat_1_0.MAX_PUBLICATION_PERIOD * 2.0
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
                        # If at least one node is in the Initialization state, the network might be starting,
                        # so we need to listen longer to minimize the chance of collision.
                        multiplier = 3.0 if msg.mode == msg.MODE_INITIALIZATION else 1.0
                        advancement = uavcan.node.Heartbeat_1_0.MAX_PUBLICATION_PERIOD * multiplier
                        _logger.info('Deadline advanced by %.1f s; %d candidates left of %d possible',
                                     advancement, len(candidates), node_id_set_cardinality)
                        deadline = max(deadline, asyncio.get_event_loop().time() + advancement)
            else:
                break

    if not candidates:
        print(f'All {node_id_set_cardinality} of the available node-ID values are occupied.', file=sys.stderr)
        return 1
    else:
        pick = random.choice(list(candidates))
        _logger.info('The set of unoccupied node-ID values contains %d elements; the randomly chosen value is %d',
                     len(candidates), pick)
        print(pick)
        return 0
