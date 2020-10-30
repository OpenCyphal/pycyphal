#
# Copyright (c) 2020 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
This convenience module implements forwarding between the standard messages ``uavcan.diagnostic.Record``
over the standard subject-ID and the local logging facilities.
"""

import logging
import textwrap
from uavcan.diagnostic import Record_1_1 as Record
from uavcan.diagnostic import Severity_1_0 as Severity
import pyuavcan


_logger = logging.getLogger(__name__)


class DiagnosticSubscriber:
    """
    When started, subscribes to ``uavcan.diagnostic.Record``
    and forwards every received message into the standard Python ``logging`` facility.
    The logger name is that of the current module.

    The log level is mapped as follows:

    +-------------------------------+-------------------+
    | ``uavcan.diagnostic.Severity``| ``logging`` level |
    +===============================+===================+
    | TRACE                         | INFO              |
    +-------------------------------+-------------------+
    | DEBUG                         | INFO              |
    +-------------------------------+-------------------+
    | INFO                          | INFO              |
    +-------------------------------+-------------------+
    | NOTICE                        | INFO              |
    +-------------------------------+-------------------+
    | WARNING                       | WARNING           |
    +-------------------------------+-------------------+
    | ERROR                         | ERROR             |
    +-------------------------------+-------------------+
    | CRITICAL                      | CRITICAL          |
    +-------------------------------+-------------------+
    | ALERT                         | CRITICAL          |
    +-------------------------------+-------------------+

    Such logging behavior is especially convenient for various CLI tools and automation scripts where the user will not
    need to implement additional logic to see log messages from the network.
    """

    _LEVEL_MAP = {
        Severity.TRACE:    logging.INFO,
        Severity.DEBUG:    logging.INFO,
        Severity.INFO:     logging.INFO,
        Severity.NOTICE:   logging.INFO,
        Severity.WARNING:  logging.WARNING,
        Severity.ERROR:    logging.ERROR,
        Severity.CRITICAL: logging.CRITICAL,
        Severity.ALERT:    logging.CRITICAL,
    }

    def __init__(self, presentation: pyuavcan.presentation.Presentation):
        self._sub_record = presentation.make_subscriber_with_fixed_subject_id(Record)

    def start(self) -> None:
        self._sub_record.receive_in_background(self._on_message)

    def close(self) -> None:
        self._sub_record.close()

    async def _on_message(self, msg: Record, meta: pyuavcan.transport.TransferFrom) -> None:
        node_id = meta.source_node_id if meta.source_node_id is not None else 'anonymous'
        diag_text = textwrap.indent(msg.text.tobytes().decode('utf8'), ' ' * 4)
        log_text = f'Received uavcan.diagnostic.Record from node {node_id}; ' \
                   f'severity {msg.severity.value}; ' \
                   f'remote ts {msg.timestamp.microsecond * 1e-6:0.6f} s, local ts {meta.timestamp}; ' \
                   f'text:\n' + diag_text
        level = self._LEVEL_MAP.get(msg.severity.value, logging.CRITICAL)
        _logger.log(level, log_text)
