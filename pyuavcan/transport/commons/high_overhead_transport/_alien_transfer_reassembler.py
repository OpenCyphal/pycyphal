# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
from pyuavcan.transport import TransferFrom, Timestamp
from . import TransferReassembler, Frame


class AlienTransferReassembler:
    """
    This is a wrapper over :class:`TransferReassembler` optimized for tracing rather than real-time communication.
    It implements heuristics optimized for diagnostics and inspection rather than real-time operation.

    The caller is expected to keep a registry (dict) of session tracers indexed by their session specifiers,
    which are extracted from captured transport frames.
    """

    _MAX_INTERVAL = 1.0
    _TID_TIMEOUT_MULTIPLIER = 2.0  # TID = 2*interval as suggested in the Specification.

    _EXTENT_BYTES = 2 ** 32
    """
    The extent is effectively unlimited -- we want to be able to process all transfers.
    """

    def __init__(self, source_node_id: int) -> None:
        self._last_error: typing.Optional[TransferReassembler.Error] = None
        self._reassembler = TransferReassembler(
            source_node_id=source_node_id,
            extent_bytes=AlienTransferReassembler._EXTENT_BYTES,
            on_error_callback=self._register_reassembly_error,
        )
        self._last_transfer_monotonic: float = 0.0
        self._interval = float(AlienTransferReassembler._MAX_INTERVAL)

    def process_frame(
        self, timestamp: Timestamp, frame: Frame
    ) -> typing.Union[TransferFrom, TransferReassembler.Error, None]:
        trf = self._reassembler.process_frame(
            timestamp=timestamp, frame=frame, transfer_id_timeout=self.transfer_id_timeout
        )
        if trf is None:
            out, self._last_error = self._last_error, None
            return out

        # Update the transfer-ID timeout.
        delta = float(trf.timestamp.monotonic) - self._last_transfer_monotonic
        delta = min(AlienTransferReassembler._MAX_INTERVAL, max(0.0, delta))
        self._interval = (self._interval + delta) * 0.5
        self._last_transfer_monotonic = float(trf.timestamp.monotonic)

        return trf

    @property
    def transfer_id_timeout(self) -> float:
        """
        The current value of the auto-deduced transfer-ID timeout.
        It is automatically adjusted whenever a new transfer is received.
        """
        return self._interval * AlienTransferReassembler._TID_TIMEOUT_MULTIPLIER

    def _register_reassembly_error(self, error: TransferReassembler.Error) -> None:
        self._last_error = error
