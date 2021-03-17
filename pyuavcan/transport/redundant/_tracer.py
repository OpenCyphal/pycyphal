# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import logging
import dataclasses
import pyuavcan
import pyuavcan.transport.redundant
from ._deduplicator import Deduplicator


@dataclasses.dataclass(frozen=True)
class RedundantCapture(pyuavcan.transport.Capture):
    """
    Composes :class:`pyuavcan.transport.Capture` with a reference to the
    transport instance that yielded this capture.
    The user may construct such captures manually when performing postmortem analysis of a network data dump
    to feed them later into :class:`RedundantTracer`.
    """

    inferior: pyuavcan.transport.Capture
    """
    The original capture from the inferior transport.
    """

    iface_id: int
    """
    A unique number that identifies this transport in its redundant group.
    """

    transfer_id_modulo: int
    """
    The number of unique transfer-ID values (that is, the maximum possible transfer-ID plus one)
    for the transport that emitted this capture.
    This is actually a transport-specific constant.
    This value is used by :class:`RedundantTracer` to select the appropriate transfer deduplication strategy.
    """

    @staticmethod
    def get_transport_type() -> typing.Type[pyuavcan.transport.redundant.RedundantTransport]:
        return pyuavcan.transport.redundant.RedundantTransport


@dataclasses.dataclass(frozen=True)
class RedundantDuplicateTransferTrace(pyuavcan.transport.Trace):
    """
    Indicates that the last capture object completed a valid transfer that was discarded as a duplicate
    (either received from another redundant interface or deterministic data loss mitigation (DDLM) is employed).

    Observe that it is NOT a subclass of :class:`pyuavcan.transport.TransferTrace`!
    It shall not be one because duplicates should not be processed normally.
    """


class RedundantTracer(pyuavcan.transport.Tracer):
    """
    The redundant tracer automatically deduplicates transfers received from multiple redundant transports.
    It can be used either in real-time or during postmortem analysis.
    In the latter case the user would construct instances of :class:`RedundantCapture` manually and feed them
    into the tracer one-by-one.
    """

    def __init__(self) -> None:
        self._deduplicators: typing.Dict[RedundantTracer._DeduplicatorSelector, Deduplicator] = {}
        self._last_transfer_id_modulo = 0
        self._inferior_tracers: typing.Dict[
            typing.Tuple[typing.Type[pyuavcan.transport.Transport], int],
            pyuavcan.transport.Tracer,
        ] = {}

    def update(self, cap: pyuavcan.transport.Capture) -> typing.Optional[pyuavcan.transport.Trace]:
        """
        All instances of :class:`pyuavcan.transport.TransferTrace` are deduplicated,
        duplicates are simply dropped and :class:`RedundantDuplicateTransferTrace` is returned.
        All other instances (such as :class:`pyuavcan.transport.ErrorTrace`) are returned unchanged.
        """
        _logger.debug("%r: Processing %r", self, cap)
        if not isinstance(cap, RedundantCapture):
            return None

        if cap.transfer_id_modulo != self._last_transfer_id_modulo:
            _logger.info(
                "%r: TID modulo change detected, resetting state (%d deduplicators dropped): %r --> %r",
                self,
                len(self._deduplicators),
                self._last_transfer_id_modulo,
                cap.transfer_id_modulo,
            )
            # Should we also drop the tracers here? If an inferior transport is removed its tracer will be sitting
            # here useless, we don't want that. But on the other hand, disturbing the state too much is also no good.
            self._last_transfer_id_modulo = cap.transfer_id_modulo
            self._deduplicators.clear()

        tracer = self._get_inferior_tracer(cap.inferior.get_transport_type(), cap.iface_id)
        trace = tracer.update(cap.inferior)
        if not isinstance(trace, pyuavcan.transport.TransferTrace):
            _logger.debug("%r: BYPASS: %r", self, trace)
            return trace

        meta = trace.transfer.metadata
        deduplicator = self._get_deduplicator(
            meta.session_specifier.destination_node_id,
            meta.session_specifier.data_specifier,
            cap.transfer_id_modulo,
        )
        should_accept = deduplicator.should_accept_transfer(
            iface_id=cap.iface_id,
            transfer_id_timeout=trace.transfer_id_timeout,
            timestamp=trace.timestamp,
            source_node_id=meta.session_specifier.source_node_id,
            transfer_id=meta.transfer_id,
        )
        if should_accept:
            _logger.debug("%r: ACCEPT: %r", self, trace)
            return trace
        _logger.debug("%r: REJECT: %r", self, trace)
        return RedundantDuplicateTransferTrace(cap.timestamp)

    def _get_deduplicator(
        self,
        destination_node_id: typing.Optional[int],
        data_specifier: pyuavcan.transport.DataSpecifier,
        transfer_id_modulo: int,
    ) -> Deduplicator:
        selector = RedundantTracer._DeduplicatorSelector(destination_node_id, data_specifier)
        try:
            return self._deduplicators[selector]
        except LookupError:
            dd = Deduplicator.new(transfer_id_modulo)
            _logger.debug("%r: New deduplicator for %r: %r", self, selector, dd)
            self._deduplicators[selector] = dd
        return self._deduplicators[selector]

    def _get_inferior_tracer(
        self,
        inferior_type: typing.Type[pyuavcan.transport.Transport],
        inferior_iface_id: int,
    ) -> pyuavcan.transport.Tracer:
        selector = inferior_type, inferior_iface_id
        try:
            return self._inferior_tracers[selector]
        except LookupError:
            it = inferior_type.make_tracer()
            _logger.debug("%r: New inferior tracer for %r: %r", self, selector, it)
            self._inferior_tracers[selector] = it
        return self._inferior_tracers[selector]

    @dataclasses.dataclass(frozen=True)
    class _DeduplicatorSelector:
        destination_node_id: typing.Optional[int]
        data_specifier: pyuavcan.transport.DataSpecifier

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, self._inferior_tracers)


_logger = logging.getLogger(__name__)
