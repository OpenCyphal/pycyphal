# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import enum
import typing
import dataclasses
import pyuavcan
from pyuavcan.transport import Trace, TransferTrace, Capture, AlienSessionSpecifier, AlienTransferMetadata
from pyuavcan.transport import AlienTransfer, TransferFrom, Timestamp
from pyuavcan.transport.commons.high_overhead_transport import AlienTransferReassembler, TransferReassembler
from ._frame import SerialFrame
from ._stream_parser import StreamParser


@dataclasses.dataclass(frozen=True)
class SerialCapture(pyuavcan.transport.Capture):
    """
    Since UAVCAN/serial operates on top of unstructured L1 data links, there is no native concept of framing.
    Therefore, the capture type defines only the timestamp, a raw chunk of bytes, and the direction (RX/TX).

    When capturing data from a live interface, it is guaranteed by this library that each capture will contain
    AT MOST one frame along with the delimiter bytes (at least the last byte of the fragment is zero).
    When reading data from a file, it is trivial to split the data into frames by looking for the frame separators,
    which are simply zero bytes.
    See also: :class:`pyuavcan.transport.serial.StreamParser`.
    """

    class Direction(enum.Enum):
        RX = enum.auto()
        """
        Fragment received by the listening node.
        When sniffing on a serial link, all fragments are marked as RX fragments.
        """

        TX = enum.auto()
        """
        This is rather uncommon, it represents the case where the capturing node is also engaged in network exchange.
        Typically, a capturing unit would remain silent, so all captures would be RX captures.
        """

    direction: Direction
    fragment: memoryview

    def __repr__(self) -> str:
        """
        Captures that contain large fragments are truncated and appended with an ellipsis.
        """
        limit = 64
        if len(self.fragment) > limit:
            fragment = bytes(self.fragment[:limit]).hex() + f'...<+{len(self.fragment) - limit}B>...'
        else:
            fragment = bytes(self.fragment).hex()
        return pyuavcan.util.repr_attributes(self, str(self.direction).split('.')[-1], fragment)


@dataclasses.dataclass(frozen=True)
class SerialErrorTrace(pyuavcan.transport.ErrorTrace):
    error: TransferReassembler.Error


@dataclasses.dataclass(frozen=True)
class SerialOutOfBandTrace(pyuavcan.transport.ErrorTrace):
    """
    Out-of-band data or a malformed frame received. See :class:`pyuavcan.serial.StreamParser`.
    """
    data: memoryview


class SerialTracer(pyuavcan.transport.Tracer):
    """
    This tracer does not differentiate between input and output traces,
    but it keeps separate parsers for input and output captures such that there is no RX/TX state conflict.
    If necessary, the user can distinguish RX/TX traces by checking :attr:`SerialCapture.direction`
    before invoking :meth:`update`.

    Return types from :meth:`update`:

    - :class:`pyuavcan.transport.TransferTrace`
    - :class:`SerialErrorTrace`
    - :class:`SerialOutOfBandTrace`
    """

    _MTU = 2 ** 32
    """Effectively unlimited."""

    def __init__(self) -> None:
        self._parsers = {
            SerialCapture.Direction.RX: StreamParser(self._on_parsed, self._MTU),
            SerialCapture.Direction.TX: StreamParser(self._on_parsed, self._MTU),
        }
        self._parser_output: typing.Optional[typing.Tuple[Timestamp, typing.Union[SerialFrame, memoryview]]] = None
        self._sessions: typing.Dict[AlienSessionSpecifier, _AlienSession] = {}

    def update(self, cap: Capture) -> typing.Optional[Trace]:
        """
        If the capture encapsulates more than one serialized frame, a :class:`ValueError` will be raised.
        To avoid this, always ensure that the captured fragments are split on the frame delimiters
        (which are simply zero bytes).
        Captures provided by PyUAVCAN are always fragmented correctly, but you may need to implement fragmentation
        manually when reading data from an external file.
        """
        if not isinstance(cap, SerialCapture):
            return None

        self._parsers[cap.direction].process_next_chunk(cap.fragment, cap.timestamp)
        if self._parser_output is None:
            return None

        timestamp, item = self._parser_output
        self._parser_output = None
        if isinstance(item, memoryview):
            return SerialOutOfBandTrace(timestamp, item)

        elif isinstance(item, SerialFrame):
            spec = AlienSessionSpecifier(source_node_id=item.source_node_id,
                                         destination_node_id=item.destination_node_id,
                                         data_specifier=item.data_specifier)
            return self._get_session(spec).update(timestamp, item)

        else:
            assert False

    def _get_session(self, specifier: AlienSessionSpecifier) -> _AlienSession:
        try:
            return self._sessions[specifier]
        except KeyError:
            self._sessions[specifier] = _AlienSession(specifier)
        return self._sessions[specifier]

    def _on_parsed(self, timestamp: Timestamp, data: memoryview, frame: typing.Optional[SerialFrame]) -> None:
        if self._parser_output is None:
            self._parser_output = timestamp, (data if frame is None else frame)
        else:
            raise ValueError(
                f'The supplied serial capture object contains more than one serialized entity. '
                f'Such arrangement cannot be processed correctly by this implementation. '
                f'Please update the caller code to always fragment the input byte stream at the frame delimiters, '
                f'which are simply zero bytes. '
                f'The timestamp of the offending capture is {timestamp}.'
            )


class _AlienSession:
    def __init__(self, specifier: AlienSessionSpecifier) -> None:
        self._specifier = specifier
        src = specifier.source_node_id
        self._reassembler = AlienTransferReassembler(src) if src is not None else None

    def update(self, timestamp: Timestamp, frame: SerialFrame) -> typing.Optional[Trace]:
        reasm = self._reassembler
        tid_timeout = reasm.transfer_id_timeout if reasm is not None else 0.0

        tr: typing.Union[TransferFrom, TransferReassembler.Error, None]
        if reasm is not None:
            tr = reasm.process_frame(timestamp, frame)
        else:
            tr = TransferReassembler.construct_anonymous_transfer(timestamp, frame)

        if isinstance(tr, TransferReassembler.Error):
            return SerialErrorTrace(timestamp=timestamp, error=tr)
        elif isinstance(tr, TransferFrom):
            meta = AlienTransferMetadata(tr.priority, tr.transfer_id, self._specifier)
            return TransferTrace(timestamp, AlienTransfer(meta, tr.fragmented_payload), tid_timeout)
        else:
            assert tr is None
        return None
