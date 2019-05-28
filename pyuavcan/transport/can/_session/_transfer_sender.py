#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import itertools
import pyuavcan.util
from .. import _frame, media as _media


_PADDING_PATTERN = b'\x55'


def serialize_transfer(can_identifier:        int,
                       transfer_id:           int,
                       fragmented_payload:    typing.Sequence[memoryview],
                       max_data_field_length: int,
                       loopback:              bool) -> typing.Iterable[_media.DataFrame]:
    if max_data_field_length < 2:  # pragma: no cover
        raise ValueError(f'Invalid max data field length: {max_data_field_length}')

    max_single_frame_payload_length = max_data_field_length - 1
    payload_length = sum(map(len, fragmented_payload))

    if payload_length <= max_single_frame_payload_length:       # SINGLE-FRAME TRANSFER
        padding_length = _media.DataFrame.get_required_padding(payload_length + 1)  # +1 for the tail byte
        refragmented = pyuavcan.util.refragment(itertools.chain(fragmented_payload,
                                                                (memoryview(_PADDING_PATTERN * padding_length),)),
                                                max_single_frame_payload_length)
        payload, = tuple(refragmented)
        assert max_single_frame_payload_length >= len(payload) >= payload_length
        ufr = _frame.UAVCANFrame(identifier=can_identifier,
                                 padded_payload=payload,
                                 transfer_id=transfer_id,
                                 start_of_transfer=True,
                                 end_of_transfer=True,
                                 toggle_bit=True,
                                 loopback=loopback)
        yield ufr.compile()
    else:                                                       # MULTI-FRAME TRANSFER
        # Compute padding
        last_frame_payload_length = payload_length % max_single_frame_payload_length
        if last_frame_payload_length + _frame.TRANSFER_CRC_LENGTH_BYTES >= max_single_frame_payload_length:
            padding = b''
        else:
            last_frame_data_length = last_frame_payload_length + _frame.TRANSFER_CRC_LENGTH_BYTES + 1
            assert last_frame_data_length <= max_data_field_length
            padding = _PADDING_PATTERN * _media.DataFrame.get_required_padding(last_frame_data_length)

        # Compute CRC; padding is also CRC-protected
        crc = pyuavcan.util.hash.CRC16CCITT()
        for frag in fragmented_payload:
            crc.add(frag)
        crc.add(padding)

        # Fragment generator that goes over the padding and CRC also
        trailing_bytes = padding + bytes([crc.value >> 8, crc.value & 0xFF])
        refragmented = pyuavcan.util.refragment(itertools.chain(fragmented_payload, (memoryview(trailing_bytes),)),
                                                max_single_frame_payload_length)

        # Serialized frame emission
        for index, (last, frag) in enumerate(pyuavcan.util.mark_last(refragmented)):
            ufr = _frame.UAVCANFrame(identifier=can_identifier,
                                     padded_payload=frag,
                                     transfer_id=transfer_id,
                                     start_of_transfer=index == 0,
                                     end_of_transfer=last,
                                     toggle_bit=index % 2 == 0,
                                     loopback=loopback)
            yield ufr.compile()


def _unittest_can_serialize_transfer() -> None:
    mv = memoryview
    meta = typing.TypeVar('meta')

    def mkf(identifier:        int,
            data:              typing.Union[bytearray, bytes],
            transfer_id:       int,
            start_of_transfer: bool,
            end_of_transfer:   bool,
            toggle_bit:        bool,
            loopback:          bool = False) -> _media.DataFrame:
        tail = transfer_id
        if start_of_transfer:
            tail |= 1 << 7
        if end_of_transfer:
            tail |= 1 << 6
        if toggle_bit:
            tail |= 1 << 5

        data = bytearray(data)
        data.append(tail)

        return _media.DataFrame(identifier=identifier,
                                data=data,
                                format=_media.FrameFormat.EXTENDED,
                                loopback=loopback)

    def one(items: typing.Iterable[meta]) -> meta:
        out, = list(items)
        return out

    assert mkf(0xbadc0fe, b'Hello', 0, True, True, True) \
        == one(serialize_transfer(0xbadc0fe, 32, [mv(b'Hell'), mv(b'o')], 8, False))

    assert mkf(0xbadc0fe, bytes(range(60)) + b'\x55\x55\x55', 19, True, True, True, True) \
        == one(serialize_transfer(0xbadc0fe, 32 + 19, [mv(bytes(range(60)))], 64, True))

    crc = pyuavcan.util.hash.CRC16CCITT()
    crc.add(bytes(range(0x1E)))
    assert crc.value == 0x3554
    assert [
        mkf(0xbadc0fe, b'\x00\x01\x02\x03\x04\x05\x06', 19, True, False, True),
        mkf(0xbadc0fe, b'\x07\x08\x09\x0a\x0b\x0c\x0d', 19, False, False, False),
        mkf(0xbadc0fe, b'\x0e\x0f\x10\x11\x12\x13\x14', 19, False, False, True),
        mkf(0xbadc0fe, b'\x15\x16\x17\x18\x19\x1a\x1b', 19, False, False, False),
        mkf(0xbadc0fe, b'\x1c\x1d\x35\x54', 19, False, True, True),
    ] == list(serialize_transfer(0xbadc0fe, 323219, [mv(bytes(range(0x1E)))], 8, False))

    crc = pyuavcan.util.hash.CRC16CCITT()
    crc.add(bytes(range(0x1D)))
    assert crc.value == 0xC46F
    assert [
        mkf(123456, b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e', 19, True, False, True, True),
        mkf(123456, b'\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\xc4', 19, False, False, False, True),
        mkf(123456, b'\x6f', 19, False, True, True, True),
    ] == list(serialize_transfer(123456, 32323219, [mv(bytes(range(0x1D)))], 16, True))

    crc = pyuavcan.util.hash.CRC16CCITT()
    crc.add(bytes(range(0x1E)) + b'\x55')
    assert crc.value == 0x38A6
    assert [
        mkf(123456, b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a', 19, True, False, True),
        mkf(123456, b'\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15', 19, False, False, False),
        mkf(123456, b'\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x55\x38\xa6', 19, False, True, True),
    ] == list(serialize_transfer(123456, 32323219, [mv(bytes(range(0x1E)))], 12, False))
