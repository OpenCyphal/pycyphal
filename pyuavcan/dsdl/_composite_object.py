#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import gzip
import numpy
import typing
import pydsdl
import pickle
import base64
import logging
from . import _serialized_representation


_logger = logging.getLogger(__name__)


class CompositeObject(abc.ABC):
    """
    Base class of an instance of a DSDL composite type.
    The entities follow the naming pattern "_.*_" to avoid collisions with DSDL attributes.
    """
    # Type definition as provided by PyDSDL.
    _MODEL_: pydsdl.CompositeType

    # Defined in generated classes.
    _SERIALIZED_REPRESENTATION_BUFFER_SIZE_IN_BYTES_: int

    @abc.abstractmethod
    def _serialize_aligned_(self, _ser_: _serialized_representation.Serializer) -> None:
        """
        Auto-generated serialization method.
        Appends the serialized representation of its object to the supplied Serializer instance.
        The current bit offset of the Serializer instance MUST be byte-aligned.
        This is not a part of the API.
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def _deserialize_aligned_(_des_: _serialized_representation.Deserializer) -> CompositeObject:
        """
        Auto-generated deserialization method. Consumes (some) data from the supplied Deserializer instance.
        Raises a Deserializer.FormatError if the supplied serialized representation is invalid.
        Always returns a valid object unless an exception is raised.
        The current bit offset of the Deserializer instance MUST be byte-aligned.
        This is not a part of the API.
        """
        raise NotImplementedError

    @staticmethod
    def _restore_constant_(encoded_string: str) -> object:
        """Recovers a pickled gzipped constant object from base85 string representation."""
        out = pickle.loads(gzip.decompress(base64.b85decode(encoded_string)))
        assert isinstance(out, object)
        return out

    # These typing hints are provided here for use in the generated classes. They are obviously not part of the API.
    _SerializerTypeVar_ = typing.TypeVar('_SerializerTypeVar_', bound=_serialized_representation.Serializer)
    _DeserializerTypeVar_ = typing.TypeVar('_DeserializerTypeVar_', bound=_serialized_representation.Deserializer)


_CompositeObjectTypeVar = typing.TypeVar('_CompositeObjectTypeVar', bound=CompositeObject)


# noinspection PyProtectedMember
def serialize(obj: CompositeObject) -> numpy.ndarray:
    """
    Constructs a serialized representation of the provided top-level object.
    The returned serialized representation is padded to one byte in accordance with the Specification.
    The type of the returned array is numpy.array(dtype=numpy.uint8) with the WRITEABLE flag set to False.
    """
    if isinstance(obj, CompositeObject) and isinstance(obj._SERIALIZED_REPRESENTATION_BUFFER_SIZE_IN_BYTES_, int):
        ser = _serialized_representation.Serializer.new(obj._SERIALIZED_REPRESENTATION_BUFFER_SIZE_IN_BYTES_)
        obj._serialize_aligned_(ser)
        return ser.buffer
    else:
        raise TypeError(f'Cannot serialize an instance of {type(obj).__name__}')


# noinspection PyProtectedMember
def try_deserialize(cls: typing.Type[_CompositeObjectTypeVar],
                    source_bytes: typing.Union[bytearray, numpy.ndarray]) -> typing.Optional[_CompositeObjectTypeVar]:
    """
    Constructs a Python object representing an instance of the supplied data type from its serialized representation.
    Returns None if the provided serialized representation is invalid.
    This function will never raise an exception for invalid input data; the only possible outcome of an invalid data
    being supplied is None at the output. A raised exception can only indicate an error in the deserialization logic.

    SAFETY WARNING: THE CONSTRUCTED OBJECT MAY CONTAIN ARRAYS REFERENCING THE MEMORY ALLOCATED FOR THE SERIALIZED
                    REPRESENTATION. THEREFORE, IN ORDER TO AVOID UNINTENDED DATA CORRUPTION, THE CALLER MUST DESTROY
                    ALL REFERENCES TO THE SERIALIZED REPRESENTATION IMMEDIATELY AFTER THE INVOCATION.

    PERFORMANCE WARNING: The supplied array containing the serialized representation should be writeable. If it is not,
                         the deserialization routine will be unable to implement zero-copy array deserialization.
                         This is why we support bytearray but not bytes.

    >> import pyuavcan.dsdl
    >> import uavcan.primitive.array
    >> b = bytearray([2, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    >> msg = pyuavcan.dsdl.try_deserialize(uavcan.primitive.array.Natural32_1_0, b)
    >> msg
    uavcan.primitive.array.Natural32.1.0(value=[67305985, 134678021])
    >> msg.value[0] = 0xFFFFFFFF
    >> list(b)                     # Source array has been updated
    [2, 255, 255, 255, 255, 5, 6, 7, 8, 9]
    """
    try:
        return cls._deserialize_aligned_(_serialized_representation.Deserializer.new(source_bytes))  # type: ignore
    except _serialized_representation.Deserializer.FormatError:
        # Use explicit level check to avoid unnecessary load in production.
        # This is necessary because we perform complex data transformations before invoking the logger.
        if _logger.isEnabledFor(logging.INFO):
            _logger.info('Invalid serialized representation of %s (in Base64): %s',
                         get_model(cls), base64.b64encode(bytes(source_bytes)).decode(), exc_info=True)
        return None


def get_model(class_or_instance: typing.Union[typing.Type[CompositeObject], CompositeObject]) -> pydsdl.CompositeType:
    # noinspection PyProtectedMember
    out = class_or_instance._MODEL_
    assert isinstance(out, pydsdl.CompositeType)
    return out
