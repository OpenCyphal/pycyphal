#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import gzip
import numpy
import typing
import pydsdl
import pickle
import base64
from ._serialized_representation import Serializer, Deserializer


class CompositeObject:
    """
    Base class of an instance of a DSDL composite type.
    The entities follow the naming pattern "_.*_" to avoid collisions with DSDL attributes.
    """
    # Type definition as provided by PyDSDL.
    _TYPE_: pydsdl.CompositeType = None

    # Defined for serializable types only.
    _SERIALIZED_REPRESENTATION_BUFFER_SIZE_IN_BYTES_: typing.Optional[int] = None

    def _serialize_(self, _ser_: Serializer) -> None:
        raise NotImplementedError

    @staticmethod
    def _deserialize_(_des_: Deserializer) -> 'CompositeObject':
        raise NotImplementedError

    @staticmethod
    def _restore_constant_(encoded_string: str) -> object:
        """Recovers a pickled gzipped constant object from base85 string representation."""
        out = pickle.loads(gzip.decompress(base64.b85decode(encoded_string)))
        assert isinstance(out, object)
        return out


_ClassOrInstance = typing.Union[typing.Type[CompositeObject], CompositeObject]


# noinspection PyProtectedMember
def serialize(o: CompositeObject) -> numpy.ndarray:
    if isinstance(o, CompositeObject) and isinstance(o._SERIALIZED_REPRESENTATION_BUFFER_SIZE_IN_BYTES_, int):
        ser = Serializer.new(o._SERIALIZED_REPRESENTATION_BUFFER_SIZE_IN_BYTES_)
        o._serialize_(ser)
        return ser.buffer
    else:
        raise TypeError(f'Cannot serialize an instance of {type(o).__name__}')


def deserialize(cls: typing.Type[CompositeObject], source_bytes: numpy.ndarray) -> CompositeObject:
    if issubclass(cls, CompositeObject) and isinstance(source_bytes, numpy.ndarray):
        # noinspection PyProtectedMember
        return cls._deserialize_(Deserializer.new(source_bytes))
    else:
        raise TypeError(f'Cannot deserialize an instance of {cls} from {type(source_bytes).__name__}')


def get_type(class_or_instance: _ClassOrInstance) -> pydsdl.CompositeType:
    # noinspection PyProtectedMember
    out = class_or_instance._TYPE_
    assert isinstance(out, pydsdl.CompositeType)
    return out
