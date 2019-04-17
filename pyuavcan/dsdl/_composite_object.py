#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
#

import typing
import pydsdl
import pickle
import gzip
import base64
from ._serialized_representation import Serializer, Deserializer


class CompositeObject:
    """
    Base class of an instance of a DSDL composite type.
    The entities follow the naming pattern "_.*_" to avoid collisions with DSDL attributes.
    """
    # Type definition as provided by PyDSDL.
    _TYPE_: pydsdl.CompositeType = None

    # The number of bytes that are necessary for holding a serialized representation of an instance of this type.
    # Undefined for service types.
    _SERIALIZED_REPRESENTATION_BUFFER_SIZE_BYTES_: typing.Optional[int] = None

    def _serialize_(self, destination: Serializer) -> None:
        raise NotImplementedError

    @staticmethod
    def _deserialize_(source: Deserializer) -> 'CompositeObject':
        raise NotImplementedError

    @staticmethod
    def _restore_constant_(encoded_string: str) -> object:
        """Recovers a pickled gzipped constant object from base85 string representation."""
        return pickle.loads(gzip.decompress(base64.b85decode(encoded_string)))


_ClassOrInstance = typing.Union[typing.Type[CompositeObject], CompositeObject]


def serialize(o: CompositeObject) -> Serializer:
    if isinstance(o, CompositeObject):
        destination = Serializer()
        # noinspection PyProtectedMember
        o._serialize_(destination)
        return destination
    else:
        raise TypeError(f'Cannot serialize an instance of {type(o).__name__}')


def deserialize(cls: typing.Type[CompositeObject], source: Deserializer) -> CompositeObject:
    if issubclass(cls, CompositeObject) and isinstance(source, Deserializer):
        # noinspection PyProtectedMember
        return cls._deserialize_(source)
    else:
        raise TypeError(f'Cannot deserialize an instance of {cls} from {type(source).__name__}')


def get_type(class_or_instance: _ClassOrInstance) -> pydsdl.CompositeType:
    # noinspection PyProtectedMember
    out = class_or_instance._TYPE_
    assert isinstance(out, pydsdl.CompositeType)
    return out


def get_serialized_representation_buffer_size_in_bytes(class_or_instance: _ClassOrInstance) -> int:
    # noinspection PyProtectedMember
    out = class_or_instance._SERIALIZED_REPRESENTATION_BUFFER_SIZE_BYTES_
    if isinstance(out, int):
        return out
    else:
        raise TypeError(f'Type {get_type(class_or_instance)} cannot be directly serialized or deserialized')
