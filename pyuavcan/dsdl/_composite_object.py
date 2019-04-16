#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
#

import typing
import pydsdl
import pickle
import gzip
import base64
from ._serialized_representation import SerializedRepresentation


class CompositeObject:
    """
    Base class of an instance of a DSDL composite type.
    The entities follow the naming pattern "_.*_" to avoid collisions with DSDL attributes.
    """
    _TYPE_: pydsdl.CompositeType = None

    def _serialize_(self) -> SerializedRepresentation:
        raise NotImplementedError

    @staticmethod
    def _deserialize_(sr: SerializedRepresentation) -> 'CompositeObject':
        raise NotImplementedError

    @staticmethod
    def _restore_constant_(encoded_string: str) -> object:
        """Recovers a pickled gzipped constant object from base85 string representation."""
        return pickle.loads(gzip.decompress(base64.b85decode(encoded_string)))


def serialize(o: CompositeObject) -> SerializedRepresentation:
    if isinstance(o, CompositeObject):
        # noinspection PyProtectedMember
        return o._serialize_()
    else:
        raise TypeError(f'Cannot serialize an instance of {type(o).__name__}')


def deserialize(cls: typing.Type[CompositeObject], sr: SerializedRepresentation) -> CompositeObject:
    if issubclass(cls, CompositeObject) and isinstance(sr, SerializedRepresentation):
        # noinspection PyProtectedMember
        return cls._deserialize_(sr)
    else:
        raise TypeError(f'Cannot deserialize an instance of {cls} from {type(sr).__name__}')


def get_type(class_or_instance: typing.Union[typing.Type[CompositeObject], CompositeObject]) -> pydsdl.CompositeType:
    if isinstance(class_or_instance, CompositeObject) or issubclass(class_or_instance, CompositeObject):
        # noinspection PyProtectedMember
        return class_or_instance._TYPE_
    else:
        raise TypeError(f'Expected a DSDL object or its type, got {class_or_instance!r}')
