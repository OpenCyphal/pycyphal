#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
from . import _composite_object, _serialized_representation


class ServiceObject(_composite_object.CompositeObject):
    """
    Base class of an instance of a DSDL service type. Remember that a service is a special case of a composite type.
    """
    # Implementations of the nested types provided in the generated implementations.
    Request: typing.Type[_composite_object.CompositeObject]
    Response: typing.Type[_composite_object.CompositeObject]

    _SERIALIZED_REPRESENTATION_BUFFER_SIZE_IN_BYTES_ = 0

    def _serialize_aligned_(self, _ser_: _serialized_representation.Serializer) -> None:
        raise TypeError(f'Service type {type(self).__name__} cannot be serialized')

    @staticmethod
    def _deserialize_aligned_(_des_: _serialized_representation.Deserializer) -> _composite_object.CompositeObject:
        raise TypeError('Service types cannot be deserialized')
