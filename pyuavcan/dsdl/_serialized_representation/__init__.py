#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import typing
from ._serializer import SerializerBase as _SerializerBase
from ._deserializer import DeserializerBase as _DeserializerBase


Serializer:   typing.Type[_SerializerBase]
Deserializer: typing.Type[_DeserializerBase]


if sys.byteorder == 'little':   # pragma: no cover
    from ._serializer import LittleEndianSerializer as Serializer
    from ._deserializer import LittleEndianDeserializer as Deserializer
elif sys.byteorder == 'big':    # pragma: no cover
    from ._serializer import BigEndianSerializer as Serializer
    from ._deserializer import BigEndianDeserializer as Deserializer
    raise RuntimeError(
        'BIG-ENDIAN PLATFORMS ARE NOT YET SUPPORTED. '
        'The current serialization code assumes that the native byte order is little-endian. Since UAVCAN uses '
        'little-endian byte order in its serialized data representations, this assumption allows us to bypass data '
        'transformation in many cases, resulting in zero-cost serialization and deserialization. '
        'Big-endian platforms are unable to take advantage of that, requiring byte swapping for multi-byte entities; '
        'fortunately, nowadays such platforms are uncommon. If you need to use this library on a big-endian platform, '
        'please implement the missing code and submit a pull request to the upstream.'
    )
else:                           # pragma: no cover
    raise RuntimeError(f'Unexpected endianness: {sys.byteorder}')
