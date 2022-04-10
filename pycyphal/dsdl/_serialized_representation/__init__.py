# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import sys
from ._serializer import Serializer as Serializer
from ._deserializer import Deserializer as Deserializer

if sys.byteorder != "little":  # pragma: no cover
    raise RuntimeError(
        "BIG-ENDIAN PLATFORMS ARE NOT YET SUPPORTED. "
        "The current serialization code assumes that the native byte order is little-endian. Since Cyphal uses "
        "little-endian byte order in its serialized data representations, this assumption allows us to bypass data "
        "transformation in many cases, resulting in zero-cost serialization and deserialization. "
        "Big-endian platforms are unable to take advantage of that, requiring byte swapping for multi-byte entities; "
        "fortunately, nowadays such platforms are uncommon. If you need to use this library on a big-endian platform, "
        "please implement the missing code and submit a pull request to the upstream, then remove this exception."
    )
