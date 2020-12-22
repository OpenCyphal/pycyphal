# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

"""
The util package contains various entities that are commonly useful in PyUAVCAN-based applications.
"""

from ._broadcast import broadcast as broadcast

from ._introspect import import_submodules as import_submodules
from ._introspect import iter_descendants as iter_descendants

from ._mark_last import mark_last as mark_last

from ._repr import repr_attributes as repr_attributes
from ._repr import repr_attributes_noexcept as repr_attributes_noexcept
