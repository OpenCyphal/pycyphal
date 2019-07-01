#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from ._compiler import generate_package as generate_package
from ._compiler import GeneratedPackageInfo as GeneratedPackageInfo

from ._composite_object import serialize as serialize
from ._composite_object import try_deserialize as try_deserialize

from ._composite_object import CompositeObject as CompositeObject
from ._composite_object import ServiceObject as ServiceObject

from ._composite_object import FixedPortCompositeObject as FixedPortCompositeObject
from ._composite_object import FixedPortServiceObject as FixedPortServiceObject
from ._composite_object import FixedPortObject as FixedPortObject

from ._composite_object import get_fixed_port_id as get_fixed_port_id
from ._composite_object import get_model as get_model
from ._composite_object import get_class as get_class
from ._composite_object import get_max_serialized_representation_size_bytes as \
    get_max_serialized_representation_size_bytes

from ._composite_object import get_attribute as get_attribute
from ._composite_object import set_attribute as set_attribute
