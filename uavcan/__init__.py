#
# Copyright (C) 2014 Pavel Kirienko <pavel.kirienko@gmail.com>
#

'''
Python UAVCAN package.
Supported Python versions: 3.2+, 2.7.
'''

from __future__ import division, absolute_import, print_function, unicode_literals
import os
import sys
import struct
import pkg_resources
import time
from logging import getLogger

try:
    time.monotonic                          # Works natively in Python 3.3+
except AttributeError:
    try:
        import monotonic                    # 3rd party dependency for old versions @UnresolvedImport
        time.monotonic = monotonic.monotonic
    except ImportError:
        time.monotonic = time.time          # Last resort - using non-monotonic time; this is no good but oh well
        print('''The package 'monotonic' is not available, the library will use real time instead of monotonic time.
This implies that the library may misbehave if system clock is adjusted while the library is running.
In order to fix this problem, consider either option:
 1. Switch to Python 3.
 2. Install the missing package, e.g. using pip:
    pip install monotonic''', file=sys.stderr)


class UAVCANException(Exception):
    pass


import uavcan.node as node
from uavcan.node import make_node
import uavcan.dsdl as dsdl
import uavcan.transport as transport


logger = getLogger(__name__)


class Module(object):
    pass


class Namespace(object):
    "Provides a nice object-based way to look up UAVCAN data types."

    def __init__(self):
        self.__namespaces = set()

    def _path(self, attrpath):
        """Returns the namespace object at the given .-separated path,
        creating any namespaces in the path that don't already exist."""

        attr, _, subpath = attrpath.partition(".")
        if attr not in self.__dict__:
            self.__dict__[attr] = Namespace()
            self.__namespaces.add(attr)

        if subpath:
            return self.__dict__[attr]._path(subpath)
        else:
            return self.__dict__[attr]

    def _namespaces(self):
        "Returns the top-level namespaces in this object"
        return set(self.__namespaces)


MODULE = Module()
DATATYPES = {}
TYPENAMES = {}


def load_dsdl(*paths, **args):
    """Loads the DSDL files under the given directory/directories, and creates
    types for each of them in the current module's namespace.

    If the exclude_dist argument is not present, or False, the DSDL
    definitions installed with this package will be loaded first.

    Also adds entries for all datatype (ID, kind)s to the DATATYPES
    dictionary, which maps datatype (ID, kind)s to their respective type
    classes."""
    global DATATYPES, TYPENAMES

    paths = list(paths)

    # Try to prepend the built-in DSDL files
    try:
        if not args.get("exclude_dist", None):
            dsdl_path = pkg_resources.resource_filename(__name__, "dsdl_files")  # @UndefinedVariable
            paths = [os.path.join(dsdl_path, "uavcan")] + paths
    except Exception:
        pass

    root_namespace = Namespace()
    dtypes = dsdl.parse_namespaces(paths)
    for dtype in dtypes:
        namespace, _, typename = dtype.full_name.rpartition(".")
        root_namespace._path(namespace).__dict__[typename] = dtype
        TYPENAMES[dtype.full_name] = dtype

        if dtype.default_dtid:
            DATATYPES[(dtype.default_dtid, dtype.kind)] = dtype
            # Add the base CRC to each data type capable of being transmitted
            dtype.base_crc = dsdl.common.crc16_from_bytes(struct.pack("<Q", dtype.get_data_type_signature()))
            logger.debug("DSDL Load {: >30} DTID: {: >4} base_crc:{: >8}"
                         .format(typename, dtype.default_dtid, hex(dtype.base_crc)))

        def create_instance_closure(closure_type):
            def create_instance(*args, **kwargs):
                return transport.CompoundValue(closure_type, _tao=True, *args, **kwargs)
            return create_instance

        dtype._instantiate = create_instance_closure(dtype)

    namespace = root_namespace._path("uavcan")
    for top_namespace in namespace._namespaces():
        MODULE.__dict__[str(top_namespace)] = namespace.__dict__[top_namespace]

    MODULE.__dict__["thirdparty"] = Namespace()
    for ext_namespace in root_namespace._namespaces():
        if str(ext_namespace) != "uavcan":
            MODULE.thirdparty.__dict__[str(ext_namespace)] = root_namespace.__dict__[ext_namespace]


__all__ = ["dsdl", "transport", "load_dsdl", "DATATYPES", "TYPENAMES"]


# Hack to support dynamically-generated attributes at the top level of the
# module. It doesn't feel right but it's recommended by Guido:
# https://mail.python.org/pipermail/python-ideas/2012-May/014969.html
MODULE.__dict__ = globals()
MODULE._module = sys.modules[MODULE.__name__]
MODULE._pmodule = MODULE
sys.modules[MODULE.__name__] = MODULE


# Completing package initialization with loading default DSDL definitions
load_dsdl()
