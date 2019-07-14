#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import types
import typing
import pkgutil
import importlib


T = typing.TypeVar('T', bound=object)


def iter_descendants(ty: typing.Type[T]) -> typing.Iterable[typing.Type[T]]:
    """
    Returns a recursively descending iterator over all subclasses of the argument.

    >>> class A: pass
    >>> class B(A): pass
    >>> class C(B): pass
    >>> class D(A): pass
    >>> set(iter_descendants(A)) == {B, C, D}
    True
    >>> set(iter_descendants(D))
    set()
    >>> bool in set(iter_descendants(int))
    True
    """
    # noinspection PyArgumentList
    for t in ty.__subclasses__():
        yield t
        yield from iter_descendants(t)


def import_submodules(root_module: types.ModuleType) -> None:
    # noinspection PyTypeChecker,PyUnresolvedReferences
    """
    Recursively imports all submodules and subpackages of the specified Python module or package.
    This is mostly intended for automatic import of all available specialized implementations
    of a certain functionality when they are spread out through several submodules which are not
    auto-imported.

    >>> import pyuavcan
    >>> pyuavcan.util.import_submodules(pyuavcan.transport)
    >>> pyuavcan.transport.can.media.socketcan.SocketCANMedia
    <class 'pyuavcan.transport.can.media.socketcan...SocketCANMedia'>
    """
    for _, module_name, _ in pkgutil.walk_packages(root_module.__path__, root_module.__name__ + '.'):
        importlib.import_module(module_name)
