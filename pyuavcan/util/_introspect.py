# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import types
import typing
import pkgutil
import importlib


T = typing.TypeVar("T", bound=object)  # https://github.com/python/mypy/issues/5374


def iter_descendants(ty: typing.Type[T]) -> typing.Iterable[typing.Type[T]]:
    # noinspection PyTypeChecker,PyUnresolvedReferences
    """
    Returns a recursively descending iterator over all subclasses of the argument.

    >>> class A: pass
    >>> class B(A): pass
    >>> class C(B): pass
    >>> class D(A): pass
    >>> set(iter_descendants(A)) == {B, C, D}
    True
    >>> list(iter_descendants(D))
    []
    >>> bool in set(iter_descendants(int))
    True

    Practical example -- discovering what transports are available:

    >>> import pyuavcan
    >>> pyuavcan.util.import_submodules(pyuavcan.transport)
    >>> list(sorted(map(lambda t: t.__name__, pyuavcan.util.iter_descendants(pyuavcan.transport.Transport))))
    [...'CANTransport'...'RedundantTransport'...'SerialTransport'...]
    """
    # noinspection PyArgumentList
    for t in ty.__subclasses__():
        yield t
        yield from iter_descendants(t)


def import_submodules(
    root_module: types.ModuleType, error_handler: typing.Optional[typing.Callable[[str, ImportError], None]] = None
) -> None:
    # noinspection PyTypeChecker,PyUnresolvedReferences
    """
    Recursively imports all submodules and subpackages of the specified Python module or package.
    This is mostly intended for automatic import of all available specialized implementations
    of a certain functionality when they are spread out through several submodules which are not
    auto-imported.

    :param root_module: The module to start the recursive descent from.

    :param error_handler: If None (default), any :class:`ImportError` is raised normally,
        thereby terminating the import process after the first import error (e.g., a missing dependency).
        Otherwise, this would be a function that is invoked whenever an import error is encountered
        instead of raising the exception. The arguments are:

        - the name of the parent module whose import could not be completed due to the error;
        - the culprit of type :class:`ImportError`.

    >>> import pyuavcan
    >>> pyuavcan.util.import_submodules(pyuavcan.transport)  # One missing dependency would fail everything.
    >>> pyuavcan.transport.loopback.LoopbackTransport
    <class 'pyuavcan.transport.loopback...LoopbackTransport'>

    >>> import tests.util.import_error  # For demo purposes, this package contains a missing import.
    >>> pyuavcan.util.import_submodules(tests.util.import_error)  # Yup, it fails.
    Traceback (most recent call last):
      ...
    ModuleNotFoundError: No module named 'nonexistent_module_should_raise_import_error'
    >>> pyuavcan.util.import_submodules(tests.util.import_error,  # The handler allows us to ignore ImportError.
    ...                                 lambda parent, ex: print(parent, ex.name))
    tests.util.import_error._subpackage nonexistent_module_should_raise_import_error
    """
    for _, module_name, _ in pkgutil.walk_packages(root_module.__path__, root_module.__name__ + "."):  # type: ignore
        try:
            importlib.import_module(module_name)
        except ImportError as ex:
            if error_handler is None:
                raise
            error_handler(module_name, ex)
