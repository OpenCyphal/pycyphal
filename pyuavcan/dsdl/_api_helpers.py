#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pydsdl
import importlib
from . import _composite_object, _service_object


def get_class(model: pydsdl.CompositeType) -> typing.Type[_composite_object.CompositeObject]:
    """
    Returns the native class implementing the specified DSDL type represented by its PyDSDL model object.
    Assumes that the Python package containing the implementation is in the import lookup path set, otherwise
    raises ImportError. If the package is found but it does not contain the requested type, raises AttributeError.
    """
    if model.parent_service is not None:    # uavcan.node.GetInfo.Request --> uavcan.node.GetInfo then Request
        out = get_class(model.parent_service)
        assert issubclass(out, _service_object.ServiceObject)
        out = getattr(out, model.short_name)
    else:
        mod = None
        for comp in model.name_components[:-1]:
            name = (mod.__name__ + '.' + comp) if mod else comp  # type: ignore
            try:
                mod = importlib.import_module(name)
            except ImportError:                         # We seem to have hit a reserved word; try with an underscore.
                mod = importlib.import_module(name + '_')
        ref = f'{model.short_name}_{model.version.major}_{model.version.minor}'
        out = getattr(mod, ref)
        assert _composite_object.get_model(out) == model

    assert issubclass(out, _composite_object.CompositeObject)
    return out


def get_attribute(obj: typing.Union[_composite_object.CompositeObject, typing.Type[_composite_object.CompositeObject]],
                  name: str) -> typing.Any:
    """
    DSDL type attributes whose names can't be represented in Python (such as "def") are suffixed with an underscore.
    This function allows the caller to read arbitrary attributes referring to them by their original DSDL names,
    e.g., "def" instead of "def_".
    """
    try:
        return getattr(obj, name)
    except AttributeError:
        return getattr(obj, name + '_')


def set_attribute(obj: _composite_object.CompositeObject, name: str, value: typing.Any) -> None:
    """
    DSDL type attributes whose names can't be represented in Python (such as "def") are suffixed with an underscore.
    This function allows the caller to assign arbitrary attributes referring to them by their original DSDL names,
    e.g., "def" instead of "def_".
    """
    suffixed = name + '_'
    # We can't call setattr() without asking first because if it doesn't exist it will be created,
    # which would be disastrous.
    if hasattr(obj, name):
        setattr(obj, name, value)
    elif hasattr(obj, suffixed):
        setattr(obj, suffixed, value)
    else:
        raise AttributeError(suffixed)
