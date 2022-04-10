# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>
# pylint: disable=protected-access

from __future__ import annotations
import typing
import logging
import importlib

import pydsdl

from . import _serialized_representation


_logger = logging.getLogger(__name__)


def serialize(obj: object) -> typing.Iterable[memoryview]:
    """
    Constructs a serialized representation of the provided top-level object.
    The resulting serialized representation is padded to one byte in accordance with the Cyphal specification.
    The constructed serialized representation is returned as a sequence of byte-aligned fragments which must be
    concatenated in order to obtain the final representation.
    The objective of this model is to avoid copying data into a temporary buffer when possible.
    Each yielded fragment is of type :class:`memoryview` pointing to raw unsigned bytes.
    It is guaranteed that at least one fragment is always returned (which may be empty).
    """
    # TODO: update the Serializer class to emit an iterable of fragments.
    ser = _serialized_representation.Serializer.new(obj._EXTENT_BYTES_)  # type: ignore
    obj._serialize_(ser)  # type: ignore
    yield ser.buffer.data


def deserialize(
    dtype: typing.Type[object] | type, fragmented_serialized_representation: typing.Sequence[memoryview]
) -> typing.Optional[object]:
    """
    Constructs an instance of the supplied DSDL-generated data type from its serialized representation.
    Returns None if the provided serialized representation is invalid.

    This function will never raise an exception for invalid input data; the only possible outcome of an invalid data
    being supplied is None at the output. A raised exception can only indicate an error in the deserialization logic.

    .. important:: The constructed object may contain arrays referencing the memory allocated for the serialized
        representation. Therefore, in order to avoid unintended data corruption, the caller should destroy all
        references to the serialized representation immediately after the invocation.

    .. important:: The supplied fragments of the serialized representation should be writeable.
        If they are not, some of the array-typed fields of the constructed object may be read-only.
    """
    deserializer = _serialized_representation.Deserializer.new(fragmented_serialized_representation)
    try:
        return dtype._deserialize_(deserializer)  # type: ignore
    except _serialized_representation.Deserializer.FormatError:
        _logger.info("Invalid serialized representation of %s: %s", get_model(dtype), deserializer, exc_info=True)
        return None


def get_model(class_or_instance: typing.Any) -> pydsdl.CompositeType:
    """
    Obtains a PyDSDL model of the supplied DSDL-generated class or its instance.
    This is the inverse of :func:`get_class`.
    """
    out = class_or_instance._MODEL_  # type: ignore
    assert isinstance(out, pydsdl.CompositeType)
    return out


def get_class(model: pydsdl.CompositeType) -> type:
    """
    Returns a generated native class implementing the specified DSDL type represented by its PyDSDL model object.
    Promotes the model to delimited type automatically if necessary.
    This is the inverse of :func:`get_model`.

    :raises:
        - :class:`ImportError` if the generated package or subpackage cannot be found.

        - :class:`AttributeError` if the package is found but it does not contain the requested type.

        - :class:`TypeError` if the requested type is found, but its model does not match the input argument.
          This error may occur if the DSDL source has changed since the type was generated.
          To fix this, regenerate the package and make sure that all components of the application use identical
          or compatible DSDL source files.
    """

    def do_import(name_components: typing.List[str]) -> typing.Any:
        mod = None
        for comp in name_components:
            name = (mod.__name__ + "." + comp) if mod else comp  # type: ignore
            try:
                mod = importlib.import_module(name)
            except ImportError:  # We seem to have hit a reserved word; try with an underscore.
                mod = importlib.import_module(name + "_")
        return mod

    if model.has_parent_service:  # uavcan.node.GetInfo.Request --> uavcan.node.GetInfo then Request
        parent_name, child_name = model.name_components[-2:]
        mod = do_import(model.name_components[:-2])
        out = getattr(mod, f"{parent_name}_{model.version.major}_{model.version.minor}")
        out = getattr(out, child_name)
    else:
        mod = do_import(model.name_components[:-1])
        out = getattr(mod, f"{model.short_name}_{model.version.major}_{model.version.minor}")

    out_model = get_model(out)
    if out_model.inner_type != model.inner_type:
        raise TypeError(
            f"The class has been generated using an incompatible DSDL definition. "
            f"Requested model: {model} defined in {model.source_file_path}. "
            f"Model found in the class: {out_model} defined in {out_model.source_file_path}."
        )

    assert str(get_model(out)) == str(model)
    assert isinstance(out, type)
    return out


def get_extent_bytes(class_or_instance: typing.Union[typing.Type[object], type, object]) -> int:
    return int(class_or_instance._EXTENT_BYTES_)  # type: ignore


def get_fixed_port_id(class_or_instance: typing.Union[typing.Type[object], type, object]) -> typing.Optional[int]:
    """
    Returns None if the supplied type has no fixed port-ID.
    """
    try:
        out = int(class_or_instance._FIXED_PORT_ID_)  # type: ignore
    except (TypeError, AttributeError):
        return None
    else:
        assert 0 <= out < 2**16
        return out


def get_attribute(obj: typing.Union[object, typing.Type[object], type], name: str) -> typing.Any:
    """
    DSDL type attributes whose names can't be represented in Python (such as ``def`` or ``type``)
    are suffixed with an underscore.
    This function allows the caller to read arbitrary attributes referring to them by their original
    DSDL names, e.g., ``def`` instead of ``def_``.

    This function behaves like :func:`getattr` if the attribute does not exist.
    """
    try:
        return getattr(obj, name)
    except AttributeError:
        return getattr(obj, name + "_")


def set_attribute(obj: object, name: str, value: typing.Any) -> None:
    """
    DSDL type attributes whose names can't be represented in Python (such as ``def`` or ``type``)
    are suffixed with an underscore.
    This function allows the caller to assign arbitrary attributes referring to them by their original DSDL names,
    e.g., ``def`` instead of ``def_``.

    If the attribute does not exist, raises :class:`AttributeError`.
    """
    suffixed = name + "_"
    # We can't call setattr() without asking first because if it doesn't exist it will be created,
    # which would be disastrous.
    if hasattr(obj, name):
        setattr(obj, name, value)
    elif hasattr(obj, suffixed):
        setattr(obj, suffixed, value)
    else:
        raise AttributeError(name)


def is_serializable(dtype: typing.Type[object] | type) -> bool:
    return (
        hasattr(dtype, "_MODEL_")
        and hasattr(dtype, "_EXTENT_BYTES_")
        and hasattr(dtype, "_serialize_")
        and hasattr(dtype, "_deserialize_")
    )


def is_message_type(dtype: typing.Type[object] | type) -> bool:
    return is_serializable(dtype) and not hasattr(dtype, "Request") and not hasattr(dtype, "Response")


def is_service_type(dtype: typing.Type[object] | type) -> bool:
    return (
        hasattr(dtype, "_MODEL_")
        and is_serializable(getattr(dtype, "Request", None))
        and is_serializable(getattr(dtype, "Response", None))
    )
