#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import gzip
import typing
import pickle
import base64
import pathlib
import keyword
import builtins
import itertools

import pydsdl
import pydsdlgen
import pydsdlgen.jinja


_AnyPath = typing.Union[str, pathlib.Path]

_ILLEGAL_IDENTIFIERS: typing.Set[str] = set(map(str, list(keyword.kwlist) + dir(builtins)))

_SOURCE_DIRECTORY: pathlib.Path = pathlib.Path(__file__).parent

_TEMPLATE_DIRECTORY: pathlib.Path = _SOURCE_DIRECTORY / pathlib.Path('_templates')


def generate_python_package_from_dsdl_namespace(package_parent_directory: _AnyPath,
                                                root_namespace_directory: _AnyPath,
                                                lookup_directories: typing.Iterable[_AnyPath],
                                                allow_unregulated_fixed_port_id: bool = False) -> pathlib.Path:
    # Read the DSDL definitions
    composite_types = pydsdl.read_namespace(root_namespace_directory=str(root_namespace_directory),
                                            lookup_directories=list(map(str, lookup_directories)),
                                            allow_unregulated_fixed_port_id=allow_unregulated_fixed_port_id)
    root_namespace_name, = set(map(lambda x: x.root_namespace, composite_types))  # type: str,

    # Template primitives
    filters = {
        'id':                _make_identifier,
        'alignment_prefix':  _make_serialization_alignment_prefix,
        'pickle':            _pickle_object,
        'numpy_scalar_type': _numpy_scalar_type,
        'longest_id_length': lambda c: max(map(len, map(lambda x: _make_identifier(x), c))),
        'imports':           _list_imports,
    }

    tests = _construct_instance_tests_from_root(pydsdl.SerializableType)
    tests['PaddingField'] = lambda x: isinstance(x, pydsdl.PaddingField)
    tests['saturated'] = _test_if_saturated

    # Generate code
    root_ns = pydsdlgen.build_namespace_tree(types=composite_types,
                                             root_namespace_dir=root_namespace_directory,
                                             output_dir=str(package_parent_directory),
                                             extension='.py',
                                             namespace_output_stem='__init__')

    generator = pydsdlgen.jinja.Generator(namespace=root_ns,
                                          generate_namespace_types=True,
                                          templates_dir=_TEMPLATE_DIRECTORY,
                                          followlinks=True,
                                          additional_filters=filters,
                                          additional_tests=tests)
    generator.generate_all()

    return pathlib.Path(package_parent_directory) / pathlib.Path(root_namespace_name)


def _make_identifier(a: pydsdl.Attribute) -> str:
    out = (a.name + '_') if a.name in _ILLEGAL_IDENTIFIERS else a.name
    assert isinstance(out, str)
    return out


def _make_serialization_alignment_prefix(offset: pydsdl.BitLengthSet) -> str:
    if isinstance(offset, pydsdl.BitLengthSet):
        return 'aligned' if offset.is_aligned_at_byte() else 'unaligned'
    else:
        raise ValueError(f'Expected BitLengthSet, got {type(offset).__name__}')


def _pickle_object(x: typing.Any) -> str:
    pck: str = base64.b85encode(gzip.compress(pickle.dumps(x, protocol=4))).decode().strip()
    segment_gen = map(''.join, itertools.zip_longest(*([iter(pck)] * 100), fillvalue=''))
    return '\n'.join(repr(x) for x in segment_gen)


def _numpy_scalar_type(t: pydsdl.Any) -> str:
    def pick_width(w: int) -> int:
        for o in [8, 16, 32, 64]:
            if w <= o:
                return o
        raise ValueError(f'Invalid bit width: {w}')

    if isinstance(t, pydsdl.BooleanType):
        return f'_np_.bool'
    elif isinstance(t, pydsdl.SignedIntegerType):
        return f'_np_.int{pick_width(t.bit_length)}'
    elif isinstance(t, pydsdl.UnsignedIntegerType):
        return f'_np_.uint{pick_width(t.bit_length)}'
    elif isinstance(t, pydsdl.FloatType):
        return f'_np_.float{pick_width(t.bit_length)}'
    else:
        assert not isinstance(t, pydsdl.PrimitiveType), 'Forgot to handle some primitive types'
        return f'_np_.object_'


def _list_imports(t: pydsdl.CompositeType) -> typing.List[str]:
    # Make a list of all attributes defined by this type
    if isinstance(t, pydsdl.ServiceType):
        atr = t.request_type.attributes + t.response_type.attributes
    else:
        atr = t.attributes

    # Extract data types of said attributes; for type constructors such as arrays extract the element type
    dep_types = list(map(lambda x: x.data_type, atr))  # type: ignore
    for t in dep_types[:]:
        if isinstance(t, pydsdl.ArrayType):
            dep_types.append(t.element_type)

    # Make a list of unique full namespaces of referenced composites
    return list(sorted(set(x.full_namespace for x in dep_types if isinstance(x, pydsdl.CompositeType))))


def _test_if_saturated(t: pydsdl.PrimitiveType) -> bool:
    if isinstance(t, pydsdl.PrimitiveType):
        return {
            pydsdl.PrimitiveType.CastMode.SATURATED: True,
            pydsdl.PrimitiveType.CastMode.TRUNCATED: False,
        }[t.cast_mode]
    else:
        raise ValueError(f'Cast mode is not defined for {type(t).__name__}')


def _construct_instance_tests_from_root(root: typing.Type[object]) \
        -> typing.Dict[str, typing.Callable[[typing.Any], bool]]:
    out = {
        root.__name__: lambda x: isinstance(x, root)
    }
    # noinspection PyArgumentList
    for derived in root.__subclasses__():
        out.update(_construct_instance_tests_from_root(derived))
    return out


# noinspection PyUnusedLocal
def _unittest_instance_tests_from_root() -> None:
    class A:
        pass

    class B(A):
        pass

    class C(B):
        pass

    class D(A):
        pass

    assert set(_construct_instance_tests_from_root(A).keys()) == {'A', 'B', 'C', 'D'}
