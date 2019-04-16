#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
#

import gzip
import typing
import pickle
import base64
import pathlib
import logging
import keyword
import builtins
import itertools

import pydsdl
import pydsdlgen
import pydsdlgen.jinja


_AnyPath = typing.Union[str, pathlib.Path]

_ILLEGAL_IDENTIFIERS: typing.Set[str] = set(map(str, keyword.kwlist + dir(builtins)))

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

    # Generate code
    type_to_file_map = pydsdlgen.create_type_map(composite_types, str(package_parent_directory), '.py')

    generator = pydsdlgen.jinja.Generator(type_to_file_map, _TEMPLATE_DIRECTORY, followlinks=True)

    env = generator._env        # https://github.com/UAVCAN/pydsdlgen/issues/20

    env.tests['boolean']        = lambda x: isinstance(x, pydsdl.BooleanType)
    env.tests['integer']        = lambda x: isinstance(x, pydsdl.IntegerType)
    env.tests['float']          = lambda x: isinstance(x, pydsdl.FloatType)
    env.tests['array']          = lambda x: isinstance(x, pydsdl.ArrayType)
    env.tests['fixed_array']    = lambda x: isinstance(x, pydsdl.FixedLengthArrayType)
    env.tests['variable_array'] = lambda x: isinstance(x, pydsdl.VariableLengthArrayType)
    env.tests['composite']      = lambda x: isinstance(x, pydsdl.CompositeType)

    env.tests['padding']    = lambda x: isinstance(x, pydsdl.PaddingField)
    env.tests['uint8']      = lambda x: isinstance(x, pydsdl.UnsignedIntegerType) and x.bit_length == 8

    env.filters['id']                   = _make_identifier
    env.filters['pickle']               = _pickle_object
    env.filters['numpy_scalar_type']    = _numpy_scalar_type
    env.filters['longest_name_length']  = lambda c: max(map(len, map(lambda x: x.name, c)))

    generator.generate_all()

    return pathlib.Path(package_parent_directory) / pathlib.Path(root_namespace_name)


def _make_identifier(a: pydsdl.Attribute) -> str:
    return (a.name + '_') if a.name in _ILLEGAL_IDENTIFIERS else a.name


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


def _unittest_dsdl_compiler() -> None:
    import shutil

    # Suppress debug logging from PyDSDL, there's too much of it and we don't want it to interfere
    logging.getLogger('pydsdl').setLevel('INFO')

    root_ns = _SOURCE_DIRECTORY.parent / pathlib.Path('public_regulated_data_types') / pathlib.Path('uavcan')

    parent_dir = _SOURCE_DIRECTORY.parent / pathlib.Path('.dsdl_generated')
    if parent_dir.exists():
        shutil.rmtree(parent_dir, ignore_errors=True)
    parent_dir.mkdir(parents=True, exist_ok=True)

    pkg_dir = generate_python_package_from_dsdl_namespace(parent_dir, root_ns, [])

    assert pkg_dir.name.endswith('uavcan')
