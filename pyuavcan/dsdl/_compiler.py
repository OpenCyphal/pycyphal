#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
#

import typing
import pathlib
import logging
import keyword
import builtins

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

    env.tests['boolean'] = lambda x: isinstance(x, pydsdl.BooleanType)
    env.tests['integer'] = lambda x: isinstance(x, pydsdl.IntegerType)
    env.tests['float'] = lambda x: isinstance(x, pydsdl.FloatType)
    env.tests['array'] = lambda x: isinstance(x, pydsdl.ArrayType)
    env.tests['composite'] = lambda x: isinstance(x, pydsdl.CompositeType)

    env.tests['padding'] = lambda x: isinstance(x, pydsdl.PaddingField)

    env.filters['type_annotation'] = _dsdl_type_to_annotation
    env.filters['id'] = _make_identifier

    generator.generate_all()

    return pathlib.Path(package_parent_directory) / pathlib.Path(root_namespace_name)


def _dsdl_type_to_annotation(t: pydsdl.Any) -> str:
    if isinstance(t, pydsdl.BooleanType):
        return 'bool'
    elif isinstance(t, pydsdl.IntegerType):
        return 'int'
    elif isinstance(t, pydsdl.FloatType):
        return 'float'
    elif isinstance(t, pydsdl.ArrayType):
        et = t.element_type
        if isinstance(et, pydsdl.UnsignedIntegerType) and et.bit_length <= 8:
            return 'bytes'
        else:
            return f'_List_[{_dsdl_type_to_annotation(et)}]'
    elif isinstance(t, (pydsdl.StructureType, pydsdl.UnionType)):
        return t.full_name
    else:
        raise ValueError(f"Don't know how to construct type annotation for {type(t).__name__}")


def _make_identifier(a: pydsdl.Attribute) -> str:
    return (a.name + '_') if a.name in _ILLEGAL_IDENTIFIERS else a.name


def _unittest_dsdl_compiler() -> None:
    import tempfile
    import shutil

    # Suppress debug logging from PyDSDL, there's too much of it and we don't want it to interfere
    logging.getLogger('pydsdl').setLevel('INFO')

    root_ns = _SOURCE_DIRECTORY.parent / pathlib.Path('public_regulated_data_types') / pathlib.Path('uavcan')

    parent_dir = pathlib.Path(tempfile.gettempdir()) / pathlib.Path('pyuavcan_dsdl_compiler_test_output')
    if parent_dir.exists():
        shutil.rmtree(parent_dir, ignore_errors=True)
    parent_dir.mkdir(parents=True, exist_ok=True)

    pkg_dir = generate_python_package_from_dsdl_namespace(parent_dir, root_ns, [])

    assert pkg_dir.name.endswith('uavcan')
