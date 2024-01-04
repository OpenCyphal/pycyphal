# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import os
import sys
import time
from typing import Sequence, Iterable, Optional, Union
import pathlib
import logging
import dataclasses

import pydsdl
import nunavut
import nunavut.lang
import nunavut.jinja


_AnyPath = Union[str, pathlib.Path]

_OUTPUT_FILE_PERMISSIONS = 0o444
"""
Read-only for all because the files are autogenerated and should not be edited manually.
"""

_logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class GeneratedPackageInfo:
    path: pathlib.Path
    """
    Path to the directory that contains the top-level ``__init__.py``.
    """

    models: Sequence[pydsdl.CompositeType]
    """
    List of PyDSDL objects describing the source DSDL definitions.
    This can be used for arbitrarily complex introspection and reflection.
    """

    name: str
    """
    The name of the generated package, which is the same as the name of the DSDL root namespace unless
    the name had to be stropped. See ``nunavut.lang.py.PYTHON_RESERVED_IDENTIFIERS``.
    """


def compile(  # pylint: disable=redefined-builtin
    root_namespace_directory: _AnyPath,
    lookup_directories: Optional[list[_AnyPath]] = None,
    output_directory: Optional[_AnyPath] = None,
    allow_unregulated_fixed_port_id: bool = False,
) -> Optional[GeneratedPackageInfo]:
    """
    This function runs the DSDL compiler, converting a specified DSDL root namespace into a Python package.
    In the generated package, nested DSDL namespaces are represented as Python subpackages,
    DSDL types as Python classes, type version numbers as class name suffixes separated via underscores
    (like ``Type_1_0``), constants as class attributes, fields as properties.
    For a more detailed information on how to use generated types, just generate them and read the resulting
    code -- it is made to be human-readable and contains docstrings.

    Generated packages can be freely moved around the file system or even deployed on other systems as long as
    their dependencies are satisfied, which are ``numpy`` and ``pydsdl``.

    Generated packages do not automatically import their nested subpackages. For example, if the application
    needs to use ``uavcan.node.Heartbeat.1.0``, it has to ``import uavcan.node`` explicitly; doing just
    ``import uavcan`` is not sufficient.

    If the source definition contains identifiers, type names, namespace components, or other entities whose
    names are listed in ``nunavut.lang.py.PYTHON_RESERVED_IDENTIFIERS``,
    the compiler applies stropping by suffixing such entities with an underscore ``_``.
    A small subset of applications may require access to a generated entity without knowing in advance whether
    its name is a reserved identifier or not (i.e., whether it's stropped or not). To simplify usage,
    the Nunavut-generated module ``nunavut_support.py`` provides helper functions
    :func:`nunavut_support.get_attribute` and :func:`nunavut_support.set_attribute` that provide access to generated
    class/object attributes using their original names before stropping.
    Likewise, the function :func:`nunavut_support.get_model` can find a generated type even if any of its name
    components are stropped; e.g., a DSDL type ``str.Type.1.0`` would be imported as ``str_.Type_1_0``.
    None of it, however, is relevant for an application that does not require genericity (vast majority of
    applications don't), so a much easier approach in that case is just to look at the generated code and see
    if there are any stropped identifiers in it, and then just use appropriate names statically.

    ..  tip::

        Production applications should compile their DSDL namespaces as part of the package build process.
        This can be done by overriding the ``build_py`` command in ``setup.py`` and invoking this function from there.

    ..  tip::

        Configure your IDE to index the compilation output directory as a source directory to enable code completion.
        For PyCharm: right click the directory --> "Mark Directory as" ->"Sources Root".

    :param root_namespace_directory:
        The source DSDL root namespace directory path. The last component of the path
        is the name of the root namespace. For example, to generate package for the root namespace ``uavcan``,
        the path would be like ``foo/bar/uavcan``.
        If set to None, only nunavut_support will be generated

    :param lookup_directories:
        An iterable of DSDL root namespace directory paths where to search for referred DSDL
        definitions. The format of each path is the same as for the previous parameter; i.e., the last component
        of each path is a DSDL root namespace name. If you are generating code for a vendor-specific DSDL root
        namespace, make sure to provide at least the path to the standard ``uavcan`` namespace directory here.

    :param output_directory:
        The generated Python package directory will be placed into this directory.
        If not specified or None, the current working directory is used.
        For example, if this argument equals ``foo/bar``, and the DSDL root namespace name is ``uavcan``,
        the top-level ``__init__.py`` of the generated package will end up in ``foo/bar/uavcan/__init__.py``.
        The directory tree will be created automatically if it does not exist (like ``mkdir -p``).
        If the destination exists, it will be silently written over.
        Applications that compile DSDL lazily are recommended to shard the output directory by the library
        version number to avoid compatibility issues with code generated by older versions of the library.
        Don't forget to add the output directory to ``PYTHONPATH``.

    :param allow_unregulated_fixed_port_id:
        If True, the compiler will not reject unregulated data types with fixed port-ID.
        If you are not sure what it means, do not use it, and read the Cyphal specification first.

    :return:
        An instance of :class:`GeneratedPackageInfo` describing the generated package,
        unless the root namespace is empty, in which case it's None.

    :raises:
        :class:`OSError` if required operations on the file system could not be performed;
        :class:`pydsdl.InvalidDefinitionError` if the source DSDL definitions are invalid;
        :class:`pydsdl.InternalError` if there is a bug in the DSDL processing front-end;
        :class:`ValueError` if any of the arguments are otherwise invalid.

    The following table is an excerpt from the Cyphal specification. Observe that *unregulated fixed port identifiers*
    are prohibited by default, but it can be overridden.

    +-------+---------------------------------------------------+----------------------------------------------+
    |Scope  | Regulated                                         | Unregulated                                  |
    +=======+===================================================+==============================================+
    |Public |Standard and contributed (e.g., vendor-specific)   |Definitions distributed separately from the   |
    |       |definitions. Fixed port identifiers are allowed;   |Cyphal specification. Fixed port identifiers  |
    |       |they are called *"regulated port-IDs"*.            |are *not allowed*.                            |
    +-------+---------------------------------------------------+----------------------------------------------+
    |Private|Nonexistent category.                              |Definitions that are not available to anyone  |
    |       |                                                   |except their authors. Fixed port identifiers  |
    |       |                                                   |are permitted (although not recommended); they|
    |       |                                                   |are called *"unregulated fixed port-IDs"*.    |
    +-------+---------------------------------------------------+----------------------------------------------+
    """
    started_at = time.monotonic()

    if isinstance(lookup_directories, (str, bytes, pathlib.Path)):
        # https://forum.opencyphal.org/t/nestedrootnamespaceerror-in-basic-usage-demo/794
        raise TypeError(f"Lookup directories shall be an iterable of paths, not {type(lookup_directories).__name__}")

    output_directory = pathlib.Path(pathlib.Path.cwd() if output_directory is None else output_directory).resolve()

    language_context = nunavut.lang.LanguageContextBuilder().set_target_language("py").create()

    root_namespace_name: str = ""

    if root_namespace_directory is not None:
        root_namespace_directory = pathlib.Path(root_namespace_directory).resolve()
        if root_namespace_directory.parent == output_directory:
            # https://github.com/OpenCyphal/pycyphal/issues/133 and https://github.com/OpenCyphal/pycyphal/issues/127
            raise ValueError(
                "The specified destination may overwrite the DSDL root namespace directory. "
                "Consider specifying a different output directory instead."
            )

        # Read the DSDL definitions
        composite_types = pydsdl.read_namespace(
            root_namespace_directory=str(root_namespace_directory),
            lookup_directories=list(map(str, lookup_directories or [])),
            allow_unregulated_fixed_port_id=allow_unregulated_fixed_port_id,
        )
        if not composite_types:
            _logger.info("Root namespace directory %r does not contain DSDL definitions", root_namespace_directory)
            return None
        (root_namespace_name,) = set(map(lambda x: x.root_namespace, composite_types))  # type: ignore
        _logger.info("Read %d definitions from root namespace %r", len(composite_types), root_namespace_name)

        # Generate code
        assert isinstance(output_directory, pathlib.Path)
        root_ns = nunavut.build_namespace_tree(
            types=composite_types,
            root_namespace_dir=str(root_namespace_directory),
            output_dir=str(output_directory),
            language_context=language_context,
        )
        code_generator = nunavut.jinja.DSDLCodeGenerator(
            namespace=root_ns,
            generate_namespace_types=nunavut.YesNoDefault.YES,
            followlinks=True,
        )
        code_generator.generate_all()
        _logger.info(
            "Generated %d types from the root namespace %r in %.1f seconds",
            len(composite_types),
            root_namespace_name,
            time.monotonic() - started_at,
        )
    else:
        root_ns = nunavut.build_namespace_tree(
            types=[],
            root_namespace_dir=str(""),
            output_dir=str(output_directory),
            language_context=language_context,
        )

    support_generator = nunavut.jinja.SupportGenerator(
        namespace=root_ns,
    )
    support_generator.generate_all()

    # A minor UX improvement; see https://github.com/OpenCyphal/pycyphal/issues/115
    for p in sys.path:
        if pathlib.Path(p).resolve() == pathlib.Path(output_directory):
            break
    else:
        if os.name == "nt":
            quick_fix = f'Quick fix: `$env:PYTHONPATH += ";{output_directory.resolve()}"`'
        elif os.name == "posix":
            quick_fix = f'Quick fix: `export PYTHONPATH="{output_directory.resolve()}"`'
        else:
            quick_fix = "Quick fix is not available for this OS."
        _logger.info(
            "Generated package is stored in %r, which is not in Python module search path list. "
            "The package will fail to import unless you add the destination directory to sys.path or PYTHONPATH. %s",
            str(output_directory),
            quick_fix,
        )

    return GeneratedPackageInfo(
        path=pathlib.Path(output_directory) / pathlib.Path(root_namespace_name),
        models=composite_types,
        name=root_namespace_name,
    )


def compile_all(
    root_namespace_directories: Iterable[_AnyPath],
    output_directory: Optional[_AnyPath] = None,
    *,
    allow_unregulated_fixed_port_id: bool = False,
) -> list[GeneratedPackageInfo]:
    """
    This is a simple convenience wrapper over :func:`compile` that addresses a very common use case
    where the application needs to compile multiple inter-dependent namespaces.

    :param root_namespace_directories:
        :func:`compile` will be invoked once for each directory in the list,
        using all of them as look-up dirs for each other.
        They may be ordered arbitrarily.
        Directories that contain no DSDL definitions are ignored.

    :param output_directory:
        See :func:`compile`.

    :param allow_unregulated_fixed_port_id:
        See :func:`compile`.

    :return:
        A list of of :class:`GeneratedPackageInfo`, one per non-empty root namespace directory.

    ..  doctest::
        :hide:

        >>> from tests import DEMO_DIR
        >>> original_sys_path = sys.path
        >>> sys.path = [x for x in sys.path if "compiled" not in x]

    >>> import sys
    >>> import pathlib
    >>> import importlib
    >>> import pycyphal
    >>> compiled_dsdl_dir = pathlib.Path(".lazy_compiled", pycyphal.__version__)
    >>> compiled_dsdl_dir.mkdir(parents=True, exist_ok=True)
    >>> sys.path.insert(0, str(compiled_dsdl_dir))
    >>> try:
    ...     import sirius_cyber_corp
    ...     import uavcan.si.sample.volumetric_flow_rate
    ... except (ImportError, AttributeError):
    ...     _ = pycyphal.dsdl.compile_all(
    ...         [
    ...             DEMO_DIR / "custom_data_types/sirius_cyber_corp",
    ...             DEMO_DIR / "public_regulated_data_types/uavcan",
    ...             DEMO_DIR / "public_regulated_data_types/reg/",
    ...         ],
    ...         output_directory=compiled_dsdl_dir,
    ...     )
    ...     importlib.invalidate_caches()
    ...     import sirius_cyber_corp
    ...     import uavcan.si.sample.volumetric_flow_rate

    ..  doctest::
        :hide:

        >>> sys.path = original_sys_path
    """
    out: list[GeneratedPackageInfo] = []
    root_namespace_directories = list(root_namespace_directories)
    for nsd in root_namespace_directories:
        gpi = compile(
            nsd,
            root_namespace_directories,
            output_directory=output_directory,
            allow_unregulated_fixed_port_id=allow_unregulated_fixed_port_id,
        )
        if gpi is not None:
            out.append(gpi)
    return out
