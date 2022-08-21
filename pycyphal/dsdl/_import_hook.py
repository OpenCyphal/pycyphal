# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.

import logging
import sys
import os
from types import ModuleType
from typing import Iterable, Optional, Sequence, Union
import pathlib
import keyword
import re
from importlib.abc import MetaPathFinder
from importlib.util import spec_from_file_location
from importlib.machinery import ModuleSpec
from . import compile  # pylint: disable=redefined-builtin


_AnyPath = Union[str, pathlib.Path]

_logger = logging.getLogger(__name__)


def root_namespace_from_module_name(module_name: str) -> str:
    """
    Tranlates python module name to DSDL root namespace.
    This handles special case where root namespace is a python keyword by removing trailing underscore.
    """
    if module_name.endswith("_") and keyword.iskeyword(module_name[-1]):
        return module_name[-1]
    return module_name


class DsdlMetaFinder(MetaPathFinder):
    def __init__(
        self,
        lookup_directories: Iterable[_AnyPath],
        output_directory: _AnyPath,
        allow_unregulated_fixed_port_id: bool,
    ) -> None:
        super().__init__()

        _logger.debug("lookup dirs: %s", lookup_directories)
        _logger.debug("output dir: %s", output_directory)

        self.lookup_directories = list(map(str, lookup_directories))
        self.output_directory = output_directory
        self.allow_unregulated_fixed_port_id = allow_unregulated_fixed_port_id
        self.root_namespace_directories: Sequence[pathlib.Path] = []

        # Build a list of root namespace directories from lookup directories.
        # Any dir inside any of the lookup directories is considered a root namespace if it matches regex
        for directory in self.lookup_directories:
            for namespace in pathlib.Path(directory).iterdir():
                if namespace.is_dir() and re.match(r"[a-zA-Z_][a-zA-Z0-9_]*", namespace.name):
                    _logger.debug("Using root namespace %s at %s", namespace.name, namespace)
                    self.root_namespace_directories.append(namespace)

    def find_source_dir(self, root_namespace: str) -> Optional[pathlib.Path]:
        """
        Finds DSDL source directory for a given root namespace name.
        """
        for namespace_dir in self.root_namespace_directories:
            if namespace_dir.name == root_namespace:
                return namespace_dir
        return None

    def is_compiled(self, root_namespace: str) -> bool:
        """
        Returns true if given root namespace exists in output directory (compiled).
        """
        return pathlib.Path(self.output_directory, root_namespace).exists()

    def find_spec(
        self, fullname: str, path: Optional[Sequence[Union[bytes, str]]], target: Optional[ModuleType] = None
    ) -> Optional[ModuleSpec]:
        _logger.debug("Attempting to load module %s as DSDL", fullname)

        # Translate module name to DSDL root namespace
        root_namespace = root_namespace_from_module_name(fullname)

        root_namespace_dir = self.find_source_dir(root_namespace)
        if not root_namespace_dir:
            return None

        _logger.debug("Found root namespace %s in DSDL source directory %s", root_namespace, root_namespace_dir)

        if not self.is_compiled(root_namespace):
            _logger.warning("Compiling DSDL namespace %s", root_namespace_dir)
            compile(
                root_namespace_dir,
                list(self.root_namespace_directories),
                self.output_directory,
                self.allow_unregulated_fixed_port_id,
            )

        compiled_module_dir = pathlib.Path(self.output_directory, root_namespace)
        module_location = compiled_module_dir.joinpath("__init__.py")
        submodule_locations = [str(compiled_module_dir)]

        return spec_from_file_location(fullname, module_location, submodule_search_locations=submodule_locations)


def get_default_lookup_dirs() -> Sequence[str]:
    return os.environ.get("CYPHAL_PATH", "").replace(os.pathsep, ";").split(";")


def get_default_output_dir() -> str:
    pycyphal_path = os.environ.get("PYCYPHAL_PATH")
    if pycyphal_path:
        return pycyphal_path
    try:
        return str(pathlib.Path.home().joinpath(".pycyphal"))
    except RuntimeError as e:
        raise RuntimeError("Please set PYCYPHAL_PATH env variable or setup a proper OS user home directory.") from e


def install_import_hook(
    lookup_directories: Optional[Iterable[_AnyPath]] = None,
    output_directory: Optional[_AnyPath] = None,
    allow_unregulated_fixed_port_id: Optional[bool] = None,
) -> None:
    """
    Installs python import hook, which automatically compiles any DSDL if package is not found.

    A default import hook is automatically installed when pycyphal is imported. To opt out, set environment variable
    ``PYCYPHAL_NO_IMPORT_HOOK=True`` before importing pycyphal.

    :param lookup_directories:
        List of directories where to look for DSDL sources. If not provided, it is sourced from ``CYPHAL_PATH``
        environment variable.

    :param output_directory:
        Directory to output compiled DSDL packages. If not provided, ``PYCYPHAL_PATH`` environment variable is used.
        If that is not available either, a default ``~/.pycyphal`` (or other OS equivalent) directory is used.

    :param allow_unregulated_fixed_port_id:
        If True, the compiler will not reject unregulated data types with fixed port-ID. If not provided, it will be
        sourced from ``CYPHAL_ALLOW_UNREGULATED_FIXED_PORT_ID`` variable or default to False.
    """
    lookup_directories = get_default_lookup_dirs() if lookup_directories is None else lookup_directories
    output_directory = get_default_output_dir() if output_directory is None else output_directory
    allow_unregulated_fixed_port_id = (
        os.environ.get("CYPHAL_ALLOW_UNREGULATED_FIXED_PORT_ID", "False").lower() in ("true", "1", "t")
        if allow_unregulated_fixed_port_id is None
        else allow_unregulated_fixed_port_id
    )

    # Install finder at the end of the list so it is the last to attempt to load a missing package
    sys.meta_path.append(DsdlMetaFinder(lookup_directories, output_directory, allow_unregulated_fixed_port_id))


# Install default import hook unless explicitly requested not to
if os.environ.get("PYCYPHAL_NO_IMPORT_HOOK", "False").lower() not in ("true", "1", "t"):
    _logger.debug("Installing default import hook.")
    install_import_hook()
else:
    _logger.debug("Default import hook installation skipped.")
