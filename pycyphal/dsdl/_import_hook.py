# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.

import logging
import sys
import os
from typing import Optional, Union
import pathlib
from . import compile

from importlib.abc import MetaPathFinder
from importlib.util import spec_from_file_location

_AnyPath = Union[str, pathlib.Path]

_logger = logging.getLogger(__name__)


class DsdlMetaFinder(MetaPathFinder):
    def __init__(
        self,
        lookup_directories: list[_AnyPath],
        output_directory: _AnyPath,
        allow_unregulated_fixed_port_id: bool,
    ) -> None:
        super().__init__()

        _logger.debug("lookup dirs: %s", lookup_directories)
        _logger.debug("output dir: %s", output_directory)

        self.lookup_directories = list(map(str, lookup_directories))
        self.output_directory = output_directory
        self.allow_unregulated_fixed_port_id = allow_unregulated_fixed_port_id
        self.root_namespace_directories: list[pathlib.Path] = list()

        # Any dir inside any of the lookup directories is considered a root namespace
        for dir in self.lookup_directories:
            for namespace in pathlib.Path(dir).iterdir():
                if namespace.is_dir():
                    _logger.debug("Using root namespace %s at %s", namespace.name, namespace)
                    self.root_namespace_directories.append(namespace)

    def find_module_dir(self, fullname):
        for namespace_dir in self.root_namespace_directories:
            if namespace_dir.name == fullname:
                return namespace_dir
        return None

    def is_module_compiled(self, fullname):
        pathlib.Path(self.output_directory, fullname).exists()

    def find_spec(self, fullname, path, target=None):
        _logger.debug("Attempting to load module %s as DSDL", fullname)

        module_dir = self.find_module_dir(fullname)
        if not module_dir:
            return None

        _logger.debug("Found module %s in DSDL source directory %s", fullname, module_dir)

        if not self.is_module_compiled(fullname):
            _logger.debug("Compiling DSDL in %s", module_dir)
            compile(
                module_dir,
                self.root_namespace_directories,
                self.output_directory,
                self.allow_unregulated_fixed_port_id,
            )

        compiled_module_dir = os.path.join(self.output_directory, fullname)
        filename = os.path.join(compiled_module_dir, "__init__.py")
        submodule_locations = [compiled_module_dir]

        return spec_from_file_location(fullname, filename, submodule_search_locations=submodule_locations)


def get_default_lookup_dirs():
    return os.environ.get("CYPHAL_PATH", "").split(os.pathsep)


def get_default_output_dir():
    pycyphal_dir = os.environ.get("PYCYPHAL_PATH", pathlib.Path.home().joinpath(".pycyphal"))
    return pathlib.Path(pycyphal_dir, "compiled")


def install_import_hook(
    lookup_directories: Optional[list[_AnyPath]] = None,
    output_directory: Optional[_AnyPath] = None,
    allow_unregulated_fixed_port_id: Optional[bool] = None,
):
    lookup_directories = get_default_lookup_dirs() if lookup_directories is None else lookup_directories
    output_directory = get_default_output_dir() if output_directory is None else output_directory
    allow_unregulated_fixed_port_id = (
        bool(os.environ.get("CYPHAL_ALLOW_UNREGULATED_FIXED_PORT_ID", "False"))
        if allow_unregulated_fixed_port_id is None
        else allow_unregulated_fixed_port_id
    )

    # Install finder at the end of the list so it is the last to attempt to load a missing package
    sys.meta_path.append(DsdlMetaFinder(lookup_directories, output_directory, allow_unregulated_fixed_port_id))
