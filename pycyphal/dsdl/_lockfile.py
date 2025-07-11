# Copyright (c) 2025 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Huong Pham <huong.pham@zubax.com>

import logging
import pathlib
import time
from io import TextIOWrapper
from pathlib import Path
from types import TracebackType

_logger = logging.getLogger(__name__)


class Locker:
    """
    This class locks the namespace to prevent multiple processes from compiling the same namespace at the same time.
    """

    def __init__(self, output_directory: pathlib.Path, root_namespace_name: str) -> None:
        self._output_directory = output_directory
        self._root_namespace_name = root_namespace_name
        self._lockfile: TextIOWrapper | None = None

    @property
    def _lockfile_path(self) -> Path:
        return self._output_directory / f"{self._root_namespace_name}.lock"

    def __enter__(self) -> bool:
        return self.create()

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        if self._lockfile is not None:
            self.remove()

    def create(self) -> bool:
        """
        True means compilation needs to proceed.
        False means another process already compiled the namespace so we just waited for the lockfile to disappear before returning.
        """
        try:
            pathlib.Path(self._output_directory).mkdir(parents=True, exist_ok=True)
            self._lockfile = open(self._lockfile_path, "x")
            _logger.debug("Created lockfile %s", self._lockfile_path)
            return True
        except FileExistsError:
            pass
        while pathlib.Path(self._lockfile_path).exists():
            _logger.debug("Waiting for lockfile %s", self._lockfile_path)
            time.sleep(1)

        _logger.debug("Done waiting %s", self._lockfile_path)

        return False

    def remove(self) -> None:
        """
        Invoking remove before creating lockfile is not allowed.
        """
        assert self._lockfile is not None
        self._lockfile.close()
        pathlib.Path(self._lockfile_path).unlink()
        _logger.debug("Removed lockfile %s", self._lockfile_path)
