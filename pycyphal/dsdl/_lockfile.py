import logging
import pathlib
import time
from pathlib import Path

_logger = logging.getLogger(__name__)


class Locker:

    def __init__(
        self,
        output_directory: pathlib.Path,
        root_namespace_name: str,
    ) -> None:
        self._output_directory = output_directory
        self._root_namespace_name = root_namespace_name

    @property
    def _lockfile_path(self) -> Path:
        return self._output_directory / f"{self._root_namespace_name}.lock"

    def create(self) -> bool:
        output_directory = self._output_directory
        lockfile_path = self._lockfile_path
        # TODO Read about context manager
        while True:
            try:
                pathlib.Path(output_directory).mkdir(parents=True, exist_ok=True)
                # TODO keep the file open
                open(lockfile_path, "x")
                return True
            except FileExistsError:
                time.sleep(1)
                # TODO how to check if the namespace is compiled
                return False
            except Exception as e:
                _logger.exception(f"Failed to create lockfile: {e}")

    def remove(self) -> None:
        pathlib.Path(self._lockfile_path).unlink()
