import logging
import pathlib
import time

_logger = logging.getLogger(__name__)


class Locker:

    def __init__(
        self,
        output_directory: pathlib.Path,
        root_namespace_name: str,
    ) -> None:
        self.output_directory = output_directory
        self.root_namespace_name = root_namespace_name

    @property
    def lockfile_path(self) -> str:
        return f"{self.output_directory}/{self.root_namespace_name}.lock"

    def create(self) -> bool:
        root_namespace_name = self.root_namespace_name
        output_directory = self.output_directory
        lockfile_path = self.lockfile_path
        # TODO Read about context manager
        while True:
            try:
                pathlib.Path(output_directory).mkdir(parents=True, exist_ok=True)
                open(lockfile_path, "x")
                return True
            except FileExistsError:
                time.sleep(1)
                if pathlib.Path.exists(output_directory / pathlib.Path(root_namespace_name)):
                    return False
            except Exception as e:
                _logger.exception(f"Failed to create lockfile: {e}")

    def remove(self) -> None:
        pathlib.Path(self.lockfile_path).unlink()
