import logging
import pathlib
import time
from os import PathLike
from typing import Optional, Union

_logger = logging.getLogger(__name__)
_AnyPath = Union[str, pathlib.Path]


class Locker:

    def __init__(
        self,
        root_namespace_directory: Optional[_AnyPath],
        output_directory: Optional[_AnyPath],
        root_namespace_name: str = "",
    ) -> None:
        self.output_directory = output_directory
        self.root_namespace_name = root_namespace_name
        self.root_namespace_directory = root_namespace_directory

    @property
    def lockfile_path(self) -> str:
        return f"{self.output_directory}/{self.root_namespace_name if self.root_namespace_name else 'support'}.lock"

    def create(self) -> bool:
        root_namespace_name = self.root_namespace_name
        output_directory = self.output_directory if self.output_directory else pathlib.Path.cwd()
        lockfile_path = self.lockfile_path

        while True:
            try:
                pathlib.Path(output_directory).mkdir(parents=True, exist_ok=True)
                fp = open(lockfile_path, "x")
                fp.close()
                return True
            except FileExistsError:
                time.sleep(1)
                if pathlib.Path.exists(output_directory / pathlib.Path(root_namespace_name)):
                    return False

    def remove(self) -> None:
        pathlib.Path(self.lockfile_path).unlink()
