# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
from ._base import Command as Command, SubsystemFactory as SubsystemFactory
from ._paths import DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL as DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL


def get_available_command_classes() -> typing.Sequence[typing.Type[Command]]:
    import pyuavcan._cli

    # noinspection PyTypeChecker
    pyuavcan.util.import_submodules(pyuavcan._cli)
    # https://github.com/python/mypy/issues/5374
    return list(pyuavcan.util.iter_descendants(Command))  # type: ignore
