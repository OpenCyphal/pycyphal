# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import os
import sys
import pathlib
import pyuavcan


VERSION_AGNOSTIC_DATA_DIR: pathlib.Path
"""
The root directory of version-specific data directories.
Its location is platform-dependent.
It is shared for all versions of the library.
"""

if hasattr(sys, "getwindowsversion"):  # pragma: no cover
    _appdata_env = os.getenv("LOCALAPPDATA") or os.getenv("APPDATA")
    assert _appdata_env, "Cannot determine the location of the app data directory"
    VERSION_AGNOSTIC_DATA_DIR = pathlib.Path(_appdata_env, "UAVCAN", "PyUAVCAN")
else:
    VERSION_AGNOSTIC_DATA_DIR = pathlib.Path("~/.uavcan/pyuavcan").expanduser()

VERSION_SPECIFIC_DATA_DIR: pathlib.Path = VERSION_AGNOSTIC_DATA_DIR / (
    "v" + ".".join(map(str, pyuavcan.__version_info__[:2]))
)
"""
The directory specific to this version of the library where resources and files are stored.
This is always a subdirectory of :data:`VERSION_AGNOSTIC_DATA_DIR`.
The version is specified down to the minor version, ignoring the patch version (e.g, 1.1),
so that versions of the library that differ only by the patch version number will use the same directory.

This directory contains the default destination path for highly volatile or low-value files.
Having such files segregated by the library version number ensures that when the library is updated,
it will not encounter compatibility issues with older formats.
"""

OUTPUT_TRANSFER_ID_MAP_DIR: pathlib.Path = VERSION_SPECIFIC_DATA_DIR / "output-transfer-id-maps"
"""
The path is version-specific so that we won't attempt to restore transfer-ID maps stored from another version.
"""

OUTPUT_TRANSFER_ID_MAP_MAX_AGE = 60.0  # [second]
"""
This is not a path but a related parameter so it's kept here. Files older that this are not used.
"""

DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL = (
    "https://github.com/UAVCAN/public_regulated_data_types/archive/master.zip"
)
