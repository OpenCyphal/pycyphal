#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import http
import shutil
import typing
import logging
import pathlib
import zipfile
import requests
import tempfile
import pyuavcan


_logger = logging.getLogger(__name__)

_AnyPath = typing.Union[str, pathlib.Path]


def fetch_dsdl_root_namespaces(archive_url: str) -> pathlib.Path:
    """
    Downloads an archive from the specified URL, unpacks it into the specified directory, and returns the path
    to the root of the target directory. The archive must contain DSDL root namespaces on its first level.
    """
    dsdl_dir = tempfile.mkdtemp(prefix='pyuavcan-cli-dsdl')
    dsdl_zip_file = str(pathlib.Path(dsdl_dir) / 'dsdl.zip')

    _logger.debug('Downloading the DSDL archive from %r into %r...', archive_url, dsdl_zip_file)
    response = requests.get(archive_url)
    if response.status_code != http.HTTPStatus.OK:
        raise RuntimeError(f'Could not download the DSDL archive; HTTP error {response.status_code}')
    with open(dsdl_zip_file, 'wb') as f:
        f.write(response.content)

    _logger.debug('Extracting the DSDL archive into %r...', dsdl_dir)
    with zipfile.ZipFile(dsdl_zip_file) as zf:
        zf.extractall(dsdl_dir)

    out, = [d for d in pathlib.Path(dsdl_dir).iterdir() if d.is_dir()]
    return out


def generate_dsdl_packages(directory_with_root_namespaces:   _AnyPath,
                           directory_for_generated_packages: _AnyPath) \
        -> typing.Sequence[pyuavcan.dsdl.GeneratedPackageInfo]:
    """
    Takes a directory where the root namespace directories are (i.e., one level above the root namespaces)
    and a directory where the generated Python packages should be put into. Returns the list of generated
    package info objects.
    """
    shutil.rmtree(directory_for_generated_packages, ignore_errors=True)
    pathlib.Path(directory_for_generated_packages).mkdir(parents=True, exist_ok=True)
    root_ns_list = [root_ns for root_ns in pathlib.Path(directory_with_root_namespaces).iterdir() if root_ns.is_dir()]
    _logger.debug('Found root namespaces: %r...', root_ns_list)
    out: typing.List[pyuavcan.dsdl.GeneratedPackageInfo] = []
    for ns in root_ns_list:
        gpi = pyuavcan.dsdl.generate_package(package_parent_directory=directory_for_generated_packages,
                                             root_namespace_directory=ns,
                                             lookup_directories=root_ns_list)
        out.append(gpi)
    return out
