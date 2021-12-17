#
# Copyright (C) 2014-2015  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Ben Dyer <ben_dyer@mac.com>
#         Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import division, absolute_import, print_function, unicode_literals
import os
from collections import defaultdict
from logging import getLogger
import pyuavcan_v0_5
import errno


logger = getLogger(__name__)


def _try_resolve_relative_path(search_in, rel_path):
    rel_path = os.path.normcase(os.path.normpath(rel_path))
    for p in search_in:
        p = os.path.normcase(os.path.abspath(p))
        if p.endswith(rel_path) and os.path.isfile(p):
            return p
        joined = os.path.join(p, rel_path)
        if os.path.isfile(joined):
            return joined


# noinspection PyBroadException
class FileServer(object):
    def __init__(self, node, lookup_paths=None):
        if node.is_anonymous:
            raise pyuavcan_v0_5.UAVCANException('File server cannot be launched on an anonymous node')

        self.lookup_paths = lookup_paths or []

        self._path_hit_counters = defaultdict(int)
        self._handles = []
        self._node_offsets = {}

        def add_handler(datatype, callback):
            self._handles.append(node.add_handler(datatype, callback))

        add_handler(pyuavcan_v0_5.protocol.file.GetInfo, self._get_info)
        add_handler(pyuavcan_v0_5.protocol.file.Read, self._read)
        # TODO: support all file services

    def close(self):
        for x in self._handles:
            x.remove()

    @property
    def path_hit_counters(self):
        return dict(self._path_hit_counters)

    @property
    def node_offsets(self):
        return dict(self._node_offsets)

    def _resolve_path(self, relative):
        rel = relative.path.decode().replace(chr(relative.SEPARATOR), os.path.sep)
        out = _try_resolve_relative_path(self.lookup_paths, rel)
        if not out:
            raise OSError(errno.ENOENT)

        self._path_hit_counters[out] += 1
        return out

    def _get_info(self, e):
        logger.debug("[#{0:03d}:pyuavcan_v0_5.protocol.file.GetInfo] {1!r}"
                     .format(e.transfer.source_node_id, e.request.path.path.decode()))
        try:
            with open(self._resolve_path(e.request.path), "rb") as f:
                data = f.read()
                resp = pyuavcan_v0_5.protocol.file.GetInfo.Response()
                resp.error.value = resp.error.OK
                resp.size = len(data)
                resp.entry_type.flags = resp.entry_type.FLAG_FILE | resp.entry_type.FLAG_READABLE
        except Exception:
            # TODO: Convert OSError codes to the error codes defined in DSDL
            logger.exception("[#{0:03d}:pyuavcan_v0_5.protocol.file.GetInfo] error", exc_info=True)
            resp = pyuavcan_v0_5.protocol.file.GetInfo.Response()
            resp.error.value = resp.error.UNKNOWN_ERROR

        return resp

    def _read(self, e):
        logger.debug("[#{0:03d}:pyuavcan_v0_5.protocol.file.Read] {1!r} @ offset {2:d}"
                     .format(e.transfer.source_node_id, e.request.path.path.decode(), e.request.offset))
        try:
            path = self._resolve_path(e.request.path)
            with open(path, "rb") as f:
                f.seek(e.request.offset)
                resp = pyuavcan_v0_5.protocol.file.Read.Response()
                read_size = pyuavcan_v0_5.get_uavcan_data_type(pyuavcan_v0_5.get_fields(resp)['data']).max_size
                resp.data = bytearray(f.read(read_size))
                resp.error.value = resp.error.OK

                file_size = os.path.getsize(path)
                self._node_offsets[int(e.transfer.source_node_id)] = [int(e.request.offset), file_size]
        except Exception:
            logger.exception("[#{0:03d}:pyuavcan_v0_5.protocol.file.Read] error")
            resp = pyuavcan_v0_5.protocol.file.Read.Response()
            resp.error.value = resp.error.UNKNOWN_ERROR

        return resp
