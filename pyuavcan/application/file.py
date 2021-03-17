# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import os
import errno
import shutil
import typing
import pathlib
import logging
import itertools
import numpy as np
import pyuavcan
import pyuavcan.application
from uavcan.file import Path_2_0 as Path
from uavcan.file import Error_1_0 as Error
from uavcan.file import Read_1_1 as Read
from uavcan.file import Write_1_1 as Write
from uavcan.file import List_0_2 as List
from uavcan.file import GetInfo_0_2 as GetInfo
from uavcan.file import Modify_1_1 as Modify
from uavcan.primitive import Unstructured_1_0 as Unstructured


class FileServer:
    """
    Exposes local filesystems via the standard RPC-services defined in ``uavcan.file``.
    The lifetime of this instance matches the lifetime of its node.
    """

    def __init__(
        self, node: pyuavcan.application.Node, roots: typing.Iterable[typing.Union[str, pathlib.Path]]
    ) -> None:
        """
        :param node:
            The node instance to initialize the file server on.
            It shall not be anonymous, otherwise it's a
            :class:`pyuavcan.transport.OperationNotDefinedForAnonymousNodeError`.

        :param roots:
            All file operations will be performed in the specified directories.
            The first directory to match takes precedence.
            New files are created in the first directory.
        """
        self._roots = [pathlib.Path(x).resolve() for x in roots]

        # noinspection PyUnresolvedReferences
        self._data_transfer_capacity = int(pyuavcan.dsdl.get_model(Unstructured)["value"].data_type.capacity)

        s_ls = node.get_server(List)
        s_if = node.get_server(GetInfo)
        s_mo = node.get_server(Modify)
        s_rd = node.get_server(Read)
        s_wr = node.get_server(Write)

        def start() -> None:
            s_ls.serve_in_background(self._serve_ls)
            s_if.serve_in_background(self._serve_if)
            s_mo.serve_in_background(self._serve_mo)
            s_rd.serve_in_background(self._serve_rd)
            s_wr.serve_in_background(self._serve_wr)

        def close() -> None:
            s_ls.close()
            s_if.close()
            s_mo.close()
            s_rd.close()
            s_wr.close()

        node.add_lifetime_hooks(start, close)

    @property
    def roots(self) -> typing.List[pathlib.Path]:
        """
        File operations will be performed within these root directories.
        The first directory to match takes precedence.
        New files are created in the first directory in the list.
        The list can be modified.
        """
        return self._roots

    def locate(self, p: typing.Union[pathlib.Path, str, Path]) -> typing.Tuple[pathlib.Path, pathlib.Path]:
        """
        Iterate through :attr:`roots` until a root r is found such that ``r/p`` exists and return ``(r, p)``.
        Otherwise, return nonexistent ``(roots[0], p)``.
        The leading slash makes no difference because we only search through the specified roots.

        :raises: :class:`FileNotFoundError` if :attr:`roots` is empty.
        """
        if isinstance(p, Path):
            p = p.path.tobytes().decode(errors="ignore").replace(chr(Path.SEPARATOR), os.sep)
        assert not isinstance(p, Path)
        p = pathlib.Path(str(pathlib.Path(p)).strip(os.sep))  # Make relative, canonicalize the trailing separator
        # See if there are existing entries under this name:
        for r in self.roots:
            if (r / p).exists():
                return r, p
        # If not, assume that we are going to create one:
        if len(self.roots) > 0:
            return self.roots[0], p
        raise FileNotFoundError(str(p))

    def glob(self, pat: str) -> typing.Iterable[typing.Tuple[pathlib.Path, pathlib.Path]]:
        """
        Search for entries matching the pattern across :attr:`roots`, in order.
        Return tuple of (root, match), where match is relative to its root.
        Ordering not enforced.
        """
        pat = pat.strip(os.sep)
        for d in self.roots:
            for e in d.glob(pat):
                yield d, e.absolute().relative_to(d.absolute())

    @staticmethod
    def convert_error(ex: Exception) -> Error:
        for ty, err in {
            FileNotFoundError: Error.NOT_FOUND,
            IsADirectoryError: Error.IS_DIRECTORY,
            NotADirectoryError: Error.NOT_SUPPORTED,
            PermissionError: Error.ACCESS_DENIED,
            FileExistsError: Error.INVALID_VALUE,
        }.items():
            if isinstance(ex, ty):
                return Error(err)
        if isinstance(ex, OSError):
            return Error(
                {
                    errno.EACCES: Error.ACCESS_DENIED,
                    errno.E2BIG: Error.FILE_TOO_LARGE,
                    errno.EINVAL: Error.INVALID_VALUE,
                    errno.EIO: Error.IO_ERROR,
                    errno.EISDIR: Error.IS_DIRECTORY,
                    errno.ENOENT: Error.NOT_FOUND,
                    errno.ENOTSUP: Error.NOT_SUPPORTED,
                    errno.ENOSPC: Error.OUT_OF_SPACE,
                }.get(ex.errno, Error.UNKNOWN_ERROR)
            )
        return Error(Error.UNKNOWN_ERROR)

    async def _serve_ls(
        self, request: List.Request, meta: pyuavcan.presentation.ServiceRequestMetadata
    ) -> List.Response:
        _logger.info("%r: Request from %r: %r", self, meta.client_node_id, request)
        try:
            d = pathlib.Path(*self.locate(request.directory_path))
            for i, e in enumerate(sorted(d.iterdir())):
                if i == request.entry_index:
                    rel = e.absolute().relative_to(d.absolute())
                    return List.Response(Path(str(rel)))
        except FileNotFoundError:
            pass
        except Exception as ex:
            _logger.exception("%r: Directory list error: %s", self, ex)
        return List.Response()

    async def _serve_if(
        self, request: GetInfo.Request, meta: pyuavcan.presentation.ServiceRequestMetadata
    ) -> GetInfo.Response:
        _logger.info("%r: Request from %r: %r", self, meta.client_node_id, request)
        try:
            p = pathlib.Path(*self.locate(request.path))
            return GetInfo.Response(
                size=p.resolve().stat().st_size,
                unix_timestamp_of_last_modification=int(p.resolve().stat().st_mtime),
                is_file_not_directory=p.is_file() or not p.is_dir(),  # Handle special files like /dev/null correctly
                is_link=os.path.islink(p),
                is_readable=os.access(p, os.R_OK),
                is_writeable=os.access(p, os.W_OK),
            )
        except Exception as ex:
            _logger.info("%r: Error: %r", self, ex, exc_info=True)
            return GetInfo.Response(self.convert_error(ex))

    async def _serve_mo(
        self, request: Modify.Request, meta: pyuavcan.presentation.ServiceRequestMetadata
    ) -> Modify.Response:
        _logger.info("%r: Request from %r: %r", self, meta.client_node_id, request)

        try:
            if len(request.destination.path) == 0:  # No destination: remove
                p = pathlib.Path(*self.locate(request.source))
                if p.is_dir():
                    shutil.rmtree(p)
                else:
                    p.unlink()
                return Modify.Response()

            if len(request.source.path) == 0:  # No source: touch
                dst = pathlib.Path(*self.locate(request.destination)).resolve()
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.touch(exist_ok=True)
                return Modify.Response()

            # Resolve paths and ensure the target directory exists.
            src = pathlib.Path(*self.locate(request.source)).resolve()
            dst = pathlib.Path(*self.locate(request.destination)).resolve()
            dst.parent.mkdir(parents=True, exist_ok=True)

            # At this point if src does not exist it is definitely an error.
            if not src.exists():
                return Modify.Response(Error(Error.NOT_FOUND))

            # Can't proceed if destination exists but overwrite is not enabled.
            if dst.exists():
                if not request.overwrite_destination:
                    return Modify.Response(Error(Error.INVALID_VALUE))
                if dst.is_dir():
                    shutil.rmtree(dst, ignore_errors=True)
                else:
                    dst.unlink()

            # Do move/copy depending on the flag.
            if request.preserve_source:
                if src.is_dir():
                    shutil.copytree(src, dst)
                else:
                    shutil.copy(src, dst)
            else:
                shutil.move(str(src), str(dst))
            return Modify.Response()
        except Exception as ex:
            _logger.info("%r: Error: %r", self, ex, exc_info=True)
            return Modify.Response(self.convert_error(ex))

    async def _serve_rd(
        self, request: Read.Request, meta: pyuavcan.presentation.ServiceRequestMetadata
    ) -> Read.Response:
        _logger.info("%r: Request from %r: %r", self, meta.client_node_id, request)
        try:
            with open(pathlib.Path(*self.locate(request.path)), "rb") as f:
                if request.offset != 0:  # Do not seek unless necessary to support non-seekable files.
                    f.seek(request.offset)
                data = f.read(self._data_transfer_capacity)
            return Read.Response(data=Unstructured(np.frombuffer(data, np.uint8)))
        except Exception as ex:
            _logger.info("%r: Error: %r", self, ex, exc_info=True)
            return Read.Response(self.convert_error(ex))

    async def _serve_wr(
        self, request: Write.Request, meta: pyuavcan.presentation.ServiceRequestMetadata
    ) -> Write.Response:
        _logger.info("%r: Request from %r: %r", self, meta.client_node_id, request)
        try:
            data = request.data.value.tobytes()
            with open(pathlib.Path(*self.locate(request.path)), "rb+") as f:
                f.seek(request.offset)
                f.write(data)
                if not data:
                    f.truncate()
            return Write.Response()
        except Exception as ex:
            _logger.info("%r: Error: %r", self, ex, exc_info=True)
            return Write.Response(self.convert_error(ex))

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, list(map(str, self.roots)))


class FileClient:
    """
    A trivial proxy that provides a higher-level and more pythonic API on top of the standard RPC-services
    from ``uavcan.file``.
    Client instances are created lazily at first request and then kept alive until this instance is closed.
    All remote operations raise :class:`FileTimeoutError` on timeout.
    """

    def __init__(
        self,
        local_node: pyuavcan.application.Node,
        server_node_id: int,
        response_timeout: float = 3.0,
        priority: pyuavcan.transport.Priority = pyuavcan.transport.Priority.SLOW,
    ) -> None:
        """
        :param local_node: RPC-service clients will be created on this node.
        :param server_node_id: All requests will be sent to this node-ID.
        :param response_timeout: Raise :class:`FileTimeoutError` if the server does not respond in this time.
        :param priority: Transfer priority for requests (and, therefore, responses).
        """
        self._node = local_node
        self._server_node_id = server_node_id
        self._response_timeout = float(response_timeout)
        # noinspection PyArgumentList
        self._priority = pyuavcan.transport.Priority(priority)

        self._clients: typing.Dict[
            typing.Type[pyuavcan.dsdl.ServiceObject],
            pyuavcan.presentation.Client[pyuavcan.dsdl.ServiceObject],
        ] = {}

        # noinspection PyUnresolvedReferences
        self._data_transfer_capacity = int(pyuavcan.dsdl.get_model(Unstructured)["value"].data_type.capacity)

    @property
    def data_transfer_capacity(self) -> int:
        """
        A convenience constant derived from DSDL: the maximum number of bytes per read/write transfer.
        Larger reads/writes are non-atomic.
        """
        return self._data_transfer_capacity

    @property
    def server_node_id(self) -> int:
        """
        The node-ID of the remote file server.
        """
        return self._server_node_id

    def close(self) -> None:
        """
        Close all RPC-service client instances created up to this point.
        """
        for c in self._clients.values():
            c.close()
        self._clients.clear()

    async def list(self, path: str) -> typing.AsyncIterable[str]:
        """
        Proxy for ``uavcan.file.List``. Invokes requests in series until all elements are listed.
        """
        for index in itertools.count():
            res = await self._call(List, List.Request(entry_index=index, directory_path=Path(path)))
            assert isinstance(res, List.Response)
            p = res.entry_base_name.path.tobytes().decode(errors="ignore")
            if p:
                yield str(p)
            else:
                break

    async def get_info(self, path: str) -> GetInfo.Response:
        """
        Proxy for ``uavcan.file.GetInfo``. Be sure to check the error code in the returned object.
        """
        res = await self._call(GetInfo, GetInfo.Request(Path(path)))
        assert isinstance(res, GetInfo.Response)
        return res

    async def remove(self, path: str) -> int:
        """
        Proxy for ``uavcan.file.Modify``.

        :returns: See ``uavcan.file.Error``
        """
        res = await self._call(Modify, Modify.Request(source=Path(path)))
        assert isinstance(res, Modify.Response)
        return int(res.error.value)

    async def touch(self, path: str) -> int:
        """
        Proxy for ``uavcan.file.Modify``.

        :returns: See ``uavcan.file.Error``
        """
        res = await self._call(Modify, Modify.Request(destination=Path(path)))
        assert isinstance(res, Modify.Response)
        return int(res.error.value)

    async def copy(self, src: str, dst: str, overwrite: bool = False) -> int:
        """
        Proxy for ``uavcan.file.Modify``.

        :returns: See ``uavcan.file.Error``
        """
        res = await self._call(
            Modify,
            Modify.Request(
                preserve_source=True,
                overwrite_destination=overwrite,
                source=Path(src),
                destination=Path(dst),
            ),
        )
        assert isinstance(res, Modify.Response)
        return int(res.error.value)

    async def move(self, src: str, dst: str, overwrite: bool = False) -> int:
        """
        Proxy for ``uavcan.file.Modify``.

        :returns: See ``uavcan.file.Error``
        """
        res = await self._call(
            Modify,
            Modify.Request(
                preserve_source=False,
                overwrite_destination=overwrite,
                source=Path(src),
                destination=Path(dst),
            ),
        )
        assert isinstance(res, Modify.Response)
        return int(res.error.value)

    async def read(self, path: str, offset: int = 0, size: typing.Optional[int] = None) -> typing.Union[int, bytes]:
        """
        Proxy for ``uavcan.file.Read``.

        :param path:
            The file to read.

        :param offset:
            Read offset from the beginning of the file.
            Currently, it must be positive; negative offsets from the end of the file may be supported later.

        :param size:
            Read requests will be stopped after the end of the file is reached or at least this many bytes are read.
            If None (default), the entire file will be read (this may exhaust local memory).
            If zero, this call is a no-op.

        :returns:
            ``uavcan.file.Error.value`` on error (e.g., no file),
            data on success (empty if the offset is out of bounds or the file is empty).
        """

        async def once() -> typing.Union[int, bytes]:
            res = await self._call(Read, Read.Request(offset=offset, path=Path(path)))
            assert isinstance(res, Read.Response)
            if res.error.value != 0:
                return int(res.error.value)
            return bytes(res.data.value.tobytes())

        if size is None:
            size = 2 ** 64
        data = b""
        while len(data) < size:
            out = await once()
            if isinstance(out, int):
                return out
            assert isinstance(out, bytes)
            if not out:
                break
            data += out
            offset += len(out)
        return data

    async def write(
        self, path: str, data: typing.Union[memoryview, bytes], offset: int = 0, *, truncate: bool = True
    ) -> int:
        """
        Proxy for ``uavcan.file.Write``.

        :param path:
            The file to write.

        :param data:
            The data to write at the specified offset.
            The number of write requests depends on the size of data.

        :param offset:
            Write offset from the beginning of the file.
            Currently, it must be positive; negative offsets from the end of the file may be supported later.

        :param truncate:
            If True, the rest of the file after ``offset + len(data)`` will be truncated.
            This is done by sending an empty write request, as prescribed by the Specification.

        :returns: See ``uavcan.file.Error``
        """

        async def once(d: typing.Union[memoryview, bytes]) -> int:
            res = await self._call(
                Write, Write.Request(offset, path=Path(path), data=Unstructured(np.frombuffer(d, np.uint8)))
            )
            assert isinstance(res, Write.Response)
            return res.error.value

        limit = self.data_transfer_capacity
        while len(data) > 0:
            frag, data = data[:limit], data[limit:]
            out = await once(frag)
            offset += len(frag)
            if out != 0:
                return out
        if truncate:
            return await once(b"")
        return 0

    async def _call(
        self, ty: typing.Type[pyuavcan.dsdl.ServiceObject], request: pyuavcan.dsdl.CompositeObject
    ) -> pyuavcan.dsdl.CompositeObject:
        try:
            cln = self._clients[ty]
        except LookupError:
            self._clients[ty] = self._node.make_client(ty, self._server_node_id)
            cln = self._clients[ty]
            cln.response_timeout = self._response_timeout
            cln.priority = self._priority

        result = await cln.call(request)
        if result is None:
            raise FileTimeoutError(f"File service call timed out on {cln}")
        return result[0]

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, self._node, server_node_id=self._server_node_id)


class FileTimeoutError(pyuavcan.application.NetworkTimeoutError):
    """
    The specialization of the network error for file access.
    """


_logger = logging.getLogger(__name__)
