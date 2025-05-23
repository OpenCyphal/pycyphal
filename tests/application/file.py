# Copyright (c) 2021 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import math
import sys
import shutil
import typing
import asyncio
import errno
from tempfile import mkdtemp
from pathlib import Path
import pytest
import pycyphal


class ProgressTracker:
    def __init__(self) -> None:
        self.counter = 0


@pytest.mark.asyncio
async def _unittest_file(compiled: list[pycyphal.dsdl.GeneratedPackageInfo]) -> None:
    from pycyphal.application import make_node, NodeInfo
    from pycyphal.transport.udp import UDPTransport
    from pycyphal.application.file import FileClient, FileServer, Error

    assert compiled
    asyncio.get_running_loop().slow_callback_duration = 3.0

    root_a = mkdtemp(".file", "a.")
    root_b = mkdtemp(".file", "b.")
    srv_node = make_node(
        NodeInfo(name="org.opencyphal.pycyphal.test.file.server"),
        transport=UDPTransport("127.0.0.1", 222, service_transfer_multiplier=2),
    )
    cln_node = make_node(
        NodeInfo(name="org.opencyphal.pycyphal.test.file.client"),
        transport=UDPTransport("127.0.0.1", 223, service_transfer_multiplier=2),
    )
    try:
        srv_node.start()
        file_server = FileServer(srv_node, [root_a, root_b])
        assert (Path(root_a), Path("abc")) == file_server.locate(Path("abc"))
        assert [] == list(file_server.glob("*"))

        cln_node.start()
        cln = FileClient(cln_node, 222)

        async def ls(path: str) -> typing.List[str]:
            out: typing.List[str] = []
            async for e in cln.list(path):
                out.append(e)
            return out

        assert [] == await ls("")
        assert [] == await ls("nonexistent/directory")
        assert (await cln.get_info("none")).error.value == Error.NOT_FOUND

        assert 0 == await cln.touch("a/foo/x")
        assert 0 == await cln.touch("a/foo/y")
        assert 0 == await cln.touch("b")
        assert ["foo"] == await ls("a")

        # Make sure files are created.
        assert [
            (file_server.roots[0], Path("a/foo/x")),
            (file_server.roots[0], Path("a/foo/y")),
        ] == list(sorted(file_server.glob("a/foo/*")))

        assert await cln.read("a/foo/x") == b""
        assert await cln.read("/a/foo/x") == b""  # Slash or no slash makes no difference.
        assert await cln.read("a/foo/z") == Error.NOT_FOUND
        assert (await cln.get_info("a/foo/z")).error.value == Error.NOT_FOUND

        # Write non-existent file
        assert await cln.write("a/foo/z", bytes(range(200)) * 3) == Error.NOT_FOUND

        # Write into empty file
        assert await cln.write("a/foo/x", bytes(range(200)) * 3) == 0
        assert await cln.read("a/foo/x") == bytes(range(200)) * 3
        assert (await cln.get_info("a/foo/x")).size == 600

        # Truncation -- this write is shorter
        hundred = bytes(x ^ 0xFF for x in range(100))
        assert await cln.write("a/foo/x", hundred * 4) == 0
        assert (await cln.get_info("a/foo/x")).size == 400
        assert await cln.read("a/foo/x") == (hundred * 4)
        assert (await cln.get_info("a/foo/x")).size == 400

        # Fill in the middle without truncation
        ref = bytearray(hundred * 4)
        for i in range(100):
            ref[i + 100] = 0x55
        assert len(ref) == 400
        assert (await cln.get_info("a/foo/x")).size == 400
        assert await cln.write("a/foo/x", b"\x55" * 100, offset=100, truncate=False) == 0
        assert (await cln.get_info("a/foo/x")).size == 400
        assert await cln.read("a/foo/x") == ref

        # Fill in the middle with truncation
        assert await cln.write("a/foo/x", b"\xaa" * 50, offset=50) == 0
        assert (await cln.get_info("a/foo/x")).size == 100
        assert await cln.read("a/foo/x") == hundred[:50] + b"\xaa" * 50

        # Directories
        info = await cln.get_info("a/foo")
        print("a/foo:", info)
        assert info.error.value == 0
        assert info.is_writeable
        assert info.is_readable
        assert not info.is_file_not_directory
        assert not info.is_link

        assert (await cln.get_info("a/foo/nothing")).error.value == Error.NOT_FOUND
        assert await cln.write("a/foo", b"123") in (Error.IS_DIRECTORY, Error.ACCESS_DENIED)  # Windows compatibility

        # Removal
        assert (await cln.remove("a/foo/z")) == Error.NOT_FOUND
        assert (await cln.remove("a/foo/x")) == 0
        assert (await cln.touch("a/foo/x")) == 0  # Put it back
        assert (await cln.remove("a/foo/")) == 0  # Removed
        assert (await cln.remove("a/foo/")) == Error.NOT_FOUND  # Not found

        # Copy
        assert (await cln.touch("r/a")) == 0
        assert (await cln.touch("r/b/0")) == 0
        assert (await cln.touch("r/b/1")) == 0
        assert not (await cln.get_info("r/b")).is_file_not_directory
        assert ["a", "b"] == await ls("r")
        assert (await cln.copy("r/b", "r/c")) == 0
        assert ["a", "b", "c"] == await ls("r")
        assert (await cln.copy("r/a", "r/c")) != 0  # Overwrite not enabled
        assert ["a", "b", "c"] == await ls("r")
        assert not (await cln.get_info("r/c")).is_file_not_directory
        assert (await cln.copy("/r/a", "r/c", overwrite=True)) == 0
        assert (await cln.get_info("r/c")).is_file_not_directory

        # Move
        assert ["a", "b", "c"] == await ls("r")
        assert (await cln.move("/r/a", "r/c")) != 0  # Overwrite not enabled
        assert (await cln.move("/r/a", "r/c", overwrite=True)) == 0
        assert ["b", "c"] == await ls("r")
        assert (await cln.move("/r/a", "r/c", overwrite=True)) == Error.NOT_FOUND
        assert ["b", "c"] == await ls("r")

        # Access protected files
        if sys.platform.startswith("linux"):  # pragma: no branch
            file_server.roots.append(Path("/"))
            info = await cln.get_info("dev/null")
            print("/dev/null:", info)
            assert info.error.value == 0
            assert not info.is_link
            assert info.is_writeable
            assert info.is_file_not_directory

            info = await cln.get_info("/bin/sh")
            print("/bin/sh:", info)
            assert info.error.value == 0
            assert not info.is_writeable
            assert info.is_file_not_directory

            assert await cln.read("/dev/null", size=100) == b""  # Read less than requested
            assert await cln.read("/dev/zero", size=100) == b"\x00" * 256  # Read more than requested
            assert await cln.write("bin/sh", b"123") == Error.ACCESS_DENIED

            file_server.roots.pop(-1)
    finally:
        srv_node.close()
        cln_node.close()
        await asyncio.sleep(1.0)
        shutil.rmtree(root_a, ignore_errors=True)
        shutil.rmtree(root_b, ignore_errors=True)


def _unittest_errormap_file2() -> None:
    from pycyphal.application.file import Error, _map

    for attr in dir(Error):
        if callable(attr) or not attr[0].isupper() or not isinstance(getattr(Error, attr), int) or attr.startswith("_"):
            # Skip methods and attributes not starting with an upper case letter
            # - hopefully only error code constants are remaining. Having these
            # constants in an enum would be better.
            continue

        code = getattr(Error, attr)
        print(attr, code)
        if code == Error.OK:
            # Error.OK is not in the map - use it to test for unknown error codes
            with pytest.raises(OSError) as e:
                _map(Error(code), "")
            assert e.value.errno == errno.EPROTO
        else:
            _map(Error(code), "")


@pytest.mark.asyncio
async def _unittest_file2(compiled: typing.List[pycyphal.dsdl.GeneratedPackageInfo]) -> None:
    from pycyphal.application import make_node, NodeInfo
    from pycyphal.transport.udp import UDPTransport
    from pycyphal.application.file import FileClient2, FileServer, Error

    assert compiled
    asyncio.get_running_loop().slow_callback_duration = 3.0

    root_a = mkdtemp(".file", "a.")
    root_b = mkdtemp(".file", "b.")
    srv_node = make_node(
        NodeInfo(name="org.opencyphal.pycyphal.test.file.server"),
        transport=UDPTransport("127.0.0.1", 222, service_transfer_multiplier=2),
    )
    cln_node = make_node(
        NodeInfo(name="org.opencyphal.pycyphal.test.file.client"),
        transport=UDPTransport("127.0.0.1", 223, service_transfer_multiplier=2),
    )
    try:
        srv_node.start()
        file_server = FileServer(srv_node, [root_a, root_b])
        assert (Path(root_a), Path("abc")) == file_server.locate(Path("abc"))
        assert [] == list(file_server.glob("*"))

        cln_node.start()
        cln = FileClient2(cln_node, 222)

        async def ls(path: str) -> typing.List[str]:
            out: typing.List[str] = []
            async for e in cln.list(path):
                out.append(e)
            return out

        assert [] == await ls("")
        assert [] == await ls("nonexistent/directory")
        with pytest.raises(OSError) as e:
            await cln.get_info("none")
        assert e.value.errno == errno.ENOENT

        await cln.touch("a/foo/x")
        await cln.touch("a/foo/y")
        await cln.touch("b")
        assert ["foo"] == await ls("a")

        # Make sure files are created.
        assert [
            (file_server.roots[0], Path("a/foo/x")),
            (file_server.roots[0], Path("a/foo/y")),
        ] == list(sorted(file_server.glob("a/foo/*")))

        assert await cln.read("a/foo/x") == b""
        assert await cln.read("/a/foo/x") == b""  # Slash or no slash makes no difference.
        with pytest.raises(OSError) as e:
            await cln.read("a/foo/z")
        assert e.value.errno == errno.ENOENT
        with pytest.raises(OSError) as e:
            await cln.get_info("a/foo/z")
        assert e.value.errno == errno.ENOENT

        # Write non-existent file
        with pytest.raises(OSError) as e:
            await cln.write("a/foo/z", bytes(range(200)) * 3)
        assert e.value.errno == errno.ENOENT

        # Write into empty file
        data = bytes(range(200)) * 3
        data_chunks = math.ceil(len(data) / cln.data_transfer_capacity)
        write_tracker = ProgressTracker()

        def write_progress_cb(bytes_written: int, bytes_total: int) -> None:
            write_tracker.counter += 1
            assert bytes_total == len(data)
            assert bytes_written == min(write_tracker.counter * cln.data_transfer_capacity, len(data))

        await cln.write("a/foo/x", data, progress=write_progress_cb)
        assert write_tracker.counter == data_chunks

        read_tracker = ProgressTracker()

        def read_progress_cb(bytes_read: int, bytes_total: int | None) -> None:
            read_tracker.counter += 1
            assert bytes_total is None
            assert bytes_read == min(read_tracker.counter * cln.data_transfer_capacity, len(data))

        assert await cln.read("a/foo/x", progress=read_progress_cb) == data
        assert read_tracker.counter == data_chunks

        assert (await cln.get_info("a/foo/x")).size == 600

        # Truncation -- this write is shorter
        hundred = bytes(x ^ 0xFF for x in range(100))
        await cln.write("a/foo/x", hundred * 4)
        assert (await cln.get_info("a/foo/x")).size == 400
        assert await cln.read("a/foo/x") == (hundred * 4)
        assert (await cln.get_info("a/foo/x")).size == 400

        # Fill in the middle without truncation
        ref = bytearray(hundred * 4)
        for i in range(100):
            ref[i + 100] = 0x55
        assert len(ref) == 400
        assert (await cln.get_info("a/foo/x")).size == 400
        await cln.write("a/foo/x", b"\x55" * 100, offset=100, truncate=False)
        assert (await cln.get_info("a/foo/x")).size == 400
        assert await cln.read("a/foo/x") == ref

        # Fill in the middle with truncation
        await cln.write("a/foo/x", b"\xaa" * 50, offset=50)
        assert (await cln.get_info("a/foo/x")).size == 100
        assert await cln.read("a/foo/x") == hundred[:50] + b"\xaa" * 50

        # Directories
        info = await cln.get_info("a/foo")
        print("a/foo:", info)
        assert info.error.value == Error.OK
        assert info.is_writeable
        assert info.is_readable
        assert not info.is_file_not_directory
        assert not info.is_link

        with pytest.raises(OSError) as e:
            await cln.get_info("a/foo/nothing")
        assert e.value.errno == errno.ENOENT
        with pytest.raises(OSError) as e:
            await cln.write("a/foo", b"123")
        assert e.value.errno in (errno.EISDIR, errno.EACCES)  # Windows compatibility

        # Removal
        with pytest.raises(OSError) as e:
            await cln.remove("a/foo/z")
        assert e.value.errno == errno.ENOENT
        await cln.remove("a/foo/x")
        await cln.touch("a/foo/x")  # Put it back
        await cln.remove("a/foo/")  # Removed
        with pytest.raises(OSError) as e:
            await cln.remove("a/foo/")
        assert e.value.errno == errno.ENOENT  # Not found

        # Copy
        await cln.touch("r/a")
        await cln.touch("r/b/0")
        await cln.touch("r/b/1")
        assert not (await cln.get_info("r/b")).is_file_not_directory
        assert ["a", "b"] == await ls("r")
        await cln.copy("r/b", "r/c")
        assert ["a", "b", "c"] == await ls("r")
        with pytest.raises(OSError) as e:
            await cln.copy("r/a", "r/c")  # Overwrite not enabled
        assert e.value.errno == errno.EINVAL
        assert ["a", "b", "c"] == await ls("r")
        assert not (await cln.get_info("r/c")).is_file_not_directory
        await cln.copy("/r/a", "r/c", overwrite=True)
        assert (await cln.get_info("r/c")).is_file_not_directory

        # Move
        assert ["a", "b", "c"] == await ls("r")
        with pytest.raises(OSError) as e:
            await cln.move("/r/a", "r/c")
        assert e.value.errno == errno.EINVAL  # Overwrite not enabled
        await cln.move("/r/a", "r/c", overwrite=True)
        assert ["b", "c"] == await ls("r")
        with pytest.raises(OSError) as e:
            await cln.move("/r/a", "r/c", overwrite=True)
        assert e.value.errno == errno.ENOENT
        assert ["b", "c"] == await ls("r")

        # Access protected files
        if sys.platform.startswith("linux"):  # pragma: no branch
            file_server.roots.append(Path("/"))
            info = await cln.get_info("dev/null")
            print("/dev/null:", info)
            assert info.error.value == 0
            assert not info.is_link
            assert info.is_writeable
            assert info.is_file_not_directory

            info = await cln.get_info("/bin/sh")
            print("/bin/sh:", info)
            assert info.error.value == 0
            assert not info.is_writeable
            assert info.is_file_not_directory

            assert await cln.read("/dev/null", size=100) == b""  # Read less than requested
            assert await cln.read("/dev/zero", size=100) == b"\x00" * 256  # Read more than requested
            # Umm, is this a good idea?! What if it succeeds :O
            with pytest.raises(OSError) as e:
                await cln.write("bin/sh", b"123")
            assert e.value.errno in {errno.EPERM, errno.EACCES}

            file_server.roots.pop(-1)
    finally:
        srv_node.close()
        cln_node.close()
        await asyncio.sleep(1.0)
        shutil.rmtree(root_a, ignore_errors=True)
        shutil.rmtree(root_b, ignore_errors=True)
