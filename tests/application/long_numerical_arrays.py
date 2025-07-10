# Copyright (c) 2025 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Huong Pham <huong.pham@zubax.com>
from typing import List


def _unittest_strictify_bool() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [True, False]
    n = _strictify(s).bit
    assert n is not None
    v = n.value
    assert (s == v).all()  # type: ignore[attr-defined]


def _unittest_strictify_u64() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [x * 1000000 for x in range(30)]
    n = _strictify(s).natural64
    assert n is not None
    v = n.value
    assert (s == v).all()  # type: ignore[attr-defined]


def _unittest_strictify_u32() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [x * 1000000 for x in range(60)]
    n = _strictify(s).natural32
    assert n is not None
    v = n.value
    assert (s == v).all()  # type: ignore[attr-defined]


def _unittest_strictify_u16() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [x * 100 for x in range(80)]
    n = _strictify(s).natural16
    assert n is not None
    v = n.value
    assert (s == v).all()  # type: ignore[attr-defined]


def _unittest_strictify_i64() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [-x * 1000000 for x in range(30)]
    n = _strictify(s).integer64
    assert n is not None
    v = n.value
    assert (s == v).all()  # type: ignore[attr-defined]


def _unittest_strictify_i32() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [-x * 1000000 for x in range(60)]
    n = _strictify(s).integer32
    assert n is not None
    v = n.value
    assert (s == v).all()  # type: ignore[attr-defined]


def _unittest_strictify_i16() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [-x * 100 for x in range(80)]
    n = _strictify(s).integer16
    assert n is not None
    v = n.value
    assert (s == v).all()  # type: ignore[attr-defined]
