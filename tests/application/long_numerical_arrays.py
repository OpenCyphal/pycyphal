# Copyright (c) 2025 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Huong Pham <huong.pham@zubax.com>


def _unittest_strictify_bool() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [True, False]
    v = _strictify(s).bit.value
    assert (s == v).all()


def _unittest_strictify_u64() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [x * 1000000 for x in range(30)]
    v = _strictify(s).natural64.value
    assert (s == v).all()


def _unittest_strictify_u32() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [x * 1000000 for x in range(60)]
    v = _strictify(s).natural32.value
    assert (s == v).all()


def _unittest_strictify_u16() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [x * 100 for x in range(80)]
    v = _strictify(s).natural16.value
    assert (s == v).all()


def _unittest_strictify_i64() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [-x * 1000000 for x in range(30)]
    v = _strictify(s).integer64.value
    assert (s == v).all()


def _unittest_strictify_i32() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [-x * 1000000 for x in range(60)]
    v = _strictify(s).integer32.value
    assert (s == v).all()


def _unittest_strictify_i16() -> None:
    # noinspection PyProtectedMember
    from pycyphal.application.register._value import _strictify

    s = [-x * 100 for x in range(80)]
    v = _strictify(s).integer16.value
    assert (s == v).all()
