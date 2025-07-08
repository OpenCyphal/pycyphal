import pycyphal
import pytest

# noinspection PyProtectedMember
from pycyphal.application.register._value import _strictify


def _unittest_strictify_bool() -> None:
    s = [True, False]
    v = _strictify(s).bit.value
    assert (s == v).all()


def _unittest_strictify_u64() -> None:
    s = [x * 1000000 for x in range(30)]
    v = _strictify(s).natural64.value
    assert (s == v).all()


def _unittest_strictify_u32() -> None:
    s = [x * 1000000 for x in range(60)]
    v = _strictify(s).natural32.value
    assert (s == v).all()


def _unittest_strictify_u16() -> None:
    s = [x * 100 for x in range(80)]
    v = _strictify(s).natural16.value
    assert (s == v).all()


def _unittest_strictify_i64() -> None:
    s = [-x * 1000000 for x in range(30)]
    v = _strictify(s).integer64.value
    assert (s == v).all()


def _unittest_strictify_int64() -> None:
    s = [-x * 1000000 for x in range(60)]
    v = _strictify(s).integer32.value
    assert (s == v).all()


def _unittest_strictify_int128() -> None:
    s = [-x * 100 for x in range(80)]
    v = _strictify(s).integer16.value
    assert (s == v).all()
