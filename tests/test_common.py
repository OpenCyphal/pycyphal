"""Comprehensive tests for pycyphal._common module."""

from __future__ import annotations

import time
from abc import ABC

import pytest

from pycyphal._common import (
    NAME_ANY,
    NAME_HOME,
    NAME_ONE,
    NAME_SEP,
    TOPIC_NAME_MAX,
    Closable,
    DeliveryError,
    Error,
    Instant,
    LivenessError,
    NackError,
    Priority,
    SendError,
    name_expand_home,
    name_is_absolute,
    name_is_homeful,
    name_is_valid,
    name_is_verbatim,
    name_join,
    name_match,
    name_normalize,
    name_resolve,
)

# =====================================================================================================================
# Exception hierarchy
# =====================================================================================================================


class TestExceptionHierarchy:
    """Test that all custom exceptions have the correct inheritance chain."""

    def test_error_is_exception(self) -> None:
        assert issubclass(Error, Exception)

    def test_send_error_inherits_error(self) -> None:
        assert issubclass(SendError, Error)
        assert issubclass(SendError, Exception)

    def test_delivery_error_inherits_error(self) -> None:
        assert issubclass(DeliveryError, Error)
        assert issubclass(DeliveryError, Exception)

    def test_liveness_error_inherits_error(self) -> None:
        assert issubclass(LivenessError, Error)
        assert issubclass(LivenessError, Exception)

    def test_nack_error_inherits_error(self) -> None:
        assert issubclass(NackError, Error)
        assert issubclass(NackError, Exception)

    def test_error_can_be_raised_and_caught_as_exception(self) -> None:
        with pytest.raises(Exception):
            raise Error("test")

    def test_send_error_caught_as_error(self) -> None:
        with pytest.raises(Error):
            raise SendError("send failed")

    def test_delivery_error_caught_as_error(self) -> None:
        with pytest.raises(Error):
            raise DeliveryError("delivery failed")

    def test_liveness_error_caught_as_error(self) -> None:
        with pytest.raises(Error):
            raise LivenessError("liveness failed")

    def test_nack_error_caught_as_error(self) -> None:
        with pytest.raises(Error):
            raise NackError("nack")

    def test_error_message_preserved(self) -> None:
        err = Error("something went wrong")
        assert str(err) == "something went wrong"

    def test_send_error_message_preserved(self) -> None:
        err = SendError("cannot send")
        assert str(err) == "cannot send"

    def test_exceptions_are_distinct(self) -> None:
        """Each exception type should not be a subclass of other siblings."""
        assert not issubclass(SendError, DeliveryError)
        assert not issubclass(DeliveryError, SendError)
        assert not issubclass(LivenessError, NackError)
        assert not issubclass(NackError, LivenessError)
        assert not issubclass(SendError, LivenessError)

    def test_error_with_no_message(self) -> None:
        err = Error()
        assert str(err) == ""

    def test_error_with_multiple_args(self) -> None:
        err = Error("a", "b", 42)
        assert err.args == ("a", "b", 42)


# =====================================================================================================================
# Instant
# =====================================================================================================================


class TestInstantCreation:
    """Test Instant construction and basic properties."""

    def test_create_with_keyword_ns(self) -> None:
        inst = Instant(ns=1000)
        assert inst.ns == 1000

    def test_ns_must_be_keyword(self) -> None:
        with pytest.raises(TypeError):
            Instant(1000)  # type: ignore[misc]

    def test_ns_coerced_to_int(self) -> None:
        inst = Instant(ns=3.7)  # type: ignore[arg-type]
        assert inst.ns == 3
        assert isinstance(inst.ns, int)

    def test_zero(self) -> None:
        inst = Instant(ns=0)
        assert inst.ns == 0

    def test_negative_ns(self) -> None:
        inst = Instant(ns=-500)
        assert inst.ns == -500

    def test_large_ns(self) -> None:
        big = 10**18
        inst = Instant(ns=big)
        assert inst.ns == big

    def test_frozen(self) -> None:
        inst = Instant(ns=100)
        with pytest.raises(AttributeError):
            inst.ns = 200  # type: ignore[misc]


class TestInstantNow:
    """Test Instant.now() static method."""

    def test_now_returns_instant(self) -> None:
        assert isinstance(Instant.now(), Instant)

    def test_now_monotonic(self) -> None:
        a = Instant.now()
        b = Instant.now()
        assert b.ns >= a.ns

    def test_now_approximately_matches_time_monotonic(self) -> None:
        before = time.monotonic_ns()
        inst = Instant.now()
        after = time.monotonic_ns()
        assert before <= inst.ns <= after


class TestInstantProperties:
    """Test the s, ms, us conversion properties."""

    def test_s_from_nanoseconds(self) -> None:
        inst = Instant(ns=1_000_000_000)
        assert inst.s == pytest.approx(1.0)

    def test_s_fractional(self) -> None:
        inst = Instant(ns=1_500_000_000)
        assert inst.s == pytest.approx(1.5)

    def test_ms_from_nanoseconds(self) -> None:
        inst = Instant(ns=1_000_000)
        assert inst.ms == pytest.approx(1.0)

    def test_ms_fractional(self) -> None:
        inst = Instant(ns=2_500_000)
        assert inst.ms == pytest.approx(2.5)

    def test_us_from_nanoseconds(self) -> None:
        inst = Instant(ns=1_000)
        assert inst.us == pytest.approx(1.0)

    def test_us_fractional(self) -> None:
        inst = Instant(ns=1_500)
        assert inst.us == pytest.approx(1.5)

    def test_zero_all_properties(self) -> None:
        inst = Instant(ns=0)
        assert inst.s == 0.0
        assert inst.ms == 0.0
        assert inst.us == 0.0

    def test_negative_properties(self) -> None:
        inst = Instant(ns=-2_000_000_000)
        assert inst.s == pytest.approx(-2.0)
        assert inst.ms == pytest.approx(-2000.0)
        assert inst.us == pytest.approx(-2_000_000.0)


class TestInstantAddition:
    """Test __add__ and __radd__ with float seconds."""

    def test_add_integer_seconds(self) -> None:
        inst = Instant(ns=0)
        result = inst + 2
        assert isinstance(result, Instant)
        assert result.ns == 2_000_000_000

    def test_add_float_seconds(self) -> None:
        inst = Instant(ns=0)
        result = inst + 1.5
        assert result.ns == 1_500_000_000

    def test_add_negative_seconds(self) -> None:
        inst = Instant(ns=5_000_000_000)
        result = inst + (-2.0)
        assert result.ns == 3_000_000_000

    def test_add_zero(self) -> None:
        inst = Instant(ns=42)
        result = inst + 0
        assert result.ns == 42

    def test_add_small_fraction(self) -> None:
        inst = Instant(ns=0)
        result = inst + 0.000000001  # 1 nanosecond
        assert result.ns == 1

    def test_radd_integer_seconds(self) -> None:
        inst = Instant(ns=0)
        result = 3 + inst
        assert isinstance(result, Instant)
        assert result.ns == 3_000_000_000

    def test_radd_float_seconds(self) -> None:
        inst = Instant(ns=1_000_000_000)
        result = 0.5 + inst
        assert result.ns == 1_500_000_000

    def test_add_unsupported_type_returns_not_implemented(self) -> None:
        inst = Instant(ns=0)
        result = inst.__add__("bad")
        assert result is NotImplemented

    def test_add_none_returns_not_implemented(self) -> None:
        inst = Instant(ns=0)
        result = inst.__add__(None)
        assert result is NotImplemented


class TestInstantSubtraction:
    """Test __sub__ with seconds and with another Instant."""

    def test_sub_float_seconds(self) -> None:
        inst = Instant(ns=5_000_000_000)
        result = inst - 2.0
        assert isinstance(result, Instant)
        assert result.ns == 3_000_000_000

    def test_sub_integer_seconds(self) -> None:
        inst = Instant(ns=3_000_000_000)
        result = inst - 1
        assert isinstance(result, Instant)
        assert result.ns == 2_000_000_000

    def test_sub_two_instants_returns_float_seconds(self) -> None:
        a = Instant(ns=5_000_000_000)
        b = Instant(ns=2_000_000_000)
        result = a - b
        assert isinstance(result, float)
        assert result == pytest.approx(3.0)

    def test_sub_two_instants_negative_result(self) -> None:
        a = Instant(ns=1_000_000_000)
        b = Instant(ns=4_000_000_000)
        result = a - b
        assert isinstance(result, float)
        assert result == pytest.approx(-3.0)

    def test_sub_two_equal_instants(self) -> None:
        a = Instant(ns=1234)
        b = Instant(ns=1234)
        result = a - b
        assert result == pytest.approx(0.0)

    def test_sub_zero(self) -> None:
        inst = Instant(ns=100)
        result = inst - 0
        assert isinstance(result, Instant)
        assert result.ns == 100

    def test_sub_unsupported_type_returns_not_implemented(self) -> None:
        inst = Instant(ns=0)
        result = inst.__sub__("bad")
        assert result is NotImplemented


class TestInstantMultiplication:
    """Test __mul__ and __rmul__ for scaling."""

    def test_mul_integer(self) -> None:
        inst = Instant(ns=1_000_000_000)
        result = inst * 3
        assert isinstance(result, Instant)
        assert result.ns == 3_000_000_000

    def test_mul_float(self) -> None:
        inst = Instant(ns=2_000_000_000)
        result = inst * 0.5
        assert result.ns == 1_000_000_000

    def test_mul_zero(self) -> None:
        inst = Instant(ns=999)
        result = inst * 0
        assert result.ns == 0

    def test_mul_negative(self) -> None:
        inst = Instant(ns=1_000_000_000)
        result = inst * -1
        assert result.ns == -1_000_000_000

    def test_rmul_integer(self) -> None:
        inst = Instant(ns=500_000_000)
        result = 4 * inst
        assert isinstance(result, Instant)
        assert result.ns == 2_000_000_000

    def test_rmul_float(self) -> None:
        inst = Instant(ns=1_000_000_000)
        result = 2.5 * inst
        assert result.ns == 2_500_000_000

    def test_mul_unsupported_type_returns_not_implemented(self) -> None:
        inst = Instant(ns=100)
        result = inst.__mul__("bad")
        assert result is NotImplemented


class TestInstantDivision:
    """Test __truediv__ for scaling down."""

    def test_div_integer(self) -> None:
        inst = Instant(ns=6_000_000_000)
        result = inst / 2
        assert isinstance(result, Instant)
        assert result.ns == 3_000_000_000

    def test_div_float(self) -> None:
        inst = Instant(ns=3_000_000_000)
        result = inst / 1.5
        assert result.ns == 2_000_000_000

    def test_div_one(self) -> None:
        inst = Instant(ns=42)
        result = inst / 1
        assert result.ns == 42

    def test_div_fractional_result_rounded(self) -> None:
        inst = Instant(ns=10)
        result = inst / 3
        # round(10/3) == round(3.333...) == 3
        assert result.ns == round(10 / 3)

    def test_div_negative(self) -> None:
        inst = Instant(ns=1_000_000_000)
        result = inst / -2
        assert result.ns == -500_000_000

    def test_div_unsupported_type_returns_not_implemented(self) -> None:
        inst = Instant(ns=100)
        result = inst.__truediv__("bad")
        assert result is NotImplemented


class TestInstantEquality:
    """Test frozen dataclass equality and hashing."""

    def test_equal_instants(self) -> None:
        a = Instant(ns=42)
        b = Instant(ns=42)
        assert a == b

    def test_not_equal_instants(self) -> None:
        a = Instant(ns=1)
        b = Instant(ns=2)
        assert a != b

    def test_hashable(self) -> None:
        a = Instant(ns=100)
        b = Instant(ns=100)
        assert hash(a) == hash(b)
        s = {a, b}
        assert len(s) == 1

    def test_repr(self) -> None:
        inst = Instant(ns=123)
        r = repr(inst)
        assert "123" in r
        assert "Instant" in r


class TestInstantComparison:
    """Frozen dataclass without order=True does not provide ordering."""

    def test_less_than_not_supported(self) -> None:
        a = Instant(ns=1)
        b = Instant(ns=2)
        with pytest.raises(TypeError):
            a < b  # type: ignore[operator]

    def test_greater_than_not_supported(self) -> None:
        a = Instant(ns=3)
        b = Instant(ns=1)
        with pytest.raises(TypeError):
            a > b  # type: ignore[operator]

    def test_less_equal_not_supported(self) -> None:
        a = Instant(ns=5)
        b = Instant(ns=5)
        with pytest.raises(TypeError):
            a <= b  # type: ignore[operator]

    def test_greater_equal_not_supported(self) -> None:
        a = Instant(ns=5)
        b = Instant(ns=5)
        with pytest.raises(TypeError):
            a >= b  # type: ignore[operator]


# =====================================================================================================================
# Priority
# =====================================================================================================================


class TestPriority:
    """Test the Priority IntEnum."""

    def test_exceptional_value(self) -> None:
        assert Priority.EXCEPTIONAL == 0

    def test_immediate_value(self) -> None:
        assert Priority.IMMEDIATE == 1

    def test_fast_value(self) -> None:
        assert Priority.FAST == 2

    def test_high_value(self) -> None:
        assert Priority.HIGH == 3

    def test_nominal_value(self) -> None:
        assert Priority.NOMINAL == 4

    def test_low_value(self) -> None:
        assert Priority.LOW == 5

    def test_slow_value(self) -> None:
        assert Priority.SLOW == 6

    def test_optional_value(self) -> None:
        assert Priority.OPTIONAL == 7

    def test_total_count(self) -> None:
        assert len(Priority) == 8

    def test_is_int(self) -> None:
        assert isinstance(Priority.NOMINAL, int)

    def test_ordering(self) -> None:
        assert Priority.EXCEPTIONAL < Priority.IMMEDIATE < Priority.FAST < Priority.HIGH
        assert Priority.HIGH < Priority.NOMINAL < Priority.LOW < Priority.SLOW < Priority.OPTIONAL

    def test_from_int(self) -> None:
        assert Priority(0) is Priority.EXCEPTIONAL
        assert Priority(7) is Priority.OPTIONAL

    def test_invalid_value(self) -> None:
        with pytest.raises(ValueError):
            Priority(8)

    def test_comparison_with_int(self) -> None:
        assert Priority.NOMINAL == 4
        assert Priority.NOMINAL > 3
        assert Priority.NOMINAL < 5

    def test_arithmetic_with_int(self) -> None:
        result = Priority.NOMINAL + 1
        assert result == 5


# =====================================================================================================================
# Closable
# =====================================================================================================================


class TestClosable:
    """Test the Closable ABC."""

    def test_closable_is_abstract(self) -> None:
        assert issubclass(Closable, ABC)
        with pytest.raises(TypeError):
            Closable()  # type: ignore[abstract]

    def test_concrete_subclass(self) -> None:
        class MyClosable(Closable):
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        obj = MyClosable()
        assert not obj.closed
        obj.close()
        assert obj.closed

    def test_subclass_without_close_raises(self) -> None:
        with pytest.raises(TypeError):

            class BadClosable(Closable):  # type: ignore[abstract]
                pass

            BadClosable()  # type: ignore[abstract]


# =====================================================================================================================
# Name constants
# =====================================================================================================================


class TestNameConstants:
    """Verify the sentinel constants."""

    def test_name_sep(self) -> None:
        assert NAME_SEP == "/"

    def test_name_home(self) -> None:
        assert NAME_HOME == "~"

    def test_name_one(self) -> None:
        assert NAME_ONE == "*"

    def test_name_any(self) -> None:
        assert NAME_ANY == ">"

    def test_topic_name_max(self) -> None:
        assert TOPIC_NAME_MAX == 200


# =====================================================================================================================
# name_normalize
# =====================================================================================================================


class TestNameNormalize:
    """Test name_normalize: strip leading/trailing/duplicate separators, validate chars."""

    def test_simple_name(self) -> None:
        assert name_normalize("foo") == "foo"

    def test_strip_leading_separator(self) -> None:
        assert name_normalize("/foo") == "foo"

    def test_strip_trailing_separator(self) -> None:
        assert name_normalize("foo/") == "foo"

    def test_strip_both_separators(self) -> None:
        assert name_normalize("/foo/") == "foo"

    def test_collapse_duplicate_separators(self) -> None:
        assert name_normalize("foo//bar") == "foo/bar"

    def test_collapse_many_separators(self) -> None:
        assert name_normalize("foo////bar///baz") == "foo/bar/baz"

    def test_all_separators(self) -> None:
        assert name_normalize("///") == ""

    def test_single_separator(self) -> None:
        assert name_normalize("/") == ""

    def test_empty_string(self) -> None:
        assert name_normalize("") == ""

    def test_multi_segment(self) -> None:
        assert name_normalize("a/b/c") == "a/b/c"

    def test_multi_segment_with_leading_trailing(self) -> None:
        assert name_normalize("/a/b/c/") == "a/b/c"

    def test_tilde_preserved(self) -> None:
        assert name_normalize("~") == "~"

    def test_tilde_with_path(self) -> None:
        assert name_normalize("~/foo") == "~/foo"

    def test_star_preserved(self) -> None:
        assert name_normalize("*") == "*"

    def test_gt_preserved(self) -> None:
        assert name_normalize(">") == ">"

    def test_mixed_wildcards(self) -> None:
        assert name_normalize("foo/*/bar/>") == "foo/*/bar/>"

    def test_invalid_space_char(self) -> None:
        with pytest.raises(ValueError, match="Invalid character"):
            name_normalize("foo bar")

    def test_invalid_tab_char(self) -> None:
        with pytest.raises(ValueError, match="Invalid character"):
            name_normalize("foo\tbar")

    def test_invalid_newline_char(self) -> None:
        with pytest.raises(ValueError, match="Invalid character"):
            name_normalize("foo\nbar")

    def test_invalid_null_char(self) -> None:
        with pytest.raises(ValueError, match="Invalid character"):
            name_normalize("foo\x00bar")

    def test_invalid_unicode_above_126(self) -> None:
        with pytest.raises(ValueError, match="Invalid character"):
            name_normalize("caf\u00e9")

    def test_invalid_del_char_127(self) -> None:
        with pytest.raises(ValueError, match="Invalid character"):
            name_normalize("foo\x7fbar")

    def test_valid_printable_edge_low(self) -> None:
        # char 33 is '!'
        assert name_normalize("!") == "!"

    def test_valid_printable_edge_high(self) -> None:
        # char 126 is '~'
        assert name_normalize("~") == "~"

    def test_invalid_char_32_space(self) -> None:
        with pytest.raises(ValueError):
            name_normalize(" ")

    def test_only_valid_ascii_chars(self) -> None:
        # All printable ASCII from 33 to 126 except '/' should pass through
        chars = "".join(chr(c) for c in range(33, 127) if chr(c) != "/")
        result = name_normalize(chars)
        assert result == chars


# =====================================================================================================================
# name_is_valid
# =====================================================================================================================


class TestNameIsValid:
    """Test name_is_valid: non-empty, <=255 chars, all printable ASCII 33-126."""

    def test_simple_valid(self) -> None:
        assert name_is_valid("foo") is True

    def test_valid_with_separator(self) -> None:
        assert name_is_valid("foo/bar") is True

    def test_valid_with_wildcards(self) -> None:
        assert name_is_valid("foo/*/bar/>") is True

    def test_empty_string_invalid(self) -> None:
        assert name_is_valid("") is False

    def test_too_long_invalid(self) -> None:
        name = "a" * (TOPIC_NAME_MAX + 1)
        assert name_is_valid(name) is False

    def test_exactly_max_length_valid(self) -> None:
        name = "a" * TOPIC_NAME_MAX
        assert name_is_valid(name) is True

    def test_space_invalid(self) -> None:
        assert name_is_valid("foo bar") is False

    def test_tab_invalid(self) -> None:
        assert name_is_valid("foo\tbar") is False

    def test_unicode_invalid(self) -> None:
        assert name_is_valid("caf\u00e9") is False

    def test_null_byte_invalid(self) -> None:
        assert name_is_valid("foo\x00") is False

    def test_single_char_valid(self) -> None:
        assert name_is_valid("a") is True

    def test_tilde_valid(self) -> None:
        assert name_is_valid("~") is True

    def test_separator_only_valid(self) -> None:
        # '/' is ord 47, which is between 33 and 126
        assert name_is_valid("/") is True

    def test_bang_valid(self) -> None:
        assert name_is_valid("!") is True

    def test_del_invalid(self) -> None:
        assert name_is_valid("\x7f") is False


# =====================================================================================================================
# name_is_verbatim
# =====================================================================================================================


class TestNameIsVerbatim:
    """Test name_is_verbatim: no '*' or '>' present."""

    def test_verbatim_simple(self) -> None:
        assert name_is_verbatim("foo/bar") is True

    def test_verbatim_empty(self) -> None:
        assert name_is_verbatim("") is True

    def test_not_verbatim_star(self) -> None:
        assert name_is_verbatim("foo/*") is False

    def test_not_verbatim_gt(self) -> None:
        assert name_is_verbatim("foo/>") is False

    def test_not_verbatim_both(self) -> None:
        assert name_is_verbatim("*/foo/>") is False

    def test_verbatim_with_tilde(self) -> None:
        assert name_is_verbatim("~/foo") is True

    def test_verbatim_with_separator_only(self) -> None:
        assert name_is_verbatim("/") is True

    def test_star_in_middle(self) -> None:
        assert name_is_verbatim("a*b") is False

    def test_gt_in_middle(self) -> None:
        assert name_is_verbatim("a>b") is False


# =====================================================================================================================
# name_is_homeful
# =====================================================================================================================


class TestNameIsHomeful:
    """Test name_is_homeful: starts with '~/' or is exactly '~'."""

    def test_tilde_only(self) -> None:
        assert name_is_homeful("~") is True

    def test_tilde_slash(self) -> None:
        assert name_is_homeful("~/foo") is True

    def test_tilde_slash_nested(self) -> None:
        assert name_is_homeful("~/foo/bar") is True

    def test_not_homeful_no_tilde(self) -> None:
        assert name_is_homeful("foo") is False

    def test_not_homeful_empty(self) -> None:
        assert name_is_homeful("") is False

    def test_not_homeful_tilde_no_slash(self) -> None:
        # '~foo' is NOT homeful because name[1] != '/'
        assert name_is_homeful("~foo") is False

    def test_not_homeful_slash_first(self) -> None:
        assert name_is_homeful("/~") is False

    def test_tilde_slash_only(self) -> None:
        assert name_is_homeful("~/") is True

    def test_tilde_with_star(self) -> None:
        assert name_is_homeful("~/*") is True


# =====================================================================================================================
# name_is_absolute
# =====================================================================================================================


class TestNameIsAbsolute:
    """Test name_is_absolute: starts with '/'."""

    def test_absolute_simple(self) -> None:
        assert name_is_absolute("/foo") is True

    def test_absolute_root(self) -> None:
        assert name_is_absolute("/") is True

    def test_not_absolute_relative(self) -> None:
        assert name_is_absolute("foo") is False

    def test_not_absolute_empty(self) -> None:
        assert name_is_absolute("") is False

    def test_not_absolute_tilde(self) -> None:
        assert name_is_absolute("~") is False

    def test_absolute_nested(self) -> None:
        assert name_is_absolute("/a/b/c") is True


# =====================================================================================================================
# name_join
# =====================================================================================================================


class TestNameJoin:
    """Test name_join: join two names with separator, normalizing both."""

    def test_join_two_simple(self) -> None:
        assert name_join("foo", "bar") == "foo/bar"

    def test_join_left_empty(self) -> None:
        assert name_join("", "bar") == "bar"

    def test_join_right_empty(self) -> None:
        assert name_join("foo", "") == "foo"

    def test_join_both_empty(self) -> None:
        assert name_join("", "") == ""

    def test_join_normalizes_left(self) -> None:
        assert name_join("/foo/", "bar") == "foo/bar"

    def test_join_normalizes_right(self) -> None:
        assert name_join("foo", "/bar/") == "foo/bar"

    def test_join_normalizes_both(self) -> None:
        assert name_join("//foo//", "//bar//") == "foo/bar"

    def test_join_multi_segment(self) -> None:
        assert name_join("a/b", "c/d") == "a/b/c/d"

    def test_join_with_tilde(self) -> None:
        assert name_join("~", "foo") == "~/foo"

    def test_join_left_all_slashes_becomes_empty(self) -> None:
        assert name_join("///", "bar") == "bar"

    def test_join_right_all_slashes_becomes_empty(self) -> None:
        assert name_join("foo", "///") == "foo"

    def test_join_invalid_char_left_raises(self) -> None:
        with pytest.raises(ValueError):
            name_join("foo bar", "baz")

    def test_join_invalid_char_right_raises(self) -> None:
        with pytest.raises(ValueError):
            name_join("foo", "bar baz")


# =====================================================================================================================
# name_expand_home
# =====================================================================================================================


class TestNameExpandHome:
    """Test name_expand_home: replace '~' prefix with home path."""

    def test_expand_tilde_only(self) -> None:
        assert name_expand_home("~", "my/home") == "my/home"

    def test_expand_tilde_slash_path(self) -> None:
        assert name_expand_home("~/foo", "my/home") == "my/home/foo"

    def test_expand_tilde_nested(self) -> None:
        assert name_expand_home("~/a/b", "root") == "root/a/b"

    def test_no_tilde_passes_through_normalized(self) -> None:
        assert name_expand_home("foo/bar", "home") == "foo/bar"

    def test_no_tilde_still_normalizes(self) -> None:
        assert name_expand_home("//foo//bar//", "home") == "foo/bar"

    def test_absolute_passes_through_normalized(self) -> None:
        assert name_expand_home("/abs/path", "home") == "abs/path"

    def test_tilde_without_slash_passes_through(self) -> None:
        # '~foo' is NOT homeful, so it is just normalized
        assert name_expand_home("~foo", "home") == "~foo"

    def test_expand_with_empty_home(self) -> None:
        # name_join("", "/foo") normalizes to "foo"
        assert name_expand_home("~/foo", "") == "foo"

    def test_expand_tilde_slash_only(self) -> None:
        # "~/" -> rest is "/" which normalizes empty via name_join
        assert name_expand_home("~/", "home") == "home"


# =====================================================================================================================
# name_resolve
# =====================================================================================================================


class TestNameResolve:
    """Test name_resolve: resolve name with namespace and home."""

    def test_absolute_ignores_namespace_and_home(self) -> None:
        assert name_resolve("/abs/path", "ns", "home") == "abs/path"

    def test_homeful_expands_home(self) -> None:
        assert name_resolve("~/foo", "ns", "my/home") == "my/home/foo"

    def test_relative_prepends_namespace(self) -> None:
        assert name_resolve("foo", "ns", "home") == "ns/foo"

    def test_relative_with_homeful_namespace(self) -> None:
        assert name_resolve("foo", "~/ns", "home") == "home/ns/foo"

    def test_absolute_normalizes(self) -> None:
        assert name_resolve("//a//b//", "ns", "home") == "a/b"

    def test_tilde_only_resolves_to_home(self) -> None:
        assert name_resolve("~", "ns", "my/home") == "my/home"

    def test_relative_with_plain_namespace(self) -> None:
        assert name_resolve("bar", "baz", "home") == "baz/bar"

    def test_relative_empty_namespace(self) -> None:
        assert name_resolve("foo", "", "home") == "foo"

    def test_homeful_namespace_expanded(self) -> None:
        # namespace is "~/sub", home is "root"
        # namespace expands to "root/sub", then joins with "topic"
        assert name_resolve("topic", "~/sub", "root") == "root/sub/topic"


# =====================================================================================================================
# name_match - verbatim patterns
# =====================================================================================================================


class TestNameMatchVerbatim:
    """Test name_match with verbatim (exact) patterns."""

    def test_exact_match_single_segment(self) -> None:
        result = name_match("foo", "foo")
        assert result == []

    def test_exact_match_multi_segment(self) -> None:
        result = name_match("foo/bar/baz", "foo/bar/baz")
        assert result == []

    def test_no_match_different_name(self) -> None:
        result = name_match("foo", "bar")
        assert result is None

    def test_no_match_different_length(self) -> None:
        result = name_match("foo/bar", "foo")
        assert result is None

    def test_no_match_name_longer(self) -> None:
        result = name_match("foo", "foo/bar")
        assert result is None

    def test_empty_pattern_empty_name(self) -> None:
        # Both split to [''], so they match exactly
        result = name_match("", "")
        assert result == []

    def test_single_segment_mismatch(self) -> None:
        result = name_match("abc", "xyz")
        assert result is None


# =====================================================================================================================
# name_match - single star patterns
# =====================================================================================================================


class TestNameMatchSingleStar:
    """Test name_match with '*' wildcard (matches exactly one segment)."""

    def test_star_matches_one_segment(self) -> None:
        result = name_match("*", "foo")
        assert result is not None
        assert len(result) == 1
        assert result[0] == ("foo", 0)

    def test_star_at_start(self) -> None:
        result = name_match("*/bar", "foo/bar")
        assert result is not None
        assert len(result) == 1
        assert result[0] == ("foo", 0)

    def test_star_at_end(self) -> None:
        result = name_match("foo/*", "foo/bar")
        assert result is not None
        assert len(result) == 1
        assert result[0] == ("bar", 1)

    def test_star_in_middle(self) -> None:
        result = name_match("foo/*/baz", "foo/bar/baz")
        assert result is not None
        assert len(result) == 1
        assert result[0] == ("bar", 1)

    def test_multiple_stars(self) -> None:
        result = name_match("*/*", "foo/bar")
        assert result is not None
        assert len(result) == 2
        assert result[0] == ("foo", 0)
        assert result[1] == ("bar", 1)

    def test_three_stars(self) -> None:
        result = name_match("*/*/*", "a/b/c")
        assert result is not None
        assert len(result) == 3
        assert result[0] == ("a", 0)
        assert result[1] == ("b", 1)
        assert result[2] == ("c", 2)

    def test_star_does_not_match_zero_segments(self) -> None:
        # Pattern "foo/*/bar" requires exactly 3 segments
        result = name_match("foo/*/bar", "foo/bar")
        assert result is None

    def test_star_does_not_match_multiple_segments(self) -> None:
        # '*' matches exactly one segment, not two
        result = name_match("*", "foo/bar")
        assert result is None

    def test_star_with_literal_prefix(self) -> None:
        result = name_match("prefix/*", "prefix/value")
        assert result is not None
        assert result[0] == ("value", 1)

    def test_star_mismatch_literal_after(self) -> None:
        result = name_match("*/baz", "foo/bar")
        assert result is None

    def test_star_only_pattern_no_name_segments(self) -> None:
        # name "" splits to [''], which is one segment that is empty string
        result = name_match("*", "")
        assert result is not None
        assert result[0] == ("", 0)


# =====================================================================================================================
# name_match - trailing '>' patterns
# =====================================================================================================================


class TestNameMatchTrailingGt:
    """Test name_match with '>' wildcard (matches 1+ remaining segments)."""

    def test_gt_matches_one_remaining(self) -> None:
        result = name_match(">", "foo")
        assert result is not None
        assert len(result) == 1
        assert result[0] == ("foo", 0)

    def test_gt_matches_multiple_remaining(self) -> None:
        result = name_match(">", "foo/bar/baz")
        assert result is not None
        assert len(result) == 3
        assert result[0] == ("foo", 0)
        assert result[1] == ("bar", 0)
        assert result[2] == ("baz", 0)

    def test_gt_after_literal(self) -> None:
        result = name_match("foo/>", "foo/bar")
        assert result is not None
        assert len(result) == 1
        assert result[0] == ("bar", 1)

    def test_gt_after_literal_multi(self) -> None:
        result = name_match("foo/>", "foo/bar/baz/qux")
        assert result is not None
        assert len(result) == 3
        assert result[0] == ("bar", 1)
        assert result[1] == ("baz", 1)
        assert result[2] == ("qux", 1)

    def test_gt_requires_at_least_one_segment(self) -> None:
        # Pattern "foo/>" against "foo" -- '>' needs at least one remaining segment
        result = name_match("foo/>", "foo")
        assert result is None

    def test_gt_must_be_last_segment(self) -> None:
        # '>' is not the last part of the pattern
        result = name_match(">/foo", "bar/foo")
        assert result is None

    def test_gt_after_star(self) -> None:
        result = name_match("*/>", "foo/bar/baz")
        assert result is not None
        assert len(result) == 3
        assert result[0] == ("foo", 0)
        assert result[1] == ("bar", 1)
        assert result[2] == ("baz", 1)

    def test_gt_only_with_empty_name(self) -> None:
        # name "" splits to [''], which has one segment (empty string)
        result = name_match(">", "")
        assert result is not None
        assert len(result) == 1
        assert result[0] == ("", 0)


# =====================================================================================================================
# name_match - mixed patterns
# =====================================================================================================================


class TestNameMatchMixed:
    """Test name_match with mixed literal, '*', and '>' segments."""

    def test_literal_star_gt(self) -> None:
        result = name_match("foo/*/bar/>", "foo/x/bar/a/b")
        assert result is not None
        assert len(result) == 3
        assert result[0] == ("x", 1)  # matched by '*'
        assert result[1] == ("a", 3)  # matched by '>'
        assert result[2] == ("b", 3)  # matched by '>'

    def test_star_literal_star(self) -> None:
        result = name_match("*/mid/*", "a/mid/b")
        assert result is not None
        assert len(result) == 2
        assert result[0] == ("a", 0)
        assert result[1] == ("b", 2)

    def test_mixed_no_match_literal_mismatch(self) -> None:
        result = name_match("foo/*/bar", "foo/x/baz")
        assert result is None

    def test_mixed_no_match_too_few_segments(self) -> None:
        result = name_match("a/*/c/>", "a/b")
        assert result is None

    def test_star_then_gt(self) -> None:
        result = name_match("*/>", "only/two")
        assert result is not None
        assert len(result) == 2
        assert result[0] == ("only", 0)
        assert result[1] == ("two", 1)

    def test_multiple_literals_then_gt(self) -> None:
        result = name_match("a/b/>", "a/b/c/d/e")
        assert result is not None
        assert len(result) == 3
        assert result[0] == ("c", 2)
        assert result[1] == ("d", 2)
        assert result[2] == ("e", 2)

    def test_all_stars_exact_length(self) -> None:
        result = name_match("*/*/*/*", "a/b/c/d")
        assert result is not None
        assert len(result) == 4

    def test_all_stars_too_short(self) -> None:
        result = name_match("*/*/*/*", "a/b/c")
        assert result is None

    def test_all_stars_too_long(self) -> None:
        result = name_match("*/*/*/*", "a/b/c/d/e")
        assert result is None


# =====================================================================================================================
# name_match - non-matching cases
# =====================================================================================================================


class TestNameMatchNoMatch:
    """Test name_match for various non-matching cases."""

    def test_pattern_longer_than_name(self) -> None:
        result = name_match("a/b/c", "a/b")
        assert result is None

    def test_name_longer_than_pattern(self) -> None:
        result = name_match("a/b", "a/b/c")
        assert result is None

    def test_completely_different(self) -> None:
        result = name_match("x/y/z", "a/b/c")
        assert result is None

    def test_case_sensitive(self) -> None:
        result = name_match("Foo", "foo")
        assert result is None

    def test_pattern_star_name_too_long(self) -> None:
        # '*' matches exactly one segment
        result = name_match("foo/*", "foo/bar/baz")
        assert result is None

    def test_gt_not_last_no_match(self) -> None:
        # '>' in non-last position
        result = name_match("a/>/b", "a/x/b")
        assert result is None

    def test_literal_mismatch_first_segment(self) -> None:
        result = name_match("wrong/bar", "foo/bar")
        assert result is None

    def test_literal_mismatch_last_segment(self) -> None:
        result = name_match("foo/wrong", "foo/bar")
        assert result is None


# =====================================================================================================================
# name_match - substitutions index tracking
# =====================================================================================================================


class TestNameMatchSubstitutionIndices:
    """Test that substitution tuples carry the correct pattern segment index."""

    def test_star_at_index_0(self) -> None:
        result = name_match("*/b", "a/b")
        assert result is not None
        assert result[0] == ("a", 0)

    def test_star_at_index_2(self) -> None:
        result = name_match("a/b/*", "a/b/c")
        assert result is not None
        assert result[0] == ("c", 2)

    def test_gt_index_preserved_for_all_captures(self) -> None:
        result = name_match("x/>", "x/a/b/c")
        assert result is not None
        # All captured by '>' at pattern index 1
        assert all(idx == 1 for _, idx in result)

    def test_mixed_indices(self) -> None:
        result = name_match("*/b/*/d/>", "a/b/c/d/e/f")
        assert result is not None
        assert result[0] == ("a", 0)  # '*' at index 0
        assert result[1] == ("c", 2)  # '*' at index 2
        assert result[2] == ("e", 4)  # '>' at index 4
        assert result[3] == ("f", 4)  # '>' at index 4


# =====================================================================================================================
# name_match - edge cases
# =====================================================================================================================


class TestNameMatchEdgeCases:
    """Edge cases and boundary conditions for name_match."""

    def test_single_empty_segment_match(self) -> None:
        result = name_match("", "")
        assert result == []

    def test_single_segment_verbatim(self) -> None:
        result = name_match("x", "x")
        assert result == []

    def test_single_star_single_segment(self) -> None:
        result = name_match("*", "anything")
        assert result is not None
        assert result == [("anything", 0)]

    def test_gt_captures_many_segments(self) -> None:
        segments = "/".join(f"s{i}" for i in range(20))
        result = name_match(">", segments)
        assert result is not None
        assert len(result) == 20

    def test_long_pattern_long_name_match(self) -> None:
        pat = "/".join(["a"] * 50)
        name = "/".join(["a"] * 50)
        result = name_match(pat, name)
        assert result == []

    def test_long_pattern_long_name_mismatch(self) -> None:
        pat = "/".join(["a"] * 50)
        name = "/".join(["a"] * 49 + ["b"])
        result = name_match(pat, name)
        assert result is None

    def test_star_matches_segment_containing_special_chars(self) -> None:
        result = name_match("*", "hello-world_123")
        assert result is not None
        assert result[0] == ("hello-world_123", 0)

    def test_gt_at_position_0_captures_all(self) -> None:
        result = name_match(">", "a/b/c")
        assert result is not None
        assert len(result) == 3
        assert [s for s, _ in result] == ["a", "b", "c"]

    def test_verbatim_match_returns_empty_list_not_none(self) -> None:
        result = name_match("exact", "exact")
        assert result is not None
        assert result == []
        assert isinstance(result, list)


# =====================================================================================================================
# Integration / cross-feature tests
# =====================================================================================================================


class TestCrossFeature:
    """Tests combining multiple features from the module."""

    def test_instant_add_then_subtract_roundtrip(self) -> None:
        original = Instant(ns=1_000_000_000)
        shifted = original + 2.5
        diff = shifted - original
        assert diff == pytest.approx(2.5)

    def test_instant_mul_div_roundtrip(self) -> None:
        original = Instant(ns=1_000_000_000)
        doubled = original * 2
        halved = doubled / 2
        assert halved.ns == original.ns

    def test_instant_chain_operations(self) -> None:
        inst = Instant(ns=0)
        result = (inst + 1.0) * 3 - 1.0
        # (0+1e9)*3 = 3e9, then -1e9 = 2e9
        assert isinstance(result, Instant)
        assert result.ns == 2_000_000_000

    def test_priority_usable_as_dict_key(self) -> None:
        d = {Priority.NOMINAL: "default", Priority.FAST: "urgent"}
        assert d[Priority.NOMINAL] == "default"
        assert d[Priority(4)] == "default"

    def test_name_normalize_then_match(self) -> None:
        pattern = name_normalize("//foo//*//")
        name = name_normalize("//foo//bar//")
        result = name_match(pattern, name)
        # pattern normalizes to "foo/*", name to "foo/bar"
        assert result is not None
        assert result[0] == ("bar", 1)

    def test_name_resolve_then_match(self) -> None:
        resolved = name_resolve("topic", "~/ns", "root")
        # resolves to "root/ns/topic"
        result = name_match("root/*/topic", resolved)
        assert result is not None
        assert result[0] == ("ns", 1)

    def test_name_join_then_is_valid(self) -> None:
        joined = name_join("foo", "bar")
        assert name_is_valid(joined)

    def test_name_expand_home_then_is_absolute(self) -> None:
        expanded = name_expand_home("~/foo", "/root")
        # home is "/root", normalize strips leading slash -> "root"
        # name_join("root", "foo") -> "root/foo"
        # This is not absolute since leading '/' was normalized away
        assert not name_is_absolute(expanded)

    def test_closable_subclass_as_exception_context(self) -> None:
        class Resource(Closable):
            def close(self) -> None:
                raise SendError("close failed")

        r = Resource()
        with pytest.raises(SendError):
            r.close()
