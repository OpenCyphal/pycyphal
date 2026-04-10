"""Tests for name resolution and pattern matching in pycyphal2._node."""

from __future__ import annotations

import pytest

from pycyphal2 import SUBJECT_ID_PINNED_MAX
from pycyphal2._node import (
    TOPIC_NAME_MAX,
    _name_consume_pin_suffix,
    _name_normalize,
    match_pattern,
    resolve_name,
)

# =====================================================================================================================
# _name_normalize
# =====================================================================================================================


def test_normalize_simple() -> None:
    assert _name_normalize("a/b/c") == "a/b/c"


def test_normalize_strips_leading_trailing() -> None:
    assert _name_normalize("/a/b/") == "a/b"


def test_normalize_collapses_multiple_slashes() -> None:
    assert _name_normalize("a//b///c") == "a/b/c"


def test_normalize_all_slashes() -> None:
    assert _name_normalize("///") == ""


def test_normalize_single_segment() -> None:
    assert _name_normalize("foo") == "foo"


def test_normalize_empty() -> None:
    assert _name_normalize("") == ""


def test_normalize_leading_slashes() -> None:
    assert _name_normalize("//a") == "a"


def test_normalize_trailing_slashes() -> None:
    assert _name_normalize("a//") == "a"


# =====================================================================================================================
# _name_consume_pin_suffix
# =====================================================================================================================


def test_pin_basic() -> None:
    assert _name_consume_pin_suffix("foo#123") == ("foo", 123)


def test_pin_zero() -> None:
    assert _name_consume_pin_suffix("foo#0") == ("foo", 0)


def test_pin_max_valid() -> None:
    assert _name_consume_pin_suffix(f"foo#{SUBJECT_ID_PINNED_MAX}") == ("foo", SUBJECT_ID_PINNED_MAX)


def test_pin_over_max() -> None:
    # Pin value exceeding SUBJECT_ID_PINNED_MAX (0x1FFF = 8191) is rejected.
    assert _name_consume_pin_suffix(f"foo#{SUBJECT_ID_PINNED_MAX + 1}") == (f"foo#{SUBJECT_ID_PINNED_MAX + 1}", None)


def test_pin_leading_zeros() -> None:
    assert _name_consume_pin_suffix("foo#01") == ("foo#01", None)
    assert _name_consume_pin_suffix("foo#007") == ("foo#007", None)


def test_pin_no_hash() -> None:
    assert _name_consume_pin_suffix("foobar") == ("foobar", None)


def test_pin_trailing_hash_no_digits() -> None:
    assert _name_consume_pin_suffix("foo#") == ("foo#", None)


def test_pin_non_digit_after_hash() -> None:
    assert _name_consume_pin_suffix("foo#abc") == ("foo#abc", None)


def test_pin_hash_in_middle() -> None:
    # Scanning from right: '42' digits, then '#' found -> pin extracted from the rightmost '#'.
    assert _name_consume_pin_suffix("a#b#42") == ("a#b", 42)


def test_pin_with_path() -> None:
    assert _name_consume_pin_suffix("a/b/c#100") == ("a/b/c", 100)


def test_pin_empty_string() -> None:
    assert _name_consume_pin_suffix("") == ("", None)


def test_pin_only_hash() -> None:
    assert _name_consume_pin_suffix("#") == ("#", None)


def test_pin_only_digits() -> None:
    # "#42" -- hash at position 0, digits after it.
    assert _name_consume_pin_suffix("#42") == ("", 42)


def test_pin_multiple_hashes_valid_suffix() -> None:
    # "x#y#5" -- scanning from right: '5' is digit, then '#' found at index 3.
    # But 'y' is not a digit, so the scan would return (name, None) before reaching the '#'.
    # Actually: scanning from right: name[-1]='5' (digit), name[-2]='#' -> hash_pos=3.
    # digits = "5", valid. Returns ("x#y", 5).
    assert _name_consume_pin_suffix("x#y#5") == ("x#y", 5)


# =====================================================================================================================
# resolve_name -- absolute names
# =====================================================================================================================


def test_resolve_absolute_simple() -> None:
    resolved, pin, verbatim = resolve_name("/foo/bar", "home", "ns")
    assert resolved == "foo/bar"
    assert pin is None
    assert verbatim is True


def test_resolve_absolute_normalizes() -> None:
    resolved, pin, verbatim = resolve_name("//foo//bar//", "home", "ns")
    assert resolved == "foo/bar"
    assert pin is None
    assert verbatim is True


def test_resolve_absolute_ignores_home_and_ns() -> None:
    resolved, _, _ = resolve_name("/x", "unused_home", "unused_ns")
    assert resolved == "x"


# =====================================================================================================================
# resolve_name -- homeful names
# =====================================================================================================================


def test_resolve_tilde_only() -> None:
    resolved, pin, verbatim = resolve_name("~", "myhome", "ns")
    assert resolved == "myhome"
    assert pin is None
    assert verbatim is True


def test_resolve_tilde_with_path() -> None:
    resolved, _, _ = resolve_name("~/foo", "myhome", "ns")
    assert resolved == "myhome/foo"


def test_resolve_tilde_with_deep_path() -> None:
    resolved, _, _ = resolve_name("~/a/b/c", "base", "ns")
    assert resolved == "base/a/b/c"


def test_resolve_tilde_ignores_namespace() -> None:
    resolved, _, _ = resolve_name("~/x", "home", "should_be_ignored")
    assert resolved == "home/x"


def test_resolve_tilde_slash_normalizes() -> None:
    resolved, _, _ = resolve_name("~///foo", "home", "ns")
    assert resolved == "home/foo"


# =====================================================================================================================
# resolve_name -- relative names
# =====================================================================================================================


def test_resolve_relative_simple() -> None:
    resolved, _, _ = resolve_name("foo", "home", "ns")
    assert resolved == "ns/foo"


def test_resolve_relative_deep_namespace() -> None:
    resolved, _, _ = resolve_name("bar", "home", "a/b")
    assert resolved == "a/b/bar"


def test_resolve_relative_empty_namespace() -> None:
    resolved, _, _ = resolve_name("bar", "home", "")
    assert resolved == "bar"


def test_resolve_relative_namespace_homeful() -> None:
    """Only exact '~' or '~/' are homeful; '~ns' stays literal."""
    resolved, _, _ = resolve_name("topic", "myhome", "~ns")
    assert resolved == "~ns/topic"


def test_resolve_relative_namespace_tilde_only() -> None:
    resolved, _, _ = resolve_name("topic", "myhome", "~")
    assert resolved == "myhome/topic"


def test_resolve_relative_namespace_tilde_slash() -> None:
    resolved, _, _ = resolve_name("topic", "myhome", "~/sub")
    assert resolved == "myhome/sub/topic"


def test_resolve_relative_name_tilde_literal() -> None:
    resolved, _, _ = resolve_name("~foo", "myhome", "ns")
    assert resolved == "ns/~foo"


# =====================================================================================================================
# resolve_name -- pin suffix
# =====================================================================================================================


def test_resolve_with_pin() -> None:
    resolved, pin, verbatim = resolve_name("foo#123", "home", "ns")
    assert resolved == "ns/foo"
    assert pin == 123
    assert verbatim is True


def test_resolve_with_pin_zero() -> None:
    _, pin, _ = resolve_name("foo#0", "home", "ns")
    assert pin == 0


def test_resolve_pin_at_max() -> None:
    _, pin, _ = resolve_name(f"foo#{SUBJECT_ID_PINNED_MAX}", "home", "ns")
    assert pin == SUBJECT_ID_PINNED_MAX


def test_resolve_pin_over_max_not_recognized() -> None:
    """A pin value > SUBJECT_ID_PINNED_MAX is not recognized; the '#9999' stays in the name."""
    resolved, pin, _ = resolve_name(f"/foo#9999", "home", "ns")
    assert pin is None
    assert resolved == "foo#9999"


def test_resolve_pin_leading_zero_not_recognized() -> None:
    resolved, pin, _ = resolve_name("/foo#01", "home", "ns")
    assert pin is None
    assert resolved == "foo#01"


def test_resolve_absolute_with_pin() -> None:
    resolved, pin, _ = resolve_name("/a/b#42", "home", "ns")
    assert resolved == "a/b"
    assert pin == 42


def test_resolve_tilde_with_pin() -> None:
    resolved, pin, _ = resolve_name("~/x#7", "home", "ns")
    assert resolved == "home/x"
    assert pin == 7


# =====================================================================================================================
# resolve_name -- patterns (wildcards)
# =====================================================================================================================


def test_resolve_pattern_star() -> None:
    resolved, pin, verbatim = resolve_name("/a/*/c", "h", "ns")
    assert resolved == "a/*/c"
    assert pin is None
    assert verbatim is False


def test_resolve_pattern_chevron() -> None:
    resolved, pin, verbatim = resolve_name("/a/>", "h", "ns")
    assert resolved == "a/>"
    assert pin is None
    assert verbatim is False


def test_resolve_pattern_star_relative() -> None:
    _, _, verbatim = resolve_name("*/foo", "h", "ns")
    assert verbatim is False


def test_resolve_pattern_with_pin_raises() -> None:
    """Pinned patterns are not allowed."""
    with pytest.raises(ValueError, match="Pattern names cannot be pinned"):
        resolve_name("/a/*#5", "h", "ns")


# =====================================================================================================================
# resolve_name -- validation / error cases
# =====================================================================================================================


def test_resolve_empty_name_raises() -> None:
    with pytest.raises(ValueError, match="Empty name"):
        resolve_name("", "home", "ns")


def test_resolve_whitespace_only_raises() -> None:
    with pytest.raises(ValueError, match="Empty name"):
        resolve_name("   ", "home", "ns")


def test_resolve_too_long_raises() -> None:
    long = "a" * (TOPIC_NAME_MAX + 1)
    with pytest.raises(ValueError, match="exceeds"):
        resolve_name(f"/{long}", "home", "ns")


def test_resolve_at_max_length_ok() -> None:
    name = "a" * TOPIC_NAME_MAX
    resolved, _, _ = resolve_name(f"/{name}", "home", "ns")
    assert resolved == name


def test_resolve_invalid_char_space() -> None:
    with pytest.raises(ValueError, match="Invalid character"):
        resolve_name("/foo bar", "home", "ns")


def test_resolve_invalid_char_tab() -> None:
    with pytest.raises(ValueError, match="Invalid character"):
        resolve_name("/foo\tbar", "home", "ns")


def test_resolve_invalid_char_null() -> None:
    with pytest.raises(ValueError, match="Invalid character"):
        resolve_name("/foo\x00bar", "home", "ns")


def test_resolve_invalid_char_high_ascii() -> None:
    with pytest.raises(ValueError, match="Invalid character"):
        resolve_name("/foo\x7fbar", "home", "ns")


def test_resolve_name_strips_whitespace() -> None:
    """Leading/trailing whitespace is stripped before processing."""
    resolved, _, _ = resolve_name("  /foo  ", "home", "ns")
    assert resolved == "foo"


def test_resolve_only_slashes_raises() -> None:
    """A name that normalizes to empty should raise."""
    with pytest.raises(ValueError, match="resolves to empty"):
        resolve_name("///", "home", "")


# =====================================================================================================================
# match_pattern -- exact (verbatim) match
# =====================================================================================================================


def test_match_exact() -> None:
    assert match_pattern("a/b", "a/b") == []


def test_match_exact_single_segment() -> None:
    assert match_pattern("foo", "foo") == []


# =====================================================================================================================
# match_pattern -- no match
# =====================================================================================================================


def test_match_no_match_different_segment() -> None:
    assert match_pattern("a/b", "a/c") is None


def test_match_no_match_length_pattern_shorter() -> None:
    assert match_pattern("a/b", "a/b/c") is None


def test_match_no_match_length_pattern_longer() -> None:
    assert match_pattern("a/b/c", "a/b") is None


def test_match_no_match_completely_different() -> None:
    assert match_pattern("x/y", "a/b") is None


# =====================================================================================================================
# match_pattern -- single wildcard (*)
# =====================================================================================================================


def test_match_star_middle() -> None:
    result = match_pattern("a/*/c", "a/b/c")
    assert result == [("b", 1)]


def test_match_star_first() -> None:
    result = match_pattern("*/b/c", "x/b/c")
    assert result == [("x", 0)]


def test_match_star_last() -> None:
    result = match_pattern("a/b/*", "a/b/z")
    assert result == [("z", 2)]


def test_match_star_no_match_wrong_literal() -> None:
    assert match_pattern("a/*/c", "a/b/d") is None


def test_match_star_no_match_length() -> None:
    """Star matches exactly one segment; cannot match if lengths differ."""
    assert match_pattern("a/*", "a/b/c") is None


# =====================================================================================================================
# match_pattern -- multi-level wildcard (>)
# =====================================================================================================================


def test_match_chevron_multiple_segments() -> None:
    result = match_pattern("a/>", "a/b/c")
    assert result == [("b/c", 1)]


def test_match_chevron_one_segment() -> None:
    result = match_pattern("a/>", "a/b")
    assert result == [("b", 1)]


def test_match_chevron_zero_segments() -> None:
    """'>' matches zero or more segments."""
    assert match_pattern("a/>", "a") == [("", 1)]


def test_match_chevron_many_segments() -> None:
    result = match_pattern("x/>", "x/a/b/c/d")
    assert result == [("a/b/c/d", 1)]


def test_match_chevron_at_start() -> None:
    result = match_pattern(">", "a/b/c")
    assert result == [("a/b/c", 0)]


def test_match_chevron_single_segment_name() -> None:
    result = match_pattern(">", "x")
    assert result == [("x", 0)]


def test_match_nonterminal_chevron_is_literal() -> None:
    assert match_pattern("a/>/c", "a/>/c") == []
    assert match_pattern("a/>/c", "a/c") is None
    assert match_pattern("a/>/c", "a/b/d/e/c") is None


def test_match_only_terminal_chevron_is_special() -> None:
    assert match_pattern("a/>/>", "a/>/b/c") == [("b/c", 2)]
    assert match_pattern("a/>/>", "a/b/c") is None


# =====================================================================================================================
# match_pattern -- multiple wildcards
# =====================================================================================================================


def test_match_multiple_stars() -> None:
    result = match_pattern("*/*/c", "x/y/c")
    assert result == [("x", 0), ("y", 1)]


def test_match_star_and_chevron() -> None:
    result = match_pattern("a/*/b/>", "a/x/b/y/z")
    assert result == [("x", 1), ("y/z", 3)]


def test_match_star_star_star() -> None:
    result = match_pattern("*/*/*", "p/q/r")
    assert result == [("p", 0), ("q", 1), ("r", 2)]


def test_match_all_star_no_match_length() -> None:
    assert match_pattern("*/*", "a/b/c") is None


def test_match_star_then_chevron() -> None:
    """'*/>'' matches name with at least two segments."""
    result = match_pattern("*/>", "a/b")
    assert result == [("a", 0), ("b", 1)]


def test_match_star_then_chevron_many() -> None:
    result = match_pattern("*/>", "a/b/c/d")
    assert result == [("a", 0), ("b/c/d", 1)]


def test_match_star_then_chevron_too_short() -> None:
    assert match_pattern("*/>", "a") == [("a", 0), ("", 1)]


def test_match_second_chevron_is_literal() -> None:
    assert match_pattern("a/>/>/c", "a/>/>/c") == []
    assert match_pattern("a/>/>/c", "a/>/d/c") is None


# =====================================================================================================================
# resolve_name -- remapping
# =====================================================================================================================


def test_remap_relative() -> None:
    """Docstring row 1: foo/bar  foo/bar  zoo  ns  me  ns/zoo  -  relative remap."""
    resolved, pin, verbatim = resolve_name("foo/bar", "me", "ns", {"foo/bar": "zoo"})
    assert resolved == "ns/zoo"
    assert pin is None
    assert verbatim is True


def test_remap_pinned_target() -> None:
    """Docstring row 2: foo/bar  foo/bar  zoo#123  ns  me  ns/zoo  123  pinned relative remap."""
    resolved, pin, _ = resolve_name("foo/bar", "me", "ns", {"foo/bar": "zoo#123"})
    assert resolved == "ns/zoo"
    assert pin == 123


def test_remap_user_pin_discarded() -> None:
    """Docstring row 3: foo/bar#456  foo/bar  zoo  ns  me  ns/zoo  -  matched rule discards user pin."""
    resolved, pin, _ = resolve_name("foo/bar#456", "me", "ns", {"foo/bar": "zoo"})
    assert resolved == "ns/zoo"
    assert pin is None


def test_remap_absolute_target() -> None:
    """Docstring row 4: foo/bar  foo/bar  /zoo  ns  me  zoo  -  absolute remap (ns ignored)."""
    resolved, pin, _ = resolve_name("foo/bar", "me", "ns", {"foo/bar": "/zoo"})
    assert resolved == "zoo"
    assert pin is None


def test_remap_homeful_target() -> None:
    """Docstring row 5: foo/bar  foo/bar  ~/zoo  ns  me  me/zoo  -  homeful remap (home expanded)."""
    resolved, pin, _ = resolve_name("foo/bar", "me", "ns", {"foo/bar": "~/zoo"})
    assert resolved == "me/zoo"
    assert pin is None


def test_remap_no_match() -> None:
    """Unmatched names pass through unchanged."""
    resolved, pin, _ = resolve_name("other", "me", "ns", {"foo/bar": "zoo"})
    assert resolved == "ns/other"
    assert pin is None


def test_remap_normalized_lookup() -> None:
    """Lookup key is normalized, so extra slashes in the user's input still match."""
    resolved, _, _ = resolve_name("/foo//bar", "me", "ns", {"foo/bar": "zoo"})
    assert resolved == "ns/zoo"
