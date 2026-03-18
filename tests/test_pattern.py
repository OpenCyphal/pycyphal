"""Comprehensive tests for pycyphal._common.name_match pattern matching."""

from __future__ import annotations

import pytest

from pycyphal._common import name_match


# =====================================================================================================================
# Exact (verbatim) match -- no wildcards
# =====================================================================================================================


class TestExactMatch:
    """When a pattern contains no wildcards, it should match the name exactly and return []."""

    def test_single_segment_exact(self) -> None:
        assert name_match("foo", "foo") == []

    def test_two_segment_exact(self) -> None:
        assert name_match("foo/bar", "foo/bar") == []

    def test_three_segment_exact(self) -> None:
        assert name_match("a/b/c", "a/b/c") == []

    def test_deeply_nested_exact(self) -> None:
        assert name_match("a/b/c/d/e/f/g", "a/b/c/d/e/f/g") == []

    def test_exact_numeric_segments(self) -> None:
        assert name_match("123/456/789", "123/456/789") == []

    def test_exact_special_chars(self) -> None:
        assert name_match("foo.bar/baz_qux", "foo.bar/baz_qux") == []

    def test_exact_mixed_case(self) -> None:
        assert name_match("Foo/Bar", "Foo/Bar") == []

    def test_exact_single_char_segments(self) -> None:
        assert name_match("a/b/c", "a/b/c") == []


# =====================================================================================================================
# No match -- verbatim patterns that differ from the name
# =====================================================================================================================


class TestNoMatchVerbatim:
    """Verbatim patterns that do not match the name should return None."""

    def test_different_single_segment(self) -> None:
        assert name_match("foo", "bar") is None

    def test_first_segment_differs(self) -> None:
        assert name_match("foo/bar", "baz/bar") is None

    def test_last_segment_differs(self) -> None:
        assert name_match("foo/bar", "foo/baz") is None

    def test_middle_segment_differs(self) -> None:
        assert name_match("a/b/c", "a/x/c") is None

    def test_pattern_longer_than_name(self) -> None:
        assert name_match("a/b/c", "a/b") is None

    def test_name_longer_than_pattern(self) -> None:
        assert name_match("a/b", "a/b/c") is None

    def test_case_sensitivity(self) -> None:
        assert name_match("foo", "Foo") is None

    def test_case_sensitivity_multi(self) -> None:
        assert name_match("foo/bar", "foo/Bar") is None

    def test_empty_vs_nonempty(self) -> None:
        # empty pattern "" splits to [""], name "foo" splits to ["foo"]
        assert name_match("", "foo") is None

    def test_nonempty_vs_empty(self) -> None:
        assert name_match("foo", "") is None

    def test_prefix_mismatch(self) -> None:
        assert name_match("prefix/a/b", "other/a/b") is None

    def test_suffix_mismatch(self) -> None:
        assert name_match("a/b/suffix", "a/b/other") is None

    def test_completely_different(self) -> None:
        assert name_match("x/y/z", "a/b/c") is None

    def test_pattern_subset(self) -> None:
        assert name_match("a", "a/b/c") is None

    def test_name_subset(self) -> None:
        assert name_match("a/b/c", "a") is None


# =====================================================================================================================
# Single '*' wildcard
# =====================================================================================================================


class TestSingleStar:
    """'*' matches exactly one segment."""

    def test_star_only(self) -> None:
        result = name_match("*", "hello")
        assert result == [("hello", 0)]

    def test_star_at_start(self) -> None:
        result = name_match("*/bar", "foo/bar")
        assert result == [("foo", 0)]

    def test_star_at_end(self) -> None:
        result = name_match("foo/*", "foo/bar")
        assert result == [("bar", 1)]

    def test_star_in_middle(self) -> None:
        result = name_match("foo/*/baz", "foo/bar/baz")
        assert result == [("bar", 1)]

    def test_star_captures_segment_text(self) -> None:
        result = name_match("a/*/c", "a/hello_world/c")
        assert result is not None
        assert len(result) == 1
        assert result[0] == ("hello_world", 1)

    def test_star_does_not_match_zero_segments(self) -> None:
        # Pattern "a/*/c" requires exactly 3 segments
        assert name_match("a/*/c", "a/c") is None

    def test_star_does_not_match_multiple_segments(self) -> None:
        # '*' matches exactly one segment, not two
        assert name_match("a/*/c", "a/x/y/c") is None

    def test_star_pattern_too_short(self) -> None:
        assert name_match("*", "a/b") is None

    def test_star_pattern_too_long(self) -> None:
        assert name_match("*/*/c", "a/b") is None

    def test_star_with_preceding_literal(self) -> None:
        result = name_match("ns/topic/*", "ns/topic/field")
        assert result == [("field", 2)]

    def test_star_with_following_literal(self) -> None:
        result = name_match("*/topic/field", "ns/topic/field")
        assert result == [("ns", 0)]

    def test_star_surrounded_by_literals(self) -> None:
        result = name_match("a/*/c/d/e", "a/X/c/d/e")
        assert result == [("X", 1)]


# =====================================================================================================================
# Multiple '*' wildcards
# =====================================================================================================================


class TestMultipleStars:
    """Multiple '*' segments, each matching exactly one segment."""

    def test_two_stars_adjacent(self) -> None:
        result = name_match("*/*", "foo/bar")
        assert result == [("foo", 0), ("bar", 1)]

    def test_two_stars_separated(self) -> None:
        result = name_match("*/mid/*", "hello/mid/world")
        assert result == [("hello", 0), ("world", 2)]

    def test_three_stars(self) -> None:
        result = name_match("*/*/*", "a/b/c")
        assert result == [("a", 0), ("b", 1), ("c", 2)]

    def test_all_stars_pattern(self) -> None:
        result = name_match("*/*/*/*", "w/x/y/z")
        assert result == [("w", 0), ("x", 1), ("y", 2), ("z", 3)]

    def test_stars_with_literals_between(self) -> None:
        result = name_match("*/lit1/*/lit2/*", "a/lit1/b/lit2/c")
        assert result == [("a", 0), ("b", 2), ("c", 4)]

    def test_two_stars_wrong_literal(self) -> None:
        assert name_match("*/mid/*", "hello/wrong/world") is None

    def test_two_stars_name_too_short(self) -> None:
        assert name_match("*/*", "only") is None

    def test_two_stars_name_too_long(self) -> None:
        assert name_match("*/*", "a/b/c") is None

    def test_stars_first_and_last(self) -> None:
        result = name_match("*/b/c/*", "a/b/c/d")
        assert result == [("a", 0), ("d", 3)]

    def test_stars_correct_indices(self) -> None:
        result = name_match("lit/*/lit2/*", "lit/x/lit2/y")
        assert result is not None
        assert result[0] == ("x", 1)
        assert result[1] == ("y", 3)

    def test_five_stars_all_match(self) -> None:
        result = name_match("*/*/*/*/*/*", "1/2/3/4/5/6")
        assert result is not None
        assert len(result) == 6
        for i in range(6):
            assert result[i] == (str(i + 1), i)


# =====================================================================================================================
# '>' wildcard -- matches one or more remaining segments
# =====================================================================================================================


class TestGreaterThan:
    """'>' matches one or more remaining name segments and must be the last pattern segment."""

    def test_gt_only_one_segment(self) -> None:
        result = name_match(">", "hello")
        assert result == [("hello", 0)]

    def test_gt_only_two_segments(self) -> None:
        result = name_match(">", "hello/world")
        assert result == [("hello", 0), ("world", 0)]

    def test_gt_only_three_segments(self) -> None:
        result = name_match(">", "a/b/c")
        assert result == [("a", 0), ("b", 0), ("c", 0)]

    def test_gt_after_literal_one_remaining(self) -> None:
        result = name_match("foo/>", "foo/bar")
        assert result == [("bar", 1)]

    def test_gt_after_literal_two_remaining(self) -> None:
        result = name_match("foo/>", "foo/bar/baz")
        assert result == [("bar", 1), ("baz", 1)]

    def test_gt_after_literal_three_remaining(self) -> None:
        result = name_match("foo/>", "foo/a/b/c")
        assert result == [("a", 1), ("b", 1), ("c", 1)]

    def test_gt_after_two_literals(self) -> None:
        result = name_match("a/b/>", "a/b/c")
        assert result == [("c", 2)]

    def test_gt_after_two_literals_multi_remaining(self) -> None:
        result = name_match("a/b/>", "a/b/c/d/e")
        assert result == [("c", 2), ("d", 2), ("e", 2)]

    def test_gt_requires_at_least_one_segment(self) -> None:
        # "foo/>" should not match "foo" alone because > requires at least one segment
        assert name_match("foo/>", "foo") is None

    def test_gt_alone_no_match_empty(self) -> None:
        # ">" on empty name: name splits to [""], which is one segment
        result = name_match(">", "")
        assert result == [("", 0)]

    def test_gt_prefix_mismatch(self) -> None:
        assert name_match("foo/>", "bar/baz") is None

    def test_gt_many_remaining(self) -> None:
        result = name_match("root/>", "root/a/b/c/d/e/f/g/h")
        assert result is not None
        assert len(result) == 8
        expected_segments = ["a", "b", "c", "d", "e", "f", "g", "h"]
        for i, seg in enumerate(expected_segments):
            assert result[i] == (seg, 1)

    def test_gt_all_substitutions_have_same_index(self) -> None:
        result = name_match("x/>", "x/1/2/3")
        assert result is not None
        for text, idx in result:
            assert idx == 1

    def test_gt_after_deep_prefix(self) -> None:
        result = name_match("a/b/c/d/>", "a/b/c/d/e/f")
        assert result == [("e", 4), ("f", 4)]


# =====================================================================================================================
# '>' not at end (invalid position)
# =====================================================================================================================


class TestGreaterThanNotAtEnd:
    """'>' must be the last pattern segment. If it isn't, match should return None."""

    def test_gt_in_middle(self) -> None:
        assert name_match(">/foo", "bar/foo") is None

    def test_gt_at_start_with_more(self) -> None:
        assert name_match(">/a/b", "x/a/b") is None

    def test_gt_first_of_two(self) -> None:
        assert name_match(">/*", "a/b") is None

    def test_gt_between_literals(self) -> None:
        assert name_match("a/>/b", "a/x/b") is None

    def test_gt_followed_by_star(self) -> None:
        assert name_match("foo/>/*", "foo/bar/baz") is None

    def test_two_gt_segments(self) -> None:
        assert name_match(">/>/end", "a/b/end") is None

    def test_gt_before_literal_deep(self) -> None:
        assert name_match("a/b/>/c/d", "a/b/x/c/d") is None


# =====================================================================================================================
# Mixed '*' and '>'
# =====================================================================================================================


class TestMixedStarAndGt:
    """Patterns with both '*' and '>' wildcards."""

    def test_star_then_gt(self) -> None:
        result = name_match("*/bar/>", "foo/bar/baz")
        assert result == [("foo", 0), ("baz", 2)]

    def test_star_then_gt_multi_tail(self) -> None:
        result = name_match("*/bar/>", "foo/bar/a/b/c")
        assert result == [("foo", 0), ("a", 2), ("b", 2), ("c", 2)]

    def test_two_stars_then_gt(self) -> None:
        result = name_match("*/*/>", "a/b/c")
        assert result == [("a", 0), ("b", 1), ("c", 2)]

    def test_two_stars_then_gt_multi_tail(self) -> None:
        result = name_match("*/*/>", "a/b/c/d/e")
        assert result == [("a", 0), ("b", 1), ("c", 2), ("d", 2), ("e", 2)]

    def test_literal_star_literal_gt(self) -> None:
        result = name_match("ns/*/topic/>", "ns/X/topic/a/b")
        assert result == [("X", 1), ("a", 3), ("b", 3)]

    def test_star_gt_indices(self) -> None:
        result = name_match("lit/*/lit2/>", "lit/x/lit2/y/z")
        assert result is not None
        assert result[0] == ("x", 1)  # star at index 1
        assert result[1] == ("y", 3)  # gt at index 3
        assert result[2] == ("z", 3)  # gt at index 3

    def test_mixed_no_match_literal_fail(self) -> None:
        assert name_match("*/mid/>", "a/wrong/b") is None

    def test_mixed_gt_needs_at_least_one(self) -> None:
        # Pattern "*/>" needs at least 2 segments (1 for *, 1+ for >)
        assert name_match("*/>", "a") is None

    def test_star_then_gt_two_segments(self) -> None:
        result = name_match("*/>", "a/b")
        assert result == [("a", 0), ("b", 1)]

    def test_star_then_gt_three_segments(self) -> None:
        result = name_match("*/>", "a/b/c")
        assert result == [("a", 0), ("b", 1), ("c", 1)]

    def test_three_stars_then_gt(self) -> None:
        result = name_match("*/*/*/>", "w/x/y/z")
        assert result == [("w", 0), ("x", 1), ("y", 2), ("z", 3)]

    def test_three_stars_then_gt_long_tail(self) -> None:
        result = name_match("*/*/*/>", "w/x/y/z1/z2/z3")
        assert result == [("w", 0), ("x", 1), ("y", 2), ("z1", 3), ("z2", 3), ("z3", 3)]


# =====================================================================================================================
# Empty pattern and name edge cases
# =====================================================================================================================


class TestEmptyEdgeCases:
    """Edge cases involving empty strings."""

    def test_both_empty(self) -> None:
        # "" splits to [""], so pattern [""] matches name [""] exactly
        assert name_match("", "") == []

    def test_empty_pattern_nonempty_name(self) -> None:
        # "" splits to [""], "foo" splits to ["foo"]; "" != "foo"
        assert name_match("", "foo") is None

    def test_nonempty_pattern_empty_name(self) -> None:
        # "foo" splits to ["foo"], "" splits to [""]; "foo" != ""
        assert name_match("foo", "") is None

    def test_star_matches_empty_string_segment(self) -> None:
        # "*" splits to ["*"], "" splits to [""]; * matches "" as a segment
        result = name_match("*", "")
        assert result == [("", 0)]

    def test_gt_matches_empty_string_segment(self) -> None:
        result = name_match(">", "")
        assert result == [("", 0)]

    def test_separator_only_pattern(self) -> None:
        # "/" splits to ["", ""]
        # matching against "/" => ["", ""] -- exact match
        assert name_match("/", "/") == []

    def test_separator_pattern_vs_nonempty(self) -> None:
        # "/" splits to ["", ""], "foo" splits to ["foo"]
        assert name_match("/", "foo") is None

    def test_double_separator_pattern(self) -> None:
        # "//" splits to ["", "", ""], matching requires same
        assert name_match("//", "//") == []

    def test_double_separator_mismatch(self) -> None:
        assert name_match("//", "/") is None


# =====================================================================================================================
# Deep nesting (many segments)
# =====================================================================================================================


class TestDeepNesting:
    """Patterns and names with many segments."""

    def test_exact_10_segments(self) -> None:
        path = "/".join(f"s{i}" for i in range(10))
        assert name_match(path, path) == []

    def test_exact_20_segments(self) -> None:
        path = "/".join(f"seg{i}" for i in range(20))
        assert name_match(path, path) == []

    def test_star_in_deep_path(self) -> None:
        segments = [f"s{i}" for i in range(10)]
        # Replace middle segment with *
        pat_segments = list(segments)
        pat_segments[5] = "*"
        pattern = "/".join(pat_segments)
        name = "/".join(segments)
        result = name_match(pattern, name)
        assert result == [("s5", 5)]

    def test_multiple_stars_in_deep_path(self) -> None:
        segments = [f"s{i}" for i in range(10)]
        pat_segments = list(segments)
        pat_segments[2] = "*"
        pat_segments[7] = "*"
        pattern = "/".join(pat_segments)
        name = "/".join(segments)
        result = name_match(pattern, name)
        assert result == [("s2", 2), ("s7", 7)]

    def test_gt_after_deep_prefix(self) -> None:
        prefix = [f"p{i}" for i in range(8)]
        tail = ["tail1", "tail2", "tail3"]
        pattern = "/".join(prefix) + "/>"
        name = "/".join(prefix + tail)
        result = name_match(pattern, name)
        assert result is not None
        assert len(result) == 3
        for i, seg in enumerate(tail):
            assert result[i] == (seg, 8)

    def test_all_stars_deep(self) -> None:
        n = 15
        pattern = "/".join(["*"] * n)
        segments = [f"v{i}" for i in range(n)]
        name = "/".join(segments)
        result = name_match(pattern, name)
        assert result is not None
        assert len(result) == n
        for i in range(n):
            assert result[i] == (f"v{i}", i)

    def test_gt_matches_many_trailing(self) -> None:
        n = 50
        pattern = "root/>"
        tail = [f"x{i}" for i in range(n)]
        name = "root/" + "/".join(tail)
        result = name_match(pattern, name)
        assert result is not None
        assert len(result) == n
        for i in range(n):
            assert result[i] == (f"x{i}", 1)

    def test_deep_mismatch_last_segment(self) -> None:
        segments = [f"s{i}" for i in range(20)]
        pattern = "/".join(segments)
        name_segments = list(segments)
        name_segments[-1] = "different"
        name = "/".join(name_segments)
        assert name_match(pattern, name) is None

    def test_deep_mismatch_first_segment(self) -> None:
        segments = [f"s{i}" for i in range(20)]
        pattern = "/".join(segments)
        name_segments = list(segments)
        name_segments[0] = "different"
        name = "/".join(name_segments)
        assert name_match(pattern, name) is None

    def test_star_in_every_other_position(self) -> None:
        n = 12
        pat_parts: list[str] = []
        name_parts: list[str] = []
        for i in range(n):
            if i % 2 == 0:
                pat_parts.append(f"lit{i}")
                name_parts.append(f"lit{i}")
            else:
                pat_parts.append("*")
                name_parts.append(f"val{i}")
        pattern = "/".join(pat_parts)
        name = "/".join(name_parts)
        result = name_match(pattern, name)
        assert result is not None
        expected = [(f"val{i}", i) for i in range(n) if i % 2 == 1]
        assert result == expected


# =====================================================================================================================
# Single-segment patterns and names
# =====================================================================================================================


class TestSingleSegment:
    """Patterns and names with only one segment (no separators)."""

    def test_single_literal_match(self) -> None:
        assert name_match("abc", "abc") == []

    def test_single_literal_no_match(self) -> None:
        assert name_match("abc", "xyz") is None

    def test_single_star(self) -> None:
        result = name_match("*", "anything")
        assert result == [("anything", 0)]

    def test_single_gt(self) -> None:
        result = name_match(">", "anything")
        assert result == [("anything", 0)]

    def test_single_star_vs_multi_segment_name(self) -> None:
        # '*' only matches one segment
        assert name_match("*", "a/b") is None

    def test_single_gt_vs_multi_segment_name(self) -> None:
        # '>' matches one or more, so it matches "a/b" split into 2 segments
        result = name_match(">", "a/b")
        assert result == [("a", 0), ("b", 0)]

    def test_single_literal_vs_multi_segment_name(self) -> None:
        assert name_match("foo", "foo/bar") is None


# =====================================================================================================================
# Pattern-segment index correctness
# =====================================================================================================================


class TestSubstitutionIndices:
    """Verify that each substitution tuple records the correct pattern segment index."""

    def test_star_index_0(self) -> None:
        result = name_match("*", "x")
        assert result is not None
        assert result[0][1] == 0

    def test_star_index_2(self) -> None:
        result = name_match("a/b/*/d", "a/b/c/d")
        assert result is not None
        assert result[0] == ("c", 2)

    def test_gt_index_after_literals(self) -> None:
        result = name_match("a/b/c/>", "a/b/c/d/e")
        assert result is not None
        assert result[0] == ("d", 3)
        assert result[1] == ("e", 3)

    def test_mixed_indices(self) -> None:
        result = name_match("*/b/*/d/>", "a/b/c/d/e/f/g")
        assert result is not None
        assert result[0] == ("a", 0)   # first *
        assert result[1] == ("c", 2)   # second *
        assert result[2] == ("e", 4)   # > start
        assert result[3] == ("f", 4)   # > continues
        assert result[4] == ("g", 4)   # > continues

    def test_adjacent_stars_indices(self) -> None:
        result = name_match("*/*/*", "x/y/z")
        assert result is not None
        assert result[0][1] == 0
        assert result[1][1] == 1
        assert result[2][1] == 2

    def test_star_at_various_positions(self) -> None:
        result = name_match("a/*/c/*/e/*/g", "a/B/c/D/e/F/g")
        assert result is not None
        assert result == [("B", 1), ("D", 3), ("F", 5)]


# =====================================================================================================================
# Return type checks
# =====================================================================================================================


class TestReturnTypes:
    """Verify the types and structure of return values."""

    def test_no_match_returns_none(self) -> None:
        result = name_match("foo", "bar")
        assert result is None

    def test_exact_match_returns_empty_list(self) -> None:
        result = name_match("foo", "foo")
        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 0

    def test_star_match_returns_list_of_tuples(self) -> None:
        result = name_match("*", "x")
        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], tuple)
        assert len(result[0]) == 2
        assert isinstance(result[0][0], str)
        assert isinstance(result[0][1], int)

    def test_gt_match_returns_list_of_tuples(self) -> None:
        result = name_match(">", "a/b")
        assert result is not None
        assert isinstance(result, list)
        for item in result:
            assert isinstance(item, tuple)
            assert len(item) == 2
            assert isinstance(item[0], str)
            assert isinstance(item[1], int)


# =====================================================================================================================
# Boundary conditions and corner cases
# =====================================================================================================================


class TestBoundaryConditions:
    """Edge cases and tricky scenarios."""

    def test_star_literal_equals_star_char(self) -> None:
        # Name segment is literally "*"; the pattern * should match it
        result = name_match("*", "*")
        assert result == [("*", 0)]

    def test_gt_literal_equals_gt_char(self) -> None:
        # Name segment is literally ">"
        result = name_match(">", ">")
        assert result == [(">", 0)]

    def test_star_matching_gt_literal(self) -> None:
        # Pattern * at index 1, name has literal ">"
        result = name_match("a/*/c", "a/>/c")
        assert result == [(">", 1)]

    def test_gt_matching_star_literal(self) -> None:
        result = name_match("a/>", "a/*")
        assert result == [("*", 1)]

    def test_name_with_many_separators(self) -> None:
        # "a//b" splits to ["a", "", "b"]
        result = name_match("a/*/b", "a//b")
        assert result == [("", 1)]

    def test_trailing_separator_in_name(self) -> None:
        # "foo/" splits to ["foo", ""]
        result = name_match("foo/*", "foo/")
        assert result == [("", 1)]

    def test_leading_separator_in_name(self) -> None:
        # "/foo" splits to ["", "foo"]
        result = name_match("/foo", "/foo")
        assert result == []

    def test_leading_separator_star(self) -> None:
        result = name_match("/*/bar", "/hello/bar")
        assert result == [("hello", 1)]

    def test_pattern_with_tilde(self) -> None:
        # Tilde is just a regular character for matching purposes
        assert name_match("~", "~") == []

    def test_pattern_with_tilde_prefix(self) -> None:
        result = name_match("~/*/b", "~/x/b")
        assert result == [("x", 1)]

    def test_dots_in_segments(self) -> None:
        result = name_match("a.b/*/c.d", "a.b/x.y/c.d")
        assert result == [("x.y", 1)]

    def test_hyphens_in_segments(self) -> None:
        result = name_match("my-ns/*", "my-ns/my-topic")
        assert result == [("my-topic", 1)]

    def test_underscores_in_segments(self) -> None:
        result = name_match("my_ns/*", "my_ns/my_topic")
        assert result == [("my_topic", 1)]

    def test_numeric_segment_match(self) -> None:
        result = name_match("port/*/status", "port/42/status")
        assert result == [("42", 1)]


# =====================================================================================================================
# Realistic NATS/Cyphal-like topic patterns
# =====================================================================================================================


class TestRealisticPatterns:
    """Patterns that resemble real-world usage in pub/sub systems."""

    def test_namespace_wildcard(self) -> None:
        result = name_match("uavcan/node/*/health", "uavcan/node/42/health")
        assert result == [("42", 2)]

    def test_namespace_gt_tail(self) -> None:
        result = name_match("uavcan/node/>", "uavcan/node/info/status")
        assert result == [("info", 2), ("status", 2)]

    def test_topic_tree_match(self) -> None:
        result = name_match("sensors/>", "sensors/imu/accel/x")
        assert result == [("imu", 1), ("accel", 1), ("x", 1)]

    def test_node_specific(self) -> None:
        result = name_match("*/heartbeat", "node42/heartbeat")
        assert result == [("node42", 0)]

    def test_service_response_pattern(self) -> None:
        result = name_match("_srv/*/response", "_srv/register/response")
        assert result == [("register", 1)]

    def test_full_wildcard_subscribe(self) -> None:
        result = name_match(">", "uavcan/node/42/health")
        assert result is not None
        assert len(result) == 4
        assert result[0] == ("uavcan", 0)
        assert result[1] == ("node", 0)
        assert result[2] == ("42", 0)
        assert result[3] == ("health", 0)

    def test_partial_wildcard_tree(self) -> None:
        result = name_match("vehicle/*/sensors/>", "vehicle/car1/sensors/lidar/front")
        assert result is not None
        assert result[0] == ("car1", 1)
        assert result[1] == ("lidar", 3)
        assert result[2] == ("front", 3)

    def test_exact_topic(self) -> None:
        assert name_match("uavcan/node/42/health", "uavcan/node/42/health") == []

    def test_wrong_namespace(self) -> None:
        assert name_match("uavcan/node/>", "other/node/info") is None

    def test_config_path_wildcard(self) -> None:
        result = name_match("config/*/enabled", "config/feature_x/enabled")
        assert result == [("feature_x", 1)]


# =====================================================================================================================
# Length and segment count mismatches
# =====================================================================================================================


class TestSegmentCountMismatches:
    """Ensure correct behavior when pattern/name have different segment counts."""

    def test_pattern_1_name_2(self) -> None:
        assert name_match("a", "a/b") is None

    def test_pattern_2_name_1(self) -> None:
        assert name_match("a/b", "a") is None

    def test_pattern_3_name_2(self) -> None:
        assert name_match("a/b/c", "a/b") is None

    def test_pattern_2_name_3(self) -> None:
        assert name_match("a/b", "a/b/c") is None

    def test_star_pattern_longer(self) -> None:
        # Pattern has 4 segments, name has 3
        assert name_match("a/*/c/d", "a/x/c") is None

    def test_star_pattern_shorter(self) -> None:
        # Pattern has 2 segments, name has 3
        assert name_match("a/*", "a/b/c") is None

    def test_gt_exactly_fills(self) -> None:
        # Pattern "a/>" (2 segments), name "a/b" (2 segments) => > matches 1 segment
        result = name_match("a/>", "a/b")
        assert result == [("b", 1)]

    def test_gt_overfills(self) -> None:
        # Pattern "a/>" (2 segments), name "a/b/c/d" (4 segments) => > matches 3
        result = name_match("a/>", "a/b/c/d")
        assert result == [("b", 1), ("c", 1), ("d", 1)]

    def test_gt_underfills(self) -> None:
        # Pattern "a/b/>" needs at least 3 segments; name has only 2
        assert name_match("a/b/>", "a/b") is None


# =====================================================================================================================
# Stress / parametrized tests
# =====================================================================================================================


class TestParametrized:
    """Parametrized tests covering many combinations systematically."""

    @pytest.mark.parametrize(
        "pattern,name,expected",
        [
            ("a", "a", []),
            ("a/b", "a/b", []),
            ("*", "x", [("x", 0)]),
            (">", "x", [("x", 0)]),
            (">", "x/y", [("x", 0), ("y", 0)]),
            ("a/*", "a/b", [("b", 1)]),
            ("*/b", "a/b", [("a", 0)]),
            ("a/>", "a/b", [("b", 1)]),
            ("a/>", "a/b/c", [("b", 1), ("c", 1)]),
            ("*/*", "a/b", [("a", 0), ("b", 1)]),
            ("*/b/*", "a/b/c", [("a", 0), ("c", 2)]),
        ],
    )
    def test_match_cases(
        self,
        pattern: str,
        name: str,
        expected: list[tuple[str, int]],
    ) -> None:
        assert name_match(pattern, name) == expected

    @pytest.mark.parametrize(
        "pattern,name",
        [
            ("a", "b"),
            ("a/b", "a/c"),
            ("a/b", "a"),
            ("a", "a/b"),
            ("a/b/c", "a/b/d"),
            ("*/b", "a/c"),
            ("a/*/c", "a/b/d"),
            ("a/b/>", "a/b"),
            (">/a", "x/a"),
            ("a/>/b", "a/x/b"),
        ],
    )
    def test_no_match_cases(self, pattern: str, name: str) -> None:
        assert name_match(pattern, name) is None


# =====================================================================================================================
# Interaction between separator handling and wildcards
# =====================================================================================================================


class TestSeparatorInteraction:
    """Tests involving edge cases with separators and wildcards together."""

    def test_leading_slash_pattern_and_name(self) -> None:
        # "/a/b" splits to ["", "a", "b"]
        assert name_match("/a/b", "/a/b") == []

    def test_leading_slash_with_star(self) -> None:
        result = name_match("/*/b", "/a/b")
        assert result == [("a", 1)]

    def test_trailing_slash_pattern_and_name(self) -> None:
        # "a/b/" splits to ["a", "b", ""]
        assert name_match("a/b/", "a/b/") == []

    def test_trailing_slash_with_gt(self) -> None:
        # "a/>" on "a/b/" => name splits to ["a", "b", ""]
        result = name_match("a/>", "a/b/")
        assert result == [("b", 1), ("", 1)]

    def test_only_separators(self) -> None:
        assert name_match("///", "///") == []

    def test_star_matches_empty_between_separators(self) -> None:
        # "a/*/b" on "a//b" => name splits to ["a", "", "b"]
        result = name_match("a/*/b", "a//b")
        assert result == [("", 1)]


# =====================================================================================================================
# Substitution list ordering
# =====================================================================================================================


class TestSubstitutionOrdering:
    """Verify substitutions appear in left-to-right order."""

    def test_left_to_right_stars(self) -> None:
        result = name_match("*/*/*/d", "a/b/c/d")
        assert result is not None
        texts = [t for t, _ in result]
        assert texts == ["a", "b", "c"]

    def test_left_to_right_star_then_gt(self) -> None:
        result = name_match("*/b/>", "a/b/c/d/e")
        assert result is not None
        texts = [t for t, _ in result]
        assert texts == ["a", "c", "d", "e"]

    def test_ordering_with_literals_interspersed(self) -> None:
        result = name_match("L1/*/L2/*/L3/>", "L1/A/L2/B/L3/C/D")
        assert result is not None
        texts = [t for t, _ in result]
        assert texts == ["A", "B", "C", "D"]

    def test_indices_increase_for_stars(self) -> None:
        result = name_match("*/*/*/*", "a/b/c/d")
        assert result is not None
        indices = [idx for _, idx in result]
        assert indices == [0, 1, 2, 3]

    def test_gt_indices_all_equal(self) -> None:
        result = name_match("a/>", "a/1/2/3/4/5")
        assert result is not None
        indices = [idx for _, idx in result]
        assert all(i == 1 for i in indices)


# =====================================================================================================================
# Idempotency and symmetry
# =====================================================================================================================


class TestIdempotencyAndSymmetry:
    """Verify that matching is deterministic and non-symmetric where appropriate."""

    def test_same_call_twice(self) -> None:
        r1 = name_match("a/*/c", "a/b/c")
        r2 = name_match("a/*/c", "a/b/c")
        assert r1 == r2

    def test_same_none_twice(self) -> None:
        r1 = name_match("a/b", "x/y")
        r2 = name_match("a/b", "x/y")
        assert r1 is None
        assert r2 is None

    def test_pattern_name_not_interchangeable(self) -> None:
        # "a/*" matches "a/b", but "a/b" does not match "a/*"
        assert name_match("a/*", "a/b") == [("b", 1)]
        assert name_match("a/b", "a/*") is None

    def test_gt_pattern_not_interchangeable(self) -> None:
        assert name_match("a/>", "a/b/c") == [("b", 1), ("c", 1)]
        assert name_match("a/b/c", "a/>") is None


# =====================================================================================================================
# Comprehensive end-to-end scenarios
# =====================================================================================================================


class TestEndToEnd:
    """Larger integration-style tests combining multiple aspects."""

    def test_subscribe_all(self) -> None:
        """Subscribe with '>' should match any non-empty name."""
        names = ["a", "a/b", "a/b/c", "x/y/z/w/v"]
        for n in names:
            result = name_match(">", n)
            assert result is not None, f"'>' should match {n!r}"
            # Number of substitutions equals number of segments
            assert len(result) == len(n.split("/"))

    def test_subscribe_namespace(self) -> None:
        """Subscribe to a namespace should match deeper topics but not unrelated ones."""
        pattern = "sensors/>"
        assert name_match(pattern, "sensors/temp") is not None
        assert name_match(pattern, "sensors/temp/cpu") is not None
        assert name_match(pattern, "sensors/temp/cpu/core0") is not None
        assert name_match(pattern, "actuators/motor") is None
        assert name_match(pattern, "sensor/temp") is None
        # "sensors" alone does not match because > requires at least one more segment
        assert name_match(pattern, "sensors") is None

    def test_subscribe_specific_slot(self) -> None:
        """Subscribe with a star in a specific position."""
        pattern = "vehicle/*/speed"
        assert name_match(pattern, "vehicle/car1/speed") == [("car1", 1)]
        assert name_match(pattern, "vehicle/truck2/speed") == [("truck2", 1)]
        assert name_match(pattern, "vehicle/car1/accel") is None
        assert name_match(pattern, "vehicle/speed") is None
        assert name_match(pattern, "vehicle/car1/extra/speed") is None

    def test_multi_level_wildcards(self) -> None:
        """Complex pattern with multiple wildcard types."""
        pattern = "*/node/*/>"
        # Matches: <any>/node/<any>/<one_or_more_tail>
        r = name_match(pattern, "uavcan/node/42/health/status")
        assert r is not None
        assert r[0] == ("uavcan", 0)
        assert r[1] == ("42", 2)
        assert r[2] == ("health", 3)
        assert r[3] == ("status", 3)

        # Too few segments -- > has nothing to match
        assert name_match(pattern, "uavcan/node/42") is None

        # Wrong fixed literal
        assert name_match(pattern, "uavcan/notnode/42/health") is None

    def test_exact_vs_wildcard_preference(self) -> None:
        """Show that exact and wildcard patterns produce different result types."""
        name = "a/b/c"
        exact_result = name_match("a/b/c", name)
        wild_result = name_match("a/*/c", name)
        gt_result = name_match("a/>", name)

        assert exact_result == []
        assert wild_result == [("b", 1)]
        assert gt_result == [("b", 1), ("c", 1)]

    def test_long_chain(self) -> None:
        """Very long pattern and name with alternating literals and stars."""
        n = 30
        pat_parts: list[str] = []
        name_parts: list[str] = []
        expected: list[tuple[str, int]] = []
        for i in range(n):
            if i % 3 == 1:
                pat_parts.append("*")
                name_parts.append(f"w{i}")
                expected.append((f"w{i}", i))
            else:
                pat_parts.append(f"s{i}")
                name_parts.append(f"s{i}")
        pattern = "/".join(pat_parts)
        name = "/".join(name_parts)
        result = name_match(pattern, name)
        assert result == expected

    def test_long_chain_with_gt_tail(self) -> None:
        """Deep pattern ending with '>' to capture arbitrary tail."""
        prefix_len = 5
        tail_len = 10
        prefix = [f"p{i}" for i in range(prefix_len)]
        tail = [f"t{i}" for i in range(tail_len)]
        pattern = "/".join(prefix) + "/>"
        name = "/".join(prefix + tail)
        result = name_match(pattern, name)
        assert result is not None
        assert len(result) == tail_len
        for i, seg in enumerate(tail):
            assert result[i] == (seg, prefix_len)
