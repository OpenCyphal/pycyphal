from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from typing import Any


class Error(Exception):
    pass


class SendError(Error):
    pass


class DeliveryError(Error):
    pass


class LivenessError(Error):
    pass


class NackError(Error):
    pass


@dataclass(frozen=True)
class Instant:
    """Monotonic time elapsed from an unspecified origin instant. Durations use plain float seconds."""

    ns: int

    def __init__(self, *, ns: int) -> None:
        object.__setattr__(self, "ns", int(ns))

    @staticmethod
    def now() -> Instant:
        return Instant(ns=time.monotonic_ns())

    @property
    def s(self) -> float:
        return self.ns * 1e-9

    @property
    def ms(self) -> float:
        return self.ns * 1e-6

    @property
    def us(self) -> float:
        return self.ns * 1e-3

    def __add__(self, other: Any) -> Instant:
        if isinstance(other, (float, int)):
            return Instant(ns=self.ns + round(other * 1e9))
        return NotImplemented

    def __radd__(self, other: Any) -> Instant:
        return self.__add__(other)

    def __sub__(self, other: Any) -> Instant | float:
        if isinstance(other, Instant):
            return (self.ns - other.ns) * 1e-9
        if isinstance(other, (float, int)):
            return Instant(ns=self.ns - round(other * 1e9))
        return NotImplemented

    def __mul__(self, other: Any) -> Instant:
        if isinstance(other, (float, int)):
            return Instant(ns=round(self.ns * other))
        return NotImplemented

    def __rmul__(self, other: Any) -> Instant:
        return self.__mul__(other)

    def __truediv__(self, other: Any) -> Instant:
        if isinstance(other, (float, int)):
            return Instant(ns=round(self.ns / other))
        return NotImplemented


class Priority(IntEnum):
    EXCEPTIONAL = 0
    IMMEDIATE = 1
    FAST = 2
    HIGH = 3
    NOMINAL = 4
    LOW = 5
    SLOW = 6
    OPTIONAL = 7


class Closable(ABC):
    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


# =====================================================================================================================
# Name utilities
# =====================================================================================================================

NAME_SEP = "/"
NAME_HOME = "~"
NAME_ONE = "*"
NAME_ANY = ">"

TOPIC_NAME_MAX = 200


def _is_valid_char(c: str) -> bool:
    o = ord(c)
    return 33 <= o <= 126


def name_normalize(name: str) -> str:
    """Remove leading/trailing/duplicate separators, validate chars (printable ASCII 33-126)."""
    out: list[str] = []
    pending_sep = False
    for c in name:
        if not _is_valid_char(c):
            raise ValueError(f"Invalid character in name: {c!r}")
        if c == NAME_SEP:
            pending_sep = len(out) > 0
            continue
        if pending_sep:
            pending_sep = False
            out.append(NAME_SEP)
        out.append(c)
    return "".join(out)


def name_is_valid(name: str) -> bool:
    if not name or len(name) > TOPIC_NAME_MAX:
        return False
    return all(_is_valid_char(c) for c in name)


def name_is_verbatim(name: str) -> bool:
    return NAME_ONE not in name and NAME_ANY not in name


def name_is_homeful(name: str) -> bool:
    return len(name) >= 1 and name[0] == NAME_HOME and (len(name) == 1 or name[1] == NAME_SEP)


def name_is_absolute(name: str) -> bool:
    return len(name) >= 1 and name[0] == NAME_SEP


def name_join(left: str, right: str) -> str:
    left = name_normalize(left)
    right = name_normalize(right)
    if not left:
        return right
    if not right:
        return left
    return left + NAME_SEP + right


def name_expand_home(name: str, home: str) -> str:
    if not name_is_homeful(name):
        return name_normalize(name)
    rest = name[1:]  # strip '~'
    return name_join(home, rest)


def name_resolve(name: str, namespace: str, home: str) -> str:
    if name_is_absolute(name):
        return name_normalize(name)
    if name_is_homeful(name):
        return name_expand_home(name, home)
    if name_is_homeful(namespace):
        namespace = name_expand_home(namespace, home)
    return name_join(namespace, name)


def name_match(pattern: str, name: str) -> list[tuple[str, int]] | None:
    """
    Match a pattern against a name. Returns substitutions list on match, None on no match.
    Each substitution is (segment_text, pattern_segment_index).
    '*' matches exactly one segment; '>' matches one or more remaining segments.
    Empty list for verbatim (exact) match.
    """
    pat_parts = pattern.split(NAME_SEP)
    name_parts = name.split(NAME_SEP)
    subs: list[tuple[str, int]] = []
    pi = 0
    ni = 0
    while pi < len(pat_parts):
        seg = pat_parts[pi]
        if seg == NAME_ANY:
            # '>' must be the last pattern segment and matches one or more remaining segments
            if pi != len(pat_parts) - 1:
                return None  # '>' must be last
            if ni >= len(name_parts):
                return None  # '>' requires at least one segment
            while ni < len(name_parts):
                subs.append((name_parts[ni], pi))
                ni += 1
            pi += 1
        elif seg == NAME_ONE:
            if ni >= len(name_parts):
                return None
            subs.append((name_parts[ni], pi))
            ni += 1
            pi += 1
        else:
            if ni >= len(name_parts):
                return None
            if name_parts[ni] != seg:
                return None
            ni += 1
            pi += 1
    if ni != len(name_parts):
        return None
    return subs
