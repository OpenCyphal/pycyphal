from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from typing import Any


# =====================================================================================================================
# Exceptions
# =====================================================================================================================


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


# =====================================================================================================================
# Instant
# =====================================================================================================================


@dataclass(frozen=True)
class Instant:
    """Monotonic time elapsed from an unspecified origin instant. Durations use float seconds."""

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


# =====================================================================================================================
# Priority
# =====================================================================================================================


class Priority(IntEnum):
    EXCEPTIONAL = 0
    IMMEDIATE = 1
    FAST = 2
    HIGH = 3
    NOMINAL = 4
    LOW = 5
    SLOW = 6
    OPTIONAL = 7


# =====================================================================================================================
# Closable
# =====================================================================================================================


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

TOPIC_NAME_MAX = 255


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


# =====================================================================================================================
# Pattern matching
# =====================================================================================================================


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


# =====================================================================================================================
# rapidhash V3 — pure-Python port of rapidhash_internal() from rapidhash.h (COMPACT, FAST mode, seed=0)
# =====================================================================================================================

_U64 = (1 << 64) - 1

_RAPID_SECRET = (
    0x2D358DCCAA6C78A5,
    0x8BB84B93962EACC9,
    0x4B33A62ED433D4A3,
    0x4D5A2DA51DE1AA47,
    0xA0761D6478BD642F,
    0xE7037ED1A0B428DB,
    0x90ED1765281C388C,
    0xAAAAAAAAAAAAAAAA,
)


def _rapid_mum(a: int, b: int) -> tuple[int, int]:
    r = a * b
    return r & _U64, (r >> 64) & _U64


def _rapid_mix(a: int, b: int) -> int:
    lo, hi = _rapid_mum(a, b)
    return lo ^ hi


def _r64(d: bytes, o: int) -> int:
    return int.from_bytes(d[o : o + 8], "little")


def _r32(d: bytes, o: int) -> int:
    return int.from_bytes(d[o : o + 4], "little")


def rapidhash(data: bytes) -> int:
    s = _RAPID_SECRET
    n = len(data)
    seed = _rapid_mix(s[2], s[1])
    a = b = 0
    i = n
    p = 0
    if n <= 16:
        if n >= 4:
            seed = (seed ^ n) & _U64
            if n >= 8:
                a = _r64(data, 0)
                b = _r64(data, n - 8)
            else:
                a = _r32(data, 0)
                b = _r32(data, n - 4)
        elif n > 0:
            a = (data[0] << 45) | data[n - 1]
            b = data[n >> 1]
    else:
        if n > 112:
            see1 = see2 = see3 = see4 = see5 = see6 = seed
            while True:
                seed = _rapid_mix(_r64(data, p) ^ s[0], _r64(data, p + 8) ^ seed)
                see1 = _rapid_mix(_r64(data, p + 16) ^ s[1], _r64(data, p + 24) ^ see1)
                see2 = _rapid_mix(_r64(data, p + 32) ^ s[2], _r64(data, p + 40) ^ see2)
                see3 = _rapid_mix(_r64(data, p + 48) ^ s[3], _r64(data, p + 56) ^ see3)
                see4 = _rapid_mix(_r64(data, p + 64) ^ s[4], _r64(data, p + 72) ^ see4)
                see5 = _rapid_mix(_r64(data, p + 80) ^ s[5], _r64(data, p + 88) ^ see5)
                see6 = _rapid_mix(_r64(data, p + 96) ^ s[6], _r64(data, p + 104) ^ see6)
                p += 112
                i -= 112
                if i <= 112:
                    break
            seed ^= see1
            see2 ^= see3
            see4 ^= see5
            seed ^= see6
            see2 ^= see4
            seed ^= see2
        if i > 16:
            seed = _rapid_mix(_r64(data, p) ^ s[2], _r64(data, p + 8) ^ seed)
            if i > 32:
                seed = _rapid_mix(_r64(data, p + 16) ^ s[2], _r64(data, p + 24) ^ seed)
                if i > 48:
                    seed = _rapid_mix(_r64(data, p + 32) ^ s[1], _r64(data, p + 40) ^ seed)
                    if i > 64:
                        seed = _rapid_mix(_r64(data, p + 48) ^ s[1], _r64(data, p + 56) ^ seed)
                        if i > 80:
                            seed = _rapid_mix(_r64(data, p + 64) ^ s[2], _r64(data, p + 72) ^ seed)
                            if i > 96:
                                seed = _rapid_mix(_r64(data, p + 80) ^ s[1], _r64(data, p + 88) ^ seed)
        a = _r64(data, p + i - 16) ^ i
        b = _r64(data, p + i - 8)
    a ^= s[1]
    b ^= seed
    a, b = _rapid_mum(a, b)
    return _rapid_mix(a ^ s[7], b ^ s[1] ^ i)
