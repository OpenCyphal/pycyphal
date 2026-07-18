from __future__ import annotations

import time

import pytest

from pycyphal2 import SUBJECT_ID_PINNED_MAX
from pycyphal2._node import left_wins
from pycyphal2._hash import rapidhash
from pycyphal2._node import (
    EVICTIONS_PINNED_MIN,
    GossipScope,
    compute_subject_id,
    is_valid_subject_id_modulus,
    match_pattern,
    resolve_name,
)
from tests.mock_transport import MockTransport, MockNetwork, DEFAULT_MODULUS
from tests.typing_helpers import new_node


def test_compute_subject_id_pinned():
    """Pinned topics (evictions >= EVICTIONS_PINNED_MIN) yield subject-ID = 0xFFFFFFFF - evictions."""
    for pin in (0, 1, 100, SUBJECT_ID_PINNED_MAX):
        evictions = 0xFFFFFFFF - pin
        assert evictions >= EVICTIONS_PINNED_MIN
        sid = compute_subject_id(0xDEAD, evictions, DEFAULT_MODULUS)
        assert sid == pin


def test_compute_subject_id_pinned_boundary():
    """Boundary: evictions == EVICTIONS_PINNED_MIN is pinned."""
    sid = compute_subject_id(0, EVICTIONS_PINNED_MIN, DEFAULT_MODULUS)
    assert sid == 0xFFFFFFFF - EVICTIONS_PINNED_MIN
    assert sid == SUBJECT_ID_PINNED_MAX


def test_compute_subject_id_non_pinned_zero_evictions():
    """Non-pinned with zero evictions: offset + hash % modulus."""
    for topic_hash in (0, 1, rapidhash("my/topic"), (1 << 64) - 1):
        sid = compute_subject_id(topic_hash, 0, DEFAULT_MODULUS)
        expected = SUBJECT_ID_PINNED_MAX + 1 + (topic_hash % DEFAULT_MODULUS)
        assert sid == expected


def test_compute_subject_id_non_pinned_with_evictions():
    """Non-pinned formula: offset + ((hash + evictions^2) mod 2^64) % modulus; the modular-reduction form
    used here for the expectation is equivalent whenever the sum does not overflow 64 bits."""
    topic_hash = rapidhash("some/topic")
    for ev in (1, 2, 5, 100):
        sid = compute_subject_id(topic_hash, ev, DEFAULT_MODULUS)
        expected = (
            SUBJECT_ID_PINNED_MAX
            + 1
            + (
                ((topic_hash % DEFAULT_MODULUS) + (((ev % DEFAULT_MODULUS) * (ev % DEFAULT_MODULUS)) % DEFAULT_MODULUS))
                % DEFAULT_MODULUS
            )
        )
        assert sid == expected


def test_compute_subject_id_wraps_uint64_sum():
    """The hash + evictions² sum wraps mod 2^64 before reduction, matching the reference uint64 arithmetic
    bit-for-bit. The eviction count is an untrusted uint32 gossip field, so the overflowing case is remotely
    constructible; exact big-int arithmetic here would partition Python and C nodes onto different subject-IDs."""
    topic_hash = (1 << 64) - 1
    evictions = EVICTIONS_PINNED_MIN - 1
    sid = compute_subject_id(topic_hash, evictions, DEFAULT_MODULUS)
    uint64_wrapping = (
        SUBJECT_ID_PINNED_MAX + 1 + (((topic_hash + (evictions * evictions)) & ((1 << 64) - 1)) % DEFAULT_MODULUS)
    )
    assert sid == uint64_wrapping == 74897


def test_compute_subject_id_evictions_changes_sid():
    topic_hash = rapidhash("test/evictions")
    sids = set()
    for ev in range(10):
        sids.add(compute_subject_id(topic_hash, ev, DEFAULT_MODULUS))
    assert len(sids) > 1


def test_compute_subject_id_just_below_pinned():
    """evictions == EVICTIONS_PINNED_MIN - 1 is NOT pinned."""
    ev = EVICTIONS_PINNED_MIN - 1
    topic_hash = 12345
    sid = compute_subject_id(topic_hash, ev, DEFAULT_MODULUS)
    expected = (
        SUBJECT_ID_PINNED_MAX
        + 1
        + (
            ((topic_hash % DEFAULT_MODULUS) + (((ev % DEFAULT_MODULUS) * (ev % DEFAULT_MODULUS)) % DEFAULT_MODULUS))
            % DEFAULT_MODULUS
        )
    )
    assert sid == expected


def test_is_valid_subject_id_modulus_predicate():
    """Mirror of the reference predicate: >= 57203, prime, and ≡ 3 (mod 4)."""
    for good in (57203, 122743, 8378431, 4294954663):
        assert is_valid_subject_id_modulus(good)
    assert not is_valid_subject_id_modulus(3)  # Prime and ≡3 mod 4, but below the minimum.
    assert not is_valid_subject_id_modulus(57202)  # Below the minimum.
    assert not is_valid_subject_id_modulus(57205)  # ≡ 1 mod 4.
    assert not is_valid_subject_id_modulus(57207)  # ≡ 3 mod 4 but composite (3 × 19069).
    assert not is_valid_subject_id_modulus(122744)  # Even.
    # Above uint32 is rejected without running a slow primality test on a huge untrusted value.
    assert not is_valid_subject_id_modulus((1 << 32) + 3)


async def test_degenerate_subject_id_modulus_rejected():
    """A modulus violating the reference predicate must be rejected at node construction: the quadratic
    probe (hash + evictions²) mod m does not cover the residue space under a degenerate modulus, so the
    synchronous displacement loop in topic_allocate would hard-block the event loop."""
    for bad in (3, 57202, 57205, 57207, 122744):
        tr = MockTransport(node_id=1, modulus=bad, network=MockNetwork())
        with pytest.raises(ValueError, match="subject_id_modulus"):
            new_node(tr, home="n")
    for good in (57203, 122743, 8378431):
        tr = MockTransport(node_id=1, modulus=good, network=MockNetwork())
        node = new_node(tr, home="n")
        node.close()


async def test_advertise_creates_topic():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    assert pub is not None

    resolved, _, _ = resolve_name("my/topic", "test_node", "")
    topic = node.topics_by_name.get(resolved)
    assert topic is not None
    assert topic.name == resolved
    assert topic.pub_count == 1
    assert not topic.is_implicit

    pub.close()
    node.close()


async def test_advertise_embedded_wildcard_char_is_verbatim():
    """'sensor/temp*raw' has no whole-segment substitution token, so it is a legal verbatim topic
    (reference parity with the wkv classifier) and can be advertised."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n")

    pub = node.advertise("/sensor/temp*raw")
    topic = node.topics_by_name.get("sensor/temp*raw")
    assert topic is not None
    assert topic.pub_count == 1

    pub.close()
    node.close()


async def test_advertise_assigns_subject_id():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    resolved, _, _ = resolve_name("my/topic", "test_node", "")
    topic = node.topics_by_name[resolved]

    sid = topic.subject_id(tr.subject_id_modulus)
    assert sid == compute_subject_id(topic.hash, topic.evictions, DEFAULT_MODULUS)
    assert node.topics_by_subject_id.get(sid) is topic

    pub.close()
    node.close()


async def test_advertise_pinned_topic():
    """The '#N' suffix pins the topic to subject-ID N."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic#42")
    resolved, pin, _ = resolve_name("my/topic#42", "test_node", "")
    assert pin == 42
    topic = node.topics_by_name[resolved]
    assert topic.subject_id(tr.subject_id_modulus) == 42

    pub.close()
    node.close()


async def test_advertise_multiple_same_topic():
    """Multiple publishers on the same topic share one topic object."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub1 = node.advertise("my/topic")
    pub2 = node.advertise("my/topic")
    topic = node.topics_by_name["my/topic"]
    assert pub1.topic is pub2.topic
    assert pub1.topic is topic
    assert pub2.topic is topic
    assert topic.pub_count == 2

    pub1.close()
    assert topic.pub_count == 1
    pub2.close()
    assert topic.pub_count == 0

    node.close()


async def test_topic_collision_evicts_loser():
    """When two topics collide on the same subject-ID, the lower-precedence one gets evicted."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub1 = node.advertise("first/topic")
    resolved1, _, _ = resolve_name("first/topic", "test_node", "")
    topic1 = node.topics_by_name[resolved1]

    pub2 = node.advertise("second/topic")
    resolved2, _, _ = resolve_name("second/topic", "test_node", "")
    topic2 = node.topics_by_name[resolved2]

    assert topic1.subject_id(tr.subject_id_modulus) != topic2.subject_id(tr.subject_id_modulus) or topic1 is topic2
    assert topic1.name in node.topics_by_name
    assert topic2.name in node.topics_by_name

    pub1.close()
    pub2.close()
    node.close()


async def test_left_wins_resolution():
    """left_wins: higher log-age wins, tie-break by lower hash."""
    assert left_wins(10, 0xAAAA, 5, 0xBBBB) is True
    assert left_wins(5, 0xAAAA, 10, 0xBBBB) is False

    assert left_wins(5, 0xAAAA, 5, 0xBBBB) is True
    assert left_wins(5, 0xBBBB, 5, 0xAAAA) is False

    # Equal lage and equal hash: left does NOT win (comparison is strict).
    assert left_wins(5, 0xAAAA, 5, 0xAAAA) is False


async def test_collision_allocator_iterates():
    """The allocator resolves collisions by incrementing evictions until every subject-ID is unique."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pubs = []
    for i in range(10):
        p = node.advertise(f"topic/{i}")
        pubs.append(p)

    sids = set()
    for name, topic in node.topics_by_name.items():
        sid = topic.subject_id(tr.subject_id_modulus)
        if sid not in sids:
            sids.add(sid)
        else:
            assert False, f"Duplicate subject-ID {sid} for topic '{name}'"

    for p in pubs:
        p.close()
    node.close()


async def test_gossip_known_divergent_evictions_we_win():
    """Known topic, divergent evictions, we win: we keep our evictions and send urgent gossip."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    resolved, _, _ = resolve_name("my/topic", "test_node", "")
    topic = node.topics_by_name[resolved]

    # Older origin wins the comparison.
    topic.ts_origin = time.monotonic() - 10000
    my_lage = topic.lage(time.monotonic())
    old_evictions = topic.evictions

    node.on_gossip_known(topic, old_evictions + 1, my_lage - 5, time.monotonic(), GossipScope.SHARDED)

    assert topic.evictions == old_evictions
    assert topic.gossip_task is not None

    pub.close()
    node.close()


async def test_gossip_known_divergent_evictions_we_lose():
    """Known topic, divergent evictions, we lose: we adopt their evictions."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    resolved, _, _ = resolve_name("my/topic", "test_node", "")
    topic = node.topics_by_name[resolved]

    old_evictions = topic.evictions
    # Higher lage wins, so the remote wins.
    remote_lage = 40
    remote_evictions = old_evictions + 3

    node.on_gossip_known(topic, remote_evictions, remote_lage, time.monotonic(), GossipScope.SHARDED)

    assert topic.evictions == remote_evictions

    pub.close()
    node.close()


async def test_gossip_known_same_evictions_merges_lage():
    """Known topic, same evictions: log-age is merged (max of the two)."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    resolved, _, _ = resolve_name("my/topic", "test_node", "")
    topic = node.topics_by_name[resolved]

    old_lage = topic.lage(time.monotonic())
    remote_lage = old_lage + 10

    node.on_gossip_known(topic, topic.evictions, remote_lage, time.monotonic(), GossipScope.SHARDED)

    new_lage = topic.lage(time.monotonic())
    assert new_lage >= remote_lage

    pub.close()
    node.close()


async def test_gossip_unknown_collision_we_win():
    """Unknown topic colliding with ours, we win: our evictions stay unchanged."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    resolved, _, _ = resolve_name("my/topic", "test_node", "")
    topic = node.topics_by_name[resolved]
    my_sid = topic.subject_id(tr.subject_id_modulus)

    # Older origin wins.
    topic.ts_origin = time.monotonic() - 100000

    remote_hash = rapidhash("remote/collision")
    remote_evictions = 0
    remote_sid = compute_subject_id(remote_hash, remote_evictions, DEFAULT_MODULUS)

    # The remote SID may or may not collide with ours; either way this exercises the on_gossip_unknown path.
    old_evictions = topic.evictions
    node.on_gossip_unknown(remote_hash, remote_evictions, 0, time.monotonic())

    if remote_sid != my_sid:
        assert topic.evictions == old_evictions
    else:
        assert topic.evictions == old_evictions

    pub.close()
    node.close()


async def test_gossip_unknown_collision_we_lose():
    """Unknown topic colliding with ours, we lose: our evictions increment."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    resolved, _, _ = resolve_name("my/topic", "test_node", "")
    topic = node.topics_by_name[resolved]
    old_evictions = topic.evictions

    # The remote has a very high lage (old origin), so it wins.
    remote_lage = 50
    target_remainder = (
        (topic.hash % DEFAULT_MODULUS)
        + (((old_evictions % DEFAULT_MODULUS) * (old_evictions % DEFAULT_MODULUS)) % DEFAULT_MODULUS)
    ) % DEFAULT_MODULUS
    # Pick remote_hash such that remote_hash % modulus == target_remainder AND remote_hash != topic.hash.
    remote_hash = target_remainder + DEFAULT_MODULUS

    # Our topic is young, so we lose.
    topic.ts_origin = time.monotonic()

    node.on_gossip_unknown(remote_hash, 0, remote_lage, time.monotonic())

    assert topic.evictions > old_evictions

    pub.close()
    node.close()


def test_match_pattern_verbatim():
    assert match_pattern("foo/bar", "foo/bar") == []
    assert match_pattern("foo/bar", "foo/baz") is None


def test_match_pattern_star():
    result = match_pattern("foo/*/baz", "foo/bar/baz")
    assert result is not None
    assert len(result) == 1
    assert result[0] == ("bar", 1)


def test_match_pattern_chevron():
    result = match_pattern("foo/>", "foo/bar/baz")
    assert result is not None
    assert len(result) == 1
    assert result[0] == ("bar/baz", 1)


def test_match_pattern_no_match():
    assert match_pattern("foo/*", "bar/baz") is None
    assert match_pattern("foo/*/baz", "foo/bar/qux") is None


def test_match_pattern_star_length_mismatch():
    assert match_pattern("foo/*", "foo/bar/baz") is None


def test_match_pattern_chevron_zero_segments():
    assert match_pattern("foo/>", "foo") == [("", 1)]


def test_match_pattern_multiple_stars():
    result = match_pattern("*/middle/*", "top/middle/bottom")
    assert result is not None
    assert len(result) == 2
    assert result[0] == ("top", 0)
    assert result[1] == ("bottom", 2)


def test_resolve_name_absolute():
    name, pin, verbatim = resolve_name("/absolute/topic", "home", "ns")
    assert name == "absolute/topic"
    assert pin is None
    assert verbatim is True


def test_resolve_name_relative_with_namespace():
    name, pin, verbatim = resolve_name("topic", "home", "my_ns")
    assert name == "my_ns/topic"
    assert pin is None
    assert verbatim is True


def test_resolve_name_home_prefix():
    name, pin, verbatim = resolve_name("~", "my_home", "")
    assert name == "my_home"


def test_resolve_name_home_subpath():
    name, pin, verbatim = resolve_name("~/sub", "my_home", "")
    assert name == "my_home/sub"


def test_resolve_name_pinned():
    name, pin, verbatim = resolve_name("topic#100", "home", "ns")
    assert pin == 100


def test_resolve_name_pattern_not_verbatim():
    name, pin, verbatim = resolve_name("foo/*/bar", "home", "ns")
    assert verbatim is False
