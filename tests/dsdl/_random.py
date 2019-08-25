#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import gc
import time
import typing
import random
import logging
import dataclasses

import numpy
import pytest
import pydsdl

import pyuavcan.dsdl
from . import _util


# Fail the test if any type takes longer than this to serialize or deserialize on average.
# This may appear huge but it's necessary to avoid false positives in the CI environment.
_MAX_ALLOWED_SERIALIZATION_DESERIALIZATION_TIME = 90e-3

# When generating random serialized representations, limit the number of fragments to this value
# for performance reasons. Also, a large number of fragments may occasionally cause the test to run out of memory
# and be killed, especially so in cloud-hosted CI systems which are always memory-impaired.
_MAX_RANDOM_SERIALIZED_REPRESENTATION_FRAGMENTS = 1000

# Values lower than this may trigger a false-negative, so we don't run stat checks if there are fewer samples than this.
_MIN_RANDOM_SAMPLES_FOR_STATISTICAL_CORRECTNESS_CHECK = 50
# Set this environment variable to a lower value to speed up the test (random tests take a very long time to run).
_NUM_RANDOM_SAMPLES = int(os.environ.get('PYUAVCAN_TEST_NUM_RANDOM_SAMPLES',
                                         _MIN_RANDOM_SAMPLES_FOR_STATISTICAL_CORRECTNESS_CHECK * 2))


_logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class _TypeTestStatistics:
    mean_serialization_time: float
    mean_deserialization_time: float
    random_serialized_representation_correctness_ratio: float

    @property
    def worst_time(self) -> float:
        return max(self.mean_serialization_time,
                   self.mean_deserialization_time)


def _unittest_slow_random(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    # The random test intentionally generates a lot of faulty data, which generates a lot of log messages.
    # We don't want them to clutter the test output, so we raise the logging level temporarily.
    pyuavcan_logger = logging.getLogger('pyuavcan.dsdl')
    original_logging_level = pyuavcan_logger.level
    pyuavcan_logger.setLevel(logging.WARNING)

    print('Number of random samples:', _NUM_RANDOM_SAMPLES)
    print('Set environment variable PYUAVCAN_TEST_NUM_RANDOM_SAMPLES to override.')

    try:
        performance: typing.Dict[pydsdl.CompositeType, _TypeTestStatistics] = {}

        for info in generated_packages:
            for model in _util.expand_service_types(info.models, keep_services=True):
                if not isinstance(model, pydsdl.ServiceType):
                    performance[model] = _test_type(model, _NUM_RANDOM_SAMPLES)
                else:
                    dtype = pyuavcan.dsdl.get_class(model)
                    with pytest.raises(TypeError):
                        assert list(pyuavcan.dsdl.serialize(dtype()))
                    with pytest.raises(TypeError):
                        pyuavcan.dsdl.deserialize(dtype, [memoryview(b'')])

        _logger.info('Tested types ordered by serialization speed, %d random samples per type', _NUM_RANDOM_SAMPLES)
        _logger.info('Columns: random SR correctness ratio; '
                     'mean serialization time [us]; mean deserialization time [us]')

        for ty, stat in sorted(performance.items(), key=lambda kv: -kv[1].worst_time):  # pragma: no branch
            assert isinstance(stat, _TypeTestStatistics)
            suffix = '' if stat.worst_time < 1e-3 else '\tSLOW!'

            _logger.info(f'%-60s %3.0f%% %6.0f %6.0f%s', ty,
                         stat.random_serialized_representation_correctness_ratio * 100,
                         stat.mean_serialization_time * 1e6,
                         stat.mean_deserialization_time * 1e6,
                         suffix)

            if _NUM_RANDOM_SAMPLES >= _MIN_RANDOM_SAMPLES_FOR_STATISTICAL_CORRECTNESS_CHECK:
                assert stat.worst_time <= _MAX_ALLOWED_SERIALIZATION_DESERIALIZATION_TIME, \
                    f'Serialization performance issues detected in type {ty}'

                assert stat.random_serialized_representation_correctness_ratio > 0, \
                    f'At least one random sample must be valid. ' \
                    f'Either the tested code is incorrect, or the number of random samples is too low. ' \
                    f'Failed type: {ty}'
            else:  # pragma: no cover
                _logger.warning('Statistical checks skipped because the number of samples is low.')
    finally:
        pyuavcan_logger.setLevel(original_logging_level)


def _test_type(model: pydsdl.CompositeType, num_random_samples: int) -> _TypeTestStatistics:
    _logger.debug('Roundtrip serialization test of %s with %d random samples', model, num_random_samples)
    dtype = pyuavcan.dsdl.get_class(model)
    samples: typing.List[typing.Tuple[float, float]] = [
        _serialize_deserialize(dtype())
    ]
    rand_sr_validness: typing.List[bool] = []

    def once(obj: pyuavcan.dsdl.CompositeObject) -> typing.Tuple[float, float]:
        s = _serialize_deserialize(obj)
        samples.append(s)
        return s

    for index in range(num_random_samples):
        ts = time.process_time()
        # Forward test: get random object, serialize, deserialize, compare
        sample_ser = once(_util.make_random_object(model))

        # Reverse test: get random serialized representation, deserialize; if successful, serialize again and compare
        sr = _make_random_fragmented_serialized_representation(pyuavcan.dsdl.get_model(dtype).bit_length_set)
        ob = pyuavcan.dsdl.deserialize(dtype, sr)
        rand_sr_validness.append(ob is not None)
        sample_des: typing.Optional[typing.Tuple[float, float]] = None
        if ob:
            sample_des = once(ob)

        elapsed = time.process_time() - ts
        if elapsed > 1.0:
            duration_ser = f'{sample_ser[0] * 1e6:.0f}/{sample_ser[1] * 1e6:.0f}'
            duration_des = f'{sample_des[0] * 1e6:.0f}/{sample_des[1] * 1e6:.0f}' if sample_des else 'N/A'
            _logger.debug(f'Random sample {index + 1} of {num_random_samples} took {elapsed:.1f} s; '
                          f'random SR correct: {ob is not None}; '
                          f'duration forward/reverse [us]: ({duration_ser})/({duration_des})')

    out = numpy.mean(samples, axis=0)
    assert out.shape == (2,)
    return _TypeTestStatistics(
        mean_serialization_time=out[0],
        mean_deserialization_time=out[1],
        random_serialized_representation_correctness_ratio=float(numpy.mean(rand_sr_validness)),
    )


def _serialize_deserialize(obj: pyuavcan.dsdl.CompositeObject) -> typing.Tuple[float, float]:
    gc.collect()
    gc.disable()        # Must be disabled, otherwise it induces spurious false-positive performance warnings

    ts = time.process_time()
    chunks = list(pyuavcan.dsdl.serialize(obj))         # GC must be disabled while we're in the timed context
    ser_sample = time.process_time() - ts

    ts = time.process_time()
    d = pyuavcan.dsdl.deserialize(type(obj), chunks)    # GC must be disabled while we're in the timed context
    des_sample = time.process_time() - ts

    gc.enable()

    assert d is not None
    assert type(obj) is type(d)
    assert pyuavcan.dsdl.get_model(obj) == pyuavcan.dsdl.get_model(d)

    if not _util.are_close(pyuavcan.dsdl.get_model(obj), obj, d):  # pragma: no cover
        assert False, f'{obj} != {d}; sr: {bytes().join(chunks).hex()}'  # Branched for performance reasons

    # Similar floats may produce drastically different string representations, so if there is at least one float inside,
    # we skip the string representation equality check.
    if pydsdl.FloatType.__name__ not in repr(pyuavcan.dsdl.get_model(d)):
        assert str(obj) == str(d)
        assert repr(obj) == repr(d)

    return ser_sample, des_sample


def _make_random_fragmented_serialized_representation(bls: pydsdl.BitLengthSet) -> typing.Sequence[memoryview]:
    bit_length = random.choice(list(bls))
    byte_length = (bit_length + 7) // 8
    return _fragment_randomly(numpy.random.randint(0, 256, size=byte_length, dtype=numpy.uint8).data)


def _fragment_randomly(data: memoryview) -> typing.List[memoryview]:
    try:
        n = random.randint(1, min(_MAX_RANDOM_SERIALIZED_REPRESENTATION_FRAGMENTS, len(data)))
    except ValueError:
        return [data]       # Nothing to fragment
    else:
        q, r = divmod(len(data), n)
        idx = [q * i + min(i, r) for i in range(n + 1)]
        return [data[idx[i]:idx[i + 1]] for i in range(n)]


def _unittest_fragment_randomly() -> None:
    assert _fragment_randomly(memoryview(b'')) == [memoryview(b'')]
    assert _fragment_randomly(memoryview(b'a')) == [memoryview(b'a')]
    for _ in range(100):
        size = random.randint(0, 100)
        data = numpy.random.randint(0, 256, size=size, dtype=numpy.uint8).data
        fragments = _fragment_randomly(data)
        assert b''.join(fragments) == data
