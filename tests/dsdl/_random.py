#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import time
import numpy
import typing
import pydsdl
import random
import logging
from dataclasses import dataclass

import pyuavcan.dsdl
from ._util import are_close, make_random_object, expand_service_types


# Fail the test if any type takes longer than this to serialize or deserialize.
_MAX_ALLOWED_SERIALIZATION_DESERIALIZATION_TIME = 5e-3

_NUM_RANDOM_SAMPLES = int(os.environ.get('PYUAVCAN_TEST_NUM_RANDOM_SAMPLES', 300))
assert _NUM_RANDOM_SAMPLES >= 20, 'Invalid configuration: low number of random samples may trigger a false-negative.'


_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _TypeTestStatistics:
    mean_serialization_time: float
    mean_deserialization_time: float
    random_serialized_representation_correctness_ratio: float

    @property
    def worst_time(self) -> float:
        return max(self.mean_serialization_time,
                   self.mean_deserialization_time)


def _unittest_random(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    logging.getLogger('pyuavcan.dsdl._composite_object').setLevel(logging.WARNING)
    performance: typing.Dict[pydsdl.CompositeType, _TypeTestStatistics] = {}

    for info in generated_packages:
        for model in expand_service_types(info.types):
            performance[model] = _test_type(model, _NUM_RANDOM_SAMPLES)

    _logger.info('Tested types ordered by serialization speed, %d random samples per type', _NUM_RANDOM_SAMPLES)
    _logger.info('Columns: random SR correctness ratio; mean serialization time (us); mean deserialization time (us)')

    for ty, stat in sorted(performance.items(), key=lambda kv: -kv[1].worst_time):
        assert isinstance(stat, _TypeTestStatistics)
        suffix = '' if stat.worst_time < 1e-3 else '\tSLOW!'

        _logger.info(f'%-60s %3.0f%% %6.0f %6.0f%s', ty,
                     stat.random_serialized_representation_correctness_ratio * 100,
                     stat.mean_serialization_time * 1e6,
                     stat.mean_deserialization_time * 1e6,
                     suffix)

        assert stat.worst_time <= _MAX_ALLOWED_SERIALIZATION_DESERIALIZATION_TIME, \
            f'Serialization performance issues detected in type {ty}'

        assert stat.random_serialized_representation_correctness_ratio > 0, \
            f'At least one random sample must be valid. ' \
            f'Either the tested code is incorrect, or the number of random samples is too low. ' \
            f'Failed type: {ty}'


def _test_type(model: pydsdl.CompositeType, num_random_samples: int) -> _TypeTestStatistics:
    _logger.debug('Roundtrip serialization test of %s with %d random samples', model, num_random_samples)
    cls = pyuavcan.dsdl.get_generated_class(model)
    samples: typing.List[typing.Tuple[float, float]] = [
        _serialize_deserialize(cls())
    ]
    rand_sr_validness: typing.List[bool] = []

    def once(obj: pyuavcan.dsdl.CompositeObject) -> None:
        samples.append(_serialize_deserialize(obj))

    for _ in range(num_random_samples):
        # Forward test: get random object, serialize, deserialize, compare
        once(make_random_object(model))

        # Reverse test: get random serialized representation, deserialize; if successful, serialize again and compare
        sr = _make_random_serialized_representation(pyuavcan.dsdl.get_model(cls).bit_length_set)
        ob = pyuavcan.dsdl.try_deserialize(cls, sr)
        rand_sr_validness.append(ob is not None)
        if ob:
            once(ob)

    out = numpy.mean(samples, axis=0)
    assert out.shape == (2,)
    return _TypeTestStatistics(
        mean_serialization_time=out[0],
        mean_deserialization_time=out[1],
        random_serialized_representation_correctness_ratio=float(numpy.mean(rand_sr_validness)),
    )


def _serialize_deserialize(obj: pyuavcan.dsdl.CompositeObject) -> typing.Tuple[float, float]:
    ts = time.process_time()
    sr = pyuavcan.dsdl.serialize(obj)
    ser_sample = time.process_time() - ts

    ts = time.process_time()
    d = pyuavcan.dsdl.try_deserialize(type(obj), sr)
    des_sample = time.process_time() - ts

    assert d is not None
    assert type(obj) is type(d)
    assert pyuavcan.dsdl.get_model(obj) == pyuavcan.dsdl.get_model(d)
    assert are_close(pyuavcan.dsdl.get_model(obj), obj, d), f'{obj} != {d}; sr: {bytes(sr).hex()}'

    # Similar floats may produce drastically different string representations, so if there is at least one float inside,
    # we skip the string representation equality check.
    if pydsdl.FloatType.__name__ not in repr(pyuavcan.dsdl.get_model(d)):
        assert str(obj) == str(d)
        assert repr(obj) == repr(d)

    return ser_sample, des_sample


def _make_random_serialized_representation(bls: pydsdl.BitLengthSet) -> numpy.ndarray:
    bit_length = random.choice(list(bls))
    byte_length = (bit_length + 7) // 8
    return numpy.random.randint(0, 256, size=byte_length, dtype=numpy.uint8)
