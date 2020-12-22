#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import logging

import pytest
import pydsdl

import pyuavcan.dsdl
from . import _util


_logger = logging.getLogger(__name__)


# noinspection PyUnusedLocal
def _unittest_slow_builtin_form_manual(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    import uavcan.node
    import uavcan.register
    import uavcan.primitive.array
    import uavcan.time

    bi = pyuavcan.dsdl.to_builtin(
        uavcan.node.Heartbeat_1_0(
            uptime=123456,
            health=uavcan.node.Health_1_0(2),
            mode=uavcan.node.Mode_1_0(3),
            vendor_specific_status_code=0xBA,
        )
    )
    assert bi == {
        "uptime": 123456,
        "health": {"value": 2},
        "mode": {"value": 3},
        "vendor_specific_status_code": 186,
    }

    bi = pyuavcan.dsdl.to_builtin(
        uavcan.node.GetInfo_1_0.Response(
            protocol_version=uavcan.node.Version_1_0(1, 2),
            hardware_version=uavcan.node.Version_1_0(3, 4),
            software_version=uavcan.node.Version_1_0(5, 6),
            software_vcs_revision_id=0xBADC0FFEE0DDF00D,
            unique_id=b"0123456789abcdef",
            name="org.node.my",
            software_image_crc=[0x0DDDEADB16B00B5],
            certificate_of_authenticity=list(range(100)),
        )
    )
    print(bi)
    assert bi == {
        "protocol_version": {"major": 1, "minor": 2},
        "hardware_version": {"major": 3, "minor": 4},
        "software_version": {"major": 5, "minor": 6},
        "software_vcs_revision_id": 0xBADC0FFEE0DDF00D,
        "unique_id": list(b"0123456789abcdef"),
        "name": "org.node.my",
        "software_image_crc": [0x0DDDEADB16B00B5],
        # The following will have to be changed when strings are supported natively in DSDL:
        "certificate_of_authenticity": bytes(range(100)).decode("unicode_escape"),
    }

    bi = pyuavcan.dsdl.to_builtin(
        uavcan.register.Access_1_0.Response(
            timestamp=uavcan.time.SynchronizedTimestamp_1_0(1234567890),
            mutable=True,
            persistent=False,
            value=uavcan.register.Value_1_0(
                real32=uavcan.primitive.array.Real32_1_0(
                    [
                        123.456,
                        -789.123,
                        float("+inf"),
                    ]
                )
            ),
        )
    )
    print(bi)
    assert bi == {
        "timestamp": {"microsecond": 1234567890},
        "mutable": True,
        "persistent": False,
        "value": {
            "real32": {
                "value": [
                    pytest.approx(123.456),
                    pytest.approx(-789.123),
                    pytest.approx(float("+inf")),
                ],
            },
        },
    }

    with pytest.raises(ValueError, match=".*field.*"):
        bi["nonexistent_field"] = 123
        pyuavcan.dsdl.update_from_builtin(uavcan.register.Access_1_0.Response(), bi)


def _unittest_slow_builtin_form_automatic(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    for info in generated_packages:
        for model in _util.expand_service_types(info.models):
            if max(model.bit_length_set) / 8 > 1024 * 1024:
                _logger.info("Automatic test of %s skipped because the type is too large", model)
                continue  # Skip large objects because they take forever to convert and test

            obj = _util.make_random_object(model)
            bi = pyuavcan.dsdl.to_builtin(obj)
            reconstructed = pyuavcan.dsdl.update_from_builtin(pyuavcan.dsdl.get_class(model)(), bi)

            if str(obj) != str(reconstructed) or repr(obj) != repr(reconstructed):  # pragma: no branch
                if pydsdl.FloatType.__name__ not in repr(model):  # pragma: no cover
                    _logger.info(
                        "Automatic comparison cannot be performed because the objects of type %s may "
                        "contain floats. Please implement proper DSDL object comparison methods and "
                        "update this test to use them.",
                        model,
                    )
                    _logger.info("Original random object: %r", obj)
                    _logger.info("Reconstructed object:   %r", reconstructed)
                    _logger.info("Built-in representation: %r", bi)
                else:
                    assert False, f"{obj} != {reconstructed}"


# noinspection PyUnusedLocal
def _unittest_issue_116(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    from uavcan.register import Access_1_0

    valid = pyuavcan.dsdl.update_from_builtin(Access_1_0.Request(), {"name": {"name": "uavcan.pub.measurement"}})
    assert valid.name.name.tobytes().decode() == "uavcan.pub.measurement"
    with pytest.raises(TypeError) as ex:
        pyuavcan.dsdl.update_from_builtin(Access_1_0.Request(), {"name": "uavcan.pub.measurement"})
    print("Exception message:", ex)
    assert "str" in str(ex)
    assert "Name_1_" in str(ex)
