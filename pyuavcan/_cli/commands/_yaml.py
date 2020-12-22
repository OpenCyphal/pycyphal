# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

"""
The YAML library we use is API-unstable at the time of writing. We can't just use the de-facto standard PyYAML
because it's kinda stuck in the past (no ordered dicts, no support for YAML v1.2). This facade shields the
rest of the code from breaking changes in the YAML library API or from migration to another library.
"""

import io
import typing
import decimal
import ruamel.yaml


class YAMLDumper:
    """
    YAML generation facade.
    """

    def __init__(self, explicit_start: bool = False):
        # We need to use the roundtrip representer to retain ordering of mappings, which is important for usability.
        self._impl = ruamel.yaml.YAML(typ="rt")
        # noinspection PyTypeHints
        self._impl.explicit_start = explicit_start  # type: ignore
        self._impl.default_flow_style = False

    def dump(self, data: typing.Any, stream: typing.TextIO) -> None:
        self._impl.dump(data, stream)

    def dumps(self, data: typing.Any) -> str:
        s = io.StringIO()
        self.dump(data, s)
        return s.getvalue()


class YAMLLoader:
    """
    YAML parsing facade.
    Natively represents decimal.Decimal as floats in the output.
    """

    def __init__(self) -> None:
        self._impl = ruamel.yaml.YAML()

    def load(self, text: str) -> typing.Any:
        return self._impl.load(text)


def _represent_decimal(self: ruamel.yaml.BaseRepresenter, data: decimal.Decimal) -> ruamel.yaml.ScalarNode:
    if data.is_finite():
        s = str(_POINT_ZERO_DECIMAL + data)  # The zero addition is to force float-like string representation
    elif data.is_nan():
        s = ".nan"
    elif data.is_infinite():
        s = ".inf" if data > 0 else "-.inf"
    else:
        assert False
    return self.represent_scalar("tag:yaml.org,2002:float", s)  # type: ignore


ruamel.yaml.add_representer(decimal.Decimal, _represent_decimal, representer=ruamel.yaml.RoundTripRepresenter)

_POINT_ZERO_DECIMAL = decimal.Decimal("0.0")


def _unittest_yaml() -> None:
    import pytest

    ref = YAMLDumper(explicit_start=True).dumps(
        {
            "abc": decimal.Decimal("-inf"),
            "def": [
                decimal.Decimal("nan"),
                {
                    "qaz": decimal.Decimal("789"),
                },
            ],
        }
    )
    assert (
        ref
        == """---
abc: -.inf
def:
- .nan
- qaz: 789.0
"""
    )
    assert YAMLLoader().load(ref) == {
        "abc": -float("inf"),
        "def": [
            pytest.approx(float("nan"), nan_ok=True),
            {
                "qaz": pytest.approx(789),
            },
        ],
    }
