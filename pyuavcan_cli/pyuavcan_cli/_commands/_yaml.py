#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
The YAML library we use is API-unstable at the time of writing. We can't just use the de-facto standard PyYAML
because it's kinda stuck in the past (no ordered dicts, no support for YAML v1.2). This facade shields the
rest of the code from breaking changes in the YAML library API or from migration to another library.
"""

import io
import typing
import decimal
import ruamel.yaml  # TODO: add a fallback option for the standard YAML library if this one is not available.


class YAMLDumper:
    """
    YAML generation facade.
    """
    def __init__(self, explicit_start: bool = False):
        self._impl = ruamel.yaml.YAML(typ='safe')
        self._impl.explicit_start = explicit_start
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
        s = 'nan'
    elif data.is_infinite():
        s = '.inf' if data > 0 else '-.inf'
    else:
        assert False
    return self.represent_scalar('tag:yaml.org,2002:float', s)


ruamel.yaml.add_representer(decimal.Decimal, _represent_decimal, representer=ruamel.yaml.SafeRepresenter)

_POINT_ZERO_DECIMAL = decimal.Decimal('0.0')
