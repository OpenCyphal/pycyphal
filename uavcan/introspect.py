#
# Copyright (C) 2014-2016  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#         Ben Dyer <ben_dyer@mac.com>
#

from __future__ import division, absolute_import, print_function, unicode_literals
import os
import uavcan
from uavcan.transport import CompoundValue, PrimitiveValue, ArrayValue, VoidValue
try:
    from io import StringIO
except ImportError:
    # noinspection PyUnresolvedReferences
    from StringIO import StringIO


def _to_yaml_impl(obj, indent_level=0, parent=None, name=None, uavcan_type=None):
    buf = StringIO()

    def write(fmt, *args):
        buf.write((fmt % args) if len(args) else fmt)

    def indent_newline():
        buf.write(os.linesep + ' ' * 2 * indent_level)

    # Decomposing PrimitiveValue to value and type. This is ugly but it's by design...
    if isinstance(obj, PrimitiveValue):
        uavcan_type = uavcan.get_uavcan_data_type(obj)
        obj = obj.value

    # CompoundValue
    if isinstance(obj, CompoundValue):
        first_field = True

        # Rendering all fields than can be rendered
        for field_name, field in uavcan.get_fields(obj).items():
            if uavcan.is_union(obj) and uavcan.get_active_union_field(obj) != field_name:
                continue
            if isinstance(field, VoidValue):
                continue
            if (first_field and indent_level > 0) or not first_field:
                indent_newline()
            first_field = False
            rendered_field = _to_yaml_impl(field, indent_level=indent_level + 1, parent=obj, name=field_name)
            write('%s: %s', field_name, rendered_field)

        # Special case - empty non-union struct is rendered as empty map
        if first_field and not uavcan.is_union(obj):
            if indent_level > 0:
                indent_newline()
            write('{}')

    # ArrayValue
    elif isinstance(obj, ArrayValue):
        t = uavcan.get_uavcan_data_type(obj)
        if t.value_type.category == t.value_type.CATEGORY_PRIMITIVE:
            def is_nice_character(ch):
                if 32 <= ch <= 126:
                    return True
                if ch in b'\n\r\t':
                    return True
                return False

            as_bytes = '[%s]' % ', '.join([_to_yaml_impl(x, indent_level=indent_level + 1, uavcan_type=t.value_type)
                                          for x in obj])
            if t.is_string_like and all(map(is_nice_character, obj)):
                write('%r # ', obj.decode())
            write(as_bytes)
        else:
            if len(obj) == 0:
                write('[]')
            else:
                for x in obj:
                    indent_newline()
                    write('- %s', _to_yaml_impl(x, indent_level=indent_level + 1, uavcan_type=t.value_type))

    # Primitive types
    elif isinstance(obj, float):
        assert uavcan_type is not None
        float_fmt = {
            16: '%.4f',
            32: '%.6f',
            64: '%.9f',
        }[uavcan_type.bitlen]
        write(float_fmt, obj)
    elif isinstance(obj, bool):
        write('%s', 'true' if obj else 'false')
    elif isinstance(obj, int):
        write('%s', obj)
        if parent is not None and name is not None:
            resolved_name = value_to_constant_name(parent, name)
            if isinstance(resolved_name, str):
                write(' # %s', resolved_name)

    # Non-printable types
    elif isinstance(obj, VoidValue):
        pass

    # Unknown types
    else:
        raise ValueError('Cannot generate YAML representation for %r' % type(obj))

    return buf.getvalue()


def to_yaml(obj):
    """
    This function returns correct YAML representation of a UAVCAN structure (message, request, or response), or
    a DSDL entity (array or primitive), or a UAVCAN transfer, with comments for human benefit.
    Args:
        obj:            Object to convert.

    Returns: Unicode string containing YAML representation of the object.
    """
    if not isinstance(obj, CompoundValue) and hasattr(obj, 'transfer'):
        if hasattr(obj, 'message'):
            payload = obj.message
            header = 'Message'
        elif hasattr(obj, 'request'):
            payload = obj.request
            header = 'Request'
        elif hasattr(obj, 'response'):
            payload = obj.response
            header = 'Response'
        else:
            raise ValueError('Cannot generate YAML representation for %r' % type(obj))

        prefix = '### %s from %s to %s  ts_mono=%.6f  ts_real=%.6f\n' % \
                 (header,
                  obj.transfer.source_node_id or 'Anon',
                  obj.transfer.dest_node_id or 'All',
                  obj.transfer.ts_monotonic, obj.transfer.ts_real)

        return prefix + _to_yaml_impl(payload)
    else:
        return _to_yaml_impl(obj)


def value_to_constant_name(struct, field_name, keep_literal=False):
    """
    This function accepts a UAVCAN struct (message, request, or response), and a field name; and returns
    the name of constant or bit mask that match the value. If no match could be established, the literal
    value will be returned as is.
    Args:
        struct:         UAVCAN struct to work with
        field_name:     Name of the field to work with
        keep_literal:   Whether to include the input integer value in the output string

    Returns: Name of the constant or flags if match could be detected, otherwise integer as is.
    """
    # Extracting constants
    uavcan_type = uavcan.get_uavcan_data_type(struct)
    if uavcan.is_request(struct):
        consts = uavcan_type.request_constants
        fields = uavcan_type.request_fields
    elif uavcan.is_response(struct):
        consts = uavcan_type.response_constants
        fields = uavcan_type.response_fields
    else:
        consts = uavcan_type.constants
        fields = uavcan_type.fields

    assert len(fields) > 0

    # noinspection PyShadowingNames
    def format_output(name, value, remove_common_prefix):
        if remove_common_prefix:
            num_seps = len(field_name.split('_'))
            parts = name.split('_')[num_seps:]
            name = '_'.join(parts)
        return ('%s (%r)' % (name, value)) if keep_literal else name

    # noinspection PyShadowingNames
    def match_one_prefix(prefix, value):
        matches = []
        for cname, cval in [(x.name, x.value) for x in consts if x.name.lower().startswith(prefix.lower())]:
            if cval == value:
                matches.append(cname)
        # Making sure we found exactly one match, otherwise it's not a correct result
        if len(matches) == 1:
            return matches[0]

    # noinspection PyShadowingNames
    def match_value(value):
        # Trying direct match
        match = match_one_prefix(field_name + '_', value)
        if match:
            return format_output(match, value, True)

        # Trying direct match without the terminal letter if it is 's' (plural). This works for 'flags'.
        # TODO: this is sketchy.
        if field_name[-1] == 's':
            match = match_one_prefix(field_name[:-1] + '_', value)
            if match:
                return format_output(match, value, True)

        # Trying match without prefix, only if there's just one field
        if len(fields) == 1:
            match = match_one_prefix('', value)
            if match:
                return format_output(match, value, False)

    # Trying single value first
    value = getattr(struct, field_name)
    match = match_value(value)
    if match:
        return match

    # Trying bit masks
    def extract_powers_of_2(x):
        i = 1
        while i <= x:
            if i & x:
                yield i
            i <<= 1

    matches = []
    for pow2 in extract_powers_of_2(value):
        match = match_value(pow2)
        if match:
            matches.append(match)
        else:
            matches = []
            break           # If at least one couldn't be matched, we're on a wrong track, stop
    if len(matches) > 0:
        return ' | '.join(matches)

    # No match could be found, returning the value as is
    return value


if __name__ == '__main__':
    # to_yaml()
    print(to_yaml(uavcan.protocol.NodeStatus()))

    info = uavcan.protocol.GetNodeInfo.Response(name='legion')
    info.hardware_version.certificate_of_authenticity = b'\x01\x02\x03\xff'
    print(to_yaml(info))

    lights = uavcan.equipment.indication.LightsCommand()
    lcmd = uavcan.equipment.indication.SingleLightCommand(light_id=123)
    lcmd.color.red = 1
    lcmd.color.green = 2
    lcmd.color.blue = 3
    lights.commands.append(lcmd)
    lcmd.light_id += 1
    lights.commands.append(lcmd)
    print(to_yaml(lights))

    print(to_yaml(uavcan.equipment.power.BatteryInfo()))
    print(to_yaml(uavcan.protocol.param.Empty()))

    getset = uavcan.protocol.param.GetSet.Response()
    print(to_yaml(getset))
    uavcan.switch_union_field(getset.value, 'empty')
    print(to_yaml(getset))

    # value_to_constant_name()
    print(value_to_constant_name(
        uavcan.protocol.NodeStatus(mode=uavcan.protocol.NodeStatus().MODE_OPERATIONAL),
        'mode'
    ))
    print(value_to_constant_name(
        uavcan.protocol.NodeStatus(mode=uavcan.protocol.NodeStatus().HEALTH_OK),
        'health'
    ))
    print(value_to_constant_name(
        uavcan.equipment.range_sensor.Measurement(reading_type=uavcan.equipment.range_sensor.Measurement()
                                                  .READING_TYPE_TOO_FAR),
        'reading_type'
    ))
    print(value_to_constant_name(
        uavcan.protocol.param.ExecuteOpcode.Request(opcode=uavcan.protocol.param.ExecuteOpcode.Request().OPCODE_ERASE),
        'opcode'
    ))
    print(value_to_constant_name(
        uavcan.protocol.file.Error(value=uavcan.protocol.file.Error().ACCESS_DENIED),
        'value'
    ))
    print(value_to_constant_name(
        uavcan.equipment.power.BatteryInfo(status_flags=
                                           uavcan.equipment.power.BatteryInfo().STATUS_FLAG_NEED_SERVICE),
        'status_flags'
    ))
    print(value_to_constant_name(
        uavcan.equipment.power.BatteryInfo(status_flags=
                                           uavcan.equipment.power.BatteryInfo().STATUS_FLAG_NEED_SERVICE |
                                           uavcan.equipment.power.BatteryInfo().STATUS_FLAG_TEMP_HOT |
                                           uavcan.equipment.power.BatteryInfo().STATUS_FLAG_CHARGED),
        'status_flags'
    ))
    print(value_to_constant_name(
        uavcan.protocol.AccessCommandShell.Response(flags=
                                                    uavcan.protocol.AccessCommandShell.Response().FLAG_SHELL_ERROR |
                                                    uavcan.protocol.AccessCommandShell.Response().
                                                    FLAG_HAS_PENDING_STDOUT),
        'flags'
    ))

    # Printing transfers
    node = uavcan.make_node('vcan0', node_id=42)
    node.request(uavcan.protocol.GetNodeInfo.Request(), 100, lambda e: print(to_yaml(e)))
    node.add_handler(uavcan.protocol.NodeStatus, lambda e: print(to_yaml(e)))
    node.spin()
