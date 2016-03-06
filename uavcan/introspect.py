#
# Copyright (C) 2014-2016  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#         Ben Dyer <ben_dyer@mac.com>
#

import uavcan


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
