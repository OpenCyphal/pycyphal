#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import pyuavcan

# Version-specific so that we won't attempt to restore transfer-ID maps stored from another version.
EMITTED_TRANSFER_ID_MAP_DIR = pyuavcan.VERSION_SPECIFIC_DATA_DIR / 'emitted-transfer-id-maps'
# This is not a path but a related parameter so it's kept here. Files older that this are not used.
EMITTED_TRANSFER_ID_MAP_MAX_AGE = 60.0  # [second]

DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL = \
    'https://github.com/UAVCAN/public_regulated_data_types/archive/a532bfa7.zip'
