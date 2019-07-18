#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import pathlib
import coverage

own_path = pathlib.Path(__file__).absolute()
print(f'  LOADING {own_path}  '.center(80, '-'))

# Profiling configuration; see https://coverage.readthedocs.io/en/coverage-4.2/subprocess.html
# This is kind of a big gun because it makes us profile everything we run, even doc generation, but it's acceptable.
os.environ['COVERAGE_PROCESS_START'] = str(own_path.parent / 'setup.cfg')
coverage.process_startup()
