# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

VIRTUAL_BUS_URI = "socket://127.0.0.1:50905"
"""
Using ``localhost`` may significantly increase initialization latency on Windows due to slow DNS lookup.
"""
