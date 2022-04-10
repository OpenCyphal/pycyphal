#!/usr/bin/env python
# Distributed under CC0 1.0 Universal (CC0 1.0) Public Domain Dedication.
# type: ignore
"""
A simplified setup.py demo that shows how to distribute compiled DSDL definitions with Python packages.
"""

import setuptools
import logging
import distutils.command.build_py
from pathlib import Path

NAME = "demo_app"


# noinspection PyUnresolvedReferences
class BuildPy(distutils.command.build_py.build_py):
    def run(self):
        import pycyphal

        pycyphal.dsdl.compile_all(
            [
                "public_regulated_data_types/uavcan",  # All Cyphal applications need the standard namespace, always.
                "custom_data_types/sirius_cyber_corp",
                # "public_regulated_data_types/reg",  # Many applications also need the non-standard regulated DSDL.
            ],
            output_directory=Path(self.build_lib, NAME, ".demo_dsdl_compiled"),  # Store in the build output archive.
        )
        super().run()


logging.basicConfig(level=logging.INFO, format="%(levelname)-3.3s %(name)s: %(message)s")

setuptools.setup(
    name=NAME,
    py_modules=["demo_app"],
    cmdclass={"build_py": BuildPy},
)
