#!/usr/bin/env python
"""Build API docs using pdoc. Invoked via ``nox -s docs``."""

from pathlib import Path
import pkgutil
import importlib
import sys

import pycyphal2

# Discover and import all public submodules so pdoc can see them,
# then inject them into their parent's __all__ so pdoc lists them in the sidebar.
# Public modules are expected to be importable in the docs environment; failures are treated as hard errors.
for mi in pkgutil.walk_packages(pycyphal2.__path__, pycyphal2.__name__ + "."):
    leaf = mi.name.rsplit(".", 1)[-1]
    if leaf.startswith("_"):
        continue
    try:
        importlib.import_module(mi.name)
    except Exception as ex:
        raise RuntimeError(f"Failed to import public module {mi.name!r} while building docs") from ex
    parent = sys.modules[mi.name.rsplit(".", 1)[0]]
    if hasattr(parent, "__all__") and leaf not in parent.__all__:
        parent.__all__.append(leaf)

import pdoc

# Customization is necessary to expose special members like __aiter__, __call__, etc.
# We also use it to tweak the colors.
pdoc.render.configure(template_directory=Path(__file__).resolve().with_name("pdoc"))
pdoc.pdoc("pycyphal2", output_directory=Path("html_docs"))
