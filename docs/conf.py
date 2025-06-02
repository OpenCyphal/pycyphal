# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config
# type: ignore

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute.
import os
import re
import sys
import pathlib
import inspect
import datetime
import subprocess


GITHUB_USER_REPO = "OpenCyphal", "pycyphal"

DESCRIPTION = "A full-featured implementation of the Cyphal protocol stack in Python."

GIT_HASH = subprocess.check_output("git rev-parse HEAD", shell=True).decode().strip()

APIDOC_GENERATED_ROOT = pathlib.Path("api")
DOC_ROOT = pathlib.Path(__file__).absolute().parent
REPOSITORY_ROOT = DOC_ROOT.parent
DSDL_GENERATED_ROOT = REPOSITORY_ROOT / ".compiled"
sys.path.insert(0, str(REPOSITORY_ROOT))

import pycyphal

pycyphal.dsdl.add_import_hook([REPOSITORY_ROOT / "demo" / "public_regulated_data_types"], DSDL_GENERATED_ROOT)
import pycyphal.application  # This may trigger DSDL compilation.

assert "/site-packages/" not in pycyphal.__file__, "Wrong import source"

PACKAGE_ROOT = pathlib.Path(pycyphal.__file__).absolute().parent

EXTERNAL_LINKS = {
    "Homepage": "https://opencyphal.org/",
    "Support forum": "https://forum.opencyphal.org/",
}

# -- Project information -----------------------------------------------------

project = "PyCyphal"
# noinspection PyShadowingBuiltins
copyright = f"2019\u2013{datetime.datetime.now().year}, {pycyphal.__author__}"  # pylint: disable=redefined-builtin
author = pycyphal.__author__

# The short semantic version
version = ".".join(map(str, pycyphal.__version_info__))
# The full version, including alpha/beta/rc tags
release = pycyphal.__version__

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.doctest",
    "sphinx.ext.coverage",
    "sphinx.ext.linkcode",
    "sphinx.ext.todo",
    "sphinx.ext.intersphinx",
    "sphinx.ext.inheritance_diagram",
    "sphinx.ext.graphviz",
    "sphinx_computron",
    "ref_fixer_hack",
]
sys.path.append(str(DOC_ROOT))  # This is for the hack to be importable

# Add any paths that contain templates here, relative to this directory.
templates_path = []

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# The suffix(es) of source filenames.
source_suffix = [".rst"]

# The master toctree document.
master_doc = "index"

# Autodoc
autoclass_content = "class"
autodoc_member_order = "bysource"
autodoc_inherit_docstrings = False
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "special-members": True,
    "imported-members": True,
    "show-inheritance": True,
    "member-order": "bysource",
    "exclude-members": "__weakref__, __module__, __dict__, __dataclass_fields__, __dataclass_params__, "
    "__annotations__, __abstractmethods__, __orig_bases__, __parameters__, __post_init__, __getnewargs__",
}

# For sphinx.ext.todo_
todo_include_todos = True

graphviz_output_format = "svg"

inheritance_graph_attrs = {
    "rankdir": "LR",
    "bgcolor": '"transparent"',  # Transparent background works with any theme.
}
# Foreground colors are from the theme; keep them up to date, please.
inheritance_node_attrs = {
    "color": '"#000000"',
    "fontcolor": '"#000000"',
}
inheritance_edge_attrs = {
    "color": inheritance_node_attrs["color"],
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pydsdl": ("https://pydsdl.readthedocs.io/en/stable/", None),
    "can": ("https://python-can.readthedocs.io/en/stable/", None),
}

pygments_style = "monokai"

# -- Options for HTML output -------------------------------------------------

html_favicon = "static/favicon.ico"

html_theme = "sphinx_rtd_theme"

html_theme_options = {
    "display_version": True,
    "prev_next_buttons_location": "bottom",
    "style_external_links": True,
    "navigation_depth": -1,
}

html_context = {}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["static"]

html_css_files = [
    "custom.css",
]

# ----------------------------------------------------------------------------


# Inspired by https://github.com/numpy/numpy/blob/27b59efd958313491d51bc45d5ffdf1173b8f903/doc/source/conf.py#L311
def linkcode_resolve(domain: str, info: dict):
    def report_exception(exc: Exception) -> None:
        print(f"linkcode_resolve(domain={domain!r}, info={info!r}) exception:", repr(exc), file=sys.stderr)

    if domain != "py":
        return None

    obj = sys.modules.get(info["module"])
    for part in info["fullname"].split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError:
            return None
        except Exception as ex:
            report_exception(ex)
            return None

    obj = inspect.unwrap(obj)

    if isinstance(obj, property):  # Manual unwrapping for special cases
        obj = obj.fget or obj.fset

    fn = None
    try:
        fn = inspect.getsourcefile(obj)
    except TypeError:
        pass
    except Exception as ex:
        report_exception(ex)
    if not fn:
        return None

    path = os.path.relpath(fn, start=str(REPOSITORY_ROOT))
    try:
        source_lines, lineno = inspect.getsourcelines(obj)
        path += f"#L{lineno}-L{lineno + len(source_lines) - 1}"
    except OSError:
        pass
    except Exception as ex:
        report_exception(ex)

    return f"https://github.com/{GITHUB_USER_REPO[0]}/{GITHUB_USER_REPO[1]}/blob/{GIT_HASH}/{path}"


for p in map(str, [REPOSITORY_ROOT]):
    if os.environ.get("PYTHONPATH"):
        os.environ["PYTHONPATH"] += os.path.pathsep + p
    else:
        os.environ["PYTHONPATH"] = p

os.environ["SPHINX_APIDOC_OPTIONS"] = ",".join(k for k, v in autodoc_default_options.items() if v is True or v is None)

subprocess.check_call(
    [
        "sphinx-apidoc",
        "-o",
        str(APIDOC_GENERATED_ROOT),
        "-d1",  # Set :maxdepth:
        "--force",
        "--follow-links",
        "--separate",
        "--no-toc",
        str(PACKAGE_ROOT),
    ]
)
# We don't need the top-level page, it's maintained manually.
os.unlink(f"{APIDOC_GENERATED_ROOT}/{pycyphal.__name__}.rst")
