# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute.
import sys
import pathlib

REPOSITORY_ROOT: pathlib.Path = pathlib.Path(__file__).absolute().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT / 'pyuavcan'))

import pyuavcan

# -- Project information -----------------------------------------------------

project = 'PyUAVCAN'
# noinspection PyShadowingBuiltins
copyright = '2019 \u00A9 UAVCAN Development Team'
author = 'UAVCAN Development Team'

# The documentation is versioned synchronously with PyUAVCAN, not the CLI.
# The short X.Y version
version = '.'.join(map(str, pyuavcan.__version_info__[:2]))
# The full version, including alpha/beta/rc tags
release = pyuavcan.__version__

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.coverage',
    'sphinx.ext.linkcode',
    'sphinx.ext.todo',
    'sphinxarg.ext',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# The suffix(es) of source filenames.
source_suffix = ['.rst']

# The master toctree document.
master_doc = 'index'

# Classes should inherit documentation from ancestors.
autodoc_inherit_docstrings = True

# Use the order members of classes and modules appear in source for
# the documentation (as opposed to alphabetical).
autodoc_member_order = 'bysource'

# For sphinx.ext.todo_
todo_include_todos = True

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'sphinx_rtd_theme'

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
}

html_context = {
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# ----------------------------------------------------------------------------


def linkcode_resolve(domain, info):
    print('PLEASE IMPLEMENT: linkcode_resolve() in conf.py', file=sys.stderr)
    return None
