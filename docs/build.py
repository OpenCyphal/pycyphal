#!/usr/bin/env python
"""Build API docs using pdoc. Invoked via ``nox -s docs``."""

import ast
from datetime import datetime, timezone
import subprocess
from pathlib import Path
import pkgutil
import importlib
import sys

import pdoc
import pycyphal2

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIRECTORY = REPOSITORY_ROOT / "html_docs"
EXAMPLES_DIRECTORY = REPOSITORY_ROOT / "examples"
REPOSITORY_URL = "https://github.com/OpenCyphal/pycyphal"
REVISION = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
REPOSITORY_REVISION_ROOT_URL = f"{REPOSITORY_URL}/blob/{REVISION}"


def _load_summary(path: Path) -> str:
    try:
        module = ast.parse(path.read_text(encoding="utf8"), filename=str(path))
    except SyntaxError as ex:
        raise RuntimeError(f"Failed to parse example {path!s} while building docs") from ex
    doc = ast.get_docstring(module, clean=True)
    if not doc:
        return ""
    for line in doc.splitlines():
        text = line.strip()
        if text and not text.startswith("Usage:"):
            return text
    return ""


def _inject_examples_section() -> None:
    lines = ["## Examples", "", "Runnable examples:"]
    for path in sorted(EXAMPLES_DIRECTORY.rglob("*.py")):
        source = path.relative_to(REPOSITORY_ROOT).as_posix()
        summary = _load_summary(path)
        suffix = f" - {summary}" if summary else ""
        lines.append(
            f'- <a href="{REPOSITORY_REVISION_ROOT_URL}/{source}" '
            f'target="_blank" rel="noopener noreferrer"><code>{path.name}</code></a>{suffix}'
        )
    pycyphal2.__doc__ = pycyphal2.__doc__.rstrip() + "\n\n" + "\n".join(lines) + "\n"


def main() -> None:
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

    now = datetime.now(timezone.utc).isoformat(timespec='seconds')

    # Customization is necessary to expose special members like __aiter__, __call__, etc.
    # We also use it to tweak the colors.
    pdoc.render.configure(
        template_directory=Path(__file__).resolve().with_name("pdoc"),
        footer_text=f"{now} #{REVISION} v{pycyphal2.__version__}",
    )
    _inject_examples_section()
    pdoc.pdoc("pycyphal2", output_directory=OUTPUT_DIRECTORY)


if __name__ == "__main__":
    main()
