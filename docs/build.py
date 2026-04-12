#!/usr/bin/env python
"""Build API docs using pdoc. Invoked via ``nox -s docs``."""

import ast
import shutil
from pathlib import Path
import pkgutil
import importlib
import sys

import pdoc
import pycyphal2

OUTPUT_DIRECTORY = Path("html_docs")
EXAMPLES_DIRECTORY = Path("examples")


def _discover_examples(directory: Path) -> list[Path]:
    if not directory.is_dir():
        raise RuntimeError(f"Examples directory {directory!s} not found while building docs")
    examples = sorted(path for path in directory.rglob("*.py") if path.is_file())
    if not examples:
        raise RuntimeError(f"No example scripts found under {directory!s} while building docs")
    return examples


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


def _make_examples_section(examples: list[Path]) -> str:
    if not examples:
        return ""
    lines = ["## Examples", "", "Runnable examples:"]
    for path in examples:
        relative = path.relative_to(EXAMPLES_DIRECTORY).as_posix()
        summary = _load_summary(path)
        suffix = f" - {summary}" if summary else ""
        lines.append(f"- [`examples/{relative}`](examples/{relative}){suffix}")
    return "\n".join(lines) + "\n"


def _inject_examples_section(examples: list[Path]) -> None:
    section = _make_examples_section(examples)
    doc = pycyphal2.__doc__ or ""
    pycyphal2.__doc__ = doc.rstrip() + f"\n\n{section}"


def _copy_examples(examples_source: Path, output_directory: Path, examples: list[Path]) -> None:
    destination = output_directory / examples_source.name
    shutil.rmtree(destination, ignore_errors=True)
    if not examples:
        return
    for source in examples:
        target = destination / source.relative_to(examples_source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


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

    # Customization is necessary to expose special members like __aiter__, __call__, etc.
    # We also use it to tweak the colors.
    pdoc.render.configure(template_directory=Path(__file__).resolve().with_name("pdoc"))
    examples = _discover_examples(EXAMPLES_DIRECTORY)
    _inject_examples_section(examples)
    pdoc.pdoc("pycyphal2", output_directory=OUTPUT_DIRECTORY)
    _copy_examples(EXAMPLES_DIRECTORY, OUTPUT_DIRECTORY, examples)


if __name__ == "__main__":
    main()
