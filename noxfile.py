from __future__ import annotations

import nox

nox.options.sessions = ["test", "mypy", "format"]


@nox.session(python=["3.11", "3.12", "3.13"])
def test(session: nox.Session) -> None:
    session.install(".[test]", "coverage")
    session.run("coverage", "run", "-m", "pytest", "tests/", *session.posargs)
    session.run("coverage", "report")
    session.run("coverage", "html")


@nox.session(python="3.12")
def mypy(session: nox.Session) -> None:
    session.install(".[dev]")
    session.run("mypy", "src/pycyphal")


@nox.session(python="3.12")
def format(session: nox.Session) -> None:
    session.install("black")
    session.run("black", "--check", "--diff", "src", "tests")
