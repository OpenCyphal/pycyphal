from __future__ import annotations

import shutil
from pathlib import Path
import nox

nox.options.sessions = ["test", "mypy", "lint", "format"]

PYTHONS = ["3.11", "3.12", "3.13"]


@nox.session(python=False, default=False)
def clean(session):
    pats = ["dist", "build", "html*", ".coverage*", ".*cache", "src/*.egg-info", "*.log", "*.tmp", ".nox"]
    for w in pats:
        for f in Path.cwd().glob(w):
            session.log(f"Removing: {f}")
            if f.is_dir():
                shutil.rmtree(f, ignore_errors=True)
            else:
                f.unlink(missing_ok=True)
    for f in Path.cwd().rglob("__pycache__"):
        session.log(f"Removing: {f}")
        shutil.rmtree(f, ignore_errors=True)


@nox.session(python=PYTHONS)
def test(session: nox.Session) -> None:
    session.install("-e", ".[udp,pythoncan]", "pytest", "pytest-asyncio", "pytest-timeout", "coverage")
    session.run("coverage", "run", "-m", "pytest", "--timeout=60", "tests/", *session.posargs)
    session.run("coverage", "report")
    session.run("coverage", "html")


@nox.session(python=PYTHONS[0])
def mypy(session: nox.Session) -> None:
    session.install(".[udp,pythoncan]", "mypy", "pytest", "pytest-asyncio")
    session.run("mypy", "src/pycyphal2", "tests")


@nox.session(python=PYTHONS[0])
def lint(session: nox.Session) -> None:
    session.install("ruff")
    session.run("ruff", "check", "src", "tests", "examples")


@nox.session(python=PYTHONS[0])
def format(session: nox.Session) -> None:
    session.install("black")
    session.run("black", "--check", "--diff", "src", "tests", "examples")


@nox.session(python=PYTHONS[0], reuse_venv=True)
def docs(session: nox.Session) -> None:
    session.install("-e", ".[udp,pythoncan]", "pdoc")
    session.run("python", "docs/build.py")
    session.log("Docs written to html_docs/")


@nox.session(python=PYTHONS[0], default=False)
def examples(session: nox.Session) -> None:
    import json as _json
    import subprocess
    import sys
    import time

    if sys.platform == "darwin":
        session.skip("Examples smoke is skipped on macOS")

    session.install(".[udp]")
    topic = "demo/time"
    python = shutil.which("python", path=session.bin)
    assert python is not None

    def terminate_process(proc: subprocess.Popen[str] | None) -> None:
        if proc is None or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

    def run_case(label: str, extra_args: list[str]) -> None:
        session.log(f"--- examples smoke: {label} ---")
        sub_proc = subprocess.Popen(
            [python, "examples/subscribe_demo.py", topic, "--timeout", "10", *extra_args],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(1)  # let the subscriber set up

        session.run(python, "examples/publish_time.py", topic, "--count", "3", *extra_args, external=True)
        time.sleep(1)  # let the last message propagate

        sub_proc.terminate()
        stdout, _ = sub_proc.communicate(timeout=5)
        lines = [ln for ln in stdout.decode().splitlines() if ln.strip()]
        session.log(f"Subscriber captured {len(lines)} line(s)")
        assert len(lines) >= 1, f"Expected at least 1 JSONL line, got {len(lines)}"
        for ln in lines:
            obj = _json.loads(ln)
            assert "ts" in obj
            assert "remote_id" in obj
            assert "topic" in obj
            assert "message_b64" in obj

    def run_streaming_case() -> None:
        session.log("--- examples smoke: streaming ---")
        server_proc = None
        try:
            server_proc = subprocess.Popen(
                [python, "examples/streaming_server.py"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            time.sleep(1)
            client_proc = subprocess.Popen(
                [python, "examples/streaming_client.py", "--count=3", "--period=0.2"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            client_stdout, client_stderr = client_proc.communicate(timeout=20)
            assert client_proc.returncode == 0, f"Streaming client failed: {client_stderr}"
            time.sleep(1)
            assert server_proc.poll() is None, "Streaming server exited unexpectedly"
        finally:
            terminate_process(server_proc)
        _, server_stderr = server_proc.communicate(timeout=5)
        lines = [ln for ln in client_stdout.splitlines() if ln.strip()]
        session.log(f"Streaming client captured {len(lines)} line(s)")
        assert len(lines) == 2, f"Expected 2 JSONL responses, got {len(lines)}"
        objs = [_json.loads(ln) for ln in lines]
        assert [obj["seqno"] for obj in objs] == [0, 1]
        assert len({obj["remote_id"] for obj in objs}) == 1
        for obj in objs:
            assert "ts" in obj
            assert "stream_id" in obj
            assert "requested_count" in obj
            assert "period" in obj
            assert "remaining" in obj
            assert "sent_at" in obj

    run_case("udp", [])
    if sys.platform == "linux" and Path("/sys/class/net/vcan0").exists():
        run_case("socketcan:vcan0", ["--transport", "socketcan:vcan0"])
    else:
        session.log("Skipping socketcan:vcan0 case (vcan0 not available)")
    run_streaming_case()
