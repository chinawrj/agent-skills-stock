"""Smoke test for run_rat_screener.py --dry-run.

Verifies the pipeline runner can parse args, resolve all step scripts,
and print the dry-run plan without requiring any real data or DB.
No external network calls; safe for CI.
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNNER = ROOT / "tools" / "run_rat_screener.py"


def _run(extra_args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(RUNNER)] + extra_args,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )


def test_dry_run_exits_zero():
    """--dry-run should print plan and exit 0 without touching any files."""
    result = _run(["--dry-run"])
    assert result.returncode == 0, (
        f"--dry-run exited {result.returncode}\nstdout={result.stdout}\nstderr={result.stderr}"
    )


def test_dry_run_shows_steps():
    """--dry-run output should mention each pipeline step."""
    result = _run(["--dry-run"])
    for step in ("fetch_hkscc", "screen_hkscc", "detect", "render_kline", "report"):
        assert step in result.stdout or step in result.stderr, (
            f"Step '{step}' not found in dry-run output"
        )


def test_status_exits_zero():
    """--status should exit 0 even when DB / parquet files are absent."""
    result = _run(["--status"])
    assert result.returncode == 0, (
        f"--status exited {result.returncode}\nstdout={result.stdout}\nstderr={result.stderr}"
    )


def test_help_exits_zero():
    """--help should exit 0."""
    result = _run(["--help"])
    assert result.returncode == 0
