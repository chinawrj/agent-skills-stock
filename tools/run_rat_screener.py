"""
M5 一键化 runner — rat-trader-screener 全流水线串联

steps:
  1. fetch_hkscc       (--symbols, optional, --skip-fetch 跳过)
  2. build_mcap_snapshot
  3. hkscc_quarterly
  4. screen_hkscc      (输出 candidates_hkscc.parquet)
  5. fetch_kline       (从 candidates_hkscc.parquet 读 codes，补 2022 起历史 K 线)
  6. detect_rat_pattern (输出 candidates_rat_pattern.parquet + _diag_rat_pattern.json)
  7. render_kline      (渲染 K 线 PNG)
  8. render_rat_report  (输出 reports/rat_candidates_YYYYMMDD.md)

每一步用 subprocess 调用既有 skill 脚本，不重写算法。
"""
from __future__ import annotations
import argparse
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKILLS = ROOT / ".github" / "skills"

STEPS = {
    "fetch_hkscc": SKILLS / "hkscc-screener" / "scripts" / "fetch_hkscc.py",
    "hkscc_quarterly": SKILLS / "hkscc-screener" / "scripts" / "hkscc_quarterly.py",
    "build_mcap": SKILLS / "db-manager" / "build_mcap_snapshot.py",
    "screen_hkscc": SKILLS / "hkscc-screener" / "scripts" / "screen_hkscc.py",
    "fetch_kline": SKILLS / "rat-pattern-detector" / "scripts" / "fetch_kline.py",
    "detect": SKILLS / "rat-pattern-detector" / "scripts" / "detect_rat_pattern.py",
    "render_kline": SKILLS / "kline-volume-review" / "scripts" / "render_kline.py",
    "report": ROOT / "tools" / "render_rat_report.py",
}


def run(name: str, script: Path, extra: list[str], log: logging.Logger) -> None:
    if not script.exists():
        raise FileNotFoundError(f"step {name}: {script} 不存在")
    cmd = [sys.executable, str(script), *extra]
    log.info("→ %s: %s", name, " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(ROOT))
    if proc.returncode != 0:
        raise SystemExit(f"step {name} 失败 (exit={proc.returncode})")


def main() -> None:
    ap = argparse.ArgumentParser(description="rat-trader 一键化流水线")
    ap.add_argument("--skip-fetch", action="store_true", help="跳过 fetch_hkscc（数据已是最新）")
    ap.add_argument("--skip-kline-fetch", action="store_true", help="跳过 fetch_kline（kline 已是最新）")
    ap.add_argument("--kline-start", default="2022-01-01", help="fetch_kline 起始日期（默认覆盖到 2022）")
    ap.add_argument("--symbols", default=None, help="fetch_hkscc 的 --symbols 透传")
    ap.add_argument("--strict", action="store_true", default=True, help="detect 用 strict 模式")
    ap.add_argument("--require-ref", action="store_true", default=True,
                    help="detect 要求 t1->t2->t3 与 hkscc_quarterly 完整对齐")
    ap.add_argument("--skip-report", action="store_true", help="跳过 markdown 报告渲染")
    ap.add_argument("--skip-figures", action="store_true", help="跳过 K 线图渲染")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    log = logging.getLogger("run_rat_screener")
    t0 = datetime.now()
    log.info("==== rat-trader-screener 一键流水线 启动 ====")

    if args.skip_fetch:
        log.info("跳过 fetch_hkscc（--skip-fetch）")
    else:
        extra = []
        if args.symbols:
            extra += ["--symbols", args.symbols]
        run("fetch_hkscc", STEPS["fetch_hkscc"], extra, log)

    run("build_mcap", STEPS["build_mcap"], [], log)
    run("hkscc_quarterly", STEPS["hkscc_quarterly"], [], log)
    run("screen_hkscc", STEPS["screen_hkscc"], [], log)

    if args.skip_kline_fetch:
        log.info("跳过 fetch_kline（--skip-kline-fetch）")
    else:
        kline_extra = [
            "--from-parquet", "data/candidates_hkscc.parquet",
            "--start", args.kline_start,
        ]
        run("fetch_kline", STEPS["fetch_kline"], kline_extra, log)

    detect_extra: list[str] = []
    if args.require_ref:
        detect_extra.append("--require-ref")
    if args.strict:
        detect_extra.append("--strict")
    run("detect", STEPS["detect"], detect_extra, log)

    if not args.skip_figures:
        run("render_kline", STEPS["render_kline"], [], log)

    if not args.skip_report:
        run("report", STEPS["report"], [], log)

    elapsed = (datetime.now() - t0).total_seconds()
    log.info("==== 完成 elapsed=%.1fs ====", elapsed)


if __name__ == "__main__":
    main()
