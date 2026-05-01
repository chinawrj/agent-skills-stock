"""filter_soe.py — 从 universe 中剔除国企/央企 (rat-trader-screener M2 step 1).

输入数据来源（按优先级）：
  1. ``--in`` 指定的 parquet（含 code/name 至少；可含 controller/controller_type）
  2. DuckDB 表 stocks（来自 db-manager）
  3. db-manager cache `stocks.csv`（离线兜底）

输出：``data/universe_non_soe.parquet``，字段 code / name / market（如有）

判定规则（来自 .github/skills/soe-filter/SKILL.md）：
  R1: controller_type ∈ {国务院国资委, 地方国资委, 中央国家机关, 财政部, 中央汇金}
  R2: controller 含 国资委|国务院|中央汇金|财政部|中投公司|中央企业|全民所有制
  R3: name 前缀属 中国/中央/国家/中铁/中船/中粮/中核/中航/中冶/中建/中交/中电/中煤/中盐/中钢/中化/中远/中国五矿/中国黄金
  R4: is_state_owned 显式为 True

任一命中 → 剔除。controller 缺失且名称前缀不命中 → 保留并写入 _review_ownership.csv 供人工复核。
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd

LOGGER = logging.getLogger("filter_soe")

DEFAULT_DB = "data/a-share.db"
DEFAULT_CACHE_CSV = ".github/skills/db-manager/cache/stocks.csv"
DEFAULT_OUT = "data/universe_non_soe.parquet"
DEFAULT_REVIEW = "data/_review_ownership.csv"

SOE_NAME_PREFIX = re.compile(
    r"^(中国|中央|国家|中铁|中船|中粮|中核|中航|中冶|中建|中交|中电|中煤|中盐|中钢|中化|中远|中国五矿|中国黄金)"
)
SOE_CTRL_RE = re.compile(r"国资委|国务院|中央汇金|财政部|中投公司|中央企业|全民所有制")
SOE_TYPES = {"国务院国资委", "地方国资委", "中央国家机关", "财政部", "中央汇金"}


def _classify(row: pd.Series, has_ctrl: bool = True) -> str:
    """Return one of: 'soe' / 'non_soe' / 'review'.

    has_ctrl: 输入数据是否提供 controller / controller_type 列。
              False 时降级为 name-only 模式：仅用 R3 名称前缀判断，命中 → soe，
              否则 → non_soe（不进 review，避免数据缺失把整池打空）。
    """
    name = str(row.get("name") or "")
    is_state_owned = row.get("is_state_owned")

    # R4 显式标志（无论是否有 controller）
    if isinstance(is_state_owned, bool) and is_state_owned:
        return "soe"

    if not has_ctrl:
        # 降级模式：只看名称
        return "soe" if (name and SOE_NAME_PREFIX.match(name)) else "non_soe"

    controller = str(row.get("controller") or "").strip()
    controller_type = str(row.get("controller_type") or "").strip()
    # R1
    if controller_type and controller_type in SOE_TYPES:
        return "soe"
    # R2
    if controller and SOE_CTRL_RE.search(controller):
        return "soe"
    # R3
    if name and SOE_NAME_PREFIX.match(name):
        return "soe"
    # 数据缺失：controller 全空 → review；否则按非国企保留
    if not controller and not controller_type:
        return "review"
    return "non_soe"


def filter_soe(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """返回 (non_soe, soe, review) 三个 DataFrame。"""
    if df.empty:
        return df, df, df
    work = df.copy()
    has_ctrl = ("controller" in work.columns) or ("controller_type" in work.columns)
    if not has_ctrl:
        LOGGER.warning(
            "输入缺少 controller/controller_type 列 → 降级 name-only 模式（FB-005）"
        )
    work["_label"] = work.apply(lambda r: _classify(r, has_ctrl=has_ctrl), axis=1)
    non_soe = work[work["_label"] == "non_soe"].drop(columns=["_label"]).reset_index(drop=True)
    soe = work[work["_label"] == "soe"].drop(columns=["_label"]).reset_index(drop=True)
    review = work[work["_label"] == "review"].drop(columns=["_label"]).reset_index(drop=True)
    return non_soe, soe, review


def _load_universe(args: argparse.Namespace) -> pd.DataFrame:
    if args.input:
        path = Path(args.input)
        LOGGER.info("从 parquet 读 universe: %s", path)
        return pd.read_parquet(path)
    db = Path(args.db)
    if db.exists():
        LOGGER.info("从 DuckDB 读 stocks: %s", db)
        con = duckdb.connect(str(db))
        try:
            cols = {
                r[1] for r in con.execute("PRAGMA table_info('stocks')").fetchall()
            }
            select_cols = ["code", "name"]
            for opt in ("market", "controller", "controller_type", "is_state_owned"):
                if opt in cols:
                    select_cols.append(opt)
            sql = f"SELECT {', '.join(select_cols)} FROM stocks"
            return con.execute(sql).fetchdf()
        finally:
            con.close()
    cache = Path(args.cache_csv)
    if cache.exists():
        LOGGER.info("DB 不存在，回退到 cache CSV: %s", cache)
        return pd.read_csv(cache, dtype={"code": str})
    raise FileNotFoundError(
        f"找不到 universe 数据：--in / DB({db}) / cache({cache}) 全部缺失"
    )


def cmd_run(args: argparse.Namespace) -> int:
    df = _load_universe(args)
    LOGGER.info("universe 大小: %d 行", len(df))
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(6)

    non_soe, soe, review = filter_soe(df)
    LOGGER.info(
        "剔除 %d / 保留 %d / 待复核 %d", len(soe), len(non_soe), len(review)
    )

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    non_soe.to_parquet(out, index=False)
    LOGGER.info("写入非国企 universe: %s (%d 行)", out, len(non_soe))

    review_path = Path(args.review)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review.to_csv(review_path, index=False)
    LOGGER.info("待复核（默认保留）: %s (%d 行)", review_path, len(review))

    # 锚定 sanity：300401 必须在非国企集合
    ref = "300401"
    if "code" in non_soe.columns and ref not in set(non_soe["code"]):
        LOGGER.error("⚠ Reference 300401 未在非国企集合中！请检查规则。")
        return 1
    LOGGER.info("✓ 300401 在 non_soe 集合中")
    return 0


def cmd_self_test(_args: argparse.Namespace) -> int:
    cases = [
        # (row, expected_label)
        ({"code": "300401", "name": "花园生物", "controller": "邵钦祥", "controller_type": "自然人"}, "non_soe"),
        ({"code": "601857", "name": "中国石油", "controller": "中国石油天然气集团", "controller_type": "国务院国资委"}, "soe"),
        ({"code": "600118", "name": "中国卫星", "controller": "", "controller_type": ""}, "soe"),  # R3 名称
        ({"code": "002460", "name": "赣锋锂业", "controller": "李良彬", "controller_type": "自然人"}, "non_soe"),
        ({"code": "688981", "name": "中芯国际", "controller": "", "controller_type": ""}, "review"),  # 缺数据无前缀
        ({"code": "600519", "name": "贵州茅台", "controller": "贵州省国资委", "controller_type": "地方国资委"}, "soe"),
        ({"code": "999999", "name": "测试民营", "controller": "张三", "controller_type": "自然人", "is_state_owned": False}, "non_soe"),
        ({"code": "888888", "name": "测试央企", "controller": "X集团", "controller_type": "X", "is_state_owned": True}, "soe"),
    ]
    df = pd.DataFrame([c[0] for c in cases])
    expected = [c[1] for c in cases]
    df["_label"] = df.apply(lambda r: _classify(r, has_ctrl=True), axis=1)
    fails = []
    for i, exp in enumerate(expected):
        got = df.loc[i, "_label"]
        if got != exp:
            fails.append((df.loc[i, "code"], df.loc[i, "name"], exp, got))
    if fails:
        for f in fails:
            LOGGER.error("FAIL %s %s expected=%s got=%s", *f)
        return 1
    non_soe, soe, review = filter_soe(df.drop(columns=["_label"]))
    LOGGER.info("self-test counts: non_soe=%d soe=%d review=%d", len(non_soe), len(soe), len(review))
    print("SELF_TEST_PASS: filter_soe")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="剔除国企/央企，输出非国企 universe")
    p.add_argument("--in", dest="input", default=None, help="输入 parquet，覆盖 DB / cache")
    p.add_argument("--db", default=DEFAULT_DB, help=f"DuckDB 路径 (默认 {DEFAULT_DB})")
    p.add_argument("--cache-csv", default=DEFAULT_CACHE_CSV, help="离线兜底 stocks.csv")
    p.add_argument("--out", dest="output", default=DEFAULT_OUT, help=f"输出 parquet (默认 {DEFAULT_OUT})")
    p.add_argument("--review", default=DEFAULT_REVIEW, help=f"待复核 CSV (默认 {DEFAULT_REVIEW})")
    p.add_argument("--self-test", action="store_true", help="合成数据自检")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    if args.self_test:
        return cmd_self_test(args)
    return cmd_run(args)


if __name__ == "__main__":
    sys.exit(main())
