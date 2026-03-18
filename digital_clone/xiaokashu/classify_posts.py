#!/usr/bin/env python3
"""
Classify all posts in the database by type:
  - bond:       mentions 可转债 or related bond terms
  - investment: no bond mention but has investment/financial keywords
  - life:       everything else (personal, lifestyle, noise)
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "posts.db"

# Primary: bond-related keywords (any match → "bond")
BOND_KEYWORDS = [
    "转债", "下修", "强赎", "回售", "转股", "正股", "到期赎回",
    "可转换", "债底", "纯债价值", "转股价", "债券持有人",
]

# Secondary: investment-related (any match → "investment")
INVESTMENT_KEYWORDS = [
    "股票", "持仓", "估值", "买入", "卖出", "涨停", "跌停",
    "投资", "收益", "基金", "打新", "套利", "溢价", "折价",
    "仓位", "止损", "止盈", "分红", "股息", "净值", "申购",
    "赎回", "市值", "PE", "PB", "ROE", "年化", "复利",
    "牛市", "熊市", "抄底", "加仓", "减仓", "清仓", "建仓",
    "逆回购", "利率", "央行", "降息", "上市", "退市",
    "白银", "黄金", "指数", "大盘", "北交所", "新股",
    "行情", "策略", "对冲", "风险", "资产", "账户",
]


def classify(text: str) -> str:
    t = text.lower()
    for kw in BOND_KEYWORDS:
        if kw.lower() in t:
            return "bond"
    for kw in INVESTMENT_KEYWORDS:
        if kw.lower() in t:
            return "investment"
    return "life"


def main():
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()

    rows = c.execute("SELECT id, full_text FROM posts").fetchall()
    counts = {"bond": 0, "investment": 0, "life": 0}

    for post_id, text in rows:
        ptype = classify(text or "")
        c.execute("UPDATE posts SET post_type = ? WHERE id = ?", (ptype, post_id))
        counts[ptype] += 1

    conn.commit()
    total = sum(counts.values())
    print(f"Classified {total} posts:")
    for k, v in counts.items():
        print(f"  {k:12s}: {v:5d} ({v*100//total}%)")

    # Verify
    for ptype in ["bond", "investment", "life"]:
        row = c.execute(
            "SELECT time_text, substr(full_text, 1, 60) FROM posts WHERE post_type = ? ORDER BY time_text DESC LIMIT 3",
            (ptype,),
        ).fetchall()
        print(f"\n  [{ptype}] samples:")
        for t, txt in row:
            print(f"    [{t}] {txt}")

    conn.close()


if __name__ == "__main__":
    main()
