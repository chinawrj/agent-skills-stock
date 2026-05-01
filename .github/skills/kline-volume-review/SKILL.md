---
name: kline-volume-review
description: M4 复盘层 — 渲染候选股 K 线 + 成交量图，并标注 t1/t2/t3 季度边界，供人工复盘"加仓-减仓-再加仓"行为是否得到 K 线/成交量印证。
---

# kline-volume-review — K 线 + 成交量复盘

## 用途

`rat-pattern-detector` 输出 `candidates_rat_pattern.parquet` (BCD 命中股) +
`_diag_rat_pattern.json` (含每股 hit triple 的 t1/t2/t3 季度)。本 skill
读取这两份产物 + DuckDB `kline_daily`，为每个候选股渲染：

1. 上图：日 K 线（前复权 close + 250 日均线）
2. 下图：日成交量柱
3. 在 t1/t2/t3 季度起止处画竖线 + 季度文字标签
4. 在减仓后 60 日窗口画"卖飞校验"参考线（post_ret_60d）

输出 PNG 嵌入 `reports/rat_candidates_YYYYMMDD.md`，使 markdown 报告自包含 K 线证据。

## 入口

```bash
python .github/skills/kline-volume-review/scripts/render_kline.py \
    [--parquet data/candidates_rat_pattern.parquet] \
    [--diag data/_diag_rat_pattern.json] \
    [--db data/a-share.db] \
    [--out-dir reports/figures] \
    [--lookback-days 500]
```

每只候选股输出 `reports/figures/{code}_{YYYYMMDD}.png`。

## 默认参数

```python
LOOKBACK_DAYS = 500           # K 线回看窗口（覆盖 t1 之前 ~2 年）
MA_WINDOW = 250               # 均线
FIG_W = 12
FIG_H = 6                     # 上 K 下 vol 4:2 比例
```

## 输出约定

- 每只候选股一个 PNG，文件名 `{code}_{run_date}.png`
- 不依赖网络，所有数据从 DuckDB `kline_daily` 拉
- 缺数据时跳过该股，logging.warning，**不 raise**（不阻塞批量）

## 与 render_rat_report 的协作（M5）

`tools/render_rat_report.py` 读取 figures 目录，把候选表中每行下方插入
`![{code}](figures/{code}_{date}.png)` 引用，使 markdown 报告自包含视觉证据。

## 状态

- 当前：骨架（SKILL.md + 占位 render_kline.py 仅写 self-test 回显）
- 下一步：matplotlib 实装 + 接入 render_rat_report
