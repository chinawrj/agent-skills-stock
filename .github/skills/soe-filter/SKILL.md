---
name: soe-filter
description: 国企/央企股票剔除工具。当用户询问排除国企、过滤央企、剔除国资控股、保留民营外资、剔除中字头股票时使用此技能。从 universe 中剔除实控人为国资委/央企集团/财政部/中央汇金的股票，作为 rat-trader-screener 流水线的第一步过滤器。
---

# SOE Filter — 国企/央企剔除

老鼠仓股票几乎只出现在治理较灵活的民营/外资公司——国资控股标的因合规、决策机制几乎不可能出现"机构小金库"型筹码运作。本 skill 在筛股流水线**第一步**剔除国企/央企。

## 触发条件

当用户：
- 想从 A 股 universe 中排除国企/央企
- 需要保留民营/外资/集体所有制控股的标的
- 在做老鼠仓 / 庄股筛选的预处理

## 输入

- DuckDB 表 `company_basic(code, name, controller, controller_type, is_state_owned, ...)`  
  或上游 parquet `data/company_info.parquet`

## 判定规则

满足任一即视为国企/央企，**剔除**：

1. `controller_type` ∈ `{国务院国资委, 地方国资委, 中央国家机关, 财政部, 中央汇金}`
2. `controller` 名称含正则：`国资委|国务院|中央汇金|财政部|中投公司|中央企业|全民所有制`
3. 公司名称前缀属：`中国|中央|国家|中铁|中船|中粮|中核|中航|中冶|中建|中交|中电|中煤|中盐|中钢|中化|中远|中国五矿|中国黄金`
4. 显式标志位 `is_state_owned == True`

## 命令用法

```bash
cd /Users/rjwang/fun/agent-skills-stock && source .venv/bin/activate

python .github/skills/soe-filter/scripts/filter_soe.py \
    --in  data/company_info.parquet \
    --out data/universe_non_soe.parquet \
    --review data/_review_ownership.csv
```

## 输出

- `universe_non_soe.parquet` — 保留的非国资股
- `_review_ownership.csv` — controller 字段缺失/无法判定的股票，供人工复核（**默认剔除**，保守）

打印示例：
```
剔除 1842 / 保留 3215 / 待复核 47
```

## 边界情况

- **混改公司** 国资 < 50% 但仍是第一大股东 → 视为国企（保守）
- **数据缺失** controller 为空 → 用名称前缀兜底；仍判定不了写到 `_review_ownership.csv`
- **更名股票** 如 *ST/退市 → 按当前最新名称判断

## Self-Test（自检）

```bash
python3 - <<'PY' && echo "SELF_TEST_PASS: rules" || echo "SELF_TEST_FAIL: rules"
import re
SOE_NAME_PREFIX = re.compile(r'^(中国|中央|国家|中铁|中船|中粮|中核|中航|中冶|中建|中交)')
SOE_CTRL_RE     = re.compile(r'国资委|国务院|中央汇金|财政部|中投公司|中央企业|全民所有制')
SOE_TYPES       = {'国务院国资委','地方国资委','中央国家机关','财政部','中央汇金'}
cases = [
  ({'name':'花园生物','controller':'邵钦祥','controller_type':'自然人'}, False),
  ({'name':'中国石油','controller':'国务院国资委','controller_type':'国务院国资委'}, True),
  ({'name':'中航机电','controller':'中航工业集团','controller_type':'国务院国资委'}, True),
  ({'name':'宁德时代','controller':'曾毓群','controller_type':'自然人'}, False),
  ({'name':'某科技','controller':'','controller_type':''}, False),
]
for row, expected in cases:
    soe = bool(row['controller_type'] in SOE_TYPES
               or (row['controller'] and SOE_CTRL_RE.search(row['controller']))
               or SOE_NAME_PREFIX.match(row['name']))
    assert soe == expected, (row, soe, expected)
print("ok")
PY
```

## Blind Test（盲测）

**Prompt:**
```
读完此 Skill，写 filter_soe.py：
1) 实现 is_soe(row) 函数覆盖 4 类规则；
2) controller 为空时用名称前缀兜底；
3) 仍判定不明的写入 _review_ownership.csv 但默认剔除；
4) stdout 打印剔除/保留/待复核 三个计数。
```

**验收标准:**
- [ ] 花园生物 (300401) 保留 / 中国石油 (601857) 剔除
- [ ] 4 条规则全部命中
- [ ] 待复核 CSV 落盘
- [ ] 计数行格式正确

## 成功标准

- [ ] 全市场约 30-50% 的股票被剔除（A 股大致比例）
- [ ] 保留集合不含名称以"中国/国家"开头的票
- [ ] 输出与下游 `hkscc-screener` 字段兼容
