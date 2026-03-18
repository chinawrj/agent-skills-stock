#!/usr/bin/env python3
"""
每日数据库更新 — A股全市场统一入口

盘后运行一次（15:30后），完成当日全部数据更新：
  Step 1: 股票行情快照 (Sina Finance) → daily_market + klines 追加
  Step 2: 可转债行情 (东方财富) → bonds 市场数据
  Step 3: 分析指标 (本地SQL) → bonds 触发进度 + 盈利状态

核心理念: 每日追加取代历史批量扫描
  - klines 每天追加全市场一行，自然积累历史
  - 再也不需要 import_klines.py / rebuild_db.py 全量重建

用法:
    cd /Users/rjwang/fun/a-share && source .venv/bin/activate

    python db/daily_update.py                   # 完整更新 (3步全跑)
    python db/daily_update.py --step stocks     # 仅股票行情
    python db/daily_update.py --step bonds      # 仅可转债
    python db/daily_update.py --step analysis   # 仅分析计算
    python db/daily_update.py --dry-run         # 仅获取+保存CSV
    python db/daily_update.py --date 2026-03-18 # 指定日期
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime

import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_PATH = os.path.abspath(os.path.join(DATA_DIR, 'a-share.db'))


# ═══════════════════ Step 1: 股票行情 (Sina Finance) ═══════════════════

SINA_API = ("https://money.finance.sina.com.cn/quotes_service/api/"
            "json_v2.php/Market_Center.getHQNodeData")

STOCK_CSV_COLS = [
    'code', 'name', 'trade_date', 'open', 'high', 'low', 'close',
    'prev_close', 'change_amount', 'change_pct', 'volume', 'amount',
    'turnover_rate', 'pe_dynamic', 'pb', 'total_mv', 'circ_mv',
]


def fetch_stocks():
    """Fetch all A-share stocks from Sina Finance (curl, proven reliable)."""
    all_data = []
    page = 1
    while True:
        print(f"\r  [股票] 第{page}页 ({len(all_data)}条)...", end="", flush=True)
        url = (f"{SINA_API}?page={page}&num=100&sort=symbol&asc=1"
               f"&node=hs_a&symbol=&_s_r_a=auto")
        result = subprocess.run(
            ["curl", "-s", "--max-time", "15", url,
             "-H", "Referer: https://finance.sina.com.cn/",
             "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"],
            capture_output=True, text=True
        )
        if result.returncode != 0 or not result.stdout.strip():
            print(f"\n  ⚠️ 第{page}页获取失败")
            break
        try:
            records = json.loads(result.stdout)
        except json.JSONDecodeError:
            break
        if not records:
            break
        all_data.extend(records)
        if len(records) < 100:
            break
        page += 1
        if page % 10 == 0:
            time.sleep(0.5)
    print(f"\n  [股票] 共获取 {len(all_data)} 条")
    return all_data


def save_stock_csv(raw_list, trade_date):
    """Transform Sina data → CSV. Returns file path."""
    path = os.path.join(DATA_DIR, f'daily_market_{trade_date}.csv')
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=STOCK_CSV_COLS)
        w.writeheader()
        for r in raw_list:
            code = r.get('code', '')
            if not code:
                continue
            # mktcap/nmc: Sina returns 万元, convert to 元
            total_mv = _safe_mul(r.get('mktcap', ''), 10000)
            circ_mv = _safe_mul(r.get('nmc', ''), 10000)
            w.writerow({
                'code': code,
                'name': r.get('name', ''),
                'trade_date': trade_date,
                'open': r.get('open', ''),
                'high': r.get('high', ''),
                'low': r.get('low', ''),
                'close': r.get('trade', ''),
                'prev_close': r.get('settlement', ''),
                'change_amount': r.get('pricechange', ''),
                'change_pct': r.get('changepercent', ''),
                'volume': r.get('volume', 0),
                'amount': r.get('amount', 0),
                'turnover_rate': r.get('turnoverratio', ''),
                'pe_dynamic': r.get('per', ''),
                'pb': r.get('pb', ''),
                'total_mv': total_mv,
                'circ_mv': circ_mv,
            })
    print(f"  [股票] 保存至 {path}")
    return path


def stock_import_sql(csv_path, trade_date):
    """Generate SQL: daily_market (replace) + klines (append)."""
    p = os.path.abspath(csv_path)
    return f"""
-- 1. daily_market: 最新单日快照 (INSERT OR REPLACE)
INSERT OR REPLACE INTO daily_market
    (code, name, trade_date, open, high, low, close, prev_close,
     change_amount, change_pct, amplitude, volume, amount,
     turnover_rate, pe_dynamic, pe_ttm, pb, total_mv, circ_mv)
SELECT
    code, name, TRY_CAST(trade_date AS DATE),
    TRY_CAST(open AS DECIMAL(10,2)),
    TRY_CAST(high AS DECIMAL(10,2)),
    TRY_CAST(low AS DECIMAL(10,2)),
    TRY_CAST(close AS DECIMAL(10,2)),
    TRY_CAST(prev_close AS DECIMAL(10,2)),
    TRY_CAST(change_amount AS DECIMAL(10,2)),
    TRY_CAST(change_pct AS DECIMAL(10,4)),
    NULL,
    TRY_CAST(volume AS BIGINT),
    TRY_CAST(amount AS DECIMAL(18,2)),
    TRY_CAST(turnover_rate AS DECIMAL(10,4)),
    TRY_CAST(pe_dynamic AS DECIMAL(12,2)),
    NULL,
    TRY_CAST(pb AS DECIMAL(10,4)),
    TRY_CAST(total_mv AS DECIMAL(18,2)),
    TRY_CAST(circ_mv AS DECIMAL(18,2))
FROM read_csv_auto('{p}', nullstr='');

-- 清理旧日期 (daily_market 仅保留最新一天)
DELETE FROM daily_market WHERE trade_date < '{trade_date}';

-- 2. klines: 追加当日K线 (每日积累, 自然形成历史)
INSERT INTO klines (code, trade_date, open, high, low, close, volume, amount)
SELECT
    code, TRY_CAST(trade_date AS DATE),
    TRY_CAST(open AS DECIMAL(10,2)),
    TRY_CAST(high AS DECIMAL(10,2)),
    TRY_CAST(low AS DECIMAL(10,2)),
    TRY_CAST(close AS DECIMAL(10,2)),
    TRY_CAST(volume AS BIGINT),
    TRY_CAST(amount AS DECIMAL(18,2))
FROM read_csv_auto('{p}', nullstr='')
WHERE TRY_CAST(close AS DECIMAL(10,2)) > 0
  AND TRY_CAST(volume AS BIGINT) > 0
ON CONFLICT DO NOTHING;
"""


# ═══════════════════ Step 2: 可转债行情 (东方财富) ═══════════════════

EM_API = 'https://datacenter-web.eastmoney.com/api/data/v1/get'

BOND_CSV_COLS = ['bond_code', 'bond_price', 'convert_value', 'premium_rate',
                 'ytm', 'remaining_size', 'convert_price',
                 'maturity_redemption_price',
                 'coupon_rate_1', 'coupon_rate_2', 'coupon_rate_3',
                 'coupon_rate_4', 'coupon_rate_5', 'coupon_rate_6',
                 'coupon_rate_7', 'coupon_rate_8', 'coupon_rate_9',
                 'coupon_rate_10']


def fetch_bonds():
    """Fetch all bond data from Eastmoney datacenter API."""
    params = {
        'sortColumns': 'PUBLIC_START_DATE', 'sortTypes': '-1',
        'pageSize': '500', 'pageNumber': '1',
        'reportName': 'RPT_BOND_CB_LIST',
        'columns': ','.join([
            'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'CONVERT_STOCK_CODE',
            'ACTUAL_ISSUE_SCALE', 'RATING', 'EXPIRE_DATE',
            'TRANSFER_PRICE', 'INITIAL_TRANSFER_PRICE',
            'REDEEM_CLAUSE', 'INTEREST_RATE_EXPLAIN',
            'LISTING_DATE', 'DELIST_DATE',
        ]),
        'quoteColumns': (
            'f2~01~CONVERT_STOCK_CODE~CONVERT_STOCK_PRICE,'
            'f2~10~SECURITY_CODE~BOND_PRICE,'
            'f235~10~SECURITY_CODE~TRANSFER_PRICE'
        ),
        'quoteType': '0', 'source': 'WEB', 'client': 'WEB',
    }
    all_results = []
    page = 1
    while True:
        params['pageNumber'] = str(page)
        print(f"\r  [转债] 第{page}页...", end="", flush=True)
        try:
            resp = requests.get(EM_API, params=params, timeout=15)
            data = resp.json()
        except Exception as e:
            print(f"\n  ⚠️ 第{page}页错误: {e}")
            break
        if not data.get('success'):
            break
        all_results.extend(data['result']['data'])
        if page >= data['result']['pages']:
            break
        page += 1
        time.sleep(0.3)
    print(f"\n  [转债] 共获取 {len(all_results)} 条")
    return all_results


def save_bond_csv(raw_list):
    """Transform Eastmoney data → CSV. Returns file path."""
    path = os.path.join(DATA_DIR, 'bond_market.csv')
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=BOND_CSV_COLS)
        w.writeheader()
        for r in raw_list:
            code = r.get('SECURITY_CODE', '')
            if not code:
                continue
            bp = _float(r.get('BOND_PRICE'))
            sp = _float(r.get('CONVERT_STOCK_PRICE'))
            tp = _float(r.get('TRANSFER_PRICE') or r.get('INITIAL_TRANSFER_PRICE'))
            rs = _float(r.get('ACTUAL_ISSUE_SCALE'))
            cv = round(sp / tp * 100, 3) if sp and tp and tp > 0 else None
            pr = round((bp / cv - 1) * 100, 4) if bp and cv and cv > 0 else None
            mv = _maturity_val(r.get('REDEEM_CLAUSE', ''))
            coupons = _parse_coupon_rates(r.get('INTEREST_RATE_EXPLAIN', ''))
            exp = str(r.get('EXPIRE_DATE', ''))[:10]
            ytm = _calc_ytm(bp, mv, coupons, exp)
            row = {
                'bond_code': code, 'bond_price': bp, 'convert_value': cv,
                'premium_rate': pr, 'ytm': ytm, 'remaining_size': rs,
                'convert_price': tp,
                'maturity_redemption_price': mv,
            }
            for i in range(10):
                row[f'coupon_rate_{i+1}'] = coupons[i] if i < len(coupons) else None
            w.writerow(row)
    print(f"  [转债] 保存至 {path}")
    return path


# ── bonds_full.csv: 完整字段（静态+市场），用于离线重建 ──

BONDS_FULL_COLS = [
    'bond_code', 'bond_name', 'stock_code', 'issue_size', 'rating',
    'maturity_date', 'convert_price', 'original_price',
    'listing_date', 'delist_date',
    'redeem_pct', 'redeem_days', 'redeem_window',
    'putback_pct', 'putback_days', 'putback_window',
    'revise_pct', 'revise_days', 'revise_window',
    'bond_price', 'convert_value', 'premium_rate', 'ytm', 'remaining_size',
    'maturity_redemption_price',
    'coupon_rate_1', 'coupon_rate_2', 'coupon_rate_3',
    'coupon_rate_4', 'coupon_rate_5', 'coupon_rate_6',
    'coupon_rate_7', 'coupon_rate_8', 'coupon_rate_9',
    'coupon_rate_10',
]


def _parse_trigger(clause):
    """Parse trigger conditions from REDEEM_CLAUSE.
    Returns (pct, days, window) or (None, None, None).
    Pattern: '连续30个交易日中至少15个交易日...85%'
    """
    if not clause:
        return None, None, None
    m = re.search(r'连续(\d+)个交易日中[^\d]*(\d+)个交易日.*?(\d+)%', clause)
    if m:
        return float(m.group(3)), int(m.group(2)), int(m.group(1))
    return None, None, None


def save_bonds_full_csv(raw_list):
    """Save complete bonds data (static + market) for offline rebuild.
    This CSV contains enough info to INSERT OR REPLACE the entire bonds table."""
    path = os.path.join(DATA_DIR, 'bonds_full.csv')
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=BONDS_FULL_COLS)
        w.writeheader()
        for r in raw_list:
            code = r.get('SECURITY_CODE', '')
            if not code:
                continue
            # Market fields (same logic as save_bond_csv)
            bp = _float(r.get('BOND_PRICE'))
            sp = _float(r.get('CONVERT_STOCK_PRICE'))
            tp = _float(r.get('TRANSFER_PRICE') or r.get('INITIAL_TRANSFER_PRICE'))
            ip = _float(r.get('INITIAL_TRANSFER_PRICE'))
            rs = _float(r.get('ACTUAL_ISSUE_SCALE'))
            cv = round(sp / tp * 100, 3) if sp and tp and tp > 0 else None
            pr = round((bp / cv - 1) * 100, 4) if bp and cv and cv > 0 else None
            clause = r.get('REDEEM_CLAUSE', '')
            mv = _maturity_val(clause)
            coupons = _parse_coupon_rates(r.get('INTEREST_RATE_EXPLAIN', ''))
            exp = str(r.get('EXPIRE_DATE', ''))[:10] or None
            ytm = _calc_ytm(bp, mv, coupons, exp)
            listing = str(r.get('LISTING_DATE', ''))[:10] or None
            delist = str(r.get('DELIST_DATE', ''))[:10] or None
            if listing == 'None': listing = None
            if delist == 'None': delist = None
            if exp == 'None': exp = None
            # Parse trigger conditions (defaults if not parseable)
            rev_pct, rev_days, rev_win = _parse_trigger(clause)
            row = {
                'bond_code': code,
                'bond_name': r.get('SECURITY_NAME_ABBR'),
                'stock_code': r.get('CONVERT_STOCK_CODE'),
                'issue_size': rs,
                'rating': r.get('RATING'),
                'maturity_date': exp,
                'convert_price': tp,
                'original_price': ip,
                'listing_date': listing,
                'delist_date': delist,
                # Trigger conditions: use parsed values or common defaults
                'redeem_pct': 130.0, 'redeem_days': 15, 'redeem_window': 30,
                'putback_pct': rev_pct or 70.0, 'putback_days': 30, 'putback_window': 30,
                'revise_pct': rev_pct or 85.0, 'revise_days': rev_days or 15,
                'revise_window': rev_win or 30,
                # Market fields
                'bond_price': bp, 'convert_value': cv, 'premium_rate': pr,
                'ytm': ytm, 'remaining_size': rs,
                'maturity_redemption_price': mv,
            }
            for i in range(10):
                row[f'coupon_rate_{i+1}'] = coupons[i] if i < len(coupons) else None
            w.writerow(row)
    print(f"  [转债] 完整数据保存至 {path} ({len(raw_list)} 条)")
    return path


def bond_import_sql(csv_path):
    """Generate SQL: UPDATE bonds with market data."""
    p = os.path.abspath(csv_path)
    return f"""
UPDATE bonds SET
    bond_price = src.bond_price,
    convert_value = src.convert_value,
    premium_rate = src.premium_rate,
    ytm = src.ytm,
    remaining_size = src.remaining_size,
    convert_price = COALESCE(src.convert_price, bonds.convert_price),
    maturity_redemption_price = COALESCE(src.maturity_redemption_price, bonds.maturity_redemption_price),
    coupon_rate_1 = COALESCE(src.coupon_rate_1, bonds.coupon_rate_1),
    coupon_rate_2 = COALESCE(src.coupon_rate_2, bonds.coupon_rate_2),
    coupon_rate_3 = COALESCE(src.coupon_rate_3, bonds.coupon_rate_3),
    coupon_rate_4 = COALESCE(src.coupon_rate_4, bonds.coupon_rate_4),
    coupon_rate_5 = COALESCE(src.coupon_rate_5, bonds.coupon_rate_5),
    coupon_rate_6 = COALESCE(src.coupon_rate_6, bonds.coupon_rate_6),
    coupon_rate_7 = COALESCE(src.coupon_rate_7, bonds.coupon_rate_7),
    coupon_rate_8 = COALESCE(src.coupon_rate_8, bonds.coupon_rate_8),
    coupon_rate_9 = COALESCE(src.coupon_rate_9, bonds.coupon_rate_9),
    coupon_rate_10 = COALESCE(src.coupon_rate_10, bonds.coupon_rate_10),
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT
        bond_code,
        TRY_CAST(bond_price AS DECIMAL(10,3)) AS bond_price,
        TRY_CAST(convert_value AS DECIMAL(10,3)) AS convert_value,
        TRY_CAST(premium_rate AS DECIMAL(10,4)) AS premium_rate,
        TRY_CAST(ytm AS DECIMAL(10,4)) AS ytm,
        TRY_CAST(remaining_size AS DECIMAL(12,4)) AS remaining_size,
        TRY_CAST(convert_price AS DECIMAL(10,3)) AS convert_price,
        TRY_CAST(maturity_redemption_price AS DECIMAL(10,3)) AS maturity_redemption_price,
        TRY_CAST(coupon_rate_1 AS DECIMAL(6,3)) AS coupon_rate_1,
        TRY_CAST(coupon_rate_2 AS DECIMAL(6,3)) AS coupon_rate_2,
        TRY_CAST(coupon_rate_3 AS DECIMAL(6,3)) AS coupon_rate_3,
        TRY_CAST(coupon_rate_4 AS DECIMAL(6,3)) AS coupon_rate_4,
        TRY_CAST(coupon_rate_5 AS DECIMAL(6,3)) AS coupon_rate_5,
        TRY_CAST(coupon_rate_6 AS DECIMAL(6,3)) AS coupon_rate_6,
        TRY_CAST(coupon_rate_7 AS DECIMAL(6,3)) AS coupon_rate_7,
        TRY_CAST(coupon_rate_8 AS DECIMAL(6,3)) AS coupon_rate_8,
        TRY_CAST(coupon_rate_9 AS DECIMAL(6,3)) AS coupon_rate_9,
        TRY_CAST(coupon_rate_10 AS DECIMAL(6,3)) AS coupon_rate_10
    FROM read_csv_auto('{p}', nullstr='')
) AS src
WHERE bonds.bond_code = src.bond_code;
"""


# ═══════════════════ Step 3: 分析指标 (本地SQL) ═══════════════════

TRIGGER_SQL = """
WITH recent_klines AS (
    SELECT k.code, k.trade_date, k.close,
           ROW_NUMBER() OVER (PARTITION BY k.code ORDER BY k.trade_date DESC) AS rn
    FROM klines k
    JOIN (SELECT DISTINCT stock_code FROM bonds WHERE maturity_date > CURRENT_DATE) b
      ON k.code = b.stock_code
),
latest_price AS (
    SELECT code, close AS latest_close FROM recent_klines WHERE rn = 1
),
trigger_counts AS (
    SELECT rk.code, b.bond_code,
           COUNT(CASE WHEN rk.close < b.convert_price * b.revise_pct / 100.0 THEN 1 END) AS revise_cnt,
           COUNT(CASE WHEN rk.close < b.convert_price * b.putback_pct / 100.0 THEN 1 END) AS putback_cnt,
           COUNT(CASE WHEN rk.close >= b.convert_price * b.redeem_pct / 100.0 THEN 1 END) AS redeem_cnt
    FROM recent_klines rk
    JOIN bonds b ON rk.code = b.stock_code AND b.maturity_date > CURRENT_DATE
    WHERE rk.rn <= COALESCE(b.revise_window, 30)
    GROUP BY rk.code, b.bond_code
)
UPDATE bonds
SET revise_trigger_count = tc.revise_cnt,
    putback_trigger_count = tc.putback_cnt,
    redeem_trigger_count = tc.redeem_cnt,
    stock_price_latest = lp.latest_close
FROM trigger_counts tc
JOIN latest_price lp ON tc.code = lp.code
WHERE bonds.bond_code = tc.bond_code;
"""

PROFITABILITY_SQL = """
WITH annual_data AS (
    SELECT code, EXTRACT(YEAR FROM report_date) AS yr, net_profit, roe
    FROM fundamentals WHERE report_type = '年报'
),
latest_year AS (
    SELECT code, MAX(yr) AS max_yr FROM annual_data GROUP BY code
),
latest_metrics AS (
    SELECT a.code, a.net_profit, a.roe
    FROM annual_data a JOIN latest_year l ON a.code = l.code AND a.yr = l.max_yr
),
profit_check AS (
    SELECT a.code, a.yr,
           CASE WHEN a.net_profit > 0 THEN 1 ELSE 0 END AS is_profit, l.max_yr
    FROM annual_data a JOIN latest_year l ON a.code = l.code
    WHERE a.yr <= l.max_yr AND a.yr >= l.max_yr - 5
),
consecutive AS (
    SELECT code,
        CASE
            WHEN SUM(CASE WHEN yr = max_yr AND is_profit = 0 THEN 1 ELSE 0 END) > 0 THEN 0
            WHEN SUM(CASE WHEN yr = max_yr - 1 AND is_profit = 0 THEN 1 ELSE 0 END) > 0 THEN 1
            WHEN SUM(CASE WHEN yr = max_yr - 2 AND is_profit = 0 THEN 1 ELSE 0 END) > 0 THEN 2
            WHEN SUM(CASE WHEN yr = max_yr - 3 AND is_profit = 0 THEN 1 ELSE 0 END) > 0 THEN 3
            WHEN SUM(CASE WHEN yr = max_yr - 4 AND is_profit = 0 THEN 1 ELSE 0 END) > 0 THEN 4
            ELSE 5
        END AS consec_years
    FROM profit_check GROUP BY code, max_yr
)
UPDATE bonds
SET is_profitable = (lm.net_profit > 0),
    latest_net_profit = lm.net_profit,
    latest_roe = lm.roe,
    consecutive_profit_years = c.consec_years
FROM latest_metrics lm
JOIN consecutive c ON lm.code = c.code
WHERE bonds.stock_code = lm.code;
"""

# Ensure analysis columns exist (idempotent)
ENSURE_COLUMNS_SQL = """
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS is_profitable BOOLEAN;
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS consecutive_profit_years INTEGER;
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS latest_roe DECIMAL(10,4);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS latest_net_profit DECIMAL(18,2);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS revise_trigger_count INTEGER;
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS putback_trigger_count INTEGER;
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS redeem_trigger_count INTEGER;
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS stock_price_latest DECIMAL(10,4);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS maturity_redemption_price DECIMAL(10,3);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_1 DECIMAL(6,3);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_2 DECIMAL(6,3);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_3 DECIMAL(6,3);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_4 DECIMAL(6,3);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_5 DECIMAL(6,3);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_6 DECIMAL(6,3);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_7 DECIMAL(6,3);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_8 DECIMAL(6,3);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_9 DECIMAL(6,3);
ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_10 DECIMAL(6,3);
"""


# ═══════════════════ Helpers ═══════════════════

def _float(val):
    try:
        return float(val) if val else None
    except (ValueError, TypeError):
        return None


def _safe_mul(val, factor):
    try:
        return round(float(val) * factor, 2) if val else ''
    except (ValueError, TypeError):
        return ''


def _maturity_val(clause):
    if not clause:
        return None
    m = re.search(r'面值的?(\d+(?:\.\d+)?)%', clause)
    return float(m.group(1)) if m else None


def _parse_coupon_rates(text):
    """Parse INTEREST_RATE_EXPLAIN → list of floats.
    e.g. '第一年0.3%、第二年0.5%...第六年2.5%' → [0.3, 0.5, 1.0, 1.5, 2.0, 2.5]
    """
    if not text:
        return []
    return [float(x) for x in re.findall(r'(\d+(?:\.\d+)?)%', text)]


def _calc_ytm(bond_price, maturity_val, coupon_rates, expire_date_str):
    """Calculate YTM considering coupon payments.
    Uses simple annual coupon sum approach (not IRR) for speed.
    """
    if not bond_price or bond_price <= 0 or not maturity_val or not expire_date_str:
        return None
    try:
        exp = datetime.strptime(expire_date_str, '%Y-%m-%d')
        remain_years = (exp - datetime.now()).days / 365.25
        if remain_years <= 0.05:
            return None
        # Total years of bond (from coupon count)
        total_years = len(coupon_rates) if coupon_rates else 6
        # Which year are we currently in? (1-indexed)
        elapsed_years = total_years - remain_years
        # Sum remaining coupons (exclude last year if included in maturity_val)
        remaining_coupons = 0.0
        for i, rate in enumerate(coupon_rates):
            year_num = i + 1  # 1-indexed
            if year_num > elapsed_years and year_num < total_years:
                # Not the last year (last year coupon is in maturity_val)
                remaining_coupons += rate
        # Total return
        total_return = maturity_val + remaining_coupons
        ytm = round((total_return / bond_price - 1) / remain_years * 100, 4)
        return ytm
    except (ValueError, TypeError):
        return None


# ═══════════════════ DB Import ═══════════════════

def run_with_db(sql_steps):
    """Try direct DuckDB import. Returns (success, failed_steps)."""
    try:
        import duckdb
        conn = duckdb.connect(DB_PATH)
        for label, sql in sql_steps:
            print(f"    ▸ {label}")
            for stmt in sql.strip().split(';'):
                stmt = stmt.strip()
                if stmt and not stmt.startswith('--'):
                    conn.execute(stmt)
        conn.execute("""
            INSERT INTO data_updates (table_name, update_type, records_count, notes)
            VALUES ('all', 'daily_update', ?, ?)
        """, [len(sql_steps), f"daily update {datetime.now():%Y-%m-%d %H:%M}"])
        conn.close()
        return True, []
    except Exception as e:
        if 'lock' in str(e).lower():
            print(f"    ⚠️  DB锁定: {e}")
            return False, sql_steps
        raise


# ═══════════════════ Main ═══════════════════

def main():
    parser = argparse.ArgumentParser(description='每日数据库更新 — A股全市场统一入口')
    parser.add_argument('--step', choices=['stocks', 'bonds', 'analysis'],
                        help='仅运行指定步骤')
    parser.add_argument('--dry-run', action='store_true', help='仅获取+保存CSV，不导入DB')
    parser.add_argument('--date', type=str, help='指定交易日期 (YYYY-MM-DD)')
    args = parser.parse_args()

    trade_date = args.date or date.today().strftime('%Y-%m-%d')
    do_stocks = args.step in (None, 'stocks')
    do_bonds = args.step in (None, 'bonds')
    do_analysis = args.step in (None, 'analysis')

    print("=" * 60)
    print(f"  每日数据库更新 — {trade_date}")
    print("=" * 60)

    sql_steps = []
    summary = {}

    # ── Step 1: 股票行情 ──
    if do_stocks:
        print(f"\n[Step 1/3] 获取股票行情 (Sina Finance)...")
        raw = fetch_stocks()
        if raw:
            csv_path = save_stock_csv(raw, trade_date)
            sql_steps.append(('股票→daily_market+klines', stock_import_sql(csv_path, trade_date)))
            summary['stocks'] = f"{len(raw)} 条"
        else:
            print("  ❌ 股票数据获取失败，跳过")

    # ── Step 2: 可转债行情 ──
    if do_bonds:
        print(f"\n[Step 2/3] 获取可转债行情 (东方财富)...")
        raw = fetch_bonds()
        if raw:
            csv_path = save_bond_csv(raw)
            save_bonds_full_csv(raw)
            sql_steps.append(('转债→bonds', bond_import_sql(csv_path)))
            summary['bonds'] = f"{len(raw)} 条"
        else:
            print("  ❌ 转债数据获取失败，跳过")

    # ── Step 3: 分析指标 ──
    if do_analysis:
        print(f"\n[Step 3/3] 准备分析指标SQL...")
        sql_steps.append(('确保分析列存在', ENSURE_COLUMNS_SQL))
        sql_steps.append(('触发进度→bonds', TRIGGER_SQL))
        sql_steps.append(('盈利状态→bonds', PROFITABILITY_SQL))
        summary['analysis'] = '触发进度 + 盈利状态'

    if args.dry_run:
        print(f"\n{'=' * 60}")
        print("  ✅ Dry run 完成 — CSV已保存，未导入DB")
        for k, v in summary.items():
            print(f"    {k}: {v}")
        print(f"{'=' * 60}")
        return

    if not sql_steps:
        print("\n  无待执行任务")
        return

    # ── DB Import ──
    print(f"\n[导入] 执行 {len(sql_steps)} 步SQL...")
    ok, failed = run_with_db(sql_steps)

    if ok:
        print(f"\n{'=' * 60}")
        print("  ✅ 每日更新完成")
        for k, v in summary.items():
            print(f"    {k}: {v}")
        print(f"{'=' * 60}")
    else:
        # Save combined fallback SQL for MCP import
        sql_path = os.path.join(DATA_DIR, f'daily_update_{trade_date}.sql')
        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write(f"-- 每日更新 {trade_date}\n")
            f.write(f"-- 生成时间: {datetime.now():%Y-%m-%d %H:%M}\n\n")
            for label, sql in failed:
                f.write(f"-- ═══ {label} ═══\n{sql}\n\n")
        print(f"\n{'=' * 60}")
        print(f"  ⚠️  DB锁定，SQL已保存: {sql_path}")
        print(f"  请用 MCP mcp_duckdb_query 工具逐条执行")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
