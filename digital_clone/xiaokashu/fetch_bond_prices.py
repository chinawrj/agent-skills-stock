#!/usr/bin/env python3
"""
卡书框架 TOP 10 可转债筛选
1. Read DB scoring from CSV (337 active bonds pre-scored)
2. Fetch real-time bond prices from Eastmoney API
3. Compute 到期正收益
4. Filter MUST-HAVE: 到期正收益 > 0
5. Rank by framework score and present TOP 10
"""
import requests
import re
import csv
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(SCRIPT_DIR)), 'data')
SCORES_CSV = os.path.join(DATA_DIR, 'bond_scores.csv')

# ---- Step 1: Read DB scoring results ----
db_bonds = {}
with open(SCORES_CSV, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        code = row['bond_code']
        db_bonds[code] = {
            'bond_code': code,
            'bond_name': row['bond_name'],
            'stock_code': row['stock_code'],
            'stock_name': row['stock_name'],
            'convert_price': float(row['convert_price']) if row['convert_price'] else None,
            'cv': float(row['cv']) if row['cv'] else None,
            'remain_years': float(row['remain_years']) if row['remain_years'] else None,
            'issue_size': float(row['issue_size']) if row['issue_size'] else None,
            'rating': row['rating'],
            'stock_close': float(row['stock_close']) if row['stock_close'] else None,
            'total_mv': float(row['total_mv']) if row['total_mv'] else None,
            'stock_pb': float(row['stock_pb']) if row['stock_pb'] else None,
            'total_score': int(row['total_score']) if row['total_score'] else 0,
            'maturity_date': row['maturity_date'],
        }
print(f'DB: {len(db_bonds)} bonds loaded')

# ---- Step 2: Fetch real-time bond prices + maturity redemption info from API ----
url = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
params = {
    'sortColumns': 'PUBLIC_START_DATE',
    'sortTypes': '-1',
    'pageSize': '500',
    'pageNumber': '1',
    'reportName': 'RPT_BOND_CB_LIST',
    'columns': 'SECURITY_CODE,SECURITY_NAME_ABBR,REDEEM_CLAUSE',
    'quoteColumns': 'f2~10~SECURITY_CODE~BOND_PRICE',
    'quoteType': '0',
    'source': 'WEB',
    'client': 'WEB',
}

api_results = []
resp = requests.get(url, params=params, timeout=15)
data = resp.json()
api_results = data['result']['data']
total_pages = data['result']['pages']
for page in range(2, total_pages + 1):
    params['pageNumber'] = str(page)
    resp = requests.get(url, params=params, timeout=15)
    d = resp.json()
    if d.get('success'):
        api_results.extend(d['result']['data'])

api_lookup = {}
for r in api_results:
    api_lookup[r.get('SECURITY_CODE', '')] = r
print(f'API: {len(api_results)} bonds fetched')

# ---- Step 3: Parse maturity redemption value ----
def parse_maturity_value(redeem_clause):
    """Extract maturity redemption % from clause text. Returns face value ratio."""
    if not redeem_clause:
        return 110  # conservative default
    m = re.search(r'面值的(\d+(?:\.\d+)?)%', redeem_clause)
    if m:
        return float(m.group(1))
    return 110

# ---- Step 4: Merge, compute maturity yield, filter ----
merged = []
for code, db in db_bonds.items():
    api = api_lookup.get(code, {})
    bond_price = api.get('BOND_PRICE')
    try:
        bond_price = float(bond_price) if bond_price else None
    except (ValueError, TypeError):
        bond_price = None
    if bond_price is None or bond_price <= 0:
        continue  # no trading price available (unlisted or suspended)
    
    redeem_clause = api.get('REDEEM_CLAUSE', '')
    maturity_val = parse_maturity_value(redeem_clause)
    
    maturity_yield = (maturity_val - bond_price) / bond_price * 100
    remain = db['remain_years'] or 0
    annual_yield = maturity_yield / remain if remain > 0 else 0
    
    cv = db['cv'] or 0
    premium = (bond_price / cv - 1) * 100 if cv > 0 else None
    
    merged.append({
        **db,
        'bond_price': bond_price,
        'maturity_val': maturity_val,
        'maturity_yield': maturity_yield,
        'annual_yield': annual_yield,
        'premium': premium,
    })

print(f'Merged: {len(merged)} bonds with prices')

# Filter for 到期正收益
positive = [m for m in merged if m['maturity_yield'] > 0]
negative = [m for m in merged if m['maturity_yield'] <= 0]
positive.sort(key=lambda x: x['total_score'], reverse=True)

print(f'到期正收益: {len(positive)} bonds')
print(f'到期负收益: {len(negative)} bonds')

# ---- Step 5: Display results ----
def fmt_mv(v):
    """Format total_mv (in raw units from DB) to 亿"""
    if v and v > 0:
        return f'{v/1e8:.1f}'
    return '-'

header = (f'{"排名":4s} {"代码":8s} {"名称":8s} {"正股":8s} {"债价":>7s} {"转股价值":>7s} '
          f'{"溢价率%":>7s} {"到期赎回":>7s} {"到期收益%":>8s} {"年化%":>6s} {"剩余年":>5s} '
          f'{"规模亿":>6s} {"评级":4s} {"总分":>4s} {"市值亿":>6s} {"PB":>5s}')
print(f'\n{"="*140}')
print('卡书框架 可转债筛选结果 — 到期正收益 TOP 10')
print(f'{"="*140}')
print(header)
print('-' * 140)

for i, r in enumerate(positive[:20]):
    pr_str = f"{r['premium']:.1f}" if r['premium'] is not None else '-'
    print(f'{i+1:4d} {r["bond_code"]:8s} {r["bond_name"]:8s} {r["stock_name"]:8s} '
          f'{r["bond_price"]:7.2f} {r["cv"]:7.2f} {pr_str:>7s} {r["maturity_val"]:7.1f} '
          f'{r["maturity_yield"]:8.2f} {r["annual_yield"]:6.2f} {r["remain_years"]:5.2f} '
          f'{r["issue_size"]:6.1f} {r["rating"]:4s} {r["total_score"]:4d} '
          f'{fmt_mv(r["total_mv"]):>6s} {r["stock_pb"]:5.2f}')

if not positive or len(positive) < 10:
    print(f'\n⚠ 当前市场仅{len(positive)}只债满足到期正收益！转债市场整体高估。')
    
    # Show bonds closest to positive yield with high scores
    # Sort negative by: penalize big negative yield, reward high score
    close_to_positive = [r for r in negative if r['maturity_yield'] > -20 and r['total_score'] >= 55]
    close_to_positive.sort(key=lambda x: (-x['total_score'], -x['maturity_yield']))
    
    print(f'\n{"="*140}')
    print('高分债（到期收益轻度为负，可关注回调）')
    print(f'{"="*140}')
    print(header)
    print('-' * 140)
    for i, r in enumerate(close_to_positive[:15]):
        pr_str = f"{r['premium']:.1f}" if r['premium'] is not None else '-'
        target_buy = r['maturity_val'] * 0.98
        print(f'{i+1:4d} {r["bond_code"]:8s} {r["bond_name"]:8s} {r["stock_name"]:8s} '
              f'{r["bond_price"]:7.2f} {r["cv"]:7.2f} {pr_str:>7s} {r["maturity_val"]:7.1f} '
              f'{r["maturity_yield"]:8.2f} {r["annual_yield"]:6.2f} {r["remain_years"]:5.2f} '
              f'{r["issue_size"]:6.1f} {r["rating"]:4s} {r["total_score"]:4d} '
              f'{fmt_mv(r["total_mv"]):>6s} {r["stock_pb"]:5.2f}')
    
    print(f'\n💡 目标买入价参考（使到期正收益≥2%）：')
    for r in close_to_positive[:10]:
        target = r['maturity_val'] / 1.02
        print(f'   {r["bond_name"]:8s} 当前{r["bond_price"]:.1f} → 目标买入<{target:.1f}（到期赎回{r["maturity_val"]:.0f}）')
