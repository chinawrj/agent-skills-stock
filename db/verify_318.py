#!/usr/bin/env python3
"""Cross-verify DB 3/18 data against Sina live quotes."""
import duckdb, urllib.request, re

con = duckdb.connect('data/a-share.db', read_only=True)

codes = [
    ('300401', '花园生物', 'sz300401'),
    ('600519', '贵州茅台', 'sh600519'),
    ('000001', '平安银行', 'sz000001'),
    ('300750', '宁德时代', 'sz300750'),
    ('601318', '中国平安', 'sh601318'),
]

sina_codes = ','.join(c[2] for c in codes)
url = f'http://hq.sinajs.cn/list={sina_codes}'
req = urllib.request.Request(url, headers={'Referer': 'http://finance.sina.com.cn'})
resp = urllib.request.urlopen(req).read().decode('gbk')

print(f"{'Stock':<12} {'Field':<8} {'DB':>12} {'Sina':>12} {'Match':>6}")
print('-' * 56)

for code, name, sina_code in codes:
    db = con.execute(
        "SELECT open, high, low, close, volume FROM klines "
        "WHERE code=? AND trade_date='2026-03-18'", [code]
    ).fetchone()

    pattern = f'var hq_str_{sina_code}="([^"]+)"'
    m = re.search(pattern, resp)
    if not m or not db:
        print(f"{name:<12} MISSING DATA")
        continue

    parts = m.group(1).split(',')
    sina_vals = {
        'open': float(parts[1]),
        'high': float(parts[4]),
        'low': float(parts[5]),
        'close': float(parts[3]),
        'volume': int(float(parts[8])),
    }

    for field, idx in [('open', 0), ('high', 1), ('low', 2), ('close', 3), ('volume', 4)]:
        db_val = float(db[idx]) if field != 'volume' else int(db[idx])
        sina_val = sina_vals[field]
        if sina_val > 0:
            match = '✓' if abs(db_val - sina_val) / max(sina_val, 1) < 0.001 else '✗'
        else:
            match = '?'
        print(f"{name:<12} {field:<8} {db_val:>12} {sina_val:>12} {match:>6}")
    print()

con.close()
