#!/usr/bin/env python3
"""Test various APIs for bulk historical OHLCV data."""
import requests
import time

print("=" * 60)
print("Test 1: Eastmoney push2his (per-stock kline)")
print("=" * 60)

url = 'https://push2his.eastmoney.com/api/qt/stock/kline/get'
base_params = {
    'fields1': 'f1,f2,f3,f4,f5,f6',
    'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
    'klt': '101',
    'fqt': '0',
    'beg': '20260317',
    'end': '20260317',
}

codes = ['1.600519', '0.000001', '1.601318', '0.000002', '1.600036']
t0 = time.time()
for code in codes:
    params = {**base_params, 'secid': code}
    r = requests.get(url, params=params, timeout=5)
    d = r.json()
    k = d.get('data', {}).get('klines', [])
    name = d.get('data', {}).get('name', '?')
    print(f"  {code} ({name}): {k[0] if k else 'EMPTY'}")
t1 = time.time()
print(f"  5 stocks in {t1-t0:.2f}s ({(t1-t0)/5*1000:.0f}ms/stock)")
print(f"  Estimated for 5500 stocks: {(t1-t0)/5*5500:.0f}s = {(t1-t0)/5*5500/60:.1f}min")

print()
print("=" * 60)
print("Test 2: Tencent kline API (per-stock)")
print("=" * 60)

url2 = 'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
t0 = time.time()
for prefix, code in [('sh', '600519'), ('sz', '000001'), ('sh', '601318'), ('sz', '000002'), ('sh', '600036')]:
    params = {'param': f'{prefix}{code},day,2026-03-17,,1,qfq'}
    r = requests.get(url2, params=params, timeout=5)
    d = r.json()
    day_data = d.get('data', {}).get(f'{prefix}{code}', {}).get('day', [])
    if not day_data:
        day_data = d.get('data', {}).get(f'{prefix}{code}', {}).get('qfqday', [])
    print(f"  {prefix}{code}: {day_data}")
t1 = time.time()
print(f"  5 stocks in {t1-t0:.2f}s ({(t1-t0)/5*1000:.0f}ms/stock)")
print(f"  Estimated for 5500 stocks: {(t1-t0)/5*5500:.0f}s = {(t1-t0)/5*5500/60:.1f}min")

print()
print("=" * 60)
print("Test 3: Eastmoney push2 batch quote (market-wide snapshot)")
print("=" * 60)

# This is the API behind stock_zh_a_spot_em - returns ALL stocks at once
# Check if it returns OHLC
url3 = 'https://push2.eastmoney.com/api/qt/clist/get'
params3 = {
    'pn': '1',
    'pz': '10',  # just 10 for test
    'po': '1',
    'np': '1',
    'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
    'fltt': '2',
    'invt': '2',
    'fid': 'f3',
    'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048',  # All A shares
    'fields': 'f2,f3,f4,f5,f6,f7,f12,f14,f15,f16,f17,f18',  # f2=latest,f15=high,f16=low,f17=open,f18=pre_close,f5=vol,f6=amount
}
r3 = requests.get(url3, params=params3, timeout=10)
d3 = r3.json()
total = d3.get('data', {}).get('total', 0)
items = d3.get('data', {}).get('diff', [])
print(f"  Total stocks available: {total}")
if items:
    for i in items[:3]:
        print(f"  {i.get('f12','?')} {i.get('f14','?')}: open={i.get('f17')}, high={i.get('f15')}, low={i.get('f16')}, close={i.get('f2')}, vol={i.get('f5')}, amount={i.get('f6')}")
    print(f"  NOTE: This returns TODAY'S data. Not historical.")

print()
print("=" * 60)
print("Test 4: Baostock historical K-line")
print("=" * 60)

try:
    import baostock as bs
    lg = bs.login()
    print(f"  Login: {lg.error_msg}")
    rs = bs.query_history_k_data_plus("sh.600519",
        "date,code,open,high,low,close,volume,amount",
        start_date='2026-03-17', end_date='2026-03-17',
        frequency="d", adjustflag="3")
    rows = []
    while (rs.error_code == '0') and rs.next():
        rows.append(rs.get_row_data())
    print(f"  600519: {rows}")
    bs.logout()
except Exception as e:
    print(f"  Baostock error: {e}")

print()
print("=" * 60)
print("Test 5: efinance library")
print("=" * 60)

try:
    import efinance as ef
    # efinance has stock.get_quote_history which can take a list of codes
    df = ef.stock.get_quote_history(['600519'], beg='20260317', end='20260317')
    print(f"  efinance 600519: {df.to_string() if len(df) > 0 else 'EMPTY'}")
except ImportError:
    print("  efinance not installed")
except Exception as e:
    print(f"  efinance error: {e}")

print()
print("=" * 60)
print("Test 6: 163 Money CSV download (per-stock)")
print("=" * 60)

try:
    # NetEase historical data download
    url6 = 'http://quotes.money.163.com/service/chddata.html'
    params6 = {
        'code': '0600519',  # 0 for SH, 1 for SZ
        'start': '20260317',
        'end': '20260317',
        'fields': 'TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER',
    }
    r6 = requests.get(url6, params=params6, timeout=10)
    lines = r6.text.strip().split('\n')
    for line in lines[:5]:
        print(f"  {line[:100]}")
except Exception as e:
    print(f"  163 error: {e}")
