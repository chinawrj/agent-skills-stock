#!/usr/bin/env python3
"""Test remaining API sources for bulk historical OHLCV data."""
import requests
import time

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'})

print("=" * 60)
print("Test 1: Eastmoney push2his - timing 5 stocks with delay")
print("=" * 60)

url = 'https://push2his.eastmoney.com/api/qt/stock/kline/get'
base_params = {
    'fields1': 'f1,f2,f3,f4,f5,f6',
    'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
    'klt': '101', 'fqt': '0',
    'beg': '20260317', 'end': '20260317',
}

codes = ['1.600519', '0.000001', '1.601318', '0.000002', '1.600036']
t0 = time.time()
for code in codes:
    try:
        params = {**base_params, 'secid': code}
        r = session.get(url, params=params, timeout=5)
        d = r.json()
        k = d.get('data', {}).get('klines', [])
        name = d.get('data', {}).get('name', '?')
        fields = k[0].split(',') if k else []
        if fields:
            print(f"  {name}: open={fields[1]} close={fields[2]} high={fields[3]} low={fields[4]} vol={fields[5]} amt={fields[6]}")
        time.sleep(0.1)  # small delay
    except Exception as e:
        print(f"  {code}: ERROR {e}")
t1 = time.time()
avg_ms = (t1-t0)/5*1000
print(f"  Avg: {avg_ms:.0f}ms/stock (with 100ms delay)")
print(f"  Est for 5500 stocks at 100ms delay: {5500*0.1/60:.1f} min + network time")

print()
print("=" * 60)
print("Test 2: Tencent kline API")
print("=" * 60)

url2 = 'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
t0 = time.time()
for prefix, code in [('sh', '600519'), ('sz', '000001'), ('sh', '601318'), ('sz', '000002'), ('sh', '600036')]:
    try:
        params = {'param': f'{prefix}{code},day,2026-03-17,,1,qfq'}
        r = session.get(url2, params=params, timeout=5)
        d = r.json()
        key = f'{prefix}{code}'
        day_data = d.get('data', {}).get(key, {}).get('day', [])
        if not day_data:
            day_data = d.get('data', {}).get(key, {}).get('qfqday', [])
        if day_data:
            row = day_data[0]
            print(f"  {key}: date={row[0]} open={row[1]} close={row[2]} high={row[3]} low={row[4]} vol={row[5]}")
        else:
            print(f"  {key}: EMPTY")
    except Exception as e:
        print(f"  {prefix}{code}: ERROR {e}")
t1 = time.time()
avg_ms2 = (t1-t0)/5*1000
print(f"  Avg: {avg_ms2:.0f}ms/stock")
print(f"  Est for 5500 stocks: {5500*avg_ms2/1000/60:.1f} min")

print()
print("=" * 60)
print("Test 3: Baostock")
print("=" * 60)

try:
    import baostock as bs
    lg = bs.login()
    print(f"  Login: {lg.error_msg}")
    t0 = time.time()
    for code in ['sh.600519', 'sz.000001', 'sh.601318', 'sz.000002', 'sh.600036']:
        rs = bs.query_history_k_data_plus(code,
            "date,code,open,high,low,close,volume,amount",
            start_date='2026-03-17', end_date='2026-03-17',
            frequency="d", adjustflag="3")
        rows = []
        while (rs.error_code == '0') and rs.next():
            rows.append(rs.get_row_data())
        if rows:
            r = rows[0]
            print(f"  {code}: open={r[2]} high={r[3]} low={r[4]} close={r[5]} vol={r[6]} amt={r[7]}")
        else:
            print(f"  {code}: EMPTY (err={rs.error_msg})")
    t1 = time.time()
    avg_ms3 = (t1-t0)/5*1000
    print(f"  Avg: {avg_ms3:.0f}ms/stock")
    print(f"  Est for 5500 stocks: {5500*avg_ms3/1000/60:.1f} min")
    bs.logout()
except ImportError:
    print("  baostock not installed. Try: pip install baostock")
except Exception as e:
    print(f"  Baostock error: {e}")

print()
print("=" * 60)
print("Test 4: efinance library")
print("=" * 60)

try:
    import efinance as ef
    t0 = time.time()
    df = ef.stock.get_quote_history(['600519', '000001', '601318'], beg='20260317', end='20260317')
    t1 = time.time()
    if isinstance(df, dict):
        for k, v in df.items():
            print(f"  {k}: {len(v)} rows")
            if len(v) > 0:
                print(f"    Columns: {list(v.columns)}")
                print(f"    {v.iloc[0].to_dict()}")
    else:
        print(f"  Result: {type(df)}, {len(df)} rows")
        if len(df) > 0:
            print(f"  Columns: {list(df.columns)}")
    print(f"  Time for 3 stocks: {t1-t0:.2f}s")
except ImportError:
    print("  efinance not installed. Try: pip install efinance")
except Exception as e:
    print(f"  efinance error: {e}")

print()
print("=" * 60)
print("Test 5: 163 Money CSV download")
print("=" * 60)

try:
    url6 = 'http://quotes.money.163.com/service/chddata.html'
    params6 = {
        'code': '0600519',
        'start': '20260317',
        'end': '20260317',
        'fields': 'TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER',
    }
    r6 = requests.get(url6, params=params6, timeout=10)
    lines = r6.text.strip().split('\n')
    for line in lines[:3]:
        print(f"  {line[:120]}")
except Exception as e:
    print(f"  163 error: {e}")

print()
print("=" * 60)
print("Test 6: Eastmoney datacenter - search for OHLCV reports")
print("=" * 60)

# Try different report names
reports = [
    'RPT_LICO_STOCKINFO_ALL',
    'RPT_MARKET_QUOTATION_SZ',
    'RPT_DAILYTRADE_STOCK',
    'RPT_MARKETQUOTATION_DAILY',
    'RPTA_APP_STOCKTRADE',
]

dc_url = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
for report in reports:
    try:
        params = {
            'reportName': report,
            'columns': 'ALL',
            'filter': "(TRADE_DATE='2026-03-17')",
            'pageSize': '3',
            'pageNumber': '1',
            'source': 'WEB', 'client': 'WEB',
        }
        r = session.get(dc_url, params=params, timeout=5)
        d = r.json()
        ok = d.get('success', False)
        count = d.get('result', {}).get('count', 0) if d.get('result') else 0
        if ok and count > 0:
            cols = list(d['result']['data'][0].keys())
            has_ohlc = any(c for c in cols if 'OPEN' in c or 'HIGH' in c or 'LOW' in c)
            print(f"  {report}: count={count}, has_OHLC={has_ohlc}")
            if has_ohlc:
                sample = d['result']['data'][0]
                ohlc_cols = [c for c in cols if any(x in c for x in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])]
                print(f"    OHLCV cols: {ohlc_cols}")
                print(f"    Sample: {dict((k, sample[k]) for k in ohlc_cols[:8])}")
        else:
            print(f"  {report}: ok={ok}, count={count}")
    except Exception as e:
        print(f"  {report}: ERROR {e}")

print()
print("=" * 60)
print("SUMMARY")
print("=" * 60)
