#!/usr/bin/env python3
"""Benchmark K-line API speed."""
import requests, time

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Referer': 'https://quote.eastmoney.com/',
}

def get_secid(code):
    if code.startswith('6') or code.startswith('9'):
        return f'1.{code}'
    return f'0.{code}'

def fetch_kline(code, start='20250316', end='20260316'):
    url = 'https://push2his.eastmoney.com/api/qt/stock/kline/get'
    params = {
        'secid': get_secid(code),
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': 101, 'fqt': 1,
        'beg': start, 'end': end,
    }
    resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
    data = resp.json()
    return data.get('data', {}).get('klines', [])

times = []
for code in ['000001', '600519', '300750', '002475', '688981']:
    t0 = time.time()
    klines = fetch_kline(code)
    elapsed = time.time() - t0
    times.append(elapsed)
    if klines:
        print(f'{code}: {len(klines)} days, {elapsed:.3f}s')
        print(f'  Sample: {klines[0]}')
    time.sleep(0.5)

avg = sum(times) / len(times)
print(f'\nAvg: {avg:.3f}s/stock')
print(f'Estimated 5000 stocks (0.5s delay): {5000*(avg+0.5)/60:.0f} min')
