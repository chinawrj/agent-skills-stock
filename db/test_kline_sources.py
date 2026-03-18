#!/usr/bin/env python3
"""Test various K-line data sources to find working one."""
import requests
import time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

def test_eastmoney_kline():
    """EastMoney push2his API"""
    print("=== Test 1: EastMoney push2his ===")
    try:
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": "1.600519",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": 101, "fqt": 1,
            "beg": "20260301", "end": "20260316",
        }
        resp = requests.get(url, params=params, headers={
            **HEADERS, "Referer": "https://quote.eastmoney.com/"
        }, timeout=10)
        data = resp.json()
        klines = data.get("data", {}).get("klines", [])
        print(f"  OK: {len(klines)} klines")
        if klines:
            print(f"  Sample: {klines[0]}")
        return True
    except Exception as e:
        print(f"  FAIL: {e}")
        return False

def test_tencent_kline():
    """Tencent ifeng/qt API"""
    print("\n=== Test 2: Tencent qt ===")
    try:
        url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        params = {
            "param": "sh600519,day,2026-03-01,2026-03-16,10,qfq",
        }
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        data = resp.json()
        klines = data.get("data", {}).get("sh600519", {}).get("qfqday", [])
        if not klines:
            klines = data.get("data", {}).get("sh600519", {}).get("day", [])
        print(f"  OK: {len(klines)} klines")
        if klines:
            print(f"  Sample: {klines[0]}")
            print(f"  Format: [date, open, close, high, low, volume]")
        return True
    except Exception as e:
        print(f"  FAIL: {e}")
        return False

def test_sina_kline():
    """Sina finance API"""
    print("\n=== Test 3: Sina ===")
    try:
        url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {
            "symbol": "sh600519",
            "scale": 240,  # daily
            "ma": "no",
            "datalen": 10,
        }
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        text = resp.text
        # Sina returns JS-like format
        import json
        data = json.loads(text)
        print(f"  OK: {len(data)} klines")
        if data:
            print(f"  Sample: {data[0]}")
        return True
    except Exception as e:
        print(f"  FAIL: {e}")
        return False

def test_eastmoney_hsgt():
    """EastMoney datacenter - daily market history"""
    print("\n=== Test 4: EastMoney datacenter (daily history) ===")
    try:
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": "0.000001",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": 101, "fqt": 1,
            "beg": "20260301", "end": "20260316",
        }
        headers = {**HEADERS, "Referer": "https://quote.eastmoney.com/"}
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        data = resp.json()
        klines = data.get("data", {}).get("klines", [])
        print(f"  OK: {len(klines)} klines (000001)")
        return True
    except Exception as e:
        print(f"  FAIL: {e}")
        return False

# Run all tests
results = []
for test_fn in [test_eastmoney_kline, test_tencent_kline, test_sina_kline]:
    ok = test_fn()
    results.append(ok)
    time.sleep(1)

print("\n=== Summary ===")
names = ["EastMoney push2his", "Tencent qt", "Sina"]
for name, ok in zip(names, results):
    print(f"  {name}: {'OK' if ok else 'FAIL'}")
