#!/usr/bin/env python3
"""Deep test of Tencent and Sina K-line APIs for full year data."""
import requests
import json
import time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

def get_prefix(code):
    """Get exchange prefix for stock code."""
    if code.startswith("6") or code.startswith("9"):
        return "sh"
    return "sz"

def test_tencent_full(code="000001"):
    """Test Tencent API with full year data."""
    prefix = get_prefix(code)
    url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
    params = {"param": f"{prefix}{code},day,2025-03-16,2026-03-16,300,qfq"}
    resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
    data = resp.json()

    stock_key = f"{prefix}{code}"
    klines = data.get("data", {}).get(stock_key, {}).get("qfqday", [])
    if not klines:
        klines = data.get("data", {}).get(stock_key, {}).get("day", [])

    print(f"Tencent {code}: {len(klines)} days")
    if klines:
        print(f"  First: {klines[0]}")
        print(f"  Last:  {klines[-1]}")
        # Format: [date, open, close, high, low, volume]
        # Note: NO amount field!
    return len(klines)

def test_sina_full(code="000001"):
    """Test Sina API with full year data."""
    prefix = get_prefix(code)
    url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
    params = {"symbol": f"{prefix}{code}", "scale": 240, "ma": "no", "datalen": 300}
    resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
    data = json.loads(resp.text)

    print(f"Sina {code}: {len(data)} days")
    if data:
        print(f"  First: {data[0]}")
        print(f"  Last:  {data[-1]}")
        # Format: {day, open, high, low, close, volume}
        # Note: NO amount, NO turnover
    return len(data)

# Test with different stock types
test_codes = [
    ("000001", "SZ main"),
    ("600519", "SH main"),
    ("300750", "ChiNext"),
    ("688981", "STAR"),
    ("001356", "SZ new"),
]

print("=" * 60)
print("Tencent API Tests")
print("=" * 60)
for code, desc in test_codes:
    try:
        n = test_tencent_full(code)
    except Exception as e:
        print(f"Tencent {code} ({desc}): FAIL - {e}")
    time.sleep(0.5)

print()
print("=" * 60)
print("Sina API Tests")
print("=" * 60)
for code, desc in test_codes:
    try:
        n = test_sina_full(code)
    except Exception as e:
        print(f"Sina {code} ({desc}): FAIL - {e}")
    time.sleep(0.5)

# Speed test
print()
print("=" * 60)
print("Speed Test: Sina 20 stocks")
print("=" * 60)
speed_codes = ["000001", "600519", "300750", "688981", "001356",
               "002475", "600036", "000858", "300059", "600309",
               "002230", "601318", "000002", "600887", "300760",
               "002594", "601012", "000725", "300408", "603259"]
t0 = time.time()
ok = 0
fail = 0
for code in speed_codes:
    try:
        prefix = get_prefix(code)
        url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {"symbol": f"{prefix}{code}", "scale": 240, "ma": "no", "datalen": 250}
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        data = json.loads(resp.text)
        ok += 1
    except:
        fail += 1
    time.sleep(0.3)
elapsed = time.time() - t0
print(f"20 stocks in {elapsed:.1f}s ({ok} ok, {fail} fail)")
print(f"Avg: {elapsed/20:.2f}s/stock (incl 0.3s delay)")
print(f"Estimated 5000 stocks: {5000 * elapsed / 20 / 60:.0f} min")
