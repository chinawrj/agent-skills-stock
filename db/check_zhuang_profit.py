import requests
import json
import time

codes = [
    # 4 consecutive periods
    "001356", "603336", "301585",
    # 3 consecutive periods  
    "603124", "603409", "603271", "001382", "301557", "301602", "688757",
    "000753", "002238", "002137", "603153", "002187", "600327",
    # 2 periods (>40% cumulative)
    "001390", "603334", "603120", "605100", "002861", "603400", "301595",
    "002918", "002875", "002943", "605155", "301227", "002582", "688583",
    "600593", "301096", "603015", "300846"
]

def fetch_profits(codes_list):
    codes_str = '","'.join(codes_list)
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_LICO_FN_CPD",
        "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,REPORTDATE,DATAYEAR,DATEMMDD,BASIC_EPS,WEIGHTAVG_ROE,PARENT_NETPROFIT,TOTAL_OPERATE_INCOME",
        "filter": f'(SECURITY_CODE in ("{codes_str}"))(DATEMMDD="年报")',
        "sortColumns": "SECURITY_CODE,DATAYEAR",
        "sortTypes": "1,-1",
        "pageSize": 500,
        "pageNumber": 1,
    }
    resp = requests.get(url, params=params, timeout=30)
    data = resp.json()
    if not data.get("result") or not data["result"].get("data"):
        print(f"ERROR: No data. Msg: {data.get('message')}")
        return {}
    results = {}
    for row in data["result"]["data"]:
        code = row["SECURITY_CODE"]
        if code not in results:
            results[code] = {"name": row["SECURITY_NAME_ABBR"], "reports": []}
        results[code]["reports"].append({
            "year": row["DATAYEAR"],
            "eps": row.get("BASIC_EPS"),
            "roe": row.get("WEIGHTAVG_ROE"),
            "net_profit": row.get("PARENT_NETPROFIT"),
            "revenue": row.get("TOTAL_OPERATE_INCOME"),
        })
    return results

all_results = {}
for i in range(0, len(codes), 50):
    batch = codes[i:i+50]
    print(f"Fetching batch {i//50+1}: {len(batch)} stocks...")
    result = fetch_profits(batch)
    all_results.update(result)
    if i + 50 < len(codes):
        time.sleep(1)

print(f"\nTotal: {len(all_results)}")

profitable = []
unprofitable = []

for code in codes:
    if code not in all_results:
        unprofitable.append({"code": code, "reason": "no_data"})
        continue
    info = all_results[code]
    reports = {r["year"]: r for r in info["reports"]}
    p2024 = reports.get("2024", {}).get("net_profit")
    p2023 = reports.get("2023", {}).get("net_profit")
    roe2024 = reports.get("2024", {}).get("roe")
    if p2024 and p2024 > 0 and p2023 and p2023 > 0:
        profitable.append({
            "code": code,
            "name": info["name"],
            "roe_2024": round(roe2024, 2) if roe2024 else None,
            "net_profit_2024": round(p2024 / 1e8, 2),
            "net_profit_2023": round(p2023 / 1e8, 2),
        })
    else:
        unprofitable.append({
            "code": code,
            "name": info.get("name", ""),
            "reason": f"2024:{p2024}, 2023:{p2023}"
        })

print(f"\nProfitable: {len(profitable)}")
for p in profitable:
    print(f"  {p['code']} {p['name']:<10s} ROE={p['roe_2024']}% NP24={p['net_profit_2024']}亿 NP23={p['net_profit_2023']}亿")

print(f"\nUnprofitable: {len(unprofitable)}")
for u in unprofitable:
    print(f"  {u['code']} {u.get('name','')} {u['reason']}")

with open("/tmp/zhuang_profit.json", "w") as f:
    json.dump({"profitable": profitable, "unprofitable": unprofitable}, f, ensure_ascii=False, indent=2)
print("\nSaved to /tmp/zhuang_profit.json")
