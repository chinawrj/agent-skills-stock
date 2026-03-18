import json

# Shareholder decrease data from DB query
sh_data = {
    # 4 consecutive periods
    "001356": {"consec": 4, "cumul": -126.26, "latest": -23.87, "mcap": 86.9},
    "603336": {"consec": 4, "cumul": -77.61, "latest": -16.22, "mcap": 56.9},
    "301585": {"consec": 4, "cumul": -73.90, "latest": -12.38, "mcap": 34.4},
    # 3 consecutive periods
    "603124": {"consec": 3, "cumul": -118.09, "latest": -60.29, "mcap": 127.0},
    "603409": {"consec": 3, "cumul": -102.47, "latest": -36.89, "mcap": 44.7},
    "603271": {"consec": 3, "cumul": -99.94, "latest": -30.77, "mcap": 79.3},
    "001382": {"consec": 3, "cumul": -99.78, "latest": -22.76, "mcap": 88.0},
    "301557": {"consec": 3, "cumul": -94.43, "latest": -22.71, "mcap": 36.0},
    "301602": {"consec": 3, "cumul": -87.22, "latest": -23.90, "mcap": 105.3},
    "688757": {"consec": 3, "cumul": -83.85, "latest": -19.79, "mcap": 129.8},
    "000753": {"consec": 3, "cumul": -62.12, "latest": -30.37, "mcap": 70.6},
    "603153": {"consec": 3, "cumul": -42.76, "latest": -10.42, "mcap": 79.2},
    "002187": {"consec": 3, "cumul": -40.72, "latest": -14.47, "mcap": 44.7},
    "600327": {"consec": 3, "cumul": -36.86, "latest": -11.89, "mcap": 41.8},
    # 2 consecutive periods (>40% cumulative)
    "001390": {"consec": 2, "cumul": -87.66, "latest": -23.62, "mcap": 48.7},
    "603334": {"consec": 2, "cumul": -82.67, "latest": -32.48, "mcap": 78.4},
    "603120": {"consec": 2, "cumul": -75.77, "latest": -27.31, "mcap": 36.7},
    "605100": {"consec": 2, "cumul": -73.46, "latest": -45.92, "mcap": 49.6},
    "603400": {"consec": 2, "cumul": -63.30, "latest": -32.78, "mcap": 61.2},
    "301595": {"consec": 2, "cumul": -60.43, "latest": -16.71, "mcap": 41.9},
    "002918": {"consec": 2, "cumul": -58.87, "latest": -41.78, "mcap": 70.0},
    "605155": {"consec": 2, "cumul": -54.84, "latest": -37.40, "mcap": 31.9},
    "688583": {"consec": 2, "cumul": -48.91, "latest": -12.33, "mcap": 72.9},
    "603015": {"consec": 2, "cumul": -40.77, "latest": -22.16, "mcap": 47.9},
}

# Profitability data from EastMoney API
profit_data = {
    "001356": {"name": "富岭股份", "roe": 18.14, "np24": 2.20, "np23": 2.16},
    "603336": {"name": "宏辉果蔬", "roe": 1.58, "np24": 0.18, "np23": 0.24},
    "301585": {"name": "蓝宇股份", "roe": 20.80, "np24": 1.01, "np23": 0.93},
    "603124": {"name": "江南新材", "roe": 13.93, "np24": 1.76, "np23": 1.42},
    "603409": {"name": "汇通控股", "roe": 24.10, "np24": 1.63, "np23": 1.52},
    "603271": {"name": "永杰新材", "roe": 21.07, "np24": 3.19, "np23": 2.38},
    "001382": {"name": "新亚电缆", "roe": 11.75, "np24": 1.35, "np23": 1.64},
    "301557": {"name": "常友科技", "roe": 15.50, "np24": 1.05, "np23": 0.83},
    "301602": {"name": "超研股份", "roe": 18.26, "np24": 1.46, "np23": 1.15},
    "688757": {"name": "胜科纳米", "roe": 14.40, "np24": 0.81, "np23": 0.99},
    "000753": {"name": "漳州发展", "roe": 1.91, "np24": 0.53, "np23": 0.78},
    "603153": {"name": "上海建科", "roe": 9.73, "np24": 3.43, "np23": 3.16},
    "002187": {"name": "广百股份", "roe": 1.18, "np24": 0.48, "np23": 0.36},
    "600327": {"name": "大东方", "roe": 1.32, "np24": 0.44, "np23": 1.61},
    "001390": {"name": "古麒绒材", "roe": 20.27, "np24": 1.68, "np23": 1.22},
    "603334": {"name": "丰倍生物", "roe": 17.91, "np24": 1.24, "np23": 1.30},
    "603120": {"name": "肯特催化", "roe": 11.53, "np24": 0.93, "np23": 0.85},
    "605100": {"name": "华丰股份", "roe": 3.94, "np24": 0.74, "np23": 1.09},
    "603400": {"name": "华之杰", "roe": 23.19, "np24": 1.54, "np23": 1.21},
    "301595": {"name": "太力科技", "roe": 19.75, "np24": 0.88, "np23": 0.85},
    "002918": {"name": "蒙娜丽莎", "roe": 3.64, "np24": 1.25, "np23": 2.66},
    "605155": {"name": "西大门", "roe": 9.89, "np24": 1.22, "np23": 0.91},
    "688583": {"name": "思看科技", "roe": 21.62, "np24": 1.21, "np23": 1.14},
    "603015": {"name": "弘讯科技", "roe": 4.72, "np24": 0.64, "np23": 0.64},
}

def score_consec(periods):
    """连续减少期数 (30%)"""
    if periods >= 4: return 100
    if periods == 3: return 80
    return 60

def score_cumul(cumul):
    """累计减少幅度 (25%) - cumul is negative"""
    cumul = abs(cumul)
    if cumul >= 80: return 100
    if cumul >= 50: return 85
    if cumul >= 30: return 70
    return 55

def score_latest(latest):
    """最新一期减少幅度 (15%) - latest is negative"""
    latest = abs(latest)
    if latest >= 30: return 100
    if latest >= 20: return 85
    if latest >= 15: return 70
    return 55

def score_roe(roe):
    """ROE (20%)"""
    if roe >= 18: return 100
    if roe >= 12: return 85
    if roe >= 8: return 70
    if roe >= 4: return 55
    return 35

def score_mcap(mcap):
    """市值适中 (10%)"""
    if 40 <= mcap <= 80: return 100
    if 80 < mcap <= 120: return 80
    return 60

results = []
for code in sh_data:
    sh = sh_data[code]
    pf = profit_data[code]
    
    s1 = score_consec(sh["consec"])
    s2 = score_cumul(sh["cumul"])
    s3 = score_latest(sh["latest"])
    s4 = score_roe(pf["roe"])
    s5 = score_mcap(sh["mcap"])
    
    total = s1 * 0.30 + s2 * 0.25 + s3 * 0.15 + s4 * 0.20 + s5 * 0.10
    
    results.append({
        "code": code,
        "name": pf["name"],
        "score": round(total, 1),
        "consec": sh["consec"],
        "cumul": sh["cumul"],
        "latest": sh["latest"],
        "mcap": sh["mcap"],
        "roe": pf["roe"],
        "np24": pf["np24"],
        "np23": pf["np23"],
        "s1": s1, "s2": s2, "s3": s3, "s4": s4, "s5": s5,
    })

results.sort(key=lambda x: -x["score"])

print("=" * 100)
print(f"{'排名':>4} {'代码':<8} {'名称':<8} {'总分':>5} {'期数':>4} {'累计%':>8} {'最新%':>7} {'ROE%':>7} {'市值亿':>7} {'净利24':>7}")
print("-" * 100)
for i, r in enumerate(results[:15], 1):
    print(f"{i:>4} {r['code']:<8} {r['name']:<8} {r['score']:>5.1f} {r['consec']:>4} {r['cumul']:>8.2f} {r['latest']:>7.2f} {r['roe']:>7.2f} {r['mcap']:>7.1f} {r['np24']:>7.2f}")
print("=" * 100)

with open("/tmp/zhuang_scores.json", "w") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
print(f"\nSaved {len(results)} scored stocks to /tmp/zhuang_scores.json")
