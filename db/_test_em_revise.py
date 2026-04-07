#!/usr/bin/env python3
"""Quick test script for eastmoney revision history API."""
import requests
import json

url = 'https://datacenter-web.eastmoney.com/api/data/v1/get'

# Try various report names
for name in ['RPT_BOND_CB_ADJ', 'RPT_BOND_CB_TRANSFER_ADJ', 
             'RPT_BOND_CB_ADJUST', 'RPTA_BOND_CB_ADJUST',
             'RPT_BOND_CB_LIST_ADJ']:
    params = {
        'sortColumns': 'SECURITY_CODE', 'sortTypes': '-1',
        'pageSize': '5', 'pageNumber': '1',
        'reportName': name,
        'columns': 'ALL',
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get('result') and data['result'].get('data'):
            total = data['result'].get('count', 0)
            print(f'{name}: {total} records')
            row = data['result']['data'][0]
            for k, v in sorted(row.items()):
                if v is not None:
                    print(f'  {k}: {repr(v)[:120]}')
            print()
        else:
            print(f'{name}: no data - {data.get("message", "")}')
    except Exception as e:
        print(f'{name}: error - {e}')

# Also check HIST_ADJ_COUNT distribution in RPT_BOND_CB_CLAUSE
print("\n=== HIST_ADJ_COUNT from RPT_BOND_CB_CLAUSE ===")
params = {
    'sortColumns': 'HIST_ADJ_COUNT', 'sortTypes': '-1',
    'pageSize': '20', 'pageNumber': '1',
    'reportName': 'RPT_BOND_CB_CLAUSE',
    'columns': 'SECURITY_CODE,BOND_NAME_ABBR,HIST_ADJ_COUNT',
    'filter': '(HIST_ADJ_COUNT>0)',
}
r = requests.get(url, params=params, timeout=15)
data = r.json()
if data.get('result'):
    total = data['result'].get('count', 0)
    print(f"Bonds with adj_count > 0: {total}")
    for row in data['result']['data'][:10]:
        print(f"  {row['SECURITY_CODE']} {row['BOND_NAME_ABBR']}: {row['HIST_ADJ_COUNT']} times")
