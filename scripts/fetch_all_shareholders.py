#!/usr/bin/env python3
"""
Batch fetch ALL historical shareholder data for ALL A-share stocks.

Strategy: 
  - Use RPT_HOLDERNUM_DET without stock filter to get ALL records
  - Paginate through all pages (pageSize=500)
  - Save to CSV for DuckDB import

This avoids 5000+ individual API calls.
"""

import asyncio
import csv
import os
import sys
import json
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')


async def test_bulk_api():
    """Test if the API works without stock filter (bulk mode)."""
    from browser_manager import get_browser_page
    
    print("Connecting to browser...")
    page = await get_browser_page()
    
    # First test: try without filter to get ALL historical data
    print("Testing bulk API (no stock filter)...")
    result = await page.evaluate("""
    async () => {
        const url = "https://datacenter-web.eastmoney.com/api/data/v1/get";
        const params = new URLSearchParams({
            sortColumns: "END_DATE",
            sortTypes: "-1",
            pageSize: "500",
            pageNumber: "1",
            reportName: "RPT_HOLDERNUM_DET",
            columns: "SECURITY_CODE,SECURITY_NAME_ABBR,END_DATE,HOLD_NOTICE_DATE,HOLDER_NUM,PRE_HOLDER_NUM,HOLDER_NUM_CHANGE,HOLDER_NUM_RATIO,INTERVAL_CHRATE,AVG_MARKET_CAP,AVG_HOLD_NUM,TOTAL_MARKET_CAP,TOTAL_A_SHARES,CHANGE_SHARES,CHANGE_REASON",
            source: "WEB",
            client: "WEB"
        });
        
        const resp = await fetch(url + "?" + params.toString());
        const data = await resp.json();
        
        return {
            success: data.success,
            pages: data.result ? data.result.pages : 0,
            count: data.result ? data.result.count : 0,
            sampleSize: data.result && data.result.data ? data.result.data.length : 0,
            sample: data.result && data.result.data ? data.result.data.slice(0, 3) : []
        };
    }
    """)
    
    print(f"  Success: {result['success']}")
    print(f"  Total pages: {result['pages']}")
    print(f"  Total records: {result['count']}")
    print(f"  Sample size: {result['sampleSize']}")
    if result['sample']:
        print(f"  Sample record keys: {list(result['sample'][0].keys())}")
        print(f"  Sample: {json.dumps(result['sample'][0], ensure_ascii=False, indent=2)}")
    
    return result


async def fetch_all_history(page_size=500, start_page=1):
    """Fetch ALL historical shareholder records for ALL stocks."""
    from browser_manager import get_browser_page
    
    page = await get_browser_page()
    
    # First, get total page count
    meta = await page.evaluate("""
    async () => {
        const resp = await fetch("https://datacenter-web.eastmoney.com/api/data/v1/get?" + new URLSearchParams({
            sortColumns: "END_DATE",
            sortTypes: "-1",
            pageSize: "500",
            pageNumber: "1",
            reportName: "RPT_HOLDERNUM_DET",
            columns: "SECURITY_CODE",
            source: "WEB",
            client: "WEB"
        }));
        const data = await resp.json();
        return { pages: data.result.pages, count: data.result.count };
    }
    """)
    
    total_pages = meta['pages']
    total_count = meta['count']
    print(f"Total records: {total_count}, Pages: {total_pages} (pageSize={page_size})")
    
    # Open CSV for writing
    csv_path = os.path.join(DATA_DIR, 'all_shareholders_history.csv')
    f = open(csv_path, 'w', newline='', encoding='utf-8')
    writer = csv.writer(f)
    writer.writerow([
        'code', 'name', 'stat_date', 'announce_date', 'shareholders',
        'shareholders_prev', 'change', 'change_ratio', 'range_change_pct',
        'avg_value', 'avg_shares', 'market_cap', 'total_shares',
        'shares_change', 'shares_change_reason'
    ])
    
    total_written = 0
    
    for pg in range(start_page, total_pages + 1):
        try:
            data = await page.evaluate(f"""
            async () => {{
                const resp = await fetch("https://datacenter-web.eastmoney.com/api/data/v1/get?" + new URLSearchParams({{
                    sortColumns: "END_DATE",
                    sortTypes: "-1",
                    pageSize: "{page_size}",
                    pageNumber: "{pg}",
                    reportName: "RPT_HOLDERNUM_DET",
                    columns: "SECURITY_CODE,SECURITY_NAME_ABBR,END_DATE,HOLD_NOTICE_DATE,HOLDER_NUM,PRE_HOLDER_NUM,HOLDER_NUM_CHANGE,HOLDER_NUM_RATIO,INTERVAL_CHRATE,AVG_MARKET_CAP,AVG_HOLD_NUM,TOTAL_MARKET_CAP,TOTAL_A_SHARES,CHANGE_SHARES,CHANGE_REASON",
                    source: "WEB",
                    client: "WEB"
                }}));
                const data = await resp.json();
                if (!data.success || !data.result || !data.result.data) return [];
                return data.result.data;
            }}
            """)
            
            for row in data:
                end_date = (row.get('END_DATE') or '')[:10]
                ann_date = (row.get('HOLD_NOTICE_DATE') or '')[:10]
                writer.writerow([
                    row.get('SECURITY_CODE', ''),
                    row.get('SECURITY_NAME_ABBR', ''),
                    end_date,
                    ann_date,
                    row.get('HOLDER_NUM', ''),
                    row.get('PRE_HOLDER_NUM', ''),
                    row.get('HOLDER_NUM_CHANGE', ''),
                    row.get('HOLDER_NUM_RATIO', ''),
                    row.get('INTERVAL_CHRATE', ''),
                    row.get('AVG_MARKET_CAP', ''),
                    row.get('AVG_HOLD_NUM', ''),
                    row.get('TOTAL_MARKET_CAP', ''),
                    row.get('TOTAL_A_SHARES', ''),
                    row.get('CHANGE_SHARES', ''),
                    row.get('CHANGE_REASON', ''),
                ])
                total_written += 1
            
            if pg % 10 == 0 or pg == total_pages:
                print(f"  Page {pg}/{total_pages} - {total_written} records written")
                f.flush()
                
        except Exception as e:
            print(f"  ERROR on page {pg}: {e}")
            # Retry once
            await asyncio.sleep(2)
            try:
                data = await page.evaluate(f"""
                async () => {{
                    const resp = await fetch("https://datacenter-web.eastmoney.com/api/data/v1/get?" + new URLSearchParams({{
                        sortColumns: "END_DATE",
                        sortTypes: "-1",
                        pageSize: "{page_size}",
                        pageNumber: "{pg}",
                        reportName: "RPT_HOLDERNUM_DET",
                        columns: "SECURITY_CODE,SECURITY_NAME_ABBR,END_DATE,HOLD_NOTICE_DATE,HOLDER_NUM,PRE_HOLDER_NUM,HOLDER_NUM_CHANGE,HOLDER_NUM_RATIO,INTERVAL_CHRATE,AVG_MARKET_CAP,AVG_HOLD_NUM,TOTAL_MARKET_CAP,TOTAL_A_SHARES,CHANGE_SHARES,CHANGE_REASON",
                        source: "WEB",
                        client: "WEB"
                    }}));
                    const data = await resp.json();
                    if (!data.success || !data.result || !data.result.data) return [];
                    return data.result.data;
                }}
                """)
                for row in data:
                    end_date = (row.get('END_DATE') or '')[:10]
                    ann_date = (row.get('HOLD_NOTICE_DATE') or '')[:10]
                    writer.writerow([
                        row.get('SECURITY_CODE', ''),
                        row.get('SECURITY_NAME_ABBR', ''),
                        end_date,
                        ann_date,
                        row.get('HOLDER_NUM', ''),
                        row.get('PRE_HOLDER_NUM', ''),
                        row.get('HOLDER_NUM_CHANGE', ''),
                        row.get('HOLDER_NUM_RATIO', ''),
                        row.get('INTERVAL_CHRATE', ''),
                        row.get('AVG_MARKET_CAP', ''),
                        row.get('AVG_HOLD_NUM', ''),
                        row.get('TOTAL_MARKET_CAP', ''),
                        row.get('TOTAL_A_SHARES', ''),
                        row.get('CHANGE_SHARES', ''),
                        row.get('CHANGE_REASON', ''),
                    ])
                    total_written += 1
                print(f"  Page {pg} retry OK - {total_written} records")
            except Exception as e2:
                print(f"  Page {pg} FAILED twice: {e2}, skipping")
    
    f.close()
    print(f"\nDone! Total {total_written} records saved to {csv_path}")
    return csv_path, total_written


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true', help='Test API only')
    parser.add_argument('--start-page', type=int, default=1, help='Resume from page N')
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"Batch Historical Shareholder Fetcher - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)
    
    if args.test:
        await test_bulk_api()
        return
    
    csv_path, count = await fetch_all_history(start_page=args.start_page)
    print(f"\nCSV saved: {csv_path}")
    print(f"Records: {count}")
    print("Import to DuckDB with:")
    print(f"  INSERT OR REPLACE INTO shareholders ...")


if __name__ == "__main__":
    asyncio.run(main())
