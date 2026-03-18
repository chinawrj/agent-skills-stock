#!/usr/bin/env python3
"""Test Xueqiu single post detail API to get full text."""
import asyncio
import json
from playwright.async_api import async_playwright

CDP_URL = "http://localhost:9222"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(CDP_URL)
        ctx = browser.contexts[0]
        page = ctx.pages[0]
        print(f"Connected. Current page: {page.url}")

        # Navigate to xueqiu first to ensure cookies are set
        if "xueqiu.com" not in page.url:
            print("Navigating to xueqiu.com...")
            await page.goto("https://xueqiu.com/u/9508203182", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
            print(f"Now on: {page.url}")

        # Test with a known truncated post
        test_id = 379659696

        # Try different API endpoints
        endpoints = [
            f"https://xueqiu.com/statuses/show.json?id={test_id}",
            f"https://xueqiu.com/v4/statuses/show.json?id={test_id}",
        ]

        for url in endpoints:
            print(f"\n--- Trying: {url}")
            result = await page.evaluate(f'''async () => {{
                const resp = await fetch("{url}", {{credentials: "include"}});
                if (!resp.ok) return {{error: resp.status, text: await resp.text().catch(e => e.message)}};
                const data = await resp.json();
                return data;
            }}''')

            if "error" in result:
                print(f"  Error: {result['error']}")
                print(f"  Text: {str(result.get('text',''))[:200]}")
            else:
                # Print the keys and description length
                print(f"  Top keys: {list(result.keys())[:15]}")
                if "status" in result:
                    s = result["status"] if isinstance(result["status"], dict) else result
                elif "data" in result:
                    s = result["data"] if isinstance(result["data"], dict) else result
                else:
                    s = result

                if isinstance(s, dict):
                    desc = s.get("description", s.get("text", ""))
                    print(f"  Status keys: {list(s.keys())[:20]}")
                    print(f"  Description length: {len(desc)}")
                    # Strip HTML for display
                    import re
                    clean = re.sub(r'<[^>]+>', '', desc)
                    print(f"  Full text ({len(clean)} chars): {clean[:300]}...")
                    if len(clean) > 300:
                        print(f"  ...{clean[-100:]}")

asyncio.run(main())
