#!/usr/bin/env python3
"""Test fast full-text extraction methods for Xueqiu posts."""
import asyncio
import json
import re
import time
from playwright.async_api import async_playwright

CDP_URL = "http://localhost:9222"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(CDP_URL)
        ctx = browser.contexts[0]
        page = ctx.pages[0]

        if "xueqiu.com" not in page.url:
            await page.goto("https://xueqiu.com/u/9508203182", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)

        uid = 9508203182
        test_ids = [379659696, 378692708, 378476918]

        # Method 1: fetch HTML of detail page via JS fetch (no navigation)
        print("=== Method 1: JS fetch of detail page HTML ===")
        for sid in test_ids:
            t0 = time.time()
            result = await page.evaluate(f'''async () => {{
                const url = "https://xueqiu.com/{uid}/{sid}";
                const resp = await fetch(url, {{credentials: "include"}});
                const html = await resp.text();
                // Extract text from SNOWMAN_STATUS or article content
                const match = html.match(/SNOWMAN_STATUS\\s*=\\s*(\\{{.*?\\}});/s);
                if (match) {{
                    try {{
                        const data = JSON.parse(match[1]);
                        const desc = data.description || data.text || "";
                        return {{method: "SNOWMAN_STATUS", len: desc.length, text: desc}};
                    }} catch(e) {{
                        return {{method: "SNOWMAN_STATUS_parse_error", error: e.message}};
                    }}
                }}
                // Fallback: extract from page HTML
                const articleMatch = html.match(/<article[^>]*class="[^"]*article__bd__detail[^"]*"[^>]*>(.*?)<\\/article>/s);
                if (articleMatch) {{
                    return {{method: "article_html", len: articleMatch[1].length, text: articleMatch[1]}};
                }}
                return {{method: "none", html_len: html.length, snippet: html.substring(0, 500)}};
            }}''')
            elapsed = time.time() - t0
            print(f"  Post {sid}: method={result.get('method')}, len={result.get('len', '?')}, time={elapsed:.2f}s")
            text = result.get('text', '')
            if text:
                # Strip HTML
                clean = re.sub(r'<[^>]+>', '', text)
                print(f"    Clean text ({len(clean)} chars): {clean[:200]}...")

        # Method 2: Try /api/post endpoint or similar
        print("\n=== Method 2: Try API variants ===")
        api_urls = [
            f"https://xueqiu.com/api/v5/statuses/show.json?id={test_ids[0]}",
            f"https://xueqiu.com/statuses/original/show.json?id={test_ids[0]}",
            f"https://xueqiu.com/query/v1/status/show.json?id={test_ids[0]}",
        ]
        for url in api_urls:
            result = await page.evaluate(f'''async () => {{
                const resp = await fetch("{url}", {{credentials: "include"}});
                if (!resp.ok) return {{error: resp.status}};
                try {{
                    const data = await resp.json();
                    return {{ok: true, keys: Object.keys(data), desc_len: ((data.description || data.text || data.data?.description || "")).length}};
                }} catch(e) {{
                    return {{error: "parse_error", msg: e.message}};
                }}
            }}''')
            name = url.split("xueqiu.com")[1].split("?")[0]
            print(f"  {name}: {result}")

asyncio.run(main())
