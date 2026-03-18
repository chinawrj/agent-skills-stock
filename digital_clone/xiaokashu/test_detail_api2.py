#!/usr/bin/env python3
"""Test fetching full post text from Xueqiu."""
import asyncio
import json
import re
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

        test_id = 379659696
        uid = 9508203182

        # 1. Check truncated field from show.json
        print("=== Test 1: statuses/show.json truncated field ===")
        result = await page.evaluate(f'''async () => {{
            const resp = await fetch("https://xueqiu.com/statuses/show.json?id={test_id}", {{credentials: "include"}});
            const data = await resp.json();
            return {{
                truncated: data.truncated,
                description_len: (data.description || "").length,
                keys: Object.keys(data),
            }};
        }}''')
        print(f"  truncated field = {result.get('truncated')}")
        print(f"  description_len = {result.get('description_len')}")

        # 2. Try the post detail page and check for __INITIAL_STATE__ or article tag
        print(f"\n=== Test 2: post detail page {uid}/{test_id} ===")
        detail_url = f"https://xueqiu.com/{uid}/{test_id}"
        # Use a new page to avoid messing up the main one
        new_page = await ctx.new_page()
        await new_page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)

        # Extract full text from the article element
        full_text = await new_page.evaluate('''() => {
            // Try article content
            const article = document.querySelector('.article__bd__detail, .status-content, article, .detail__content');
            if (article) return {source: 'article_element', text: article.innerText, len: article.innerText.length};

            // Try initial state
            const scripts = document.querySelectorAll('script');
            for (const s of scripts) {
                if (s.textContent.includes('SNOWMAN_STATUS')) {
                    return {source: 'script_state', text: s.textContent.substring(0, 500)};
                }
            }

            // Dump all text content of main area
            const main = document.querySelector('main, #app, .container');
            if (main) return {source: 'main_element', text: main.innerText.substring(0, 2000), len: main.innerText.length};

            return {source: 'body', text: document.body.innerText.substring(0, 2000), len: document.body.innerText.length};
        }''')
        print(f"  Source: {full_text.get('source')}")
        text = full_text.get('text', '')
        print(f"  Length: {full_text.get('len', len(text))}")
        print(f"  Text preview: {text[:500]}")

        # 3. Also try the status page API (sometimes xueqiu embeds data in page)
        print(f"\n=== Test 3: check page source for JSON data ===")
        json_data = await new_page.evaluate('''() => {
            const scripts = document.querySelectorAll('script');
            for (const s of scripts) {
                const t = s.textContent;
                if (t.includes('status') && t.includes('description') && t.length > 200) {
                    // Find JSON-like content
                    const match = t.match(/SNB\\.data\\.data\s*=\s*({.*?});/s) ||
                                  t.match(/window\.__INITIAL_STATE__\s*=\s*({.*?});/s);
                    if (match) return {found: true, snippet: match[1].substring(0, 500)};
                    return {found: true, snippet: t.substring(0, 500)};
                }
            }
            return {found: false};
        }''')
        print(f"  Found JSON data: {json_data.get('found')}")
        if json_data.get('found'):
            print(f"  Snippet: {json_data.get('snippet', '')[:300]}")

        await new_page.close()

asyncio.run(main())
