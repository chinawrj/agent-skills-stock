#!/usr/bin/env python3
"""Quick API availability test."""
import asyncio
from playwright.async_api import async_playwright

async def test():
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp("http://localhost:9222")
        ctx = browser.contexts[0]
        page = ctx.pages[0]
        print(f"Page: {page.url}", flush=True)

        for pg in [1, 2, 3]:
            result = await page.evaluate('''async (pg) => {
                const url = `https://xueqiu.com/v4/statuses/user_timeline.json?user_id=9508203182&page=${pg}&type=0`;
                const resp = await fetch(url, {credentials: "include"});
                if (!resp.ok) return {error: resp.status, text: (await resp.text()).substring(0, 200)};
                const data = await resp.json();
                return {ok: true, maxPage: data.maxPage, count: (data.statuses||[]).length};
            }''', pg)
            print(f"Page {pg}: {result}", flush=True)
            await asyncio.sleep(2)

asyncio.run(test())
