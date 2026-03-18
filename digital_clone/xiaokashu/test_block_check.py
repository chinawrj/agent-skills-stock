#!/usr/bin/env python3
"""Test if Xueqiu blocked specifically 小卡叔 or all API access."""
import asyncio
from playwright.async_api import async_playwright

async def test():
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp("http://localhost:9222")
        ctx = browser.contexts[0]
        page = ctx.pages[0]
        print(f"Current page: {page.url}", flush=True)

        # Test 1: Can we load xueqiu.com homepage?
        print("\n=== Test 1: Homepage ===", flush=True)
        r = await page.evaluate('''async () => {
            const resp = await fetch("https://xueqiu.com/", {credentials: "include"});
            return {status: resp.status, ok: resp.ok};
        }''')
        print(f"  Homepage: {r}", flush=True)

        # Test 2: 小卡叔 profile page
        print("\n=== Test 2: 小卡叔 profile page ===", flush=True)
        r = await page.evaluate('''async () => {
            const resp = await fetch("https://xueqiu.com/u/9508203182", {credentials: "include"});
            return {status: resp.status, ok: resp.ok, len: (await resp.text()).length};
        }''')
        print(f"  小卡叔 profile: {r}", flush=True)

        # Test 3: 小卡叔 API
        print("\n=== Test 3: 小卡叔 API ===", flush=True)
        r = await page.evaluate('''async () => {
            const resp = await fetch("https://xueqiu.com/v4/statuses/user_timeline.json?user_id=9508203182&page=1&type=0", {credentials: "include"});
            if (!resp.ok) return {error: resp.status, text: (await resp.text()).substring(0, 300)};
            const data = await resp.json();
            return {ok: true, count: (data.statuses||[]).length, maxPage: data.maxPage};
        }''')
        print(f"  小卡叔 API: {r}", flush=True)

        # Test 4: Another user's API (e.g. a popular user)
        print("\n=== Test 4: Other user API ===", flush=True)
        r = await page.evaluate('''async () => {
            const resp = await fetch("https://xueqiu.com/v4/statuses/user_timeline.json?user_id=5124430882&page=1&type=0", {credentials: "include"});
            if (!resp.ok) return {error: resp.status, text: (await resp.text()).substring(0, 300)};
            const data = await resp.json();
            return {ok: true, count: (data.statuses||[]).length};
        }''')
        print(f"  Other user API: {r}", flush=True)

        # Test 5: 小卡叔 single post detail
        print("\n=== Test 5: 小卡叔 single post ===", flush=True)
        r = await page.evaluate('''async () => {
            const resp = await fetch("https://xueqiu.com/statuses/show.json?id=379659696", {credentials: "include"});
            if (!resp.ok) return {error: resp.status, text: (await resp.text()).substring(0, 300)};
            const data = await resp.json();
            return {ok: true, id: data.id, desc_len: (data.description||"").length};
        }''')
        print(f"  Single post: {r}", flush=True)

asyncio.run(test())
