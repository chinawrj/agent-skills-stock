#!/usr/bin/env python3
"""Verify SNOWMAN_STATUS extraction for getting full post text."""
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
        print(f"Connected. Current page: {page.url}")

        if "xueqiu.com" not in page.url:
            await page.goto("https://xueqiu.com/u/9508203182", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)

        uid = 9508203182
        # Test with 3 known posts
        test_ids = [379659696, 378692708, 378476918]

        print("=== JS fetch + SNOWMAN_STATUS extraction ===")
        for sid in test_ids:
            t0 = time.time()
            result = await page.evaluate('''async (args) => {
                const [uid, sid] = args;
                const url = `https://xueqiu.com/${uid}/${sid}`;
                const resp = await fetch(url, {credentials: "include"});
                const html = await resp.text();

                // Try multiple extraction patterns
                const patterns = [
                    /SNOWMAN_STATUS\s*=\s*(\{.*?\});/s,
                    /window\.SNOWMAN_STATUS\s*=\s*(\{.*?\});/s,
                ];

                for (const pat of patterns) {
                    const match = html.match(pat);
                    if (match) {
                        try {
                            const data = JSON.parse(match[1]);
                            const desc = data.description || data.text || "";
                            // Strip HTML
                            const div = document.createElement("div");
                            div.innerHTML = desc;
                            const clean = div.textContent || div.innerText || "";
                            return {
                                method: "SNOWMAN_STATUS",
                                raw_len: desc.length,
                                clean_len: clean.length,
                                text: clean,
                                has_truncation: clean.endsWith("...") || desc.endsWith("..."),
                                keys: Object.keys(data).slice(0, 15)
                            };
                        } catch(e) {
                            return {method: "parse_error", error: e.message};
                        }
                    }
                }

                // Fallback: extract from article element
                const artMatch = html.match(/<article[^>]*class="[^"]*article__bd__detail[^"]*"[^>]*>([\s\S]*?)<\/article>/);
                if (artMatch) {
                    const div = document.createElement("div");
                    div.innerHTML = artMatch[1];
                    const clean = div.textContent || div.innerText || "";
                    return {method: "article_html", clean_len: clean.length, text: clean};
                }

                return {method: "none", html_len: html.length, snippet: html.substring(0, 300)};
            }''', [uid, sid])
            elapsed = time.time() - t0

            method = result.get("method", "?")
            clean_len = result.get("clean_len", 0)
            truncated = result.get("has_truncation", "?")
            print(f"\nPost {sid}: method={method}, {clean_len} chars, truncated={truncated}, time={elapsed:.2f}s")
            text = result.get("text", "")
            if text:
                print(f"  First 200: {text[:200]}")
                if len(text) > 200:
                    print(f"  Last 100: ...{text[-100:]}")
            if result.get("keys"):
                print(f"  SNOWMAN keys: {result['keys']}")

        # Also test: how does the timeline page get full text for "展开"?
        print("\n\n=== Checking timeline page initial data ===")
        result = await page.evaluate('''() => {
            // Check if there's a global data object with full posts
            const checks = {};
            if (window.SNOWMAN_STATUS) checks["SNOWMAN_STATUS"] = typeof window.SNOWMAN_STATUS;
            if (window.__INITIAL_STATE__) checks["__INITIAL_STATE__"] = typeof window.__INITIAL_STATE__;
            if (window.__NEXT_DATA__) checks["__NEXT_DATA__"] = typeof window.__NEXT_DATA__;

            // Check Vue/React state
            const app = document.querySelector('#app');
            if (app && app.__vue__) {
                checks["vue_found"] = true;
                const vm = app.__vue__;
                if (vm.$store) {
                    const state = vm.$store.state;
                    checks["vuex_keys"] = Object.keys(state).slice(0, 10);
                }
            }
            return checks;
        }''')
        print(f"Global state: {result}")

asyncio.run(main())
