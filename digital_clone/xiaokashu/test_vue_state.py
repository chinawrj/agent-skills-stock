#!/usr/bin/env python3
"""Deep inspect: where does the timeline page get full text for 展开?"""
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

        if "/u/9508203182" not in page.url:
            await page.goto("https://xueqiu.com/u/9508203182", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)

        # 1. Check Vue component data for full text
        print("=== 1. Vue store / component state ===")
        result = await page.evaluate('''() => {
            const app = document.querySelector('#app');
            if (!app || !app.__vue__) return {error: "no vue"};
            const vm = app.__vue__;

            // Check $store
            const info = {};
            if (vm.$store) {
                const state = vm.$store.state;
                info.vuex_keys = Object.keys(state);

                // Look for timeline/status related state
                for (const key of Object.keys(state)) {
                    const val = state[key];
                    if (val && typeof val === 'object') {
                        info[`state.${key}_keys`] = Object.keys(val).slice(0, 15);
                    }
                }
            }
            return info;
        }''')
        print(json.dumps(result, indent=2, ensure_ascii=False))

        # 2. Check the actual API response - maybe there's a full text field we missed
        print("\n=== 2. Full API response fields for first post ===")
        result = await page.evaluate('''async () => {
            const resp = await fetch("https://xueqiu.com/v4/statuses/user_timeline.json?user_id=9508203182&page=1&type=0", {credentials: "include"});
            const data = await resp.json();
            if (data.statuses && data.statuses.length > 0) {
                const post = data.statuses[0];
                // Return ALL keys and field lengths for text fields
                const analysis = {};
                for (const [k, v] of Object.entries(post)) {
                    if (typeof v === 'string') {
                        analysis[k] = {type: 'string', len: v.length, preview: v.substring(0, 100)};
                    } else if (typeof v === 'number' || typeof v === 'boolean') {
                        analysis[k] = v;
                    } else if (v === null) {
                        analysis[k] = null;
                    } else if (typeof v === 'object') {
                        analysis[k] = {type: typeof v, keys: Object.keys(v).slice(0, 10)};
                    }
                }
                return analysis;
            }
            return {error: "no posts"};
        }''')
        print(json.dumps(result, indent=2, ensure_ascii=False))

        # 3. Check if there's a separate text/content field
        print("\n=== 3. Search for long text fields in API response ===")
        result = await page.evaluate('''async () => {
            const resp = await fetch("https://xueqiu.com/v4/statuses/user_timeline.json?user_id=9508203182&page=1&type=0", {credentials: "include"});
            const data = await resp.json();
            if (!data.statuses) return {error: "no statuses"};

            const post = data.statuses[0];
            // Find ALL string fields longer than 50 chars
            const longFields = {};
            function search(obj, path) {
                if (typeof obj === 'string' && obj.length > 50) {
                    longFields[path] = {len: obj.length, preview: obj.substring(0, 150) + (obj.length > 150 ? "..." : "")};
                } else if (obj && typeof obj === 'object') {
                    for (const [k, v] of Object.entries(obj)) {
                        search(v, path ? `${path}.${k}` : k);
                    }
                }
            }
            search(post, "");
            return longFields;
        }''')
        print(json.dumps(result, indent=2, ensure_ascii=False))

        # 4. Check the DOM - get the FULL text that "展开" reveals
        print("\n=== 4. DOM inspection: full text in hidden element ===")
        result = await page.evaluate('''() => {
            const items = document.querySelectorAll('.timeline-item-content, [class*="timeline__content"], [class*="timeline__item"]');
            const results = [];
            for (const item of Array.from(items).slice(0, 2)) {
                const allText = item.innerText;
                const hiddenEls = item.querySelectorAll('[style*="display: none"], [style*="display:none"], .hidden, [v-show]');
                results.push({
                    class: item.className,
                    totalText: allText.length,
                    textPreview: allText.substring(0, 300),
                    hiddenCount: hiddenEls.length,
                    childClasses: Array.from(item.children).map(c => c.className).slice(0, 10),
                    innerHTML_len: item.innerHTML.length,
                });
            }
            return results;
        }''')
        print(json.dumps(result, indent=2, ensure_ascii=False))

        # 5. Get the actual full text from the DOM for the first post
        print("\n=== 5. Get ALL text content from first post element ===")
        result = await page.evaluate('''() => {
            // Find the first timeline expand control
            const expand = document.querySelector('.timeline__expand__control');
            if (!expand) return {error: "no expand button"};

            // Get the parent timeline item
            let parent = expand.closest('[class*="timeline"]');
            while (parent && !parent.className.includes('item')) {
                parent = parent.parentElement;
            }
            if (!parent) parent = expand.parentElement.parentElement.parentElement;

            // Get all innerHTML to see the structure
            return {
                parentClass: parent.className,
                innerHTML: parent.innerHTML.substring(0, 2000),
                innerText: parent.innerText.substring(0, 1000)
            };
        }''')
        if "innerHTML" in result:
            print(f"Parent class: {result['parentClass']}")
            print(f"Text length: {len(result.get('innerText', ''))}")
            print(f"HTML length: {len(result.get('innerHTML', ''))}")
            print(f"\nHTML structure:\n{result['innerHTML'][:1500]}")

asyncio.run(main())
