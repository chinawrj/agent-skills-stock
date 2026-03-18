#!/usr/bin/env python3
"""Test what happens when clicking '展开' button on Xueqiu timeline page.
Intercept network requests to find the full-text API."""
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

        # Navigate to user timeline
        url = "https://xueqiu.com/u/9508203182"
        if "/u/9508203182" not in page.url:
            print("Navigating to 小卡叔 profile...")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)

        # Set up network request interception to capture what "展开" triggers
        captured_requests = []

        def on_request(request):
            url = request.url
            if "xueqiu.com" in url and ("statuses" in url or "status" in url or "expand" in url or "show" in url or "detail" in url):
                captured_requests.append({
                    "url": url,
                    "method": request.method,
                })

        async def on_response(response):
            url = response.url
            if "xueqiu.com" in url and ("statuses" in url or "status" in url or "expand" in url or "show" in url or "detail" in url):
                try:
                    body = await response.text()
                    print(f"\n  [RESPONSE] {url}")
                    print(f"    Status: {response.status}, Size: {len(body)}")
                    if response.status == 200 and body.startswith("{"):
                        data = json.loads(body)
                        # Look for description/text fields
                        if "description" in str(data)[:500]:
                            # Find the description
                            def find_desc(obj, path=""):
                                if isinstance(obj, dict):
                                    if "description" in obj:
                                        d = obj["description"]
                                        if isinstance(d, str) and len(d) > 50:
                                            print(f"    Found description at {path}: {len(d)} chars")
                                            import re
                                            clean = re.sub(r'<[^>]+>', '', d)
                                            print(f"    Clean: {clean[:200]}...")
                                    for k, v in obj.items():
                                        find_desc(v, f"{path}.{k}")
                                elif isinstance(obj, list):
                                    for i, v in enumerate(obj[:3]):
                                        find_desc(v, f"{path}[{i}]")
                            find_desc(data)
                except Exception as e:
                    pass

        page.on("request", on_request)
        page.on("response", on_response)

        # Find all "展开" buttons/links on the page
        print("\n=== Looking for '展开' elements ===")
        expand_elements = await page.query_selector_all('text=展开')
        print(f"Found {len(expand_elements)} elements with '展开' text")

        # Also search by common CSS patterns
        for selector in ['a.expand', '.timeline-content a:has-text("展开")',
                         'a:has-text("展开")', 'span:has-text("展开")',
                         'button:has-text("展开")', '[class*="expand"]']:
            els = await page.query_selector_all(selector)
            if els:
                print(f"  Selector '{selector}': {len(els)} elements")
                for i, el in enumerate(els[:3]):
                    tag = await el.evaluate('e => e.tagName')
                    cls = await el.evaluate('e => e.className')
                    href = await el.evaluate('e => e.href || ""')
                    text = await el.inner_text()
                    parent_text = await el.evaluate('e => e.parentElement?.innerText?.substring(0, 100) || ""')
                    print(f"    [{i}] <{tag} class='{cls}' href='{href}'>{text}</a>")
                    print(f"         Parent text: {parent_text[:80]}...")

        if not expand_elements:
            print("\nNo '展开' found. Let me check the page structure...")
            # Maybe need to scroll to see posts
            await page.evaluate('window.scrollBy(0, 500)')
            await asyncio.sleep(1)
            expand_elements = await page.query_selector_all('a:has-text("展开")')
            print(f"After scroll: {len(expand_elements)} '展开' elements")

        # Click the first "展开" button and observe network traffic
        if expand_elements:
            print(f"\n=== Clicking first '展开' button ===")
            captured_requests.clear()
            el = expand_elements[0]

            # Get context around the expand button
            parent_html = await el.evaluate('e => e.closest("div")?.innerHTML?.substring(0, 300) || ""')
            print(f"Parent HTML snippet: {parent_html[:200]}...")

            await el.click()
            await asyncio.sleep(2)

            print(f"\nCaptured {len(captured_requests)} relevant requests:")
            for req in captured_requests:
                print(f"  {req['method']} {req['url']}")

            # Check if text expanded inline (DOM changed)
            new_text = await el.evaluate('e => e.closest("div.timeline-content, div.status-content, article")?.innerText?.substring(0, 500) || ""')
            print(f"\nText after expand ({len(new_text)} chars): {new_text[:300]}...")
        else:
            print("\nCould not find any '展开' button. Dumping visible post structure...")
            # Get page structure around posts
            html = await page.evaluate('''() => {
                const posts = document.querySelectorAll('.timeline-item, .status-item, [class*="timeline"], [class*="status"]');
                return Array.from(posts).slice(0, 2).map(p => ({
                    class: p.className,
                    html: p.innerHTML.substring(0, 500)
                }));
            }''')
            for item in html:
                print(f"\n  Class: {item['class']}")
                print(f"  HTML: {item['html'][:300]}...")

asyncio.run(main())
