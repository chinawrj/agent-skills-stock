#!/usr/bin/env python3
"""测试 bind_to_page HTTP bridge + async proxy backend."""
import json
import sys
import logging
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

sys.path.insert(0, ".")
from scripts.web_proxy_hub import WebProxyHub

# Step 1: sync connect
print("step1: sync connect")
hub = WebProxyHub()
hub.connect()
print(f"  tabs: {len(hub._ctx.pages)}")

# Step 2: find jisilu page
page = None
for p in hub._ctx.pages:
    if "jisilu" in p.url:
        page = p
        break

if not page:
    print("ERROR: no jisilu page found")
    hub.close()
    sys.exit(1)

print(f"  jisilu: {page.url[:50]}")

# Step 3: bind_to_page
print("step2: bind_to_page (dual CDP)...")
hub.bind_to_page(page)
print("  bind done!")

# Step 4: verify JS injection
check = page.evaluate(
    "() => ({ ok: !!window.__proxyHub, sites: window.__proxyHub?.sites })"
)
print(f"  __proxyHub: {json.dumps(check)}")

# Step 5: test urllib -> HTTP bridge -> async proxy -> eastmoney
print("step3: urllib -> bridge -> eastmoney...")
req_data = json.dumps({
    "site": "eastmoney",
    "url": "https://push2.eastmoney.com/api/qt/clist/get",
    "method": "GET",
    "params": {"pn": "1", "pz": "3", "fs": "m:0+t:6", "fields": "f12,f14,f2,f3"},
}).encode()
req = urllib.request.Request(
    "http://127.0.0.1:18234/proxy",
    data=req_data,
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    resp = urllib.request.urlopen(req, timeout=40)
    result = json.loads(resp.read())
    if result.get("ok") and result.get("data"):
        diff = result["data"].get("data", {}).get("diff", {})
        items = list(diff.values()) if isinstance(diff, dict) else diff
        names = [f'{i["f14"]}({i["f12"]})' for i in items[:3]]
        print(f"  bridge OK: {names}")
    else:
        status = result.get("statusText", "")
        print(f"  bridge resp: ok={result.get('ok')}, status={result.get('status')}, err={status[:100]}")
except Exception as e:
    print(f"  bridge FAILED: {e}")

# Step 6: test from page JS via page.route() interception
print("step4: page JS -> page.route() -> async proxy -> eastmoney...")
JS = """async () => {
    try {
        const r = await window.__proxyHub.get('eastmoney',
            'https://push2.eastmoney.com/api/qt/clist/get',
            { pn: '1', pz: '3', fs: 'm:0+t:6', fields: 'f12,f14,f2,f3' });
        if (r.data && r.data.data && r.data.data.diff) {
            const diff = r.data.data.diff;
            const items = Array.isArray(diff) ? diff : Object.values(diff);
            return {
                ok: true, elapsed: r.elapsed,
                stocks: items.slice(0, 3).map(i => i.f14 + '(' + i.f12 + ')')
            };
        }
        return { ok: r.ok, status: r.status, elapsed: r.elapsed };
    } catch(e) {
        return { error: e.name + ': ' + e.message };
    }
}"""
result = page.evaluate(JS)
print(f"  result: {json.dumps(result, ensure_ascii=False)}")

hub.close()
print("DONE!")
