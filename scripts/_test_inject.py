#!/usr/bin/env python3
"""CDP test - inject content.js via evaluate wrapper."""
import json, time
from patchright.sync_api import sync_playwright

pw = sync_playwright().start()
browser = pw.chromium.connect_over_cdp('http://127.0.0.1:9222')
ctx = browser.contexts[0]

page = None
for pg in ctx.pages:
    if 'jisilu.cn/data/cbnew' in pg.url:
        page = pg
        break

if not page:
    page = ctx.new_page()
    page.goto('https://www.jisilu.cn/data/cbnew/#cb', wait_until='domcontentloaded', timeout=30000)
    time.sleep(3)

print(f'Page: {page.url[:60]}')

with open('extensions/xiaokashu-screener/content.css') as f:
    css = f.read()
with open('extensions/xiaokashu-screener/content.js') as f:
    js = f.read()

# Replace chrome API with window.chrome for compatibility with Function constructor scope
js = js.replace(
    "chrome.runtime.onMessage.addListener",
    "window.chrome.runtime.onMessage.addListener"
)

# Inject CSS
page.evaluate("""(css) => {
    let el = document.getElementById('xks-style');
    if (el) el.remove();
    const style = document.createElement('style');
    style.id = 'xks-style';
    style.textContent = css;
    document.head.appendChild(style);
}""", css)

# Clean old panel and define chrome stub
page.evaluate("""() => { 
    const p = document.getElementById('xks-panel'); 
    if (p) p.remove(); 
    window.__xksPanel = null;
    if (typeof chrome === 'undefined' || !chrome.runtime) {
        window.chrome = { runtime: { onMessage: { addListener: function(){} } } };
    }
}""")

# Wrap the IIFE in a Function constructor to bypass syntax parsing issues
# The content is an IIFE that returns undefined, so we need to wrap it
page.evaluate("""(code) => {
    try {
        const fn = new Function(code);
        fn();
    } catch(e) {
        console.error('XKS inject error:', e);
        window.__xks_error = e.message + ' | ' + e.stack;
    }
}""", js)

print('JS injected via Function constructor')
time.sleep(6)

# Check error
err = page.evaluate("() => window.__xks_error || 'no error'")
print('Error:', err)

# Check __xksPanel
has = page.evaluate("() => ({ panel: !!window.__xksPanel, chrome: typeof chrome })")
print('State:', json.dumps(has))

# Check results
result = page.evaluate("""() => {
    const rows = document.querySelectorAll('#xks-table tbody tr');
    const top5 = [];
    for (let i = 0; i < Math.min(5, rows.length); i++) {
        const cells = rows[i].querySelectorAll('td');
        top5.push({
            bond: cells[0] ? cells[0].textContent.trim() : '',
            score: cells[2] ? cells[2].textContent.trim() : '',
            revision: cells[11] ? cells[11].textContent.trim() : '',
            tags: cells[12] ? cells[12].textContent.trim() : '',
        });
    }
    return {
        stats: document.getElementById('xks-stats') ? document.getElementById('xks-stats').textContent.trim() : '',
        rows: rows.length,
        body: document.getElementById('xks-body') ? document.getElementById('xks-body').innerHTML.substring(0, 300) : 'none',
        top5: top5
    };
}""")
print(json.dumps(result, ensure_ascii=False, indent=2))

pw.stop()
