#!/usr/bin/env python3
"""
集思录可转债数据 CDP 插件 — 小卡叔选债框架（悬浮框版）

通过 CDP 连接浏览器，在集思录网页中注入悬浮筛选面板。
面板直接在页面内展示小卡叔选债框架的筛选结果，支持:
  - 实时获取集思录全量转债数据
  - 100分制评分 + 负面清单排除
  - 交互式筛选条件调节
  - 按评分/双低/YTM排序
  - 一键导出CSV
  - 可拖拽/折叠/调整大小

前置条件:
  1. pip install patchright
  2. 启动浏览器:
     .github/skills/anti-detect-browser/scripts/start_browser.sh
  3. 集思录账号: 首次需在浏览器中手动登录 (免费注册)

用法:
    cd /Users/rjwang/fun/a-share && source .venv/bin/activate

    python scripts/jisilu_cdp.py              # 注入悬浮框到集思录页面
    python scripts/jisilu_cdp.py --port 9222  # 指定 CDP 端口
"""

import argparse
import os
import sys
import time

DEFAULT_CDP_PORT = 9222
JISILU_URL = 'https://www.jisilu.cn/data/cbnew/#cb'

# Extension directory — single source of truth for CSS/JS
EXTENSION_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                             'extensions', 'xiaokashu-screener')


def load_panel_assets():
    """Load CSS and JS from the Chrome extension (single source of truth).
    
    Falls back to embedded strings if extension files are not found.
    """
    css_path = os.path.join(EXTENSION_DIR, 'content.css')
    js_path = os.path.join(EXTENSION_DIR, 'content.js')

    if os.path.exists(css_path) and os.path.exists(js_path):
        with open(css_path, 'r', encoding='utf-8') as f:
            panel_css = f.read()
        with open(js_path, 'r', encoding='utf-8') as f:
            panel_js = f.read()
        # Replace chrome extension API block with stub for CDP mode
        import re
        panel_js = re.sub(
            r'chrome\.runtime\.onMessage\.addListener\(\(msg\)\s*=>\s*\{.*?\}\);',
            '// CDP stub: chrome.runtime.onMessage block removed',
            panel_js,
            flags=re.DOTALL
        )
        return panel_css, panel_js
    else:
        print(f'⚠️  Extension files not found at {EXTENSION_DIR}', file=sys.stderr)
        print('   Using embedded fallback (may be outdated)', file=sys.stderr)
        return _FALLBACK_CSS, _FALLBACK_JS


# ═══════════════════ Fallback Embedded CSS/JS ═══════════════════
# These are kept as fallback only. The primary source is the extension.

_FALLBACK_CSS = r"""
#xks-panel {
  position: fixed;
  top: 60px;
  right: 20px;
  width: 920px;
  max-height: 85vh;
  background: #1a1a2e;
  color: #e0e0e0;
  border: 1px solid #16213e;
  border-radius: 10px;
  box-shadow: 0 8px 32px rgba(0,0,0,0.45);
  z-index: 99999;
  font-family: -apple-system, 'PingFang SC', 'Microsoft YaHei', sans-serif;
  font-size: 13px;
  display: flex;
  flex-direction: column;
  resize: both;
  overflow: hidden;
  min-width: 680px;
  min-height: 300px;
}
#xks-header {
  background: linear-gradient(135deg, #0f3460, #16213e);
  padding: 10px 16px;
  cursor: move;
  user-select: none;
  display: flex;
  align-items: center;
  justify-content: space-between;
  border-radius: 10px 10px 0 0;
  flex-shrink: 0;
}
#xks-header .xks-title {
  font-size: 15px;
  font-weight: 600;
  color: #e94560;
  letter-spacing: 1px;
}
#xks-header .xks-subtitle {
  font-size: 11px;
  color: #888;
  margin-left: 10px;
}
#xks-header .xks-btns button {
  background: none;
  border: 1px solid #333;
  color: #aaa;
  width: 28px;
  height: 28px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  margin-left: 4px;
  transition: all 0.2s;
}
#xks-header .xks-btns button:hover {
  background: #e94560;
  color: #fff;
  border-color: #e94560;
}
#xks-controls {
  padding: 8px 16px;
  background: #16213e;
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
  border-bottom: 1px solid #0f3460;
  flex-shrink: 0;
}
#xks-controls label {
  font-size: 12px;
  color: #888;
}
#xks-controls input, #xks-controls select {
  background: #1a1a2e;
  border: 1px solid #333;
  color: #e0e0e0;
  padding: 3px 8px;
  border-radius: 4px;
  font-size: 12px;
  width: 65px;
}
#xks-controls select { width: 90px; }
#xks-controls .xks-filter-group {
  display: flex;
  align-items: center;
  gap: 4px;
}
#xks-controls button {
  background: #e94560;
  border: none;
  color: #fff;
  padding: 4px 14px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 12px;
  font-weight: 500;
  transition: background 0.2s;
}
#xks-controls button:hover { background: #c73050; }
#xks-controls button.xks-secondary {
  background: #0f3460;
  border: 1px solid #333;
  color: #ccc;
}
#xks-controls button.xks-secondary:hover { background: #16213e; }
#xks-stats {
  padding: 6px 16px;
  background: #16213e;
  font-size: 11px;
  color: #888;
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  border-bottom: 1px solid #0f3460;
  flex-shrink: 0;
}
#xks-stats .xks-stat-val {
  color: #e94560;
  font-weight: 600;
}
#xks-stats .xks-temp {
  margin-left: auto;
  color: #ffa500;
  font-weight: 500;
}
#xks-body {
  overflow-y: auto;
  flex: 1;
  scrollbar-width: thin;
  scrollbar-color: #333 #1a1a2e;
}
#xks-table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}
#xks-table thead {
  position: sticky;
  top: 0;
  z-index: 2;
}
#xks-table th {
  background: #0f3460;
  color: #ccc;
  padding: 6px 6px;
  text-align: right;
  font-weight: 500;
  font-size: 11px;
  cursor: pointer;
  white-space: nowrap;
  border-bottom: 2px solid #e94560;
  user-select: none;
}
#xks-table th:nth-child(1), #xks-table th:nth-child(2) { text-align: left; }
#xks-table th:hover { background: #1a3a6e; }
#xks-table th.xks-sorted { color: #e94560; }
#xks-table td {
  padding: 5px 6px;
  text-align: right;
  border-bottom: 1px solid #222;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
#xks-table td:nth-child(1), #xks-table td:nth-child(2) { text-align: left; }
#xks-table tr:hover { background: #16213e; }
#xks-table tr.xks-top { background: rgba(233,69,96,0.08); }
.xks-tag {
  display: inline-block;
  padding: 1px 5px;
  border-radius: 3px;
  font-size: 10px;
  margin-right: 3px;
  white-space: nowrap;
}
.xks-tag-green { background: #0a3d2a; color: #4caf50; }
.xks-tag-red { background: #3d0a0a; color: #ef5350; }
.xks-tag-blue { background: #0a2a3d; color: #42a5f5; }
.xks-tag-orange { background: #3d2a0a; color: #ffa726; }
.xks-tag-gray { background: #2a2a2a; color: #999; }
.xks-score-bar {
  display: inline-block;
  height: 14px;
  border-radius: 3px;
  background: linear-gradient(90deg, #e94560, #ffa500);
  min-width: 4px;
  vertical-align: middle;
}
.xks-positive { color: #4caf50; }
.xks-negative { color: #ef5350; }
.xks-neutral { color: #888; }
.xks-link {
  color: #42a5f5;
  text-decoration: none;
  transition: color 0.2s;
}
.xks-link:hover {
  color: #e94560;
  text-decoration: underline;
}
#xks-panel.xks-collapsed #xks-controls,
#xks-panel.xks-collapsed #xks-stats,
#xks-panel.xks-collapsed #xks-body { display: none; }
#xks-panel.xks-collapsed { max-height: none; height: auto !important; resize: none; }
#xks-loading {
  padding: 40px;
  text-align: center;
  color: #888;
  font-size: 14px;
}
"""

_FALLBACK_JS = r"""
(function() {
  'use strict';
  // ═══ Prevent duplicate injection ═══
  if (window.__xksPanel) { window.__xksPanel.refresh(); return; }

  const API = '/data/cbnew/cb_list_new/';
  let allBonds = [];
  let screenedBonds = [];
  let currentSort = { key: 'score', asc: false };
  let filters = { maxPrice: 130, minYtm: 0, strict: false, sortBy: 'score' };

  // ═══ Panel HTML ═══
  const panel = document.createElement('div');
  panel.id = 'xks-panel';
  panel.innerHTML = `
    <div id="xks-header">
      <div>
        <span class="xks-title">🔥 小卡叔选债</span>
        <span class="xks-subtitle">价值投机 · 满仓轮动</span>
      </div>
      <div class="xks-btns">
        <button id="xks-btn-refresh" title="刷新数据">⟳</button>
        <button id="xks-btn-export" title="导出CSV">⤓</button>
        <button id="xks-btn-collapse" title="折叠/展开">−</button>
        <button id="xks-btn-close" title="关闭">×</button>
      </div>
    </div>
    <div id="xks-controls">
      <div class="xks-filter-group">
        <label>最高价</label>
        <input type="number" id="xks-max-price" value="130" step="5">
      </div>
      <div class="xks-filter-group">
        <label>最低YTM%</label>
        <input type="number" id="xks-min-ytm" value="0" step="1">
      </div>
      <div class="xks-filter-group">
        <label>排序</label>
        <select id="xks-sort">
          <option value="score">综合评分</option>
          <option value="dblow">双低值↑</option>
          <option value="ytm_rt">YTM↓</option>
          <option value="price">价格↑</option>
          <option value="premium_rt">溢价率↑</option>
          <option value="year_left">剩余年↑</option>
          <option value="curr_iss_amt">规模↑</option>
        </select>
      </div>
      <div class="xks-filter-group">
        <label><input type="checkbox" id="xks-strict"> 严格(YTM>0)</label>
      </div>
      <button id="xks-btn-apply">筛选</button>
      <button id="xks-btn-reset" class="xks-secondary">重置</button>
    </div>
    <div id="xks-stats"></div>
    <div id="xks-body">
      <div id="xks-loading">⏳ 正在获取数据...</div>
    </div>
  `;
  document.body.appendChild(panel);

  // ═══ Drag support ═══
  const header = panel.querySelector('#xks-header');
  let isDragging = false, dragX, dragY;
  header.addEventListener('mousedown', e => {
    if (e.target.tagName === 'BUTTON') return;
    isDragging = true;
    dragX = e.clientX - panel.offsetLeft;
    dragY = e.clientY - panel.offsetTop;
    document.body.style.userSelect = 'none';
  });
  document.addEventListener('mousemove', e => {
    if (!isDragging) return;
    panel.style.left = (e.clientX - dragX) + 'px';
    panel.style.top = (e.clientY - dragY) + 'px';
    panel.style.right = 'auto';
  });
  document.addEventListener('mouseup', () => {
    isDragging = false;
    document.body.style.userSelect = '';
  });

  // ═══ Button events ═══
  panel.querySelector('#xks-btn-close').onclick = () => panel.remove();
  panel.querySelector('#xks-btn-collapse').onclick = () => {
    panel.classList.toggle('xks-collapsed');
    panel.querySelector('#xks-btn-collapse').textContent =
      panel.classList.contains('xks-collapsed') ? '+' : '−';
  };
  panel.querySelector('#xks-btn-refresh').onclick = () => fetchAndScreen();
  panel.querySelector('#xks-btn-apply').onclick = () => { readFilters(); applyScreen(); };
  panel.querySelector('#xks-btn-reset').onclick = () => {
    panel.querySelector('#xks-max-price').value = 130;
    panel.querySelector('#xks-min-ytm').value = 0;
    panel.querySelector('#xks-strict').checked = false;
    panel.querySelector('#xks-sort').value = 'score';
    filters = { maxPrice: 130, minYtm: 0, strict: false, sortBy: 'score' };
    applyScreen();
  };
  panel.querySelector('#xks-btn-export').onclick = exportCSV;

  // ═══ Safe number parser ═══
  function sf(v, d) {
    if (v === null || v === undefined || v === '' || v === '-' || v === 'buy') return d === undefined ? null : d;
    const n = parseFloat(v);
    return isNaN(n) ? (d === undefined ? null : d) : n;
  }

  function readFilters() {
    filters.maxPrice = sf(panel.querySelector('#xks-max-price').value, 130);
    filters.minYtm = sf(panel.querySelector('#xks-min-ytm').value, 0);
    filters.strict = panel.querySelector('#xks-strict').checked;
    filters.sortBy = panel.querySelector('#xks-sort').value;
  }

  // ═══ Fetch data from 集思录 API ═══
  async function fetchData() {
    const resp = await fetch(API + '?___jsl=LST___t=' + Date.now(), {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'X-Requested-With': 'XMLHttpRequest' },
      credentials: 'same-origin'
    });
    const data = await resp.json();
    return data;
  }

  // ═══ 小卡叔 screening (JS port) ═══
  function screen(rows) {
    const ratingScores = { 'AAA': 5, 'AA+': 5, 'AA': 4, 'AA-': 3, 'A+': 2, 'A': 1 };
    const results = [];
    let excluded = 0;

    for (const cell of rows) {
      const price = sf(cell.price);
      const ytm = sf(cell.ytm_rt);
      const premium = sf(cell.premium_rt);
      const dblow = sf(cell.dblow);
      const yearLeft = sf(cell.year_left);
      const remaining = sf(cell.curr_iss_amt);
      const rating = cell.rating_cd || '';
      const pb = sf(cell.pb);
      const stockName = cell.stock_nm || '';
      const bondName = cell.bond_nm || '';
      const changePct = sf(cell.increase_rt, 0);
      const btype = cell.btype || 'C';
      const revCnt = sf(cell.adj_cnt, 0);

      // ═══ Negative filter ═══
      if (btype !== 'C') { excluded++; continue; }
      if (stockName.includes('ST')) { excluded++; continue; }
      if (bondName.includes('退')) { excluded++; continue; }
      if (premium !== null && premium > 100 && yearLeft !== null && yearLeft < 1) { excluded++; continue; }
      if (price === null) { excluded++; continue; }
      if (filters.strict && (ytm === null || ytm < filters.minYtm)) { excluded++; continue; }

      let score = 0;
      const tags = [];

      // 1. YTM (25)
      if (ytm !== null) {
        if (ytm >= filters.minYtm) {
          score += 15; tags.push({ t: '正收益', c: 'green' });
          if (ytm >= 5) { score += 5; tags.push({ t: 'YTM ' + ytm.toFixed(1) + '%', c: 'green' }); }
          if (ytm >= 10) score += 5;
        } else {
          tags.push({ t: 'YTM ' + ytm.toFixed(1) + '%', c: 'red' });
        }
      }

      // 2. Price (20)
      if (price <= 110) { score += 20; tags.push({ t: '深度低价', c: 'green' }); }
      else if (price <= 116) { score += 18; tags.push({ t: '低价吸纳', c: 'green' }); }
      else if (price <= 120) { score += 14; tags.push({ t: '低价', c: 'blue' }); }
      else if (price <= filters.maxPrice) { score += 8; }

      // 3. Premium (15)
      if (premium !== null) {
        if (premium < 10) { score += 15; tags.push({ t: '极低溢价', c: 'green' }); }
        else if (premium < 30) { score += 12; tags.push({ t: '低溢价', c: 'blue' }); }
        else if (premium < 50) score += 8;
        else if (premium < 100) score += 3;
      }

      // 4. Dblow (15)
      if (dblow !== null) {
        if (dblow < 110) { score += 15; tags.push({ t: '极低双低', c: 'green' }); }
        else if (dblow < 120) { score += 12; tags.push({ t: '低双低', c: 'blue' }); }
        else if (dblow < 130) score += 8;
        else if (dblow < 150) score += 3;
      }

      // 5. Term (10)
      if (yearLeft !== null) {
        if (yearLeft >= 1.5 && yearLeft <= 3) { score += 10; tags.push({ t: '黄金期', c: 'blue' }); }
        else if (yearLeft >= 1 && yearLeft < 1.5) score += 6;
        else if (yearLeft > 3 && yearLeft <= 4) score += 5;
        else if (yearLeft < 1) tags.push({ t: '临期', c: 'orange' });
      }

      // 6. Size (10)
      if (remaining !== null) {
        if (remaining < 3) { score += 10; tags.push({ t: '迷你', c: 'blue' }); }
        else if (remaining < 5) { score += 7; tags.push({ t: '小盘', c: 'gray' }); }
        else if (remaining < 10) score += 3;
      }

      // 7. Rating (5)
      score += ratingScores[rating] || 0;

      // Special tags
      if (pb !== null && pb < 1) tags.push({ t: '破净', c: 'orange' });
      if (revCnt > 0) tags.push({ t: '下修' + revCnt, c: 'blue' });
      if (Math.abs(changePct) > 15) tags.push({ t: '异常波动', c: 'red' });
      if (pb !== null && pb > 5) tags.push({ t: '优等生', c: 'gray' });
      if (yearLeft !== null && yearLeft < 1.5 && premium !== null && premium > 50 && ytm !== null && ytm > 0) {
        tags.push({ t: '现金替代', c: 'orange' });
      }

      results.push({ ...cell, score, tags });
    }

    // Sort
    const sortKey = filters.sortBy;
    if (sortKey === 'score') {
      results.sort((a, b) => b.score - a.score || sf(a.dblow, 999) - sf(b.dblow, 999));
    } else {
      const asc = ['dblow', 'price', 'premium_rt', 'year_left', 'curr_iss_amt'].includes(sortKey);
      results.sort((a, b) => {
        const va = sf(a[sortKey], asc ? 9999 : -9999);
        const vb = sf(b[sortKey], asc ? 9999 : -9999);
        return asc ? va - vb : vb - va;
      });
    }

    return { results, excluded };
  }

  // ═══ Render stats bar ═══
  function renderStats(results, total, excluded) {
    const statsEl = panel.querySelector('#xks-stats');
    const ytmPos = results.filter(b => sf(b.ytm_rt, -1) > 0).length;
    const lowPrice = results.filter(b => sf(b.price, 999) <= 116).length;
    const mini = results.filter(b => sf(b.curr_iss_amt, 999) < 3).length;
    const golden = results.filter(b => { const y = sf(b.year_left, 0); return y >= 1.5 && y <= 3; }).length;
    const lowDb = results.filter(b => sf(b.dblow, 999) < 130).length;
    const ytm5 = results.filter(b => sf(b.ytm_rt, -1) >= 5).length;
    const ytm10 = results.filter(b => sf(b.ytm_rt, -1) >= 10).length;

    let tempMsg = '';
    if (ytm10 > 30) tempMsg = '🔥 YTM10%+超30只 = 激进买入信号';
    else if (ytm5 < 50) tempMsg = '⚠️ 高收益债稀缺，市场偏贵';

    statsEl.innerHTML = `
      <span>总计 <span class="xks-stat-val">${total}</span></span>
      <span>通过 <span class="xks-stat-val">${results.length}</span></span>
      <span>排除 <span class="xks-stat-val">${excluded}</span></span>
      <span>正收益 <span class="xks-stat-val">${ytmPos}</span></span>
      <span>低价≤116 <span class="xks-stat-val">${lowPrice}</span></span>
      <span>迷你<3亿 <span class="xks-stat-val">${mini}</span></span>
      <span>黄金期 <span class="xks-stat-val">${golden}</span></span>
      <span>低双低 <span class="xks-stat-val">${lowDb}</span></span>
      <span>YTM≥5% <span class="xks-stat-val">${ytm5}</span></span>
      ${tempMsg ? '<span class="xks-temp">' + tempMsg + '</span>' : ''}
    `;
  }

  // ═══ Render table ═══
  function renderTable(results) {
    const body = panel.querySelector('#xks-body');
    const cols = [
      { k: 'bond_nm', l: '转债', w: '80px' },
      { k: 'stock_nm', l: '正股', w: '70px' },
      { k: 'score', l: '评分', w: '50px' },
      { k: 'price', l: '价格', w: '55px' },
      { k: 'premium_rt', l: '溢价率', w: '55px' },
      { k: 'ytm_rt', l: 'YTM', w: '50px' },
      { k: 'dblow', l: '双低', w: '50px' },
      { k: 'year_left', l: '剩余年', w: '45px' },
      { k: 'curr_iss_amt', l: '规模亿', w: '50px' },
      { k: 'rating_cd', l: '评级', w: '35px' },
      { k: 'pb', l: 'PB', w: '40px' },
      { k: 'adj_cnt', l: '下修', w: '35px' },
      { k: '_tags', l: '标签', w: '200px' },
    ];

    let html = '<table id="xks-table"><thead><tr>';
    for (const c of cols) {
      html += '<th style="width:' + c.w + '">' + c.l + '</th>';
    }
    html += '</tr></thead><tbody>';

    for (let i = 0; i < results.length; i++) {
      const b = results[i];
      const isTop = i < 10;
      html += '<tr class="' + (isTop ? 'xks-top' : '') + '">';

      for (const c of cols) {
        if (c.k === '_tags') {
          html += '<td style="text-align:left">';
          for (const tag of (b.tags || [])) {
            html += '<span class="xks-tag xks-tag-' + tag.c + '">' + tag.t + '</span>';
          }
          html += '</td>';
        } else if (c.k === 'score') {
          const pct = Math.min(b.score, 100);
          html += '<td><span class="xks-score-bar" style="width:' + pct + '%;max-width:40px"></span> ' + b.score + '</td>';
        } else if (c.k === 'price') {
          const p = sf(b.price);
          const cls = p !== null && p <= 110 ? 'xks-positive' : (p !== null && p > 150 ? 'xks-negative' : '');
          html += '<td class="' + cls + '">' + (p !== null ? p.toFixed(1) : '-') + '</td>';
        } else if (c.k === 'premium_rt') {
          const p = sf(b.premium_rt);
          html += '<td>' + (p !== null ? p.toFixed(1) + '%' : '-') + '</td>';
        } else if (c.k === 'ytm_rt') {
          const y = sf(b.ytm_rt);
          const cls = y !== null ? (y > 0 ? 'xks-positive' : 'xks-negative') : '';
          html += '<td class="' + cls + '">' + (y !== null ? y.toFixed(1) + '%' : '-') + '</td>';
        } else if (c.k === 'dblow') {
          const d = sf(b.dblow);
          const cls = d !== null && d < 120 ? 'xks-positive' : '';
          html += '<td class="' + cls + '">' + (d !== null ? d.toFixed(1) : '-') + '</td>';
        } else if (c.k === 'curr_iss_amt') {
          const s = sf(b.curr_iss_amt);
          html += '<td>' + (s !== null ? s.toFixed(1) : '-') + '</td>';
        } else if (c.k === 'year_left') {
          const y = sf(b.year_left);
          html += '<td>' + (y !== null ? y.toFixed(1) : '-') + '</td>';
        } else if (c.k === 'pb') {
          const p = sf(b.pb);
          const cls = p !== null && p < 1 ? 'xks-negative' : '';
          html += '<td class="' + cls + '">' + (p !== null ? p.toFixed(2) : '-') + '</td>';
        } else if (c.k === 'bond_nm') {
          const id = b.bond_id || '';
          html += '<td style="text-align:left" title="' + id + '"><a href="https://www.jisilu.cn/data/convert_bond_detail/' + id + '" target="_blank" class="xks-link">' + (b.bond_nm || '') + '</a></td>';
        } else if (c.k === 'stock_nm') {
          const sid = b.stock_id || '';
          html += '<td style="text-align:left"><a href="https://www.jisilu.cn/data/stock/' + sid + '" target="_blank" class="xks-link">' + (b.stock_nm || '') + '</a></td>';
        } else {
          const v = b[c.k];
          html += '<td>' + (v !== null && v !== undefined ? v : '-') + '</td>';
        }
      }
      html += '</tr>';
    }

    html += '</tbody></table>';
    body.innerHTML = html;

    // Column sort click
    panel.querySelectorAll('#xks-table th').forEach((th, idx) => {
      th.onclick = () => {
        const key = cols[idx].k;
        if (key === '_tags') return;
        if (key === currentSort.key) {
          currentSort.asc = !currentSort.asc;
        } else {
          currentSort = { key, asc: ['price', 'premium_rt', 'dblow', 'year_left', 'curr_iss_amt', 'pb'].includes(key) };
        }
        const sorted = [...screenedBonds].sort((a, b) => {
          const va = key === 'score' ? a.score : sf(a[key], currentSort.asc ? 9999 : -9999);
          const vb = key === 'score' ? b.score : sf(b[key], currentSort.asc ? 9999 : -9999);
          return currentSort.asc ? va - vb : vb - va;
        });
        screenedBonds = sorted;
        renderTable(sorted);
      };
    });
  }

  // ═══ CSV export ═══
  function exportCSV() {
    if (!screenedBonds.length) return;
    const headers = ['代码','转债','正股','评分','价格','溢价率','YTM','双低','剩余年','规模亿','评级','PB','下修次数','标签'];
    const rows = screenedBonds.map(b => [
      b.bond_id, b.bond_nm, b.stock_nm, b.score,
      sf(b.price,''), sf(b.premium_rt,''), sf(b.ytm_rt,''), sf(b.dblow,''),
      sf(b.year_left,''), sf(b.curr_iss_amt,''), b.rating_cd||'', sf(b.pb,''),
      sf(b.adj_cnt,''), (b.tags||[]).map(t => t.t).join('/')
    ]);
    let csv = '\uFEFF' + headers.join(',') + '\n';
    for (const r of rows) csv += r.map(v => '"' + String(v).replace(/"/g, '""') + '"').join(',') + '\n';
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = '小卡叔选债_' + new Date().toISOString().slice(0,10) + '.csv';
    a.click();
    URL.revokeObjectURL(a.href);
  }

  // ═══ Main flow ═══
  function applyScreen() {
    const { results, excluded } = screen(allBonds);
    screenedBonds = results;
    renderStats(results, allBonds.length, excluded);
    renderTable(results);
  }

  async function fetchAndScreen() {
    const body = panel.querySelector('#xks-body');
    body.innerHTML = '<div id="xks-loading">⏳ 正在获取数据...</div>';
    try {
      const data = await fetchData();
      if (!data || !data.rows) {
        body.innerHTML = '<div id="xks-loading">❌ 获取数据失败</div>';
        return;
      }
      if (data.warn) {
        console.log('[小卡叔] ' + data.warn);
      }
      allBonds = data.rows.map(r => r.cell);
      readFilters();
      applyScreen();
    } catch (e) {
      body.innerHTML = '<div id="xks-loading">❌ 错误: ' + e.message + '</div>';
    }
  }

  window.__xksPanel = { refresh: fetchAndScreen, panel };
  fetchAndScreen();
})();
"""


# ═══════════════════ CDP Injection ═══════════════════

def connect_cdp(port):
    """Connect to an existing browser via CDP."""
    try:
        from patchright.sync_api import sync_playwright
    except ImportError:
        print('❌ patchright 未安装: pip install patchright', file=sys.stderr)
        sys.exit(1)

    pw = sync_playwright().start()
    try:
        browser = pw.chromium.connect_over_cdp(f'http://127.0.0.1:{port}')
    except Exception as e:
        pw.stop()
        print(f'❌ 无法连接 CDP 端口 {port}: {e}', file=sys.stderr)
        print( '   请先启动浏览器:', file=sys.stderr)
        print( '   .github/skills/anti-detect-browser/scripts/start_browser.sh', file=sys.stderr)
        sys.exit(1)

    ctx = browser.contexts[0]
    return pw, ctx


def find_jisilu_page(ctx):
    """Find or create 集思录 page."""
    for pg in ctx.pages:
        if 'jisilu.cn/data/cbnew' in pg.url:
            return pg
    page = ctx.new_page()
    page.goto(JISILU_URL, wait_until='domcontentloaded', timeout=30000)
    time.sleep(3)
    return page


def inject_panel(page):
    """Inject floating panel CSS + JS into the page."""
    panel_css, panel_js = load_panel_assets()

    # Define chrome stub for CDP mode
    page.evaluate("""() => {
        if (typeof chrome === 'undefined' || !chrome.runtime) {
            window.chrome = { runtime: { onMessage: { addListener: function(){} } } };
        }
    }""")

    # Inject CSS
    page.evaluate("""(css) => {
        let el = document.getElementById('xks-style');
        if (el) el.remove();
        const style = document.createElement('style');
        style.id = 'xks-style';
        style.textContent = css;
        document.head.appendChild(style);
    }""", panel_css)

    # Remove old panel if exists
    page.evaluate("() => { const p = document.getElementById('xks-panel'); if (p) p.remove(); window.__xksPanel = null; }")

    # Inject JS via Function constructor (avoids CSP and IIFE parsing issues)
    page.evaluate("""(code) => {
        try {
            const fn = new Function(code);
            fn();
        } catch(e) {
            console.error('XKS inject error:', e);
        }
    }""", panel_js)


def main():
    parser = argparse.ArgumentParser(
        description='集思录 × 小卡叔选债 — 悬浮框插件')
    parser.add_argument('--port', type=int, default=DEFAULT_CDP_PORT,
                        help=f'CDP 端口 (default: {DEFAULT_CDP_PORT})')
    args = parser.parse_args()

    print('🔌 连接浏览器...')
    pw, ctx = connect_cdp(args.port)

    try:
        print('🌐 定位集思录页面...')
        page = find_jisilu_page(ctx)
        print(f'  ✅ {page.url[:60]}')

        print('💉 注入小卡叔选债悬浮框...')
        inject_panel(page)
        print('  ✅ 悬浮框已注入! 请切换到浏览器查看')
        print()
        print('  功能:')
        print('    · 拖拽标题栏移动位置')
        print('    · 点击列头排序')
        print('    · 调节筛选条件后点"筛选"')
        print('    · ⟳ 刷新数据  ⤓ 导出CSV  − 折叠  × 关闭')
        print('    · 再次运行脚本可刷新面板')

    finally:
        pw.stop()


if __name__ == '__main__':
    main()
