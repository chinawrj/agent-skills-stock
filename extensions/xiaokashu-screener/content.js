// ═══════════════════════════════════════════════════════════
// 小卡叔选债 — 集思录可转债筛选 Chrome Extension
// Content Script: 自动注入悬浮筛选面板
// 价值投机 · 满仓轮动 · 低价潜伏 · 主动维权
// ═══════════════════════════════════════════════════════════

(function () {
  'use strict';

  // Prevent duplicate injection
  if (window.__xksPanel) {
    window.__xksPanel.refresh();
    return;
  }
  // Also check DOM (handles cross-context injections)
  if (document.getElementById('xks-panel')) return;

  const API = '/data/cbnew/cb_list_new/';
  let allBonds = [];
  let screenedBonds = [];
  let currentSort = { key: 'score', asc: false };
  const financialCache = {};  // stock_id → {net_profit, revenue}
  let filters = { maxPrice: 130, minPrice: 0, minYtm: 0, maxYear: 0, minYear: 0, strict: false, onlyRevised: false, hideRedeemed: true, sortBy: 'score' };

  // ═══ Load saved settings ═══
  try {
    const saved = localStorage.getItem('xks_filters');
    if (saved) {
      const parsed = JSON.parse(saved);
      filters = { ...filters, ...parsed };
    }
  } catch (_) { /* ignore */ }

  function saveFilters() {
    try { localStorage.setItem('xks_filters', JSON.stringify(filters)); } catch (_) { /* ignore */ }
  }

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
        <button id="xks-btn-close" title="隐藏面板 (点击插件图标重新打开)">×</button>
      </div>
    </div>
    <div id="xks-controls">
      <div class="xks-filter-group">
        <label>最高价</label>
        <input type="number" id="xks-max-price" value="${filters.maxPrice}" step="5">
      </div>
      <div class="xks-filter-group">
        <label>最低价</label>
        <input type="number" id="xks-min-price" value="${filters.minPrice}" step="5">
      </div>
      <div class="xks-filter-group">
        <label>最低YTM%</label>
        <input type="number" id="xks-min-ytm" value="${filters.minYtm}" step="1">
      </div>
      <div class="xks-filter-group">
        <label>最长年限</label>
        <input type="number" id="xks-max-year" value="${filters.maxYear}" step="0.5">
      </div>
      <div class="xks-filter-group">
        <label>最短年限</label>
        <input type="number" id="xks-min-year" value="${filters.minYear}" step="0.5">
      </div>
      <div class="xks-filter-group">
        <label>排序</label>
        <select id="xks-sort">
          <option value="score"${filters.sortBy === 'score' ? ' selected' : ''}>综合评分</option>
          <option value="dblow"${filters.sortBy === 'dblow' ? ' selected' : ''}>双低值↑</option>
          <option value="ytm_rt"${filters.sortBy === 'ytm_rt' ? ' selected' : ''}>YTM↓</option>
          <option value="price"${filters.sortBy === 'price' ? ' selected' : ''}>价格↑</option>
          <option value="premium_rt"${filters.sortBy === 'premium_rt' ? ' selected' : ''}>溢价率↑</option>
          <option value="year_left"${filters.sortBy === 'year_left' ? ' selected' : ''}>剩余年↑</option>
          <option value="curr_iss_amt"${filters.sortBy === 'curr_iss_amt' ? ' selected' : ''}>规模↑</option>
          <option value="redeem_yield"${filters.sortBy === 'redeem_yield' ? ' selected' : ''}>强赎收益↓</option>
        </select>
      </div>
      <div class="xks-filter-group">
        <label><input type="checkbox" id="xks-strict"${filters.strict ? ' checked' : ''}> 严格(YTM>0)</label>
      </div>
      <div class="xks-filter-group">
        <label><input type="checkbox" id="xks-only-revised"${filters.onlyRevised ? ' checked' : ''}> 仅下修成功</label>
      </div>
      <div class="xks-filter-group">
        <label><input type="checkbox" id="xks-hide-redeemed"${filters.hideRedeemed ? ' checked' : ''}> 排除强赎</label>
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
  header.addEventListener('mousedown', (e) => {
    if (e.target.tagName === 'BUTTON') return;
    isDragging = true;
    dragX = e.clientX - panel.offsetLeft;
    dragY = e.clientY - panel.offsetTop;
    document.body.style.userSelect = 'none';
  });
  document.addEventListener('mousemove', (e) => {
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
  panel.querySelector('#xks-btn-close').onclick = () => {
    panel.classList.add('xks-hidden');
  };
  panel.querySelector('#xks-btn-collapse').onclick = () => {
    panel.classList.toggle('xks-collapsed');
    panel.querySelector('#xks-btn-collapse').textContent =
      panel.classList.contains('xks-collapsed') ? '+' : '−';
  };
  panel.querySelector('#xks-btn-refresh').onclick = () => fetchAndScreen();
  panel.querySelector('#xks-btn-apply').onclick = () => { readFilters(); saveFilters(); applyScreen(); };
  panel.querySelector('#xks-btn-reset').onclick = () => {
    panel.querySelector('#xks-max-price').value = 130;
    panel.querySelector('#xks-min-price').value = 0;
    panel.querySelector('#xks-min-ytm').value = 0;
    panel.querySelector('#xks-max-year').value = 0;
    panel.querySelector('#xks-min-year').value = 0;
    panel.querySelector('#xks-strict').checked = false;
    panel.querySelector('#xks-only-revised').checked = false;
    panel.querySelector('#xks-hide-redeemed').checked = true;
    panel.querySelector('#xks-sort').value = 'score';
    filters = { maxPrice: 130, minPrice: 0, minYtm: 0, maxYear: 0, minYear: 0, strict: false, onlyRevised: false, hideRedeemed: true, sortBy: 'score' };
    saveFilters();
    applyScreen();
  };
  panel.querySelector('#xks-btn-export').onclick = exportCSV;

  // ═══ Listen for toggle message from popup ═══
  chrome.runtime.onMessage.addListener((msg) => {
    if (msg.action === 'toggle') {
      panel.classList.toggle('xks-hidden');
    } else if (msg.action === 'refresh') {
      panel.classList.remove('xks-hidden');
      fetchAndScreen();
    }
  });

  // ═══ Safe number parser ═══
  function sf(v, d) {
    if (v === null || v === undefined || v === '' || v === '-' || v === 'buy') return d === undefined ? null : d;
    const n = parseFloat(v);
    return isNaN(n) ? (d === undefined ? null : d) : n;
  }

  function readFilters() {
    filters.maxPrice = sf(panel.querySelector('#xks-max-price').value, 130);
    filters.minPrice = sf(panel.querySelector('#xks-min-price').value, 0);
    filters.minYtm = sf(panel.querySelector('#xks-min-ytm').value, 0);
    filters.maxYear = sf(panel.querySelector('#xks-max-year').value, 0);
    filters.minYear = sf(panel.querySelector('#xks-min-year').value, 0);
    filters.strict = panel.querySelector('#xks-strict').checked;
    filters.onlyRevised = panel.querySelector('#xks-only-revised').checked;
    filters.hideRedeemed = panel.querySelector('#xks-hide-redeemed').checked;
    filters.sortBy = panel.querySelector('#xks-sort').value;
  }

  // ═══ Fetch data from 集思录 API ═══
  async function fetchData() {
    const resp = await fetch(API + '?___jsl=LST___t=' + Date.now(), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-Requested-With': 'XMLHttpRequest'
      },
      credentials: 'same-origin'
    });
    const data = await resp.json();
    return data;
  }

  // ═══ 小卡叔 screening algorithm ═══
  // 100分制: 下修博弈(30) + YTM(20) + 价格(15) + 双低(12) + 溢价率(8) + 存续期(5) + 规模(5) + 评级(5)
  // "下修是转债最强的条款" — 小卡叔
  // 负面清单: 可交换债/ST/退市/高溢价临期
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
      const adjCnt = sf(cell.adj_cnt, 0);      // 下修总次数
      const adjScnt = sf(cell.adj_scnt, 0);     // 成功下修次数
      const adjusted = cell.adjusted;            // 当前是否在下修 Y/N
      const pbFlag = cell.pb_flag;               // 是否破净约束 Y/N

      // ═══ Negative filter ═══
      if (btype !== 'C') { excluded++; continue; }
      if (stockName.includes('ST')) { excluded++; continue; }
      if (bondName.includes('退')) { excluded++; continue; }
      if (premium !== null && premium > 100 && yearLeft !== null && yearLeft < 1) {
        excluded++; continue;
      }
      if (price === null) { excluded++; continue; }
      if (filters.maxPrice > 0 && price > filters.maxPrice) { excluded++; continue; }
      if (filters.minPrice > 0 && price < filters.minPrice) { excluded++; continue; }
      if (filters.maxYear > 0 && yearLeft !== null && yearLeft > filters.maxYear) { excluded++; continue; }
      if (filters.minYear > 0 && (yearLeft === null || yearLeft < filters.minYear)) { excluded++; continue; }
      if (filters.strict && (ytm === null || ytm < filters.minYtm)) { excluded++; continue; }
      if (filters.onlyRevised && adjScnt <= 0) { excluded++; continue; }

      // ═══ 强赎公告检测 ═══
      const refInfo = cell.ref_yield_info || '';
      const isRedeemAnnounced = refInfo.includes('已公告要强赎') || refInfo.includes('强赎登记日') || (cell.redeem_dt != null && cell.redeem_dt !== '');
      if (filters.hideRedeemed && isRedeemAnnounced) { excluded++; continue; }

      let score = 0;
      const tags = [];

      // ═══ 1. 下修博弈 (30分) — 最高权重 ═══
      // "下修是促转股的关键工具"、"81.2%的公司下修到最低价"
      // adj_cnt: 下修总次数, adj_scnt: 成功次数, adjusted: 当前进行中

      // 1a. 当前正在下修 → 最强信号 (+12)
      if (adjusted === 'Y') {
        score += 12;
        tags.push({ t: '🔥下修中', c: 'red' });
      }

      // 1b. 历史下修成功次数 (+10)
      //     成功下修过 = 公司有下修意愿和能力，历史预测未来
      if (adjScnt >= 3) {
        score += 10;
        tags.push({ t: '下修' + adjScnt + '次✓', c: 'green' });
      } else if (adjScnt >= 2) {
        score += 8;
        tags.push({ t: '下修' + adjScnt + '次✓', c: 'green' });
      } else if (adjScnt === 1) {
        score += 5;
        tags.push({ t: '下修1次✓', c: 'blue' });
      }

      // 1c. 成功率 (+5)
      //     "品德差公司下修变卦风险大" — 区分诚意下修vs形式下修
      if (adjCnt > 0) {
        const successRate = adjScnt / adjCnt;
        if (successRate >= 1) { score += 5; }           // 100%成功
        else if (successRate >= 0.5) { score += 2; }    // 部分成功
        else { tags.push({ t: '下修失败', c: 'red' }); } // 提案被否/不到底
      }

      // 1d. 破净约束惩罚 (-3)
      //     "破净条款观察 — 有些公司有心无力"
      if (pbFlag === 'Y') {
        score -= 3;
        tags.push({ t: 'PB受限', c: 'red' });
      }

      // 1e. 下修潜力评估 (+3~6)
      //     短期债+未下修过+非破净 = 潜在下修标的
      if (adjCnt === 0 && yearLeft !== null && yearLeft < 2 && pbFlag !== 'Y') {
        if (ytm !== null && ytm > 0) {
          // 短期正收益+没下修过 = "有点烂但不能太烂" → 下修潜力
          score += 3;
          tags.push({ t: '潜在下修', c: 'orange' });
        }
      }
      // 临期+未下修+大额余量 = 下修压力大
      if (adjCnt === 0 && yearLeft !== null && yearLeft < 1.5 && remaining !== null && remaining > 3) {
        score += 3;
        tags.push({ t: '下修压力', c: 'orange' });
      }

      // ═══ 2. YTM — 到期收益率 (20分) ═══
      if (ytm !== null) {
        if (ytm >= filters.minYtm) {
          score += 12;
          tags.push({ t: '正收益', c: 'green' });
          if (ytm >= 5) { score += 4; tags.push({ t: 'YTM ' + ytm.toFixed(1) + '%', c: 'green' }); }
          if (ytm >= 10) score += 4;
        } else {
          tags.push({ t: 'YTM ' + ytm.toFixed(1) + '%', c: 'red' });
        }
      }

      // ═══ 3. Price — 债价 (15分) ═══
      if (price <= 110) { score += 15; tags.push({ t: '深度低价', c: 'green' }); }
      else if (price <= 116) { score += 13; tags.push({ t: '低价吸纳', c: 'green' }); }
      else if (price <= 120) { score += 10; tags.push({ t: '低价', c: 'blue' }); }
      else if (price <= filters.maxPrice) { score += 5; }

      // ═══ 4. Dblow — 双低值 (12分) ═══
      if (dblow !== null) {
        if (dblow < 110) { score += 12; tags.push({ t: '极低双低', c: 'green' }); }
        else if (dblow < 120) { score += 9; tags.push({ t: '低双低', c: 'blue' }); }
        else if (dblow < 130) score += 6;
        else if (dblow < 150) score += 2;
      }

      // ═══ 5. Premium — 溢价率 (8分) ═══
      if (premium !== null) {
        if (premium < 10) { score += 8; tags.push({ t: '极低溢价', c: 'green' }); }
        else if (premium < 30) { score += 6; tags.push({ t: '低溢价', c: 'blue' }); }
        else if (premium < 50) score += 4;
        else if (premium < 100) score += 1;
      }

      // ═══ 6. Term — 存续期 (5分) ═══
      if (yearLeft !== null) {
        if (yearLeft >= 1.5 && yearLeft <= 3) { score += 5; tags.push({ t: '黄金期', c: 'blue' }); }
        else if (yearLeft >= 1 && yearLeft < 1.5) score += 3;
        else if (yearLeft > 3 && yearLeft <= 4) score += 2;
        else if (yearLeft < 1) tags.push({ t: '临期', c: 'orange' });
      }

      // ═══ 7. Size — 规模 (5分) ═══
      if (remaining !== null) {
        if (remaining < 3) { score += 5; tags.push({ t: '迷你', c: 'blue' }); }
        else if (remaining < 5) { score += 3; tags.push({ t: '小盘', c: 'gray' }); }
        else if (remaining < 10) score += 1;
      }

      // ═══ 8. Rating — 评级 (5分) ═══
      score += ratingScores[rating] || 0;

      // ═══ Special tags ═══
      if (isRedeemAnnounced) {
        const hasDate = cell.redeem_dt != null && cell.redeem_dt !== '';
        tags.push({ t: hasDate ? '强赎' + cell.redeem_dt : '公告强赎', c: 'red' });
        score -= 10; // 强赎惩罚
      }
      if (pb !== null && pb < 1 && pbFlag !== 'Y') tags.push({ t: '破净', c: 'orange' });
      if (Math.abs(changePct) > 15) tags.push({ t: '异常波动', c: 'red' });
      if (pb !== null && pb > 5) tags.push({ t: '优等生', c: 'gray' });
      if (yearLeft !== null && yearLeft < 1.5 && premium !== null && premium > 50 && ytm !== null && ytm > 0) {
        tags.push({ t: '现金替代', c: 'orange' });
      }

      // ═══ 强赎博弈收益率 ═══
      const forcePrice = sf(cell.force_redeem_price);
      const convPrice = sf(cell.convert_price);
      const redeemYield = (forcePrice && convPrice && price) ? ((forcePrice / convPrice * 100 / price - 1) * 100) : null;

      results.push({ ...cell, score, tags, redeem_yield: redeemYield });
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

    // 下修统计
    const hasRevision = results.filter(b => sf(b.adj_cnt, 0) > 0).length;
    const adjusting = results.filter(b => b.adjusted === 'Y').length;
    const revSuccess = results.filter(b => sf(b.adj_scnt, 0) > 0).length;

    // 市场温度判断
    let tempMsg = '';
    if (ytm10 > 30) tempMsg = '🔥 YTM10%+超30只 = 激进买入信号';
    else if (ytm5 < 50) tempMsg = '⚠️ 高收益债稀缺，市场偏贵';

    statsEl.innerHTML = `
      <span>总计 <span class="xks-stat-val">${total}</span></span>
      <span>通过 <span class="xks-stat-val">${results.length}</span></span>
      <span>排除 <span class="xks-stat-val">${excluded}</span></span>
      <span>正收益 <span class="xks-stat-val">${ytmPos}</span></span>
      <span>低价≤116 <span class="xks-stat-val">${lowPrice}</span></span>
      <span>有下修 <span class="xks-stat-val">${hasRevision}</span></span>
      <span>下修成功 <span class="xks-stat-val">${revSuccess}</span></span>
      ${adjusting ? '<span>🔥下修中 <span class="xks-stat-val">' + adjusting + '</span></span>' : ''}
      <span>迷你<3亿 <span class="xks-stat-val">${mini}</span></span>
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
      { k: 'redeem_yield', l: '强赎收益', w: '55px' },
      { k: 'pb', l: 'PB', w: '40px' },
      { k: 'adj_cnt', l: '下修', w: '50px' },
      { k: 'net_profit', l: '净利润', w: '60px' },
      { k: 'revenue', l: '营收', w: '60px' },
      { k: 'debt_np_ratio', l: '债/利', w: '50px' },
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
        } else if (c.k === 'redeem_yield') {
          const ry = sf(b.redeem_yield);
          const cls = ry !== null ? (ry > 10 ? 'xks-positive' : ry < 0 ? 'xks-negative' : '') : '';
          html += '<td class="' + cls + '">' + (ry !== null ? ry.toFixed(1) + '%' : '-') + '</td>';
        } else if (c.k === 'pb') {
          const p = sf(b.pb);
          const cls = p !== null && p < 1 ? 'xks-negative' : '';
          html += '<td class="' + cls + '">' + (p !== null ? p.toFixed(2) : '-') + '</td>';
        } else if (c.k === 'adj_cnt') {
          const ac = sf(b.adj_cnt, 0);
          const as = sf(b.adj_scnt, 0);
          const isAdj = b.adjusted === 'Y';
          let adjText = '-';
          let adjCls = '';
          if (ac > 0 || isAdj) {
            adjText = as + '/' + ac;
            if (isAdj) { adjText += '🔥'; adjCls = 'xks-negative'; }
            else if (as > 0) { adjCls = 'xks-positive'; }
          }
          html += '<td class="' + adjCls + '">' + adjText + '</td>';
        } else if (c.k === 'net_profit' || c.k === 'revenue') {
          const sid = b.stock_id || '';
          const fin = financialCache[sid];
          let val = '-';
          let cls = '';
          if (fin && fin[c.k] !== undefined) {
            const raw = fin[c.k];
            const yi = raw / 1e8;  // 转为亿元
            if (Math.abs(yi) >= 1) {
              val = yi.toFixed(1) + '亿';
            } else {
              val = (raw / 1e4).toFixed(0) + '万';
            }
            if (c.k === 'net_profit') cls = raw > 0 ? 'xks-positive' : 'xks-negative';
          } else if (fin === null) {
            val = '⚠';
          } else if (!fin) {
            val = '...';
          }
          html += '<td class="' + cls + '" data-fin-sid="' + sid + '" data-fin-key="' + c.k + '">' + val + '</td>';
        } else if (c.k === 'debt_np_ratio') {
          const sid = b.stock_id || '';
          const fin = financialCache[sid];
          const remaining = sf(b.curr_iss_amt);  // 亿元
          let val = '-', cls = '';
          if (fin && fin.net_profit !== null && fin.net_profit !== undefined && remaining !== null) {
            const npYi = fin.net_profit / 1e8;  // 元→亿元
            if (npYi !== 0) {
              const ratio = remaining / npYi;
              if (ratio < 0) {
                val = '亏损'; cls = 'xks-negative';
              } else {
                val = ratio.toFixed(1);
                cls = ratio > 3 ? 'xks-negative' : ratio > 1 ? '' : 'xks-positive';
              }
            } else {
              val = '∞';
            }
          } else if (fin === null) {
            val = '⚠';
          } else if (!fin) {
            val = '...';
          }
          html += '<td class="' + cls + '" data-fin-sid="' + sid + '" data-fin-key="debt_np_ratio">' + val + '</td>';
        } else if (c.k === 'bond_nm') {
          const id = b.bond_id || '';
          html += '<td style="text-align:left" title="' + id + '"><a href="https://www.jisilu.cn/data/convert_bond_detail/' + id + '" target="_blank" class="xks-link">' + (b.bond_nm || '') + '</a></td>';
        } else if (c.k === 'stock_nm') {
          const sid = b.stock_id || '';
          html += '<td style="text-align:left"><a href="https://stockpage.10jqka.com.cn/' + sid + '" target="_blank" class="xks-link">' + (b.stock_nm || '') + '</a></td>';
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
          currentSort = { key, asc: ['price', 'premium_rt', 'dblow', 'year_left', 'curr_iss_amt', 'pb', 'debt_np_ratio'].includes(key) };
        }
        const sorted = [...screenedBonds].sort((a, b) => {
          let va, vb;
          if (key === 'score') { va = a.score; vb = b.score; }
          else if (key === 'adj_cnt') { va = sf(a.adj_scnt, -1); vb = sf(b.adj_scnt, -1); } // sort by success count
          else if (key === 'net_profit' || key === 'revenue') {
            const fa = financialCache[a.stock_id], fb = financialCache[b.stock_id];
            const dv = currentSort.asc ? -1e18 : 1e18;
            va = fa && fa[key] !== null && fa[key] !== undefined ? fa[key] : dv;
            vb = fb && fb[key] !== null && fb[key] !== undefined ? fb[key] : dv;
          }
          else if (key === 'debt_np_ratio') {
            const fa = financialCache[a.stock_id], fb = financialCache[b.stock_id];
            const dv = currentSort.asc ? 1e18 : -1e18;
            const ra = sf(a.curr_iss_amt), rb = sf(b.curr_iss_amt);
            const npa = fa && fa.net_profit, npb = fb && fb.net_profit;
            va = (npa && ra !== null && npa !== 0) ? ra / (npa / 1e8) : dv;
            vb = (npb && rb !== null && npb !== 0) ? rb / (npb / 1e8) : dv;
          }
          else { va = sf(a[key], currentSort.asc ? 9999 : -9999); vb = sf(b[key], currentSort.asc ? 9999 : -9999); }
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
    const headers = ['代码', '转债', '正股', '评分', '价格', '溢价率', 'YTM', '双低', '剩余年', '规模亿', '评级', 'PB', '下修成功', '下修总次', '下修中', 'PB受限', '净利润(亿)', '营收(亿)', '债/利', '标签'];
    const rows = screenedBonds.map(b => {
      const fin = financialCache[b.stock_id] || {};
      const np = fin.net_profit !== undefined ? (fin.net_profit / 1e8).toFixed(2) : '';
      const rev = fin.revenue !== undefined ? (fin.revenue / 1e8).toFixed(2) : '';
      return [
        b.bond_id, b.bond_nm, b.stock_nm, b.score,
        sf(b.price, ''), sf(b.premium_rt, ''), sf(b.ytm_rt, ''), sf(b.dblow, ''),
        sf(b.year_left, ''), sf(b.curr_iss_amt, ''), b.rating_cd || '', sf(b.pb, ''),
        sf(b.adj_scnt, ''), sf(b.adj_cnt, ''), b.adjusted || '', b.pb_flag || '',
        np, rev,
        (np && parseFloat(np) !== 0 && sf(b.curr_iss_amt, '') !== '') ? (sf(b.curr_iss_amt) / parseFloat(np)).toFixed(2) : '',
        (b.tags || []).map(t => t.t).join('/')
      ];
    });
    let csv = '\uFEFF' + headers.join(',') + '\n';
    for (const r of rows) csv += r.map(v => '"' + String(v).replace(/"/g, '""') + '"').join(',') + '\n';
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = '小卡叔选债_' + new Date().toISOString().slice(0, 10) + '.csv';
    a.click();
    URL.revokeObjectURL(a.href);
  }

  // ═══ Main flow ═══
  function applyScreen() {
    const { results, excluded } = screen(allBonds);
    screenedBonds = results;
    renderStats(results, allBonds.length, excluded);
    renderTable(results);
    fetchFinancials(results);
  }

  async function fetchAndScreen() {
    const body = panel.querySelector('#xks-body');
    body.innerHTML = '<div id="xks-loading">⏳ 正在获取数据...</div>';
    try {
      const data = await fetchData();
      if (!data || !data.rows) {
        body.innerHTML = '<div id="xks-loading">❌ 获取数据失败，请确认已登录集思录</div>';
        return;
      }
      if (data.warn) {
        console.log('[小卡叔选债] ' + data.warn);
      }
      allBonds = data.rows.map(r => r.cell);
      readFilters();
      applyScreen();
    } catch (e) {
      body.innerHTML = '<div id="xks-loading">❌ 错误: ' + e.message + '</div>';
    }
  }

  // ═══ Fetch annual report data (batch via extension background or WebProxyHub) ═══
  function stockIdToSecucode(sid) {
    // 6/688 → SH, 0/3 → SZ
    const suffix = sid.startsWith('6') ? '.SH' : '.SZ';
    return sid + suffix;
  }

  async function fetchFinancials(results) {
    const stockIds = [...new Set(results.map(b => b.stock_id).filter(Boolean))];
    const toFetch = stockIds.filter(id => !(id in financialCache));
    if (toFetch.length === 0) { window.__xksFinancialsDone = true; return; }

    console.log('[小卡叔选债] 获取年报财务数据: ' + toFetch.length + ' 只');

    // Prefer chrome extension background (batch API, no proxy needed)
    if (typeof chrome !== 'undefined' && chrome.runtime && chrome.runtime.sendMessage) {
      try {
        const codes = toFetch.map(stockIdToSecucode);
        // Split into batches of 50 to avoid URL length limits
        for (let i = 0; i < codes.length; i += 50) {
          const batch = codes.slice(i, i + 50);
          const batchIds = toFetch.slice(i, i + 50);
          const resp = await new Promise(resolve => {
            chrome.runtime.sendMessage({ action: 'fetchAnnualFinancials', codes: batch }, resolve);
          });
          if (chrome.runtime.lastError) {
            console.warn('[小卡叔选债] background 通道异常:', chrome.runtime.lastError);
            break;
          }
          if (resp && resp.ok && resp.data) {
            for (const sid of batchIds) {
              const sc = stockIdToSecucode(sid);
              const row = resp.data[sc];
              if (row) {
                financialCache[sid] = {
                  net_profit: typeof row.PARENTNETPROFIT === 'number' ? row.PARENTNETPROFIT : null,
                  revenue: typeof row.TOTALOPERATEREVE === 'number' ? row.TOTALOPERATEREVE : null,
                };
              } else {
                financialCache[sid] = null;
              }
              updateFinancialCells(sid);
            }
            console.log('[小卡叔选债] 年报批次完成: ' + Math.min(i + 50, toFetch.length) + '/' + toFetch.length);
          } else {
            console.warn('[小卡叔选债] 年报批次失败:', resp);
            // Mark failed
            for (const sid of batchIds) { financialCache[sid] = null; updateFinancialCells(sid); }
          }
        }
        window.__xksFinancialsDone = true;
        console.log('[小卡叔选债] 年报财务数据完成');
        return;
      } catch (e) {
        console.warn('[小卡叔选债] background 批量获取异常:', e);
      }
    }

    // Fallback: direct fetch (datacenter API supports CORS with Access-Control-Allow-Origin: *)
    try {
      const codes = toFetch.map(stockIdToSecucode);
      for (let i = 0; i < codes.length; i += 50) {
        const batch = codes.slice(i, i + 50);
        const batchIds = toFetch.slice(i, i + 50);
        const inList = batch.map(c => '"' + c + '"').join(',');
        const url = 'https://datacenter.eastmoney.com/securities/api/data/get?'
          + 'type=RPT_F10_FINANCE_MAINFINADATA'
          + '&sty=SECUCODE,REPORT_DATE,PARENTNETPROFIT,TOTALOPERATEREVE'
          + '&filter=(SECUCODE+in+(' + encodeURIComponent(inList) + '))(REPORT_TYPE=%22%E5%B9%B4%E6%8A%A5%22)'
          + '&pageSize=' + (batch.length * 2)
          + '&sortColumns=REPORT_DATE&sortTypes=-1';
        const resp = await fetch(url, {
          headers: { 'Referer': 'https://emweb.securities.eastmoney.com/' },
        });
        const json = await resp.json();
        // Dedup: take latest per SECUCODE
        const seen = new Set();
        if (json && json.result && json.result.data) {
          for (const row of json.result.data) {
            if (seen.has(row.SECUCODE)) continue;
            seen.add(row.SECUCODE);
            const sid = row.SECUCODE.split('.')[0];
            financialCache[sid] = {
              net_profit: typeof row.PARENTNETPROFIT === 'number' ? row.PARENTNETPROFIT : null,
              revenue: typeof row.TOTALOPERATEREVE === 'number' ? row.TOTALOPERATEREVE : null,
            };
            updateFinancialCells(sid);
          }
        }
        // Mark stocks without data
        for (const sid of batchIds) {
          if (!(sid in financialCache)) { financialCache[sid] = null; updateFinancialCells(sid); }
        }
        console.log('[小卡叔选债] 年报批次完成: ' + Math.min(i + 50, toFetch.length) + '/' + toFetch.length);
      }
      window.__xksFinancialsDone = true;
      console.log('[小卡叔选债] 年报财务数据完成');
      return;
    } catch (e) {
      console.warn('[小卡叔选债] 年报直接获取异常:', e);
    }

    console.log('[小卡叔选债] 无可用数据通道，跳过财务数据');
    window.__xksFinancialsDone = true;
  }

  function updateFinancialCells(sid) {
    const cells = panel.querySelectorAll('td[data-fin-sid="' + sid + '"]');
    const fin = financialCache[sid];
    for (const td of cells) {
      const key = td.getAttribute('data-fin-key');
      if (key === 'debt_np_ratio') {
        if (fin && fin.net_profit !== null && fin.net_profit !== undefined) {
          const npYi = fin.net_profit / 1e8;
          // Find curr_iss_amt from the same row
          const row = td.closest('tr');
          const amtCell = row && row.querySelector('td[data-fin-key="net_profit"]');
          // Get remaining from screenedBonds by matching stock link
          const stockLink = row && row.querySelector('a[href*="10jqka"]');
          const rowSid = stockLink ? (stockLink.href.match(/\/([0-9]{6})$/)||[])[1] : '';
          const bond = screenedBonds.find(b => b.stock_id === rowSid);
          const remaining = bond ? sf(bond.curr_iss_amt) : null;
          if (remaining !== null && npYi !== 0) {
            const ratio = remaining / npYi;
            if (ratio < 0) { td.textContent = '亏损'; td.className = 'xks-negative'; }
            else { td.textContent = ratio.toFixed(1); td.className = ratio > 3 ? 'xks-negative' : ratio > 1 ? '' : 'xks-positive'; }
          } else if (npYi === 0) {
            td.textContent = '∞';
          } else {
            td.textContent = '-';
          }
        } else if (fin === null) {
          td.textContent = '⚠';
        }
      } else if (fin && fin[key] !== undefined && fin[key] !== null) {
        const raw = fin[key];
        const yi = raw / 1e8;
        if (Math.abs(yi) >= 1) {
          td.textContent = yi.toFixed(1) + '亿';
        } else {
          td.textContent = (raw / 1e4).toFixed(0) + '万';
        }
        if (key === 'net_profit') {
          td.className = raw > 0 ? 'xks-positive' : 'xks-negative';
        }
      } else if (fin === null) {
        td.textContent = '⚠';
      }
    }
  }

  // ═══ Expose API ═══
  window.__xksFinancialsDone = false;
  window.__xksPanel = { refresh: fetchAndScreen, panel };
  fetchAndScreen();
})();
