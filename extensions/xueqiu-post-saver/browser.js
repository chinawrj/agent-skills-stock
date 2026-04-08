(function () {
  'use strict';

  const PAGE_SIZE = 50;

  let allPosts = [];
  let filteredPosts = [];
  let currentPage = 1;

  // ===== 通过 background service worker 获取数据 =====

  function sendMsg(msg) {
    return new Promise((resolve, reject) => {
      chrome.runtime.sendMessage(msg, (resp) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message));
        } else {
          resolve(resp);
        }
      });
    });
  }

  async function loadAllPosts() {
    const resp = await sendMsg({ action: 'getAllPosts' });
    if (!resp || !resp.ok) throw new Error(resp?.error || 'Failed to load posts');
    return resp.posts;
  }

  // ===== 工具函数 =====

  function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  function formatTime(iso) {
    if (!iso) return '-';
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    const pad = n => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  function truncate(str, len) {
    if (!str) return '';
    return str.length > len ? str.substring(0, len) + '...' : str;
  }

  // ===== 筛选与排序 =====

  function applyFilter() {
    const query = document.getElementById('search').value.toLowerCase();
    const authorFilter = document.getElementById('filter-author').value;

    filteredPosts = allPosts.filter(p => {
      if (authorFilter && p.authorId !== authorFilter) return false;
      if (query) {
        const hay = `${p.contentText} ${p.author} ${p.authorId} ${p.postId} ${p.timeText}`.toLowerCase();
        if (!hay.includes(query)) return false;
      }
      return true;
    });

    applySort();
  }

  function applySort() {
    const sortVal = document.getElementById('sort-by').value;
    const [field, dir] = sortVal.split('-');
    const asc = dir === 'asc' ? 1 : -1;

    filteredPosts.sort((a, b) => {
      const va = a[field] || '';
      const vb = b[field] || '';
      if (va < vb) return -1 * asc;
      if (va > vb) return 1 * asc;
      return 0;
    });

    currentPage = 1;
    render();
  }

  // ===== 渲染 =====

  function render() {
    renderStats();
    renderList();
    renderPagination();
  }

  function renderStats() {
    const el = document.getElementById('stats');
    const authors = new Set(allPosts.map(p => p.author).filter(Boolean));
    el.textContent = `共 ${allPosts.length} 条帖子 · ${authors.size} 位作者 · 当前显示 ${filteredPosts.length} 条`;
  }

  function renderList() {
    const container = document.getElementById('post-list');
    const start = (currentPage - 1) * PAGE_SIZE;
    const end = start + PAGE_SIZE;
    const page = filteredPosts.slice(start, end);

    if (page.length === 0) {
      container.innerHTML = '<div class="empty">没有找到帖子</div>';
      return;
    }

    container.innerHTML = page.map((p, i) => `
      <div class="post-card" data-index="${start + i}">
        <div class="post-header">
          <span class="post-author" title="UID: ${escapeHtml(p.authorId)}">${escapeHtml(p.author || '未知')}</span>
          <span class="post-time" title="原文: ${escapeHtml(p.timeText)}">${formatTime(p.postTime)}</span>
          <span class="post-id">#${escapeHtml(p.postId)}</span>
        </div>
        <div class="post-content">${escapeHtml(truncate(p.contentText, 300))}</div>
        <div class="post-footer">
          <a href="${escapeHtml(p.postUrl)}" target="_blank" rel="noopener">查看原文</a>
          <span class="post-saved" title="保存时间">💾 ${formatTime(p.savedAt)}</span>
        </div>
      </div>
    `).join('');

    // 点击卡片展开详情
    container.querySelectorAll('.post-card').forEach(card => {
      card.addEventListener('click', (e) => {
        if (e.target.tagName === 'A') return;
        const idx = parseInt(card.dataset.index);
        showDetail(filteredPosts[idx]);
      });
    });
  }

  function renderPagination() {
    const container = document.getElementById('pagination');
    const total = Math.ceil(filteredPosts.length / PAGE_SIZE);
    if (total <= 1) { container.innerHTML = ''; return; }

    let html = '';
    if (currentPage > 1) html += `<button data-page="${currentPage - 1}">‹ 上一页</button>`;

    const start = Math.max(1, currentPage - 3);
    const end = Math.min(total, currentPage + 3);
    for (let i = start; i <= end; i++) {
      html += `<button data-page="${i}" class="${i === currentPage ? 'active' : ''}">${i}</button>`;
    }

    if (currentPage < total) html += `<button data-page="${currentPage + 1}">下一页 ›</button>`;
    html += `<span class="page-info">第${currentPage}/${total}页</span>`;

    container.innerHTML = html;
    container.querySelectorAll('button[data-page]').forEach(btn => {
      btn.addEventListener('click', () => {
        currentPage = parseInt(btn.dataset.page);
        renderList();
        renderPagination();
        window.scrollTo(0, 0);
      });
    });
  }

  // ===== 详情弹窗 =====

  function showDetail(post) {
    const overlay = document.getElementById('modal-overlay');
    const body = document.getElementById('modal-body');

    body.innerHTML = `
      <div class="detail-header">
        <span class="detail-author">${escapeHtml(post.author)}</span>
        <span class="detail-uid">UID: ${escapeHtml(post.authorId)}</span>
        <span class="detail-time">${formatTime(post.postTime)}</span>
      </div>
      <div class="detail-meta">
        <span>帖子ID: ${escapeHtml(post.postId)}</span>
        <span>原始时间: ${escapeHtml(post.timeText)}</span>
        <span>保存时间: ${formatTime(post.savedAt)}</span>
      </div>
      <div class="detail-content">${post.contentHtml || escapeHtml(post.contentText)}</div>
      <div class="detail-footer">
        <a href="${escapeHtml(post.postUrl)}" target="_blank" rel="noopener">🔗 查看原文</a>
        <a href="${escapeHtml(post.pageUrl || '')}" target="_blank" rel="noopener">📄 来源页面</a>
      </div>
    `;

    overlay.classList.add('visible');
  }

  function hideDetail() {
    document.getElementById('modal-overlay').classList.remove('visible');
  }

  // ===== 导出 =====

  function exportJSON() {
    const data = filteredPosts.length < allPosts.length ? filteredPosts : allPosts;
    const json = JSON.stringify(data, null, 2);
    download(json, 'application/json', `xueqiu_posts_${dateStr()}.json`);
  }

  function exportCSV() {
    const data = filteredPosts.length < allPosts.length ? filteredPosts : allPosts;
    const headers = ['postId', 'author', 'authorId', 'postTime', 'timeText', 'contentText', 'postUrl', 'savedAt'];
    const rows = data.map(p => headers.map(h => csvCell(p[h] || '')));
    const csv = '\uFEFF' + [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
    download(csv, 'text/csv;charset=utf-8', `xueqiu_posts_${dateStr()}.csv`);
  }

  function csvCell(val) {
    const s = String(val).replace(/"/g, '""');
    return /[,"\n\r]/.test(s) ? `"${s}"` : s;
  }

  function dateStr() {
    return new Date().toISOString().slice(0, 10);
  }

  function download(content, type, filename) {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  // ===== 作者下拉菜单 =====

  function populateAuthorFilter() {
    const select = document.getElementById('filter-author');
    const authors = {};
    allPosts.forEach(p => {
      if (p.author) {
        if (!authors[p.authorId]) authors[p.authorId] = { name: p.author, count: 0 };
        authors[p.authorId].count++;
      }
    });
    const sorted = Object.entries(authors).sort((a, b) => b[1].count - a[1].count);
    sorted.forEach(([id, { name, count }]) => {
      const opt = document.createElement('option');
      opt.value = id;
      opt.textContent = `${name} (${count})`;
      select.appendChild(opt);
    });
  }

  // ===== 初始化 =====

  async function init() {
    try {
      allPosts = await loadAllPosts();
    } catch (err) {
      document.getElementById('stats').textContent = '加载失败: ' + err.message;
      return;
    }

    populateAuthorFilter();
    applyFilter();

    document.getElementById('search').addEventListener('input', debounce(applyFilter, 300));
    document.getElementById('sort-by').addEventListener('change', applyFilter);
    document.getElementById('filter-author').addEventListener('change', applyFilter);
    document.getElementById('export-btn').addEventListener('click', exportJSON);
    document.getElementById('export-csv-btn').addEventListener('click', exportCSV);
    document.getElementById('modal-close').addEventListener('click', hideDetail);
    document.getElementById('modal-overlay').addEventListener('click', (e) => {
      if (e.target === e.currentTarget) hideDetail();
    });
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') hideDetail();
    });
  }

  function debounce(fn, ms) {
    let t;
    return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
  }

  init();
})();
