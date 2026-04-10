/**
 * 雪球帖子保存器 - Content Script
 * 
 * 功能：
 * 1. 自动扫描页面上出现的帖子（article.timeline__item）
 * 2. 提取帖子信息（ID、作者、时间、内容）保存到 IndexedDB
 * 3. 已保存的帖子用绿色边框高亮显示
 * 4. 使用 MutationObserver 监听新帖子出现（滚动加载）
 * 5. 右下角显示统计浮窗
 */

(function () {
  'use strict';

  const DB_NAME = 'XueqiuPostSaver';
  const STORE_NAME = 'posts';
  const AUTHOR_META_STORE = 'authorMeta';
  const DB_VERSION = 3;

  // ===== IndexedDB 操作 =====

  function openDB() {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION);

      request.onupgradeneeded = (event) => {
        const db = event.target.result;
        let store;
        if (!db.objectStoreNames.contains(STORE_NAME)) {
          store = db.createObjectStore(STORE_NAME, { keyPath: 'postId' });
          store.createIndex('authorId', 'authorId', { unique: false });
          store.createIndex('savedAt', 'savedAt', { unique: false });
          store.createIndex('postUrl', 'postUrl', { unique: false });
          store.createIndex('postTime', 'postTime', { unique: false });
        } else {
          store = event.target.transaction.objectStore(STORE_NAME);
          if (!store.indexNames.contains('postTime')) {
            store.createIndex('postTime', 'postTime', { unique: false });
          }
        }
        // v3: authorMeta store
        if (!db.objectStoreNames.contains(AUTHOR_META_STORE)) {
          db.createObjectStore(AUTHOR_META_STORE, { keyPath: 'authorId' });
        }
      };

      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  function savePost(db, post) {
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, 'readwrite');
      const store = tx.objectStore(STORE_NAME);

      // 先检查是否已存在
      const getReq = store.get(post.postId);
      getReq.onsuccess = () => {
        const existing = getReq.result;
        if (existing) {
          // 更新缺失的 postTime 或更长的内容（从截断升级到完整）
          let needUpdate = false;
          if (!existing.postTime && post.postTime) {
            existing.postTime = post.postTime;
            needUpdate = true;
          }
          if (post.contentText && post.contentText.length > (existing.contentText || '').length) {
            existing.contentText = post.contentText;
            existing.contentHtml = post.contentHtml;
            existing.title = post.title;
            needUpdate = true;
          }
          if (needUpdate) {
            store.put(existing);
            tx.oncomplete = () => resolve(false);
          } else {
            resolve(false); // already saved
          }
        } else {
          store.put(post);
          tx.oncomplete = () => resolve(true); // newly saved
        }
      };
      getReq.onerror = () => reject(getReq.error);
    });
  }

  function getAllSavedIds(db) {
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, 'readonly');
      const store = tx.objectStore(STORE_NAME);
      const req = store.getAllKeys();
      req.onsuccess = () => resolve(new Set(req.result));
      req.onerror = () => reject(req.error);
    });
  }

  function countPosts(db) {
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, 'readonly');
      const req = tx.objectStore(STORE_NAME).count();
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  function getAllPosts(db) {
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, 'readonly');
      const req = tx.objectStore(STORE_NAME).getAll();
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  // ===== 作者元数据 =====

  function getCurrentAuthorId() {
    const m = location.pathname.match(/\/u\/(\d+)/);
    return m ? m[1] : '';
  }

  function getAuthorMeta(db, authorId) {
    return new Promise((resolve, reject) => {
      const tx = db.transaction(AUTHOR_META_STORE, 'readonly');
      const req = tx.objectStore(AUTHOR_META_STORE).get(authorId);
      req.onsuccess = () => resolve(req.result || null);
      req.onerror = () => reject(req.error);
    });
  }

  function setAuthorMeta(db, authorId, data) {
    return new Promise((resolve, reject) => {
      const tx = db.transaction(AUTHOR_META_STORE, 'readwrite');
      tx.objectStore(AUTHOR_META_STORE).put({ authorId, ...data });
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  // ===== 时间解析 =====

  /**
   * 解析雪球时间文本为 ISO 字符串
   * 格式示例：
   *   "23分钟前· 来自雪球"  → 当前时间 - 23分钟
   *   "今天 15:30· 来自雪球" → 今天 15:30
   *   "昨天 20:18· 来自雪球" → 昨天 20:18
   *   "04-05 09:42· 来自雪球" → 今年04-05 09:42
   *   "2025-12-01 10:00· 来自雪球" → 2025-12-01 10:00
   */
  function parsePostTime(timeText) {
    if (!timeText) return '';
    // 去掉 "· 来自xxx" 后缀（·可能是各种Unicode中点字符）
    const cleaned = timeText.replace(/\s*来自.*$/, '').replace(/[·•\u00b7\u2022\u30fb]/g, '').trim();
    const now = new Date();

    // "X分钟前" / "X小时前"
    let m = cleaned.match(/(\d+)\s*分钟前/);
    if (m) {
      const d = new Date(now.getTime() - parseInt(m[1]) * 60000);
      return d.toISOString();
    }
    m = cleaned.match(/(\d+)\s*小时前/);
    if (m) {
      const d = new Date(now.getTime() - parseInt(m[1]) * 3600000);
      return d.toISOString();
    }
    // "刚刚"
    if (cleaned.includes('刚刚')) return now.toISOString();

    // "今天 HH:MM"
    m = cleaned.match(/今天\s*(\d{1,2}):(\d{2})/);
    if (m) {
      const d = new Date(now.getFullYear(), now.getMonth(), now.getDate(), parseInt(m[1]), parseInt(m[2]));
      return d.toISOString();
    }

    // "昨天 HH:MM"
    m = cleaned.match(/昨天\s*(\d{1,2}):(\d{2})/);
    if (m) {
      const d = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1, parseInt(m[1]), parseInt(m[2]));
      return d.toISOString();
    }

    // "YYYY-MM-DD HH:MM"
    m = cleaned.match(/(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})/);
    if (m) {
      const d = new Date(parseInt(m[1]), parseInt(m[2]) - 1, parseInt(m[3]), parseInt(m[4]), parseInt(m[5]));
      return d.toISOString();
    }

    // "MM-DD HH:MM" (当年)
    m = cleaned.match(/(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})/);
    if (m) {
      const d = new Date(now.getFullYear(), parseInt(m[1]) - 1, parseInt(m[2]), parseInt(m[3]), parseInt(m[4]));
      // 如果得到的日期在未来，说明是去年
      if (d > now) d.setFullYear(d.getFullYear() - 1);
      return d.toISOString();
    }

    return ''; // 无法解析
  }

  // ===== 帖子提取 =====

  // inject.js 在 MAIN world 中运行，读取 Vue 组件的 item.text 并写入 data-xq-fulltext
  // content.js 在 ISOLATED world 中读取该属性

  function requestVueData() {
    document.dispatchEvent(new CustomEvent('xq-request-vue-data'));
  }

  function extractPost(article) {
    const idLink = article.querySelector('a[data-id]');
    if (!idLink) return null;

    const postId = idLink.getAttribute('data-id');
    if (!postId) return null;

    const postUrl = idLink.getAttribute('href') || '';
    const timeText = idLink.textContent.trim();

    const authorEl = article.querySelector('.user-name');
    const author = authorEl ? authorEl.textContent.trim() : '';
    const authorId = authorEl ? (authorEl.getAttribute('data-tooltip') || '') : '';

    // 优先从 data-xq-fulltext 属性获取完整内容（由注入的主世界脚本写入）
    const fullText = article.getAttribute('data-xq-fulltext') || '';

    const contentEl = article.querySelector('.content');
    // 如果有完整文本，使用它；否则降级到 DOM
    const contentHtml = fullText || (contentEl ? contentEl.innerHTML : '');
    const contentText = fullText
      ? fullText.replace(/<br\s*\/?>/gi, '\n').replace(/<[^>]+>/g, '').trim()
      : (contentEl ? contentEl.textContent.trim() : '');

    return {
      postId,
      postUrl: postUrl.startsWith('http') ? postUrl : 'https://xueqiu.com' + postUrl,
      timeText,
      postTime: parsePostTime(timeText),
      author,
      authorId,
      title: contentText.substring(0, 80),
      contentText,
      contentHtml,
      pageUrl: location.href,
      savedAt: new Date().toISOString()
    };
  }

  // ===== 高亮显示 =====

  function highlightArticle(article) {
    if (article.classList.contains('xq-saved')) return;
    article.classList.add('xq-saved');

    if (!article.querySelector('.xq-saved-badge')) {
      const badge = document.createElement('div');
      badge.className = 'xq-saved-badge';
      badge.textContent = '✓ 已保存';
      article.appendChild(badge);
    }
  }

  // ===== 统计浮窗 =====

  let statsEl = null;
  let sessionSaved = 0;

  // ===== 自动翻页 =====

  let autoPageTimer = null;
  let autoPageRunning = false;
  let fetchLatestMode = false; // true = 「抓取最新」模式（遇到全部重复则停）
  let fetchLatestStopOnDup = false; // true = 作者已全量扫描过，遇重复可停

  function getRandomDelay() {
    return Math.floor(Math.random() * 7000) + 3000; // 3~10s
  }

  function findNextPageBtn() {
    return document.querySelector('a.pagination__next');
  }

  function getCurrentPageInfo() {
    const active = document.querySelector('.pagination a.active');
    const allPages = document.querySelectorAll('.pagination a:not(.pagination__prev):not(.pagination__next):not(.pagination__more)');
    const lastPage = allPages.length > 0 ? allPages[allPages.length - 1] : null;
    return {
      current: active ? active.textContent.trim() : '?',
      total: lastPage ? lastPage.textContent.trim() : '?'
    };
  }

  function isLastPage() {
    const nextBtn = findNextPageBtn();
    if (!nextBtn) return true;
    // 雪球在最后一页会隐藏"下一页"按钮或添加disabled样式
    const style = window.getComputedStyle(nextBtn);
    if (style.display === 'none' || style.visibility === 'hidden') return true;
    if (nextBtn.classList.contains('disabled')) return true;
    // 也可通过当前页==总页数判断
    const info = getCurrentPageInfo();
    return info.current === info.total;
  }

  function updateAutoPageStatus(text) {
    const el = document.getElementById('xq-autopage-status');
    if (el) el.textContent = text;
  }

  async function doAutoPageStep() {
    if (!autoPageRunning) return;

    // 等待当前页帖子保存完成，获取本页新增数
    const { newCount, totalOnPage } = await scanAndSave();

    // 「抓取最新」模式：已全量扫描过的作者，本页无新帖=和已有数据接上了
    if (fetchLatestMode && fetchLatestStopOnDup && totalOnPage > 0 && newCount === 0) {
      await markAuthorFullyScanned();
      stopAutoPage();
      updateAutoPageStatus('✅ 已与现有数据接上');
      console.log('[XQ-Saver] Fetch-latest: caught up with existing data');
      return;
    }

    if (isLastPage()) {
      await markAuthorFullyScanned();
      stopAutoPage();
      updateAutoPageStatus('✅ 已到最后一页');
      console.log('[XQ-Saver] Auto-page: reached last page');
      return;
    }

    const nextBtn = findNextPageBtn();
    if (!nextBtn) {
      stopAutoPage();
      updateAutoPageStatus('⚠️ 未找到下一页');
      return;
    }

    const info = getCurrentPageInfo();
    const delay = getRandomDelay();
    updateAutoPageStatus(`⏳ 第${info.current}/${info.total}页 ${(delay/1000).toFixed(1)}s后翻页`);
    console.log(`[XQ-Saver] Page ${info.current}/${info.total}, next click in ${delay}ms`);

    autoPageTimer = setTimeout(() => {
      if (!autoPageRunning) return;
      nextBtn.click();
      // 等页面加载后再继续
      setTimeout(() => {
        if (autoPageRunning) {
          window.scrollTo(0, 0);
          setTimeout(() => doAutoPageStep(), 1000);
        }
      }, 2000);
    }, delay);
  }

  async function markAuthorFullyScanned() {
    const authorId = getCurrentAuthorId();
    if (authorId && db) {
      await setAuthorMeta(db, authorId, { fullyScanedAt: new Date().toISOString() });
      console.log(`[XQ-Saver] Marked author ${authorId} as fully scanned`);
      updateFullyScanedBadge();
    }
  }

  function startAutoPage() {
    autoPageRunning = true;
    fetchLatestMode = false;
    fetchLatestStopOnDup = false;
    const btn = document.getElementById('xq-autopage-btn');
    if (btn) {
      btn.textContent = '⏹ 停止翻页';
      btn.classList.add('xq-autopage-active');
    }
    console.log('[XQ-Saver] Auto-page started');
    doAutoPageStep();
  }

  function stopAutoPage() {
    autoPageRunning = false;
    fetchLatestMode = false;
    fetchLatestStopOnDup = false;
    if (autoPageTimer) {
      clearTimeout(autoPageTimer);
      autoPageTimer = null;
    }
    const btn = document.getElementById('xq-autopage-btn');
    if (btn) {
      btn.textContent = '▶ 自动翻页';
      btn.classList.remove('xq-autopage-active');
    }
    const btn2 = document.getElementById('xq-fetch-latest-btn');
    if (btn2) {
      btn2.textContent = '🔄 抓取最新';
      btn2.classList.remove('xq-autopage-active');
    }
    console.log('[XQ-Saver] Auto-page stopped');
  }

  function toggleAutoPage() {
    if (autoPageRunning) {
      stopAutoPage();
      updateAutoPageStatus('已停止');
    } else {
      startAutoPage();
    }
  }

  async function fetchLatest() {
    if (autoPageRunning) {
      stopAutoPage();
      updateAutoPageStatus('已停止');
      return;
    }

    const authorId = getCurrentAuthorId();
    const meta = authorId && db ? await getAuthorMeta(db, authorId) : null;
    fetchLatestMode = true;
    fetchLatestStopOnDup = !!(meta && meta.fullyScanedAt);

    const mode = fetchLatestStopOnDup ? '增量' : '全量';
    console.log(`[XQ-Saver] Fetch-latest: ${mode} mode for author ${authorId}`);
    updateAutoPageStatus(`🔄 ${mode}抓取中...`);

    // 如果不在第1页，先跳到第1页
    const info = getCurrentPageInfo();
    if (info.current !== '1') {
      const firstPageLink = document.querySelector('.pagination a:not(.pagination__prev):not(.pagination__next):not(.pagination__more)');
      if (firstPageLink && firstPageLink.textContent.trim() === '1') {
        firstPageLink.click();
        await new Promise(r => setTimeout(r, 3000));
      }
    }

    autoPageRunning = true;
    const btn = document.getElementById('xq-fetch-latest-btn');
    if (btn) {
      btn.textContent = '⏹ 停止抓取';
      btn.classList.add('xq-autopage-active');
    }
    doAutoPageStep();
  }

  function updateFullyScanedBadge() {
    const el = document.getElementById('xq-fully-scaned');
    if (!el) return;
    const authorId = getCurrentAuthorId();
    if (authorId && db) {
      getAuthorMeta(db, authorId).then(meta => {
        if (meta && meta.fullyScanedAt) {
          el.textContent = `✅ ${meta.fullyScanedAt.slice(0, 10)}`;
          el.title = `全量扫描于 ${meta.fullyScanedAt}`;
        } else {
          el.textContent = '未扫描';
          el.title = '';
        }
      });
    }
  }

  function createStatsPanel() {
    statsEl = document.createElement('div');
    statsEl.id = 'xq-saver-stats';
    statsEl.innerHTML = `
      <div class="xq-stats-title">📋 帖子保存器</div>
      <div class="xq-stats-row">
        <span>本次新增</span>
        <span class="xq-stats-num" id="xq-session-count">0</span>
      </div>
      <div class="xq-stats-row">
        <span>总计帖子</span>
        <span class="xq-stats-num" id="xq-total-count">-</span>
      </div>
      <div class="xq-stats-row">
        <span>用户数量</span>
        <span class="xq-stats-num" id="xq-author-count">-</span>
      </div>
      <div class="xq-stats-row">
        <span>当前页面</span>
        <span class="xq-stats-num" id="xq-page-count">-</span>
      </div>
      <div class="xq-stats-row xq-stats-sub">
        <span>时间跨度</span>
        <span class="xq-stats-val" id="xq-date-range">-</span>
      </div>
      <div class="xq-stats-row xq-stats-sub">
        <span>全量扫描</span>
        <span class="xq-stats-val" id="xq-fully-scaned">-</span>
      </div>
      <button class="xq-export-btn" id="xq-export-btn">导出JSON</button>
      <div class="xq-btn-row">
        <button class="xq-autopage-btn" id="xq-autopage-btn">▶ 自动翻页</button>
        <button class="xq-fetch-latest-btn" id="xq-fetch-latest-btn">🔄 抓取最新</button>
      </div>
      <div class="xq-autopage-status" id="xq-autopage-status"></div>
    `;
    document.body.appendChild(statsEl);

    document.getElementById('xq-export-btn').addEventListener('click', exportPosts);
    document.getElementById('xq-autopage-btn').addEventListener('click', toggleAutoPage);
    document.getElementById('xq-fetch-latest-btn').addEventListener('click', fetchLatest);
  }

  function updateStats(db) {
    getAllPosts(db).then(posts => {
      const sessionEl = document.getElementById('xq-session-count');
      const totalEl = document.getElementById('xq-total-count');
      const authorEl = document.getElementById('xq-author-count');
      const pageEl = document.getElementById('xq-page-count');
      const dateEl = document.getElementById('xq-date-range');

      if (sessionEl) sessionEl.textContent = sessionSaved;
      if (totalEl) totalEl.textContent = posts.length;

      // 用户数量
      if (authorEl) {
        const authors = new Set(posts.map(p => p.authorId || p.author).filter(Boolean));
        authorEl.textContent = authors.size;
      }

      // 当前页面帖子
      if (pageEl) {
        const onPage = document.querySelectorAll('article.timeline__item.xq-saved').length;
        const total = document.querySelectorAll('article.timeline__item').length;
        pageEl.textContent = `${onPage}/${total}`;
      }

      // 时间跨度
      if (dateEl) {
        const times = posts.map(p => p.postTime).filter(Boolean).sort();
        if (times.length >= 2) {
          const fmt = t => t.slice(0, 10);
          dateEl.textContent = `${fmt(times[0])} ~ ${fmt(times[times.length - 1])}`;
        } else if (times.length === 1) {
          dateEl.textContent = times[0].slice(0, 10);
        }
      }
    });
  }

  async function exportPosts() {
    const db = await openDB();
    const posts = await getAllPosts(db);
    const json = JSON.stringify(posts, null, 2);
    const blob = new Blob([json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `xueqiu_posts_${new Date().toISOString().slice(0, 10)}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  // ===== 核心逻辑 =====

  // 防抖处理
  let scanTimer = null;
  function scheduleScan() {
    if (scanTimer) clearTimeout(scanTimer);
    scanTimer = setTimeout(() => scanAndSave(), 500);
  }

  let db = null;
  let savedIds = new Set();

  async function scanAndSave() {
    if (!db) return;

    // 请求主世界脚本刷新 data-xq-fulltext 属性
    requestVueData();
    // 给 inject.js (MAIN world) 时间写入 DOM 属性
    await new Promise(r => setTimeout(r, 300));

    const articles = document.querySelectorAll('article.timeline__item');
    let newCount = 0;
    const totalOnPage = articles.length;
    const newPosts = [];

    for (const article of articles) {
      const idLink = article.querySelector('a[data-id]');
      if (!idLink) continue;
      const postId = idLink.getAttribute('data-id');

      // 已处理过的跳过
      if (savedIds.has(postId)) {
        highlightArticle(article);
        continue;
      }

      const post = extractPost(article);
      if (!post) continue;

      try {
        const isNew = await savePost(db, post);
        savedIds.add(postId);
        highlightArticle(article);
        if (isNew) {
          newCount++;
          sessionSaved++;
          newPosts.push(post);
        }
      } catch (err) {
        console.error('[XQ-Saver] Save error:', err);
      }
    }

    if (newPosts.length > 0) {
      console.log(`[XQ-Saver] Saved ${newCount} new posts`);
      // 同步到 background service worker（extension origin IndexedDB）
      try {
        chrome.runtime.sendMessage({ action: 'savePosts', posts: newPosts });
      } catch (e) {
        // service worker 可能未就绪，忽略
      }
    }

    // 始终更新统计面板（含当前页面帖子数等）
    updateStats(db);

    return { newCount, totalOnPage };
  }

  // ===== 同步到 background =====

  async function syncToBackground() {
    try {
      const posts = await getAllPosts(db);
      if (posts.length === 0) return;
      // 分批发送，每批 100 条
      for (let i = 0; i < posts.length; i += 100) {
        const batch = posts.slice(i, i + 100);
        chrome.runtime.sendMessage({ action: 'savePosts', posts: batch });
      }
      console.log(`[XQ-Saver] Synced ${posts.length} posts to background`);
    } catch (e) {
      console.warn('[XQ-Saver] Background sync failed:', e.message);
    }
  }

  // ===== 初始化 =====

  async function init() {
    console.log('[XQ-Saver] Initializing on', location.href);

    try {
      db = await openDB();
      savedIds = await getAllSavedIds(db);
      console.log(`[XQ-Saver] Loaded ${savedIds.size} saved post IDs`);

      // 同步已有数据到 background（extension origin）
      syncToBackground();

      // 创建统计面板
      createStatsPanel();
      updateStats(db);
      updateFullyScanedBadge();

      // 首次扫描
      await scanAndSave();

      // MutationObserver 监听新帖子
      const observer = new MutationObserver((mutations) => {
        let hasNewArticles = false;
        for (const mutation of mutations) {
          for (const node of mutation.addedNodes) {
            if (node.nodeType === 1) {
              if (node.matches && node.matches('article.timeline__item')) {
                hasNewArticles = true;
              } else if (node.querySelector && node.querySelector('article.timeline__item')) {
                hasNewArticles = true;
              }
            }
          }
        }
        if (hasNewArticles) {
          scheduleScan();
        }
      });

      observer.observe(document.body, {
        childList: true,
        subtree: true
      });

      console.log('[XQ-Saver] MutationObserver active');
    } catch (err) {
      console.error('[XQ-Saver] Init error:', err);
    }
  }

  // 等待 DOM 就绪后启动
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
