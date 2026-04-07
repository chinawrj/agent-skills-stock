// ═══════════════════════════════════════════════════════════
// 小卡叔选债 — Background Service Worker
// 处理跨域 API 请求 (content script 无法直接 fetch 东方财富)
// ═══════════════════════════════════════════════════════════

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.action === 'fetchFinancial' && msg.url) {
    fetch(msg.url)
      .then(r => {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(data => sendResponse({ ok: true, data }))
      .catch(e => sendResponse({ ok: false, error: e.message }));
    return true; // keep channel open for async response
  }

  // Batch fetch annual report data from datacenter API
  if (msg.action === 'fetchAnnualFinancials' && msg.codes) {
    const codes = msg.codes; // ["300614.SZ", "002475.SZ", ...]
    const codesStr = codes.map(c => '"' + c + '"').join(',');
    const params = new URLSearchParams({
      type: 'RPT_F10_FINANCE_MAINFINADATA',
      sty: 'SECUCODE,REPORT_DATE,REPORT_DATE_NAME,PARENTNETPROFIT,TOTALOPERATEREVE',
      filter: '(SECUCODE in (' + codesStr + '))(REPORT_TYPE="年报")',
      pageSize: String(codes.length * 2), // at most 2 years per stock
      sortColumns: 'REPORT_DATE',
      sortTypes: '-1',
    });
    const url = 'https://datacenter.eastmoney.com/securities/api/data/get?' + params.toString();

    fetch(url, {
      headers: { 'Referer': 'https://emweb.securities.eastmoney.com/' },
    })
      .then(r => {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(data => {
        if (data.result && data.result.data) {
          // Deduplicate: take latest annual report per stock
          const latest = {};
          for (const row of data.result.data) {
            const sc = row.SECUCODE;
            if (!latest[sc]) latest[sc] = row; // sorted desc, first = latest
          }
          sendResponse({ ok: true, data: latest });
        } else {
          sendResponse({ ok: false, error: data.message || 'No data' });
        }
      })
      .catch(e => sendResponse({ ok: false, error: e.message }));
    return true;
  }
});
