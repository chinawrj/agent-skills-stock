#!/usr/bin/env python3
"""
WebProxyHub — 多站点浏览器内代理请求框架

通过 CDP 连接反检测浏览器，在目标网站页面内注入 JS 代理脚本，
利用页面自身的会话/Cookie/指纹发送请求，绕过反爬检测。

架构:
    ┌──────────────────────────────────────────┐
    │  Python: WebProxyHub                      │
    │  hub.fetch('eastmoney', '/api/...')       │
    └──────────┬───────────────────────────────┘
               │ CDP (page.evaluate)
    ┌──────────▼───────────────────────────────┐
    │  浏览器页面 (已登录、真实指纹)              │
    │  window.__webProxyHub.fetch(url, opts)    │
    │  → 页面内 fetch() (携带 Cookie/Session)   │
    └──────────┬───────────────────────────────┘
               │ 正常 HTTP 请求
    ┌──────────▼───────────────────────────────┐
    │  目标 API (看到正常用户请求)               │
    └──────────────────────────────────────────┘

前置条件:
    1. pip install patchright
    2. 启动反检测浏览器:
       .github/skills/anti-detect-browser/scripts/start_browser.sh
    3. 目标网站需在浏览器中手动登录一次

用法:
    # 作为模块导入
    from scripts.web_proxy_hub import WebProxyHub

    hub = WebProxyHub()
    hub.connect()

    # 通过东方财富页面发请求
    data = hub.fetch('eastmoney', 'https://datacenter-web.eastmoney.com/api/data/v1/get',
                     params={'reportName': 'RPT_DMSK_TS_STOCKNEW', 'pageSize': '50'})

    # 通过集思录页面发请求
    data = hub.fetch('jisilu', 'https://www.jisilu.cn/data/cbnew/cb_list_new/',
                     method='POST', body='rp=50&page=1')

    # 通过同花顺页面发请求
    data = hub.fetch('10jqka', 'https://basic.10jqka.com.cn/api/stockph/...')

    hub.close()

    # 也可用 context manager
    with WebProxyHub() as hub:
        data = hub.fetch('eastmoney', url)

    # 命令行测试
    python scripts/web_proxy_hub.py                    # 测试所有站点连通性
    python scripts/web_proxy_hub.py --site eastmoney   # 测试单个站点
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════
#  站点配置
# ═══════════════════════════════════════════════════════

@dataclass
class SiteConfig:
    """单个站点的配置."""
    name: str                          # 站点标识符
    display_name: str                  # 显示名称
    url: str                           # 首页/入口 URL
    url_match: str                     # 页面匹配模式 (用于查找已打开的标签)
    test_url: str = ''                 # 连通性测试 URL
    test_validator: str = ''           # 测试结果验证 JS 表达式 (返回 bool)
    rate_limit: float = 0.5            # 最小请求间隔 (秒)
    extra_headers: dict = field(default_factory=dict)
    needs_login: bool = False          # 是否需要登录


# 预置站点配置
SITES: dict[str, SiteConfig] = {
    'eastmoney': SiteConfig(
        name='eastmoney',
        display_name='东方财富',
        url='https://data.eastmoney.com/xg/xg/',
        url_match='eastmoney.com',
        test_url='https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1&fs=m:0+t:6&fields=f12,f14',
        test_validator='d.data && d.data.total > 0',
        rate_limit=0.3,
    ),
    'jisilu': SiteConfig(
        name='jisilu',
        display_name='集思录',
        url='https://www.jisilu.cn/data/cbnew/#cb',
        url_match='jisilu.cn',
        test_url='__POST__https://www.jisilu.cn/data/cbnew/cb_list_new/',
        test_validator='d.rows && d.rows.length > 0',
        rate_limit=1.0,
        needs_login=True,
    ),
    '10jqka': SiteConfig(
        name='10jqka',
        display_name='同花顺',
        url='https://www.10jqka.com.cn/',
        url_match='10jqka.com.cn',
        test_url='https://stockpage.10jqka.com.cn/HQ_v4.html#refCountry=CN&498',
        test_validator='true',  # 同花顺只测页面可达
        rate_limit=0.5,
    ),
}


# ═══════════════════════════════════════════════════════
#  注入到页面的 JS 代理脚本
# ═══════════════════════════════════════════════════════

PROXY_JS = r"""
(() => {
    if (window.__webProxyHub) return;

    window.__webProxyHub = {
        /**
         * 通用 fetch 代理 — 利用页面自身会话发送请求
         * @param {string} url - 完整 URL
         * @param {object} opts - { method, headers, body, timeout }
         * @returns {Promise<{ok, status, statusText, headers, body, elapsed}>}
         */
        fetch: async (url, opts = {}) => {
            const method = (opts.method || 'GET').toUpperCase();
            const headers = opts.headers || {};
            const timeout = opts.timeout || 30000;
            const start = Date.now();

            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), timeout);

            try {
                const fetchOpts = {
                    method,
                    headers,
                    credentials: 'include',
                    signal: controller.signal,
                };
                if (method !== 'GET' && method !== 'HEAD' && opts.body) {
                    fetchOpts.body = opts.body;
                    if (!headers['Content-Type']) {
                        headers['Content-Type'] = 'application/x-www-form-urlencoded';
                    }
                }

                const resp = await fetch(url, fetchOpts);
                const body = await resp.text();
                const elapsed = Date.now() - start;

                // 提取关键响应头
                const respHeaders = {};
                for (const [k, v] of resp.headers.entries()) {
                    respHeaders[k] = v;
                }

                return {
                    ok: resp.ok,
                    status: resp.status,
                    statusText: resp.statusText,
                    headers: respHeaders,
                    body: body,
                    elapsed: elapsed,
                };
            } catch (e) {
                return {
                    ok: false,
                    status: 0,
                    statusText: e.name === 'AbortError' ? 'Timeout' : e.message,
                    headers: {},
                    body: '',
                    elapsed: Date.now() - start,
                };
            } finally {
                clearTimeout(timer);
            }
        },

        /** 辅助: JSON GET */
        getJSON: async (url, opts = {}) => {
            const result = await window.__webProxyHub.fetch(url, { ...opts, method: 'GET' });
            if (result.ok) {
                try { result.data = JSON.parse(result.body); } catch(e) { result.data = null; }
            }
            return result;
        },

        /** 辅助: JSON POST */
        postJSON: async (url, body, opts = {}) => {
            const result = await window.__webProxyHub.fetch(url, { ...opts, method: 'POST', body });
            if (result.ok) {
                try { result.data = JSON.parse(result.body); } catch(e) { result.data = null; }
            }
            return result;
        },

        /** 标识 */
        version: '1.0.0',
        injectedAt: new Date().toISOString(),
    };

    console.log('[WebProxyHub] Proxy injected at', window.__webProxyHub.injectedAt);
})();
"""


# ═══════════════════════════════════════════════════════
#  WebProxyHub 主类
# ═══════════════════════════════════════════════════════

class WebProxyHub:
    """多站点浏览器内代理请求中心."""

    def __init__(self, port: int = 9222, sites: dict[str, SiteConfig] | None = None):
        self.port = port
        self.sites = dict(sites or SITES)
        self._pw = None
        self._ctx = None
        self._pages: dict[str, Any] = {}       # site_name -> Page
        self._injected: set[str] = set()        # 已注入代理的站点
        self._last_request: dict[str, float] = {}  # rate limiting

    # ─── Lifecycle ───

    def connect(self, ctx=None):
        """连接 CDP 浏览器.

        Args:
            ctx: 可选，已有的 BrowserContext 对象。传入时不创建新连接。
        """
        if ctx is not None:
            # 复用已有连接 (不管理 playwright 生命周期)
            self._pw = None
            self._ctx = ctx
            logger.info(f'已复用连接, {len(self._ctx.pages)} 个标签页')
            return self

        try:
            from patchright.sync_api import sync_playwright
        except ImportError:
            raise RuntimeError('patchright 未安装: pip install patchright')

        self._pw = sync_playwright().start()
        try:
            browser = self._pw.chromium.connect_over_cdp(f'http://127.0.0.1:{self.port}')
        except Exception as e:
            self._pw.stop()
            self._pw = None
            raise ConnectionError(
                f'无法连接 CDP 端口 {self.port}: {e}\n'
                '请先启动浏览器: .github/skills/anti-detect-browser/scripts/start_browser.sh'
            ) from e

        self._ctx = browser.contexts[0]
        logger.info(f'已连接 CDP 端口 {self.port}, {len(self._ctx.pages)} 个标签页')
        return self

    def close(self):
        """断开连接 (不关闭浏览器)."""
        # Shutdown HTTP bridge if running
        if hasattr(self, '_bridge_server') and self._bridge_server:
            self._bridge_server.shutdown()
            self._bridge_server = None
        # Shutdown async proxy backend
        if hasattr(self, '_proxy_loop') and self._proxy_loop:
            import asyncio
            try:
                async def _cleanup():
                    pw = self._async_ctx.get('pw')
                    if pw:
                        await pw.stop()
                asyncio.run_coroutine_threadsafe(
                    _cleanup(), self._proxy_loop
                ).result(timeout=5)
            except Exception:
                pass
            self._proxy_loop.call_soon_threadsafe(self._proxy_loop.stop)
            self._proxy_loop = None
            self._async_ctx = {}
        self._pages.clear()
        self._injected.clear()
        if self._pw:
            self._pw.stop()
            self._pw = None
            self._ctx = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *exc):
        self.close()

    # ─── 站点管理 ───

    def register_site(self, config: SiteConfig):
        """注册新站点."""
        self.sites[config.name] = config

    def _find_page(self, site_name: str):
        """查找已打开的站点标签页."""
        config = self.sites[site_name]
        for pg in self._ctx.pages:
            if config.url_match in pg.url:
                return pg
        return None

    def _ensure_page(self, site_name: str):
        """确保站点标签页已打开，按需创建."""
        if site_name in self._pages:
            page = self._pages[site_name]
            # 检查页面是否仍有效
            try:
                page.evaluate('1')
                return page
            except Exception:
                del self._pages[site_name]
                self._injected.discard(site_name)

        page = self._find_page(site_name)
        if not page:
            config = self.sites[site_name]
            logger.info(f'打开 {config.display_name}: {config.url}')
            page = self._ctx.new_page()
            page.goto(config.url, wait_until='domcontentloaded', timeout=30000)
            time.sleep(2)

        self._pages[site_name] = page
        return page

    def _inject_proxy(self, site_name: str):
        """向页面注入代理 JS (幂等)."""
        if site_name in self._injected:
            # 验证注入是否仍有效
            page = self._pages[site_name]
            try:
                alive = page.evaluate('() => !!window.__webProxyHub')
                if alive:
                    return
            except Exception:
                pass
            self._injected.discard(site_name)

        page = self._ensure_page(site_name)
        page.evaluate(PROXY_JS)
        self._injected.add(site_name)
        logger.info(f'[{site_name}] 代理 JS 已注入')

    # ─── Rate Limiting ───

    def _rate_limit(self, site_name: str):
        """遵守站点请求频率限制."""
        config = self.sites[site_name]
        last = self._last_request.get(site_name, 0)
        elapsed = time.time() - last
        if elapsed < config.rate_limit:
            time.sleep(config.rate_limit - elapsed)
        self._last_request[site_name] = time.time()

    # ─── 核心 API ───

    def fetch(self, site_name: str, url: str, *,
              method: str = 'GET',
              params: dict | None = None,
              body: str | None = None,
              headers: dict | None = None,
              timeout: int = 30000,
              parse_json: bool = True) -> dict:
        """
        通过指定站点页面发送 HTTP 请求.

        Args:
            site_name: 站点标识符 (eastmoney/jisilu/10jqka/...)
            url: 完整请求 URL
            method: HTTP 方法
            params: URL 查询参数 (自动拼接)
            body: POST 请求体
            headers: 额外请求头
            timeout: 超时 (ms)
            parse_json: 是否自动解析 JSON 响应

        Returns:
            {ok, status, statusText, headers, body, data?, elapsed}
        """
        if site_name not in self.sites:
            raise ValueError(f'未注册站点: {site_name}，可用: {list(self.sites.keys())}')

        self._ensure_page(site_name)
        self._inject_proxy(site_name)
        self._rate_limit(site_name)

        # 拼接查询参数
        if params:
            sep = '&' if '?' in url else '?'
            url = url + sep + urlencode(params)

        # 合并 headers
        merged_headers = dict(self.sites[site_name].extra_headers)
        if headers:
            merged_headers.update(headers)

        page = self._pages[site_name]

        # 通过 CDP evaluate 在页面内执行 fetch
        opts = {
            'method': method.upper(),
            'headers': merged_headers,
            'timeout': timeout,
        }
        if body is not None:
            opts['body'] = body

        result = page.evaluate(
            '([url, opts]) => window.__webProxyHub.fetch(url, opts)',
            [url, opts]
        )

        # 自动解析 JSON
        if parse_json and result.get('ok') and result.get('body'):
            try:
                result['data'] = json.loads(result['body'])
            except (json.JSONDecodeError, TypeError):
                result['data'] = None

        return result

    def get(self, site_name: str, url: str, **kwargs) -> dict:
        """GET 请求快捷方法."""
        return self.fetch(site_name, url, method='GET', **kwargs)

    def post(self, site_name: str, url: str, body: str = '', **kwargs) -> dict:
        """POST 请求快捷方法."""
        return self.fetch(site_name, url, method='POST', body=body, **kwargs)

    # ─── 站点状态 ───

    def status(self) -> dict[str, dict]:
        """获取所有站点状态."""
        result = {}
        for name, config in self.sites.items():
            page = self._find_page(name) if self._ctx else None
            injected = name in self._injected
            result[name] = {
                'display_name': config.display_name,
                'page_open': page is not None,
                'page_url': page.url if page else None,
                'proxy_injected': injected,
                'needs_login': config.needs_login,
            }
        return result

    def test_site(self, site_name: str) -> dict:
        """测试单个站点连通性."""
        config = self.sites[site_name]
        if not config.test_url:
            return {'site': site_name, 'ok': True, 'msg': '无测试 URL，跳过'}

        try:
            self._ensure_page(site_name)
            self._inject_proxy(site_name)

            # 判断测试类型
            test_url = config.test_url
            method = 'GET'
            body = None
            if test_url.startswith('__POST__'):
                test_url = test_url[8:]
                method = 'POST'
                body = 'rp=1&page=1'

            result = self.fetch(site_name, test_url, method=method, body=body,
                                timeout=15000, parse_json=True)

            if not result.get('ok'):
                return {
                    'site': site_name,
                    'ok': False,
                    'msg': f'HTTP {result.get("status")} {result.get("statusText")}',
                    'elapsed': result.get('elapsed'),
                }

            # 验证响应内容
            if config.test_validator and result.get('data') is not None:
                page = self._pages[site_name]
                valid = page.evaluate(f'(d) => {{ return {config.test_validator}; }}',
                                      result['data'])
                if not valid:
                    return {
                        'site': site_name,
                        'ok': False,
                        'msg': '响应内容验证失败',
                        'elapsed': result.get('elapsed'),
                    }

            return {
                'site': site_name,
                'ok': True,
                'msg': f'OK ({result.get("elapsed")}ms)',
                'elapsed': result.get('elapsed'),
            }

        except Exception as e:
            return {'site': site_name, 'ok': False, 'msg': str(e)}

    def test_all(self) -> list[dict]:
        """测试所有站点连通性."""
        results = []
        for name in self.sites:
            results.append(self.test_site(name))
        return results

    # ─── 页面绑定 (HTTP bridge + async CDP proxy) ───

    def bind_to_page(self, page, bridge_port: int = 18234):
        """启动代理桥并将 __proxyHub 注入到指定页面.

        双通道架构 (解决 Patchright sync greenlet 线程安全
        + Chrome Private Network Access 限制):

        通道 1 — 页面 JS (page.route 拦截):
          页面 JS: fetch('/__webproxyhub__', {body: ...})
            → Patchright page.route() 拦截 (绕过浏览器安全限制)
              → asyncio.run_coroutine_threadsafe()
                → 独立 async CDP 连接 evaluate()
                  → 目标站点 API

        通道 2 — Python/外部调用 (HTTP 桥接):
          urllib.request('http://127.0.0.1:18234/proxy', ...)
            → HTTPServer handler (独立线程)
              → 同上 async proxy 路径

        页面内调用:
            const r = await window.__proxyHub.get('eastmoney', url, {pn:'1'});

        Args:
            page: Patchright Page 对象 (来自主线程 sync API)
            bridge_port: HTTP 桥接端口 (默认 18234, 供 Python 调用)
        """
        import threading
        import asyncio
        from http.server import HTTPServer, BaseHTTPRequestHandler

        hub = self
        _proxy_loop = asyncio.new_event_loop()
        _proxy_ready = threading.Event()
        _async_ctx = {}  # 异步 proxy 共享状态

        # ── 1. 异步 proxy 后端 (独立线程 + 独立 CDP 连接) ──

        async def _setup_async_proxy():
            """在独立线程中创建 async patchright CDP 连接."""
            from patchright.async_api import async_playwright
            pw = await async_playwright().start()
            browser = await pw.chromium.connect_over_cdp(
                f'http://127.0.0.1:{hub.port}')
            ctx = browser.contexts[0]
            _async_ctx['pw'] = pw
            _async_ctx['ctx'] = ctx
            _async_ctx['pages'] = {}
            _async_ctx['injected'] = set()

        async def _async_fetch(site_name, url, method='GET',
                               params=None, body=None, headers=None,
                               timeout=30000):
            """异步版 fetch: 在 proxy 线程的事件循环中运行."""
            config = hub.sites[site_name]
            ctx = _async_ctx['ctx']

            # 查找/创建目标站点页面
            pg = _async_ctx['pages'].get(site_name)
            if pg:
                try:
                    await pg.evaluate('1')
                except Exception:
                    pg = None
                    _async_ctx['injected'].discard(site_name)

            if not pg:
                for p in ctx.pages:
                    if config.url_match in p.url:
                        pg = p
                        break
                if not pg:
                    logger.info(f'[proxy-async] 打开 {config.display_name}')
                    pg = await ctx.new_page()
                    await pg.goto(config.url, wait_until='domcontentloaded',
                                  timeout=30000)
                    await asyncio.sleep(2)
                _async_ctx['pages'][site_name] = pg

            # 注入代理 JS
            if site_name not in _async_ctx['injected']:
                await pg.evaluate(PROXY_JS)
                _async_ctx['injected'].add(site_name)
                logger.info(f'[proxy-async] [{site_name}] 代理 JS 已注入')

            # 拼接 URL 参数
            if params:
                sep = '&' if '?' in url else '?'
                url = url + sep + urlencode(params)

            # 合并 headers
            merged = dict(config.extra_headers)
            if headers:
                merged.update(headers)

            opts = {'method': method.upper(), 'headers': merged,
                    'timeout': timeout}
            if body is not None:
                opts['body'] = body

            result = await pg.evaluate(
                '([url, opts]) => window.__webProxyHub.fetch(url, opts)',
                [url, opts])

            # 解析 JSON
            if result.get('ok') and result.get('body'):
                try:
                    result['data'] = json.loads(result['body'])
                except (json.JSONDecodeError, TypeError):
                    result['data'] = None
            return result

        async def _cleanup_async():
            pw = _async_ctx.get('pw')
            if pw:
                await pw.stop()

        def _proxy_thread():
            asyncio.set_event_loop(_proxy_loop)
            _proxy_loop.run_until_complete(_setup_async_proxy())
            _proxy_ready.set()
            _proxy_loop.run_forever()

        proxy_t = threading.Thread(target=_proxy_thread, daemon=True,
                                   name='proxy-async')
        proxy_t.start()
        _proxy_ready.wait(timeout=15)
        if not _proxy_ready.is_set():
            raise RuntimeError('Proxy async 后端启动超时')
        logger.info('Proxy async 后端已就绪 (独立 CDP 连接)')

        # ── 2. HTTP 桥接服务 (供 Python urllib 等外部调用) ──

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                logger.debug(f'[bridge] {fmt % args}')

            def _cors(self):
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
                self.send_header('Access-Control-Allow-Headers', 'Content-Type')
                self.send_header('Access-Control-Allow-Private-Network', 'true')

            def do_OPTIONS(self):
                self.send_response(204)
                self._cors()
                self.end_headers()

            def do_POST(self):
                length = int(self.headers.get('Content-Length', 0))
                raw = self.rfile.read(length)
                try:
                    req = json.loads(raw)
                    future = asyncio.run_coroutine_threadsafe(
                        _async_fetch(
                            req['site'], req['url'],
                            method=req.get('method', 'GET'),
                            params=req.get('params'),
                            body=req.get('body'),
                            headers=req.get('headers'),
                            timeout=req.get('timeout', 30000),
                        ),
                        _proxy_loop,
                    )
                    result = future.result(timeout=35)
                    resp = json.dumps(
                        result, ensure_ascii=False).encode('utf-8')
                    self.send_response(200)
                    self.send_header(
                        'Content-Type', 'application/json; charset=utf-8')
                    self._cors()
                    self.end_headers()
                    self.wfile.write(resp)
                except Exception as e:
                    logger.error(f'[bridge] proxy 请求失败: {e}')
                    err = json.dumps({
                        'ok': False, 'status': 0,
                        'statusText': str(e), 'body': '', 'data': None,
                    }, ensure_ascii=False).encode('utf-8')
                    self.send_response(200)
                    self.send_header(
                        'Content-Type', 'application/json; charset=utf-8')
                    self._cors()
                    self.end_headers()
                    self.wfile.write(err)

        server = HTTPServer(('127.0.0.1', bridge_port), _Handler)
        http_t = threading.Thread(target=server.serve_forever, daemon=True,
                                  name='bridge-http')
        http_t.start()
        self._bridge_server = server
        self._bridge_port = bridge_port
        self._proxy_loop = _proxy_loop
        self._async_ctx = _async_ctx
        logger.info(f'HTTP 代理桥已启动: http://127.0.0.1:{bridge_port}')

        # ── 3. page.route() 拦截 (页面 JS → async proxy) ──
        # Chrome Private Network Access 阻止 HTTPS 页面 fetch 到 localhost,
        # 改用 page.route() 拦截同源虚拟 URL, 完全绕过浏览器安全限制.

        def _route_handler(route):
            try:
                body = json.loads(route.request.post_data or '{}')
                future = asyncio.run_coroutine_threadsafe(
                    _async_fetch(
                        body['site'], body['url'],
                        method=body.get('method', 'GET'),
                        params=body.get('params'),
                        body=body.get('body'),
                        headers=body.get('headers'),
                        timeout=body.get('timeout', 30000),
                    ),
                    _proxy_loop,
                )
                result = future.result(timeout=35)
                route.fulfill(
                    status=200,
                    content_type='application/json; charset=utf-8',
                    body=json.dumps(result, ensure_ascii=False),
                )
            except Exception as e:
                logger.error(f'[route] proxy 请求失败: {e}')
                route.fulfill(
                    status=200,
                    content_type='application/json; charset=utf-8',
                    body=json.dumps({
                        'ok': False, 'status': 0,
                        'statusText': str(e), 'body': '', 'data': None,
                    }, ensure_ascii=False),
                )

        page.route('**/__webproxyhub__', _route_handler)
        logger.info('page.route() 拦截已注册: **/__webproxyhub__')

        # ── 4. 注入 JS helper ──

        sites_json = json.dumps(list(self.sites.keys()))
        page.evaluate("""(sitesJSON) => {
            if (window.__proxyHub) return;
            const sites = JSON.parse(sitesJSON);
            window.__proxyHub = {
                sites,
                async fetch(site, url, opts) {
                    const resp = await fetch('/__webproxyhub__', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ site, url, ...(opts || {}) }),
                    });
                    return resp.json();
                },
                async get(site, url, params) {
                    return this.fetch(site, url, { method: 'GET', params });
                },
                async post(site, url, body) {
                    return this.fetch(site, url, { method: 'POST', body });
                },
            };
            console.log('[WebProxyHub] page.route() bridge bound, sites:', sites);
        }""", sites_json)


# ═══════════════════════════════════════════════════════
#  CLI 入口
# ═══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='WebProxyHub — 多站点浏览器内代理请求框架')
    parser.add_argument('--port', type=int, default=9222,
                        help='CDP 端口 (default: 9222)')
    parser.add_argument('--site', type=str, default=None,
                        help='测试指定站点 (eastmoney/jisilu/10jqka)')
    parser.add_argument('--status', action='store_true',
                        help='显示所有站点状态')
    parser.add_argument('--fetch', type=str, default=None,
                        help='发送测试请求: site:url (如 eastmoney:https://...)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='详细输出')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(message)s'
    )

    with WebProxyHub(port=args.port) as hub:
        if args.status:
            print('📊 站点状态:')
            for name, info in hub.status().items():
                icon = '✅' if info['page_open'] else '❌'
                login = ' 🔐' if info['needs_login'] else ''
                print(f'  {icon} {info["display_name"]}{login}: '
                      f'{"已打开" if info["page_open"] else "未打开"}'
                      f'{" | " + info["page_url"][:60] if info["page_url"] else ""}')
            return

        if args.fetch:
            if ':' not in args.fetch:
                print('格式: --fetch site:url')
                return
            site, url = args.fetch.split(':', 1)
            print(f'🔗 {site} → {url[:80]}...')
            result = hub.fetch(site, url)
            print(f'  状态: {result["status"]} {result["statusText"]}')
            print(f'  耗时: {result.get("elapsed")}ms')
            if result.get('data'):
                print(f'  数据: {json.dumps(result["data"], ensure_ascii=False)[:200]}...')
            elif result.get('body'):
                print(f'  内容: {result["body"][:200]}...')
            return

        # 默认: 测试连通性
        if args.site:
            sites = [args.site]
        else:
            sites = list(hub.sites.keys())

        print('🔌 WebProxyHub 连通性测试\n')
        for site_name in sites:
            config = hub.sites.get(site_name)
            if not config:
                print(f'  ❌ 未知站点: {site_name}')
                continue
            print(f'  测试 {config.display_name}...')
            result = hub.test_site(site_name)
            icon = '✅' if result['ok'] else '❌'
            print(f'    {icon} {result["msg"]}')
        print()


if __name__ == '__main__':
    main()
