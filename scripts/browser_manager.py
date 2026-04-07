#!/usr/bin/env python3
"""
共享浏览器管理模块

提供全局浏览器实例，供所有股东人数相关脚本复用。
支持两种模式：
1. 连接到持久化浏览器服务（推荐，跨进程复用）
2. 本地启动浏览器（单进程内复用）

使用方法：
    # 先启动浏览器服务（在后台运行）
    python browser_manager.py start
    
    # 然后脚本会自动连接到服务
    python screen_shareholders.py
    
    # 不再需要时关闭服务
    python browser_manager.py stop
"""

import asyncio
import sys
import os
import signal
import socket

try:
    from patchright.async_api import async_playwright, Browser, Page, Playwright, BrowserContext
except ImportError:
    print("请先安装 patchright: pip install patchright", file=sys.stderr)
    sys.exit(1)

USER_DATA_DIR = os.path.expanduser("~/.patchright-userdata")


# 配置
BROWSER_WS_PORT = 9222
BROWSER_WS_ENDPOINT_FILE = os.path.expanduser("~/.playwright_ws_endpoint")

# 全局浏览器实例（本地模式）
_playwright: Playwright = None
_browser: Browser = None
_context: BrowserContext = None
_page: Page = None
_initialized: bool = False
_connected_to_server: bool = False


def _is_port_in_use(port: int) -> bool:
    """检查端口是否被占用"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def _read_ws_endpoint() -> str:
    """读取保存的 WebSocket endpoint"""
    if os.path.exists(BROWSER_WS_ENDPOINT_FILE):
        with open(BROWSER_WS_ENDPOINT_FILE, 'r') as f:
            return f.read().strip()
    return None


def _write_ws_endpoint(endpoint: str):
    """保存 WebSocket endpoint"""
    with open(BROWSER_WS_ENDPOINT_FILE, 'w') as f:
        f.write(endpoint)


def _remove_ws_endpoint():
    """删除 WebSocket endpoint 文件"""
    if os.path.exists(BROWSER_WS_ENDPOINT_FILE):
        os.remove(BROWSER_WS_ENDPOINT_FILE)


def _auto_start_browser_server():
    """
    自动在后台启动浏览器服务（独立进程）
    
    Returns:
        bool: 是否成功启动
    """
    import subprocess
    import time
    
    if _is_port_in_use(BROWSER_WS_PORT):
        return True  # 已经在运行
    
    print("自动启动浏览器服务...", file=sys.stderr)
    
    # 获取当前脚本路径
    script_path = os.path.abspath(__file__)
    
    # 使用 subprocess 启动独立进程
    # 使用 nohup 和 setsid 确保进程完全独立
    try:
        # 构建命令 - 使用当前 Python 解释器
        python_exe = sys.executable
        
        # 启动后台进程
        subprocess.Popen(
            [python_exe, script_path, 'start'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,  # 创建新会话，脱离父进程
        )
        
        # 等待服务启动（最多10秒）
        for _ in range(20):
            time.sleep(0.5)
            if _is_port_in_use(BROWSER_WS_PORT) and _read_ws_endpoint():
                print("浏览器服务已自动启动", file=sys.stderr)
                return True
        
        print("浏览器服务启动超时", file=sys.stderr)
        return False
    except Exception as e:
        print(f"启动浏览器服务失败: {e}", file=sys.stderr)
        return False


async def get_browser_page(auto_start_server: bool = True) -> Page:
    """
    获取或创建浏览器页面
    
    优先连接到持久化浏览器服务，如果不存在则自动启动服务。
    
    Args:
        auto_start_server: 是否在没有服务时自动启动（默认True）
    
    Returns:
        Page: Playwright 页面对象
    """
    global _playwright, _browser, _context, _page, _initialized, _connected_to_server
    
    if _initialized and _page is not None:
        # 检查页面是否仍然有效
        try:
            await _page.evaluate("1")
            return _page
        except:
            # 页面已失效，重新获取
            _initialized = False
            _page = None
            _context = None
    
    # 如果没有服务在运行，自动启动
    if auto_start_server and not _is_port_in_use(BROWSER_WS_PORT):
        _auto_start_browser_server()
    
    # 尝试连接到持久化浏览器服务
    ws_endpoint = _read_ws_endpoint()
    if ws_endpoint and _is_port_in_use(BROWSER_WS_PORT):
        try:
            if _playwright is None:
                _playwright = await async_playwright().start()
            
            _browser = await _playwright.chromium.connect_over_cdp(ws_endpoint)
            
            # 获取已有的 context，或创建新的
            contexts = _browser.contexts
            if contexts:
                _context = contexts[0]
                pages = _context.pages
                if pages:
                    _page = pages[0]
                else:
                    _page = await _context.new_page()
            else:
                _context = await _browser.new_context()
                _page = await _context.new_page()
                await _page.goto("https://data.eastmoney.com/gdhs/", wait_until="domcontentloaded")
                await asyncio.sleep(1)
            
            _initialized = True
            _connected_to_server = True
            return _page
        except Exception as e:
            print(f"连接到浏览器服务失败: {e}，将本地启动浏览器", file=sys.stderr)
            _remove_ws_endpoint()
    
    # 本地启动浏览器（备选方案 — Patchright 反检测模式）
    if _playwright is None:
        _playwright = await async_playwright().start()
    
    os.makedirs(USER_DATA_DIR, exist_ok=True)
    _context = await _playwright.chromium.launch_persistent_context(
        user_data_dir=USER_DATA_DIR,
        channel="chrome",
        headless=False,
        no_viewport=True,
        ignore_default_args=["--no-sandbox"],
    )
    _page = _context.pages[0] if _context.pages else await _context.new_page()
    
    # 访问东方财富建立 session
    await _page.goto("https://data.eastmoney.com/gdhs/", wait_until="domcontentloaded")
    await asyncio.sleep(1)
    _initialized = True
    _connected_to_server = False
    
    return _page


async def close_browser():
    """关闭浏览器并清理资源（仅关闭本地启动的浏览器）"""
    global _playwright, _browser, _context, _page, _initialized, _connected_to_server
    
    # 如果是连接到服务的，不关闭浏览器，只断开连接
    if _connected_to_server:
        if _browser:
            try:
                await _browser.close()  # 这只是断开连接，不会关闭服务端浏览器
            except:
                pass
        _browser = None
        _context = None
        _page = None
        _initialized = False
        _connected_to_server = False
        return
    
    # 本地启动的浏览器，完全关闭
    if _browser:
        await _browser.close()
        _browser = None
        _context = None
        _page = None
    
    if _playwright:
        await _playwright.stop()
        _playwright = None
    
    _initialized = False


def is_browser_open() -> bool:
    """检查浏览器是否已打开"""
    return _initialized and _page is not None


def is_browser_server_running() -> bool:
    """检查浏览器服务是否在运行"""
    return _is_port_in_use(BROWSER_WS_PORT) and _read_ws_endpoint() is not None


# 同步包装器，方便非异步代码调用
def get_page_sync() -> Page:
    """同步获取页面（仅在已有事件循环时使用）"""
    return asyncio.get_event_loop().run_until_complete(get_browser_page())


def close_browser_sync():
    """同步关闭浏览器"""
    asyncio.get_event_loop().run_until_complete(close_browser())


# ============== 浏览器服务管理 ==============

async def start_browser_server():
    """
    启动持久化浏览器服务
    
    浏览器会在后台运行，其他脚本可以通过 WebSocket 连接复用。
    """
    global _playwright, _browser, _context, _page
    
    if _is_port_in_use(BROWSER_WS_PORT):
        print(f"浏览器服务已在运行 (端口 {BROWSER_WS_PORT})")
        ws_endpoint = _read_ws_endpoint()
        if ws_endpoint:
            print(f"WebSocket: {ws_endpoint}")
        return
    
    print("启动浏览器服务 (Patchright 反检测模式)...")
    
    _playwright = await async_playwright().start()
    
    os.makedirs(USER_DATA_DIR, exist_ok=True)
    _context = await _playwright.chromium.launch_persistent_context(
        user_data_dir=USER_DATA_DIR,
        channel="chrome",
        headless=False,
        no_viewport=True,
        ignore_default_args=["--no-sandbox"],
        args=[
            f'--remote-debugging-port={BROWSER_WS_PORT}',
            '--remote-debugging-host=127.0.0.1',
            '--remote-allow-origins=*',
        ],
    )
    
    ws_endpoint = f"http://localhost:{BROWSER_WS_PORT}"
    _write_ws_endpoint(ws_endpoint)
    
    _page = _context.pages[0] if _context.pages else await _context.new_page()
    
    # 访问东方财富建立 session
    await _page.goto("https://data.eastmoney.com/gdhs/", wait_until="domcontentloaded")
    
    print(f"浏览器服务已启动")
    print(f"  端口: {BROWSER_WS_PORT}")
    print(f"  WebSocket: {ws_endpoint}")
    print(f"  按 Ctrl+C 停止服务")
    
    # 保持运行
    try:
        while True:
            await asyncio.sleep(60)
            # 定期检查浏览器是否还活着
            try:
                if _page:
                    await _page.evaluate("1")
            except:
                print("浏览器已关闭，服务退出")
                break
    except asyncio.CancelledError:
        pass
    finally:
        await stop_browser_server()


async def stop_browser_server():
    """停止浏览器服务"""
    global _playwright, _browser, _context, _page
    
    print("正在停止浏览器服务...")
    
    if _context:
        try:
            await _context.close()
        except:
            pass
        _context = None
        _page = None
    
    _browser = None
    
    if _playwright:
        try:
            await _playwright.stop()
        except:
            pass
        _playwright = None
    
    _remove_ws_endpoint()
    print("浏览器服务已停止")


def main():
    """命令行入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description='浏览器服务管理')
    parser.add_argument('action', choices=['start', 'stop', 'status'],
                        help='start: 启动服务, stop: 停止服务, status: 查看状态')
    
    args = parser.parse_args()
    
    if args.action == 'start':
        def signal_handler(sig, frame):
            print("\n收到停止信号...")
            asyncio.get_event_loop().stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        asyncio.run(start_browser_server())
    
    elif args.action == 'stop':
        if is_browser_server_running():
            # 发送信号停止服务（通过删除 endpoint 文件触发）
            _remove_ws_endpoint()
            print("已发送停止信号，请手动关闭浏览器窗口")
        else:
            print("浏览器服务未运行")
    
    elif args.action == 'status':
        if is_browser_server_running():
            ws_endpoint = _read_ws_endpoint()
            print(f"浏览器服务: 运行中")
            print(f"  端口: {BROWSER_WS_PORT}")
            print(f"  WebSocket: {ws_endpoint}")
        else:
            print("浏览器服务: 未运行")


if __name__ == "__main__":
    main()
