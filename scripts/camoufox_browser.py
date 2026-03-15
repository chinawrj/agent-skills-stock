#!/usr/bin/env python3
"""
Camoufox 持久化浏览器启动脚本
- 使用非持久化上下文 + 手动保存/恢复 cookies（解决 macOS 键盘输入问题）
- 引擎级指纹伪造，绕过高级反爬检测

用法:
  python scripts/camoufox_browser.py              # 默认启动
  python scripts/camoufox_browser.py --url https://www.temu.com/  # 启动并访问指定URL
"""

import argparse
import json
import signal
import sys
import os

# cookies 持久化文件
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DATA_DIR = os.path.abspath(DATA_DIR)
COOKIES_FILE = os.path.join(DATA_DIR, 'camoufox-cookies.json')


def load_cookies(context):
    """从文件恢复 cookies"""
    if os.path.exists(COOKIES_FILE):
        with open(COOKIES_FILE, 'r') as f:
            cookies = json.load(f)
        if cookies:
            context.add_cookies(cookies)
            print(f"🍪 已恢复 {len(cookies)} 条 cookies")


def save_cookies(context):
    """保存 cookies 到文件"""
    os.makedirs(DATA_DIR, exist_ok=True)
    cookies = context.cookies()
    with open(COOKIES_FILE, 'w') as f:
        json.dump(cookies, f, indent=2)
    print(f"💾 已保存 {len(cookies)} 条 cookies")


def main():
    parser = argparse.ArgumentParser(description='启动 Camoufox 持久化浏览器')
    parser.add_argument('--url', default='about:blank', help='启动后访问的URL')
    parser.add_argument('--headless', action='store_true', help='无头模式')
    args = parser.parse_args()

    print(f"🌐 目标 URL: {args.url}")
    print("🚀 正在启动 Camoufox 浏览器...")

    from camoufox.sync_api import Camoufox

    with Camoufox(
        headless=args.headless,
        os="macos",
    ) as browser:
        context = browser.new_context()
        load_cookies(context)
        page = context.new_page()

        if args.url != 'about:blank':
            print(f"📡 正在导航到 {args.url} ...")
            page.goto(args.url, wait_until='domcontentloaded', timeout=30000)
            page.wait_for_timeout(3000)
            print(f"✅ 页面标题: {page.title()}")

        print("\n🟢 浏览器已启动！键盘输入正常，cookies 会在关闭时自动保存。")
        print("   按 Ctrl+C 关闭浏览器。\n")

        def shutdown():
            try:
                save_cookies(context)
            except Exception as e:
                print(f"⚠️ 保存 cookies 失败: {e}")
            print("👋 浏览器已关闭。")

        def handle_signal(sig, frame):
            print("\n🛑 正在关闭浏览器...")
            shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        try:
            while True:
                page.wait_for_timeout(1000)
        except (KeyboardInterrupt, SystemExit):
            shutdown()


if __name__ == '__main__':
    main()
