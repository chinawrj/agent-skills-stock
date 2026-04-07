#!/usr/bin/env python3
"""
反检测浏览器启动脚本 (Patchright)

使用 Patchright launch_persistent_context 启动真实 Chrome，
绕过所有反爬检测，保持浏览器后台运行。

用法:
  python launch_browser.py                              # 默认启动
  python launch_browser.py --url https://www.temu.com/  # 启动并访问URL
  python launch_browser.py --profile ~/.patchright-userdata  # 指定profile目录
  python launch_browser.py --port 9222                  # 指定CDP端口
"""

import argparse
import os
import signal
import sys
import time

DEFAULT_PROFILE = os.path.expanduser("~/.patchright-userdata")
DEFAULT_CDP_PORT = 9222


def main():
    parser = argparse.ArgumentParser(description='启动 Patchright 反检测浏览器')
    parser.add_argument('--url', default=None, help='启动后访问的URL')
    parser.add_argument('--profile', default=DEFAULT_PROFILE, help='Chrome用户数据目录路径')
    parser.add_argument('--port', type=int, default=DEFAULT_CDP_PORT, help='CDP调试端口')
    args = parser.parse_args()

    os.makedirs(args.profile, exist_ok=True)

    try:
        from patchright.sync_api import sync_playwright
    except ImportError:
        print('❌ patchright 未安装: pip install patchright', file=sys.stderr)
        sys.exit(1)

    print(f"📁 Profile 目录: {args.profile}")
    print(f"🔌 CDP 端口: {args.port}")
    print("🚀 正在启动 Patchright 浏览器...")

    pw = sync_playwright().start()
    context = pw.chromium.launch_persistent_context(
        user_data_dir=args.profile,
        channel="chrome",
        headless=False,
        no_viewport=True,
        ignore_default_args=["--no-sandbox"],
        args=[
            f"--remote-debugging-port={args.port}",
            "--remote-debugging-host=127.0.0.1",
            "--remote-allow-origins=*",
        ],
    )

    page = context.pages[0] if context.pages else context.new_page()

    if args.url:
        print(f"📡 正在导航到 {args.url} ...")
        page.goto(args.url, wait_until='domcontentloaded', timeout=30000)
        time.sleep(2)
        title = page.title()
        print(f"✅ 页面标题: {title}")

    print()
    print("🟢 浏览器已启动！保持后台运行中...")
    print(f"   Patchright CDP: http://127.0.0.1:{args.port}")
    print("   按 Ctrl+C 停止")

    # 保持浏览器后台运行
    signal.signal(signal.SIGINT, lambda *_: None)
    signal.signal(signal.SIGTERM, lambda *_: None)
    try:
        while True:
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        print("👋 正在关闭浏览器...")
        context.close()
        pw.stop()


if __name__ == '__main__':
    main()
