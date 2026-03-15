#!/usr/bin/env python3
"""
反检测浏览器启动脚本 (Anti-Detect Browser Launcher)

使用 nodriver 启动真实 Chrome，绕过所有反爬检测。
启动后自动开启 CDP 调试端口，供 Playwright 连接操控。

用法:
  python launch_browser.py                              # 默认启动
  python launch_browser.py --url https://www.temu.com/  # 启动并访问URL
  python launch_browser.py --profile /path/to/profile   # 指定profile目录
"""

import argparse
import asyncio
import os
import sys

DEFAULT_PROFILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'data', 'nodriver-profile')
)


async def main():
    parser = argparse.ArgumentParser(description='启动反检测浏览器')
    parser.add_argument('--url', default=None, help='启动后访问的URL')
    parser.add_argument('--profile', default=DEFAULT_PROFILE, help='Chrome用户数据目录路径')
    args = parser.parse_args()

    os.makedirs(args.profile, exist_ok=True)
    print(f"📁 Profile 目录: {args.profile}")
    print("🚀 正在启动 nodriver 浏览器...")

    import nodriver as uc

    browser = await uc.start(
        user_data_dir=args.profile,
        browser_args=[
            '--no-first-run',
            '--no-default-browser-check',
        ],
    )

    if args.url:
        print(f"📡 正在导航到 {args.url} ...")
        page = await browser.get(args.url)
        await page.sleep(3)
        print(f"✅ 页面标题: {await page.evaluate('document.title')}")
    else:
        page = await browser.get('about:blank')

    print("\n🟢 浏览器已启动！")
    print("   - 键盘输入正常，数据自动保存到 profile 目录")
    print("   - 使用 Playwright connect_over_cdp() 连接操控")
    print("   - 关闭浏览器窗口即可退出\n")

    # 保持运行直到浏览器关闭
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("👋 浏览器已关闭。")


if __name__ == '__main__':
    asyncio.run(main())
