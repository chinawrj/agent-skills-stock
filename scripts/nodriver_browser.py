#!/usr/bin/env python3
"""
Nodriver 反检测浏览器启动脚本
- 不走 CDP/WebDriver 协议，极难被检测
- 使用持久化 user-data-dir 保存 cookies、登录状态
- 键盘输入完全正常

用法:
  python scripts/nodriver_browser.py                              # 默认启动
  python scripts/nodriver_browser.py --url https://www.temu.com/  # 启动并访问指定URL
"""

import argparse
import asyncio
import os
import sys

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'nodriver-profile')
DATA_DIR = os.path.abspath(DATA_DIR)


async def main():
    parser = argparse.ArgumentParser(description='启动 Nodriver 反检测浏览器')
    parser.add_argument('--url', default=None, help='启动后访问的URL')
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"📁 Profile 目录: {DATA_DIR}")
    print("🚀 正在启动 Nodriver 浏览器...")

    import nodriver as uc

    browser = await uc.start(
        user_data_dir=DATA_DIR,
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

    print("\n🟢 浏览器已启动！键盘输入正常，数据自动保存到 profile 目录。")
    print("   关闭浏览器窗口即可退出。\n")

    # 保持运行直到浏览器关闭
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("👋 浏览器已关闭。")


if __name__ == '__main__':
    asyncio.run(main())
