#!/usr/bin/env python3
"""
完整筛选流程 - 共享浏览器运行所有脚本

运行顺序:
1. screen_shareholders.py - 筛选股东人数持续减少的股票
2. fetch_latest.py - 获取最新公告股东人数的公司
3. query_shareholders.py - 查询指定股票的详细股东历史

所有脚本复用同一个浏览器实例。
"""

import asyncio
import sys
import os
import subprocess

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_manager import get_browser_page, close_browser


async def run_screening():
    """运行完整筛选流程"""
    # 脚本路径
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    screen_script = os.path.join(base_dir, '.github/skills/baostock-guide/scripts/screen_shareholders.py')
    fetch_script = os.path.join(base_dir, '.github/skills/shareholders-latest/fetch_latest.py')
    query_script = os.path.join(base_dir, '.github/skills/baostock-guide/scripts/query_shareholders.py')
    
    try:
        # 先初始化浏览器
        print("=" * 60)
        print("正在初始化浏览器...")
        print("=" * 60)
        await get_browser_page()
        print("浏览器已启动\n")
        
        # 1. 运行筛选脚本
        print("=" * 60)
        print("步骤 1/3: 筛选股东人数持续减少的股票")
        print("=" * 60)
        result = subprocess.run([
            sys.executable, screen_script, 
            '--no-close'
        ], cwd=base_dir)
        if result.returncode != 0:
            print(f"筛选脚本执行失败: {result.returncode}")
        
        print()
        
        # 2. 运行最新公告脚本
        print("=" * 60)
        print("步骤 2/3: 获取最新公告股东人数")
        print("=" * 60)
        result = subprocess.run([
            sys.executable, fetch_script,
            '-n', '30', '--decrease-only', '--no-close'
        ], cwd=base_dir)
        if result.returncode != 0:
            print(f"最新公告脚本执行失败: {result.returncode}")
        
        print()
        
        # 3. 查询具体股票（如果有推荐）
        print("=" * 60)
        print("步骤 3/3: 完成")
        print("=" * 60)
        print("\n如需查询具体股票详情，可运行:")
        print(f"  python {query_script} <股票代码>")
        
    finally:
        print("\n" + "=" * 60)
        print("关闭浏览器...")
        print("=" * 60)
        await close_browser()
        print("完成!")


def main():
    """主入口"""
    print("""
╔══════════════════════════════════════════════════════════╗
║            A股庄股筛选完整流程                              ║
║         (共享浏览器，避免重复打开)                          ║
╚══════════════════════════════════════════════════════════╝
""")
    asyncio.run(run_screening())


if __name__ == '__main__':
    main()
