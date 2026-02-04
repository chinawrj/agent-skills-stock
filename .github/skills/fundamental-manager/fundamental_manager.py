#!/usr/bin/env python3
"""
财务数据管理模块
提供财务数据的缓存读取、在线获取和数据库更新功能

智能缓存策略：
- 对调用者透明，自动判断是否需要更新
- 只在财报披露窗口期内且可能有新数据时尝试更新
- 同一股票检查更新间隔不短于24小时
"""

import os
import sys
import csv
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Tuple

import duckdb
import pandas as pd

# 数据库路径
DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'a-share.db')

# 表名
TABLE_NAME = 'fundamentals'
UPDATE_LOG_TABLE = 'fundamental_update_log'

# 检查间隔（小时）
CHECK_INTERVAL_HOURS = 24


def get_db_connection() -> duckdb.DuckDBPyConnection:
    """获取数据库连接"""
    db_path = os.path.abspath(DB_PATH)
    return duckdb.connect(db_path)


def ensure_table_exists():
    """确保表存在"""
    conn = get_db_connection()
    try:
        # 主数据表
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                code VARCHAR NOT NULL,
                name VARCHAR,
                report_date DATE NOT NULL,
                report_type VARCHAR,
                eps DECIMAL(10,4),
                bps DECIMAL(10,4),
                roe DECIMAL(10,4),
                net_profit DECIMAL(18,2),
                revenue DECIMAL(18,2),
                profit_yoy DECIMAL(10,4),
                revenue_yoy DECIMAL(10,4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (code, report_date)
            )
        """)
        # 更新日志表（记录每只股票的最后检查时间）
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {UPDATE_LOG_TABLE} (
                code VARCHAR PRIMARY KEY,
                last_check_time TIMESTAMP NOT NULL,
                last_report_date DATE
            )
        """)
    finally:
        conn.close()


# ============================================================
# 智能缓存策略
# ============================================================

def get_expected_latest_report(today: date = None) -> Tuple[date, str]:
    """
    根据当前日期，返回预期应该有的最新报告日期和类型
    
    财报披露时间表：
    - 年报(12-31)：次年1月1日 ~ 4月30日
    - 一季报(3-31)：4月1日 ~ 4月30日
    - 半年报(6-30)：7月1日 ~ 8月31日
    - 三季报(9-30)：10月1日 ~ 10月31日
    
    Returns:
        (report_date, report_type)
    """
    if today is None:
        today = date.today()
    
    year = today.year
    month = today.month
    day = today.day
    
    # 根据当前月份判断预期的最新报告
    if month >= 11:
        # 11-12月：三季报应该已出
        return date(year, 9, 30), '三季报'
    elif month == 10:
        # 10月：三季报披露期
        if day >= 31:
            return date(year, 9, 30), '三季报'
        else:
            return date(year, 6, 30), '半年报'
    elif month == 9:
        # 9月：半年报应该已出
        return date(year, 6, 30), '半年报'
    elif month in [7, 8]:
        # 7-8月：半年报披露期
        if month == 8 and day >= 31:
            return date(year, 6, 30), '半年报'
        else:
            return date(year, 3, 31), '一季报'
    elif month in [5, 6]:
        # 5-6月：一季报应该已出
        return date(year, 3, 31), '一季报'
    elif month == 4:
        # 4月：年报和一季报披露期
        if day >= 30:
            return date(year, 3, 31), '一季报'
        else:
            return date(year - 1, 12, 31), '年报'
    elif month in [1, 2, 3]:
        # 1-3月：上年年报披露期
        return date(year - 1, 12, 31), '年报'
    
    return date(year - 1, 12, 31), '年报'


def is_in_disclosure_window(today: date = None) -> bool:
    """
    判断当前是否在财报披露窗口期
    
    披露窗口期：1-4月, 7-8月, 10月
    """
    if today is None:
        today = date.today()
    
    return today.month in [1, 2, 3, 4, 7, 8, 10]


def get_last_check_time(code: str) -> Optional[datetime]:
    """获取某只股票的最后检查时间"""
    ensure_table_exists()
    conn = get_db_connection()
    try:
        result = conn.execute(
            f"SELECT last_check_time FROM {UPDATE_LOG_TABLE} WHERE code = ?", [code]
        ).fetchone()
        return result[0] if result else None
    finally:
        conn.close()


def update_check_time(code: str, latest_report_date: date = None):
    """更新某只股票的检查时间"""
    ensure_table_exists()
    conn = get_db_connection()
    try:
        conn.execute(f"""
            INSERT OR REPLACE INTO {UPDATE_LOG_TABLE} (code, last_check_time, last_report_date)
            VALUES (?, ?, ?)
        """, [code, datetime.now(), latest_report_date])
    finally:
        conn.close()


def get_cached_latest_report_date(code: str) -> Optional[date]:
    """获取缓存中某只股票的最新报告日期"""
    ensure_table_exists()
    conn = get_db_connection()
    try:
        result = conn.execute(
            f"SELECT MAX(report_date) FROM {TABLE_NAME} WHERE code = ?", [code]
        ).fetchone()
        return result[0] if result and result[0] else None
    finally:
        conn.close()


def should_check_update(code: str, silent: bool = True) -> Tuple[bool, str]:
    """
    判断是否应该检查更新
    
    条件：
    1. 在财报披露窗口期内
    2. 缓存中的最新报告日期早于预期
    3. 距离上次检查超过24小时
    
    Returns:
        (should_update, reason)
    """
    today = date.today()
    
    # 条件1：检查是否在披露窗口期
    if not is_in_disclosure_window(today):
        return False, "不在披露窗口期"
    
    # 条件2：检查缓存数据是否可能过期
    cached_latest = get_cached_latest_report_date(code)
    expected_latest, expected_type = get_expected_latest_report(today)
    
    if cached_latest and cached_latest >= expected_latest:
        return False, f"缓存已是最新({cached_latest})"
    
    # 条件3：检查更新间隔
    last_check = get_last_check_time(code)
    if last_check:
        hours_since_check = (datetime.now() - last_check).total_seconds() / 3600
        if hours_since_check < CHECK_INTERVAL_HOURS:
            return False, f"距上次检查仅{hours_since_check:.1f}小时"
    
    reason = f"可能有新{expected_type}(预期{expected_latest})"
    if cached_latest:
        reason += f"，缓存最新为{cached_latest}"
    else:
        reason += "，缓存为空"
    
    return True, reason


def get_cached_latest(code: str, limit: int = 10) -> pd.DataFrame:
    """
    从缓存获取单只股票最新的财务数据
    
    Args:
        code: 股票代码（如 "300401"）
        limit: 返回的记录数量
    
    Returns:
        DataFrame 包含最新的财务历史记录
    """
    ensure_table_exists()
    conn = get_db_connection()
    try:
        df = conn.execute(f"""
            SELECT code, name, report_date, report_type,
                   eps, bps, roe, net_profit, revenue, 
                   profit_yoy, revenue_yoy, created_at
            FROM {TABLE_NAME} 
            WHERE code = ?
            ORDER BY report_date DESC
            LIMIT ?
        """, [code, limit]).df()
        return df
    finally:
        conn.close()


def get_cached_all(code: str) -> pd.DataFrame:
    """
    从缓存获取单只股票所有历史财务数据
    
    Args:
        code: 股票代码
    
    Returns:
        DataFrame 包含所有历史记录
    """
    ensure_table_exists()
    conn = get_db_connection()
    try:
        df = conn.execute(f"""
            SELECT * FROM {TABLE_NAME} 
            WHERE code = ?
            ORDER BY report_date DESC
        """, [code]).df()
        return df
    finally:
        conn.close()


def get_cached_count(code: str) -> int:
    """获取缓存中某只股票的记录数"""
    ensure_table_exists()
    conn = get_db_connection()
    try:
        result = conn.execute(
            f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE code = ?", [code]
        ).fetchone()
        return result[0] if result else 0
    finally:
        conn.close()


def get_cached_codes() -> List[str]:
    """获取缓存中所有股票代码"""
    ensure_table_exists()
    conn = get_db_connection()
    try:
        result = conn.execute(f"SELECT DISTINCT code FROM {TABLE_NAME}").fetchall()
        return [row[0] for row in result]
    finally:
        conn.close()


async def fetch_online_data(codes: List[str], close: bool = False) -> List[Dict]:
    """
    从东方财富在线获取财务数据
    
    使用 stock-fundamental 的 fetch_fundamental 逻辑
    
    Args:
        codes: 股票代码列表
        close: 是否关闭浏览器
    
    Returns:
        财务数据列表
    """
    # 添加 fetch_fundamental 所在路径
    fetch_script = os.path.join(
        os.path.dirname(__file__), '..', 'stock-fundamental', 'scripts', 'fetch_fundamental.py'
    )
    
    if not os.path.exists(fetch_script):
        raise FileNotFoundError(f"找不到 fetch_fundamental.py: {fetch_script}")
    
    # 动态导入
    import importlib.util
    spec = importlib.util.spec_from_file_location("fetch_fundamental", fetch_script)
    fetch_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fetch_module)
    
    # 调用获取函数
    results = await fetch_module.fetch_fundamental_data_via_dom(codes, close=close)
    return results


def _safe_get(d: dict, key: str, default=None):
    """安全获取值，处理 NaN 和 None"""
    val = d.get(key, default)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    return val


def _parse_report_type(report_date: str) -> str:
    """从报告日期推断报告类型"""
    if not report_date:
        return None
    if report_date.endswith('12-31'):
        return '年报'
    elif report_date.endswith('06-30'):
        return '半年报'
    elif report_date.endswith('03-31'):
        return '一季报'
    elif report_date.endswith('09-30'):
        return '三季报'
    return '其他'


def save_to_cache(data_list: List[Dict], silent: bool = True) -> int:
    """
    将数据保存到缓存数据库
    
    Args:
        data_list: 从 fetch_online_data 获取的数据列表
        silent: 是否静默模式
    
    Returns:
        插入/更新的记录数
    """
    if not data_list:
        return 0
    
    ensure_table_exists()
    conn = get_db_connection()
    total_count = 0
    
    try:
        for data in data_list:
            code = data.get('code')
            name = data.get('name')
            reports = data.get('reports', [])
            
            for report in reports:
                report_date_str = report.get('report_date')
                if not report_date_str:
                    continue
                
                try:
                    # 解析日期
                    report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
                except:
                    continue
                
                report_type = _parse_report_type(report_date_str)
                
                record = (
                    code,
                    name,
                    report_date,
                    report_type,
                    _safe_get(report, 'eps'),
                    None,  # bps - 暂无数据
                    _safe_get(report, 'roe'),
                    _safe_get(report, 'net_profit'),
                    _safe_get(report, 'revenue'),
                    _safe_get(report, 'profit_yoy'),
                    _safe_get(report, 'revenue_yoy'),
                )
                
                conn.execute(f"""
                    INSERT OR REPLACE INTO {TABLE_NAME} 
                    (code, name, report_date, report_type, eps, bps, roe, 
                     net_profit, revenue, profit_yoy, revenue_yoy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, record)
                total_count += 1
            
            # 更新检查时间
            latest_report = get_cached_latest_report_date(code)
            update_check_time(code, latest_report)
        
        if not silent:
            print(f"已保存 {total_count} 条记录到缓存")
        
        return total_count
    finally:
        conn.close()


# ============================================================
# 智能获取函数（对调用者透明）
# ============================================================

async def get_smart(code: str, limit: int = 10, silent: bool = True) -> pd.DataFrame:
    """
    智能获取财务数据（对调用者透明的缓存策略）
    
    自动判断是否需要从远端更新：
    1. 在财报披露窗口期内
    2. 缓存数据可能过期
    3. 距上次检查超过24小时
    
    Args:
        code: 股票代码
        limit: 返回记录数
        silent: 是否静默模式
    
    Returns:
        DataFrame 财务数据
    """
    should_update, reason = should_check_update(code, silent=silent)
    
    if should_update:
        if not silent:
            print(f"[{code}] 检查更新: {reason}")
        
        try:
            # 尝试从远端获取
            online_data = await fetch_online_data([code], close=False)
            if online_data:
                save_to_cache(online_data, silent=True)
                if not silent:
                    print(f"[{code}] 已更新缓存")
            else:
                # 更新检查时间，避免频繁请求
                update_check_time(code, get_cached_latest_report_date(code))
                if not silent:
                    print(f"[{code}] 远端无新数据")
        except Exception as e:
            # 更新失败也记录检查时间
            update_check_time(code, get_cached_latest_report_date(code))
            if not silent:
                print(f"[{code}] 更新失败: {e}")
    
    # 返回缓存数据
    return get_cached_latest(code, limit)


def get_smart_sync(code: str, limit: int = 10, silent: bool = True) -> pd.DataFrame:
    """
    智能获取财务数据（同步版本）
    """
    import asyncio
    return asyncio.run(get_smart(code, limit, silent))


async def batch_get_smart(codes: List[str], silent: bool = True) -> Dict[str, pd.DataFrame]:
    """
    批量智能获取财务数据
    
    只更新需要更新的股票，其他直接从缓存读取
    
    Args:
        codes: 股票代码列表
        silent: 是否静默模式
    
    Returns:
        {code: DataFrame} 字典
    """
    codes_to_update = []
    results = {}
    
    # 检查哪些需要更新
    for code in codes:
        should_update, reason = should_check_update(code, silent=True)
        if should_update:
            codes_to_update.append(code)
            if not silent:
                print(f"[{code}] 需更新: {reason}")
    
    # 批量更新
    if codes_to_update:
        if not silent:
            print(f"批量更新 {len(codes_to_update)} 只股票...")
        try:
            online_data = await fetch_online_data(codes_to_update, close=False)
            if online_data:
                save_to_cache(online_data, silent=True)
                if not silent:
                    print(f"已更新 {len(online_data)} 只股票")
        except Exception as e:
            if not silent:
                print(f"批量更新失败: {e}")
            # 记录检查时间
            for code in codes_to_update:
                update_check_time(code, get_cached_latest_report_date(code))
    
    # 从缓存读取所有数据
    for code in codes:
        results[code] = get_cached_latest(code)
    
    return results


async def update_stock_data(code: str, silent: bool = True, close: bool = False) -> Dict[str, Any]:
    """
    更新单只股票的财务数据
    
    Args:
        code: 股票代码
        silent: 是否静默模式
        close: 是否关闭浏览器
    
    Returns:
        更新结果字典
    """
    result = {
        'code': code,
        'success': False,
        'cached_before': 0,
        'online_fetched': 0,
        'cached_after': 0,
        'new_records': 0,
        'error': None
    }
    
    try:
        result['cached_before'] = get_cached_count(code)
        
        # 从在线获取
        online_data = await fetch_online_data([code], close=close)
        
        if not online_data:
            result['error'] = '在线数据为空'
            return result
        
        result['online_fetched'] = len(online_data[0].get('reports', []))
        
        # 保存到缓存
        save_to_cache(online_data, silent=True)
        
        result['cached_after'] = get_cached_count(code)
        result['new_records'] = result['cached_after'] - result['cached_before']
        result['success'] = True
        
        if not silent:
            name = online_data[0].get('name', code)
            print(f"✓ {name}({code}): 在线{result['online_fetched']}条, "
                  f"缓存{result['cached_before']}→{result['cached_after']}条, "
                  f"新增{result['new_records']}条")
        
        return result
        
    except Exception as e:
        result['error'] = str(e)
        if not silent:
            print(f"✗ {code}: 更新失败 - {e}")
        return result


async def batch_update(codes: List[str], silent: bool = False, close: bool = False) -> List[Dict[str, Any]]:
    """
    批量更新多只股票的财务数据
    
    Args:
        codes: 股票代码列表
        silent: 是否静默模式
        close: 是否关闭浏览器
    
    Returns:
        更新结果列表
    """
    if not codes:
        return []
    
    results = []
    
    if not silent:
        print(f"开始批量更新 {len(codes)} 只股票...")
    
    try:
        # 批量获取在线数据
        online_data = await fetch_online_data(codes, close=close)
        
        # 保存到缓存
        saved = save_to_cache(online_data, silent=silent)
        
        if not silent:
            print(f"\n更新完成: 获取 {len(online_data)} 只，保存 {saved} 条记录")
        
        for data in online_data:
            results.append({
                'code': data.get('code'),
                'success': True,
                'reports': len(data.get('reports', []))
            })
            
    except Exception as e:
        if not silent:
            print(f"批量更新失败: {e}")
        for code in codes:
            results.append({
                'code': code,
                'success': False,
                'error': str(e)
            })
    
    return results


def get_cache_summary() -> pd.DataFrame:
    """获取缓存数据汇总"""
    ensure_table_exists()
    conn = get_db_connection()
    try:
        df = conn.execute(f"""
            SELECT 
                code,
                name,
                COUNT(*) as record_count,
                MIN(report_date) as earliest_date,
                MAX(report_date) as latest_date,
                MAX(created_at) as last_updated
            FROM {TABLE_NAME}
            GROUP BY code, name
            ORDER BY latest_date DESC
        """).df()
        return df
    finally:
        conn.close()


def is_profitable(code: str, years: int = 2) -> bool:
    """
    判断股票是否盈利（从缓存判断）
    
    Args:
        code: 股票代码
        years: 要求盈利的年数
        
    Returns:
        是否盈利
    """
    df = get_cached_all(code)
    
    if df.empty:
        return False
    
    # 筛选年报
    annual_reports = df[df['report_type'] == '年报'].head(years)
    
    if len(annual_reports) < years:
        # 年报不够，检查最近的报告
        recent = df.head(years * 4)
        for _, row in recent.iterrows():
            net_profit = row.get('net_profit')
            if net_profit is not None and net_profit < 0:
                return False
        return len(df) > 0
    
    # 检查年报净利润
    for _, row in annual_reports.iterrows():
        net_profit = row.get('net_profit')
        if net_profit is not None and net_profit < 0:
            return False
    
    return True


def filter_profitable(codes: List[str], years: int = 2) -> List[str]:
    """
    筛选盈利股票
    
    Args:
        codes: 股票代码列表
        years: 要求盈利的年数
    
    Returns:
        盈利股票代码列表
    """
    return [code for code in codes if is_profitable(code, years)]


def normalize_code(code: str) -> str:
    """标准化股票代码"""
    code = str(code).strip()
    if '.' in code:
        code = code.split('.')[-1]
    return code.zfill(6)


def read_codes_from_csv(filepath: str) -> List[str]:
    """从CSV文件读取股票代码"""
    codes = []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get('代码') or row.get('code') or row.get('股票代码') or row.get('证券代码')
            if code:
                codes.append(normalize_code(code))
    return codes


def display_data(df: pd.DataFrame, title: str = "财务数据"):
    """展示数据"""
    if df.empty:
        print("无数据")
        return
    
    print(f"\n{'='*60}")
    print(f"【{title}】共 {len(df)} 条")
    print('='*60)
    
    # 格式化输出
    for _, row in df.iterrows():
        report_date = row.get('report_date', '')
        report_type = row.get('report_type', '')
        roe = row.get('roe')
        net_profit = row.get('net_profit')
        
        roe_str = f"{roe:.2f}%" if roe is not None else "-"
        profit_str = f"{net_profit/1e8:.2f}亿" if net_profit is not None else "-"
        
        print(f"  {report_date} {report_type:<6} ROE={roe_str:<8} 净利润={profit_str}")


# 命令行接口
if __name__ == '__main__':
    import argparse
    import asyncio
    
    subcommands = {'get', 'update', 'summary', 'profitable'}
    
    if len(sys.argv) > 1 and sys.argv[1] in subcommands:
        # 子命令模式
        parser = argparse.ArgumentParser(description='财务数据管理')
        subparsers = parser.add_subparsers(dest='command')
        
        get_parser = subparsers.add_parser('get', help='从远端获取最新数据并缓存')
        get_parser.add_argument('codes', nargs='+', help='股票代码')
        get_parser.add_argument('--close', action='store_true', help='执行后关闭浏览器')
        
        update_parser = subparsers.add_parser('update', help='从远端更新财务数据')
        update_parser.add_argument('codes', nargs='*', help='股票代码（支持多个）')
        update_parser.add_argument('-f', '--file', type=str, help='从CSV文件读取代码')
        update_parser.add_argument('--limit', type=int, default=None, help='限制数量')
        update_parser.add_argument('-s', '--silent', action='store_true', help='静默模式')
        update_parser.add_argument('--close', action='store_true', help='执行后关闭浏览器')
        
        summary_parser = subparsers.add_parser('summary', help='查看缓存数据汇总')
        
        profit_parser = subparsers.add_parser('profitable', help='筛选盈利股票')
        profit_parser.add_argument('codes', nargs='*', help='股票代码')
        profit_parser.add_argument('-f', '--file', type=str, help='从CSV文件读取代码')
        profit_parser.add_argument('--years', type=int, default=2, help='要求盈利年数')
        
        args = parser.parse_args()
        
        if args.command == 'get':
            async def _get():
                data = await fetch_online_data(args.codes, close=args.close)
                save_to_cache(data, silent=False)
                for d in data:
                    code = d.get('code')
                    df = get_cached_latest(code)
                    display_data(df, f"{d.get('name', code)} ({code})")
            asyncio.run(_get())
            
        elif args.command == 'update':
            codes = list(args.codes) if args.codes else []
            if args.file:
                codes.extend(read_codes_from_csv(args.file))
            if args.limit:
                codes = codes[:args.limit]
            if codes:
                async def _update():
                    await batch_update(codes, silent=args.silent, close=args.close)
                asyncio.run(_update())
            else:
                print("请提供股票代码或CSV文件")
                
        elif args.command == 'summary':
            df = get_cache_summary()
            if df.empty:
                print("缓存为空")
            else:
                print(df.to_string())
                
        elif args.command == 'profitable':
            codes = list(args.codes) if args.codes else []
            if args.file:
                codes.extend(read_codes_from_csv(args.file))
            if not codes:
                # 使用缓存中所有代码
                codes = get_cached_codes()
            
            profitable = filter_profitable(codes, args.years)
            print(f"\n最近{args.years}年盈利的股票 ({len(profitable)}/{len(codes)}):")
            for code in profitable:
                print(f"  {code}")
    else:
        # 默认模式：智能获取（自动判断是否需要更新）
        parser = argparse.ArgumentParser(description='财务数据管理（智能缓存策略）')
        parser.add_argument('code', nargs='?', help='股票代码')
        parser.add_argument('-n', '--limit', type=int, default=10, help='返回记录数')
        parser.add_argument('-a', '--all', action='store_true', help='返回所有历史记录')
        parser.add_argument('--no-update', action='store_true', help='禁用自动更新检查')
        parser.add_argument('-v', '--verbose', action='store_true', help='显示更新检查信息')
        
        args = parser.parse_args()
        
        if args.code:
            code = normalize_code(args.code)
            
            if args.no_update:
                # 纯缓存模式
                if args.all:
                    df = get_cached_all(code)
                else:
                    df = get_cached_latest(code, args.limit)
            else:
                # 智能获取模式
                async def _smart_get():
                    return await get_smart(code, args.limit, silent=not args.verbose)
                df = asyncio.run(_smart_get())
                
                if args.all:
                    df = get_cached_all(code)
            
            if df.empty:
                print(f"缓存中没有 {code} 的数据")
                print("提示: 使用 'update' 命令从远端获取数据")
            else:
                display_data(df, f"{code} 财务数据")
        else:
            df = get_cache_summary()
            if df.empty:
                print("缓存为空")
                print("提示: 使用 'update <代码>' 命令从远端获取数据")
            else:
                print(f"\n缓存汇总 (共 {len(df)} 只股票):\n")
                print(df.to_string())
