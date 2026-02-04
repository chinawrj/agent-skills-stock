#!/usr/bin/env python3
"""
财务数据缓存模块 - 供 fetch_fundamental.py 自动缓存使用
"""

import os
from datetime import datetime
from typing import List, Dict

import duckdb
import pandas as pd

# 数据库路径
DB_PATH = os.path.join(os.path.dirname(__file__), '../../../../data/a-share.db')
TABLE_NAME = 'fundamentals'


def get_db_connection():
    """获取数据库连接"""
    db_path = os.path.abspath(DB_PATH)
    return duckdb.connect(db_path)


def ensure_table_exists():
    """确保表存在"""
    conn = get_db_connection()
    try:
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
    finally:
        conn.close()


def _safe_get(d: dict, key: str, default=None):
    """安全获取值"""
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
        data_list: 从 fetch_fundamental_data_via_dom 获取的数据列表
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
                    None,  # bps
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
        
        return total_count
    finally:
        conn.close()
