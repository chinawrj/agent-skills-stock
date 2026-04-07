#!/usr/bin/env python3
"""
A股数据库管理 — 建库 + 日常维护统一入口

位置: .github/skills/db-manager/manage.py

用法:
    cd /Users/rjwang/fun/a-share && source .venv/bin/activate

    # 从零建库（约2小时，含K线历史拉取）
    python .github/skills/db-manager/manage.py init

    # 每日更新（盘后15:30，约2分钟）
    python .github/skills/db-manager/manage.py daily

    # 每周维护（股东人数+下修历史，约15分钟）
    python .github/skills/db-manager/manage.py weekly

    # 数据库健康检查
    python .github/skills/db-manager/manage.py status

    # 单独步骤
    python .github/skills/db-manager/manage.py init --step schema
    python .github/skills/db-manager/manage.py init --step fundamentals
    python .github/skills/db-manager/manage.py daily --step bonds
    python .github/skills/db-manager/manage.py weekly --step shareholders
"""

import argparse
import glob
import os
import shutil
import subprocess
import sys
import time

# ── 路径 ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..', '..'))
DB_DIR = os.path.join(WORKSPACE, 'db')
DATA_DIR = os.path.join(WORKSPACE, 'data')
DB_PATH = os.path.join(DATA_DIR, 'a-share.db')
CACHE_DIR = os.path.join(SCRIPT_DIR, 'cache')
VENV_PYTHON = os.path.join(WORKSPACE, '.venv', 'bin', 'python3')

# ── 用哪个 Python ──
PYTHON = VENV_PYTHON if os.path.exists(VENV_PYTHON) else sys.executable

# ── Cache 文件映射: data/源文件 → cache/目标文件 ──
CACHE_MAP = {
    # init 阶段
    'all_stocks.csv':         'stocks.csv',
    'listing_dates.csv':      'listing_dates.csv',
    'fundamentals_annual.csv':'fundamentals.csv',
    'klines_daily.csv':       'klines.csv',
    'all_shareholders.csv':   'shareholders.csv',
    'bond_putback_dates.csv': 'bond_putback_dates.csv',
    # daily 阶段
    'bonds_full.csv':         'bonds_full.csv',
    'bond_market.csv':        'bonds_market.csv',
    # weekly 阶段
    'revise_history.csv':     'revise_history.csv',
    'shareholders_incremental.csv': None,  # merged into shareholders.csv by script
}


def cache_copy(data_file, cache_file=None):
    """Copy a file from data/ to cache/. Returns True on success."""
    src = os.path.join(DATA_DIR, data_file)
    if not os.path.exists(src):
        return False
    dst_name = cache_file or CACHE_MAP.get(data_file, data_file)
    if dst_name is None:
        return False
    dst = os.path.join(CACHE_DIR, dst_name)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(src, dst)
    size_mb = os.path.getsize(dst) / 1024 / 1024
    print(f"    💾 缓存: {data_file} → cache/{dst_name} ({size_mb:.1f}MB)")
    return True


def cache_daily_market(date_str=None):
    """Copy daily_market CSV to cache/daily_market/YYYY-MM-DD.csv."""
    pattern = os.path.join(DATA_DIR, 'daily_market_*.csv')
    files = sorted(glob.glob(pattern))
    if not files:
        return
    latest = files[-1]
    basename = os.path.basename(latest)
    # e.g. daily_market_2026-03-18.csv
    dst = os.path.join(CACHE_DIR, 'daily_market', basename.replace('daily_market_', ''))
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(latest, dst)
    size_kb = os.path.getsize(dst) / 1024
    print(f"    💾 缓存: {basename} → cache/daily_market/ ({size_kb:.0f}KB)")


def run_script(script_name, args=None, description=None):
    """Run a db/ script via subprocess."""
    path = os.path.join(DB_DIR, script_name)
    if not os.path.exists(path):
        print(f"  ❌ 脚本不存在: {path}")
        return False

    cmd = [PYTHON, path] + (args or [])
    label = description or script_name
    print(f"\n{'─' * 50}")
    print(f"  ▸ {label}")
    print(f"    {' '.join(cmd)}")
    print(f"{'─' * 50}")

    result = subprocess.run(cmd, cwd=WORKSPACE)
    if result.returncode != 0:
        print(f"  ❌ {label} 失败 (exit={result.returncode})")
        return False
    return True


def run_sql(sql_file, description=None):
    """Run a SQL file against DuckDB."""
    path = os.path.join(DB_DIR, sql_file)
    if not os.path.exists(path):
        print(f"  ❌ SQL文件不存在: {path}")
        return False

    label = description or sql_file
    print(f"\n{'─' * 50}")
    print(f"  ▸ {label}")
    print(f"{'─' * 50}")

    try:
        import duckdb
        conn = duckdb.connect(DB_PATH)
        with open(path, 'r', encoding='utf-8') as f:
            sql = f.read()
        for stmt in sql.split(';'):
            stmt = stmt.strip()
            if stmt and not stmt.startswith('--'):
                try:
                    conn.execute(stmt)
                except Exception as e:
                    if 'already exists' not in str(e).lower():
                        print(f"    ⚠️ {e}")
        conn.close()
        print(f"  ✅ {label} 完成")
        return True
    except Exception as e:
        if 'lock' in str(e).lower():
            print(f"  ⚠️ DB锁定: {e}")
            print(f"  请关闭其他DuckDB连接后重试，或通过MCP工具执行SQL")
            return False
        print(f"  ❌ 错误: {e}")
        return False


def db_exists():
    """Check if the database file exists."""
    return os.path.exists(DB_PATH)


def db_query(sql):
    """Quick query helper. Returns rows or None on error."""
    try:
        import duckdb
        conn = duckdb.connect(DB_PATH, read_only=True)
        result = conn.execute(sql).fetchall()
        conn.close()
        return result
    except Exception as e:
        if 'lock' in str(e).lower():
            return 'LOCKED'
        return None


# ═══════════════════ INIT: 从零建库 ═══════════════════

INIT_STEPS = {
    'schema':        ('创建Schema',           lambda: run_sql('init_db.sql', '初始化表结构')),
    'stocks':        ('导入股票基本信息',      lambda: run_script('rebuild_db.py', ['--step', '1'], '导入全A股股票列表')),
    'listing':       ('补全上市日期',          lambda: run_script('fetch_listing_dates.py', [], '补全stocks.listing_date')),
    'bonds':         ('导入转债数据',          lambda: run_script('daily_update.py', ['--step', 'bonds'], '导入可转债行情+基础数据')),
    'putback':       ('补全回售起始日',        lambda: run_script('fetch_putback_dates.py', [], '从东财补全bonds.putback_start')),
    'fundamentals':  ('导入财务数据',          lambda: run_script('import_fundamentals.py', [], '导入年度财务数据(2020-2024)')),
    'klines':        ('导入K线历史',           lambda: run_script('import_klines.py', [], '导入1年日K线(~85分钟)')),
    'shareholders':  ('导入股东人数',          lambda: run_script('fetch_all_shareholders.py', [], '导入全市场股东人数历史')),
    'revise':        ('导入下修历史',          lambda: run_script('fetch_revise_history.py', ['--all'], '导入转债下修事件历史')),
    'analysis':      ('计算分析指标',          lambda: run_script('daily_update.py', ['--step', 'analysis'], '计算触发进度+盈利状态')),
}

INIT_ORDER = ['schema', 'stocks', 'listing', 'bonds', 'putback', 'fundamentals',
              'klines', 'shareholders', 'revise', 'analysis']


def cmd_init(args):
    """Initialize database from scratch."""
    print("=" * 60)
    print("  A股数据库初始化 — 从零建库")
    print("=" * 60)

    if db_exists() and not args.force:
        print(f"\n  ⚠️ 数据库已存在: {DB_PATH}")
        print("  使用 --force 覆盖，或先备份")
        ans = input("  继续? [y/N] ").strip().lower()
        if ans != 'y':
            print("  取消")
            return

    steps = [args.step] if args.step else INIT_ORDER
    total = len(steps)
    ok = 0
    failed = []

    for i, step in enumerate(steps, 1):
        if step not in INIT_STEPS:
            print(f"\n  ❌ 未知步骤: {step}")
            print(f"  可选: {', '.join(INIT_ORDER)}")
            return
        desc, func = INIT_STEPS[step]
        print(f"\n[{i}/{total}] {desc}...")
        if func():
            ok += 1
            # Auto-cache after each step
            INIT_CACHE_MAP = {
                'stocks':       ['all_stocks.csv'],
                'listing':      ['listing_dates.csv'],
                'bonds':        ['bonds_full.csv', 'bond_market.csv'],
                'putback':      ['bond_putback_dates.csv'],
                'fundamentals': ['fundamentals_annual.csv'],
                'klines':       ['klines_daily.csv'],
                'shareholders': ['all_shareholders.csv'],
                'revise':       ['revise_history.csv'],
            }
            for f in INIT_CACHE_MAP.get(step, []):
                cache_copy(f)
        else:
            failed.append(step)

    print(f"\n{'=' * 60}")
    print(f"  初始化完成: {ok}/{total} 成功")
    if failed:
        print(f"  失败步骤: {', '.join(failed)}")
        print(f"  可单独重跑: python manage.py init --step <步骤名>")
    print(f"{'=' * 60}")


# ═══════════════════ DAILY: 每日更新 ═══════════════════

def cmd_daily(args):
    """Daily post-market update."""
    print("=" * 60)
    print(f"  每日更新 — {args.date or '今日'}")
    print("=" * 60)

    extra = []
    if args.step:
        extra.extend(['--step', args.step])
    if args.date:
        extra.extend(['--date', args.date])
    if args.dry_run:
        extra.append('--dry-run')

    success = run_script('daily_update.py', extra, '每日数据库更新')

    # Auto-cache after daily update
    if success:
        print(f"\n  📦 缓存每日产出...")
        cache_copy('bonds_full.csv')
        cache_copy('bond_market.csv')
        cache_daily_market(args.date)


# ═══════════════════ WEEKLY: 每周维护 ═══════════════════

WEEKLY_STEPS = {
    'shareholders': ('更新股东人数',  lambda: run_script('update_shareholders.py', [], '增量更新股东人数数据')),
    'revise':       ('更新下修历史',  lambda: run_script('fetch_revise_history.py', [], '刷新可转债下修事件历史')),
}

WEEKLY_ORDER = ['shareholders', 'revise']


def cmd_weekly(args):
    """Weekly maintenance tasks."""
    print("=" * 60)
    print("  每周维护")
    print("=" * 60)

    steps = [args.step] if args.step else WEEKLY_ORDER
    for step in steps:
        if step not in WEEKLY_STEPS:
            print(f"\n  ❌ 未知步骤: {step}")
            print(f"  可选: {', '.join(WEEKLY_ORDER)}")
            return
        desc, func = WEEKLY_STEPS[step]
        print(f"\n▸ {desc}...")
        success = func()
        # Auto-cache after each step
        if success:
            WEEKLY_CACHE_MAP = {
                'shareholders': ['all_shareholders.csv'],
                'revise':       ['revise_history.csv'],
            }
            for f in WEEKLY_CACHE_MAP.get(step, []):
                cache_copy(f)


# ═══════════════════ STATUS: 健康检查 ═══════════════════

def cmd_status(args):
    """Database health check."""
    print("=" * 60)
    print("  数据库健康检查")
    print("=" * 60)

    if not db_exists():
        print(f"\n  ❌ 数据库不存在: {DB_PATH}")
        print("  运行: python manage.py init")
        return

    checks = [
        ("stocks",
         "SELECT COUNT(*), MIN(listing_date), MAX(listing_date) FROM stocks",
         lambda r: f"{r[0]}只 | 上市日期 {r[1]} ~ {r[2]}"),
        ("daily_market",
         "SELECT COUNT(*), MAX(trade_date) FROM daily_market",
         lambda r: f"{r[0]}条 | 最新日期 {r[1]}"),
        ("klines",
         "SELECT COUNT(*), MIN(trade_date), MAX(trade_date), COUNT(DISTINCT code) FROM klines",
         lambda r: f"{r[0]}条 | {r[3]}只股 | {r[1]} ~ {r[2]}"),
        ("bonds (活跃)",
         "SELECT COUNT(*), COUNT(CASE WHEN bond_price IS NOT NULL THEN 1 END), "
         "COUNT(CASE WHEN ytm IS NOT NULL THEN 1 END) "
         "FROM bonds WHERE delist_date IS NULL",
         lambda r: f"{r[0]}只 | 有价格{r[1]} | 有YTM{r[2]}"),
        ("bonds (分析)",
         "SELECT COUNT(CASE WHEN revise_trigger_count IS NOT NULL THEN 1 END), "
         "COUNT(CASE WHEN is_profitable IS NOT NULL THEN 1 END) "
         "FROM bonds WHERE delist_date IS NULL",
         lambda r: f"有触发进度{r[0]} | 有盈利状态{r[1]}"),
        ("fundamentals",
         "SELECT COUNT(*), COUNT(DISTINCT code), MIN(report_date), MAX(report_date) FROM fundamentals",
         lambda r: f"{r[0]}条 | {r[1]}只股 | {r[2]} ~ {r[3]}"),
        ("shareholders",
         "SELECT COUNT(*), COUNT(DISTINCT code), MAX(announce_date) FROM shareholders",
         lambda r: f"{r[0]}条 | {r[1]}只股 | 最新公告 {r[2]}"),
        ("revise_history",
         "SELECT COUNT(*), COUNT(DISTINCT bond_code), MAX(meeting_date) FROM revise_history",
         lambda r: f"{r[0]}条 | {r[1]}只债 | 最新下修 {r[2]}"),
    ]

    for label, sql, fmt in checks:
        result = db_query(sql)
        if result == 'LOCKED':
            print(f"\n  ⚠️ 数据库被其他进程锁定 (MCP DuckDB?)")
            print(f"  请通过 MCP DuckDB 工具查询，或关闭 MCP 后重试")
            print(f"  查找锁: lsof {DB_PATH}")
            return
        if result and result[0]:
            print(f"\n  {label:20s}  {fmt(result[0])}")
        else:
            print(f"\n  {label:20s}  ❌ 表不存在或为空")

    # 数据新鲜度警告
    _check_freshness()


def _check_freshness():
    """Check data freshness and print warnings."""
    from datetime import date as dt_date

    print(f"\n{'─' * 50}")
    dm = db_query("SELECT MAX(trade_date) FROM daily_market")
    if dm and dm[0] and dm[0][0]:
        days_old = (dt_date.today() - dm[0][0]).days
        if days_old > 1:
            print(f"  ⚠️ daily_market 数据已过期 {days_old} 天")
            print(f"     运行: python .github/skills/db-manager/manage.py daily")
        else:
            print(f"  ✅ 行情数据为最新")

    rh = db_query("SELECT MAX(meeting_date) FROM revise_history")
    if rh and rh[0] and rh[0][0]:
        days_old = (dt_date.today() - rh[0][0]).days
        if days_old > 14:
            print(f"  ⚠️ 下修历史数据超过 {days_old} 天未更新")
            print(f"     运行: python .github/skills/db-manager/manage.py weekly --step revise")

    sh = db_query("SELECT MAX(announce_date) FROM shareholders")
    if sh and sh[0] and sh[0][0]:
        days_old = (dt_date.today() - sh[0][0]).days
        if days_old > 7:
            print(f"  ⚠️ 股东人数数据超过 {days_old} 天未更新")
            print(f"     运行: python .github/skills/db-manager/manage.py weekly --step shareholders")


# ═══════════════════ REBUILD: 从Cache重建 ═══════════════════

def cmd_rebuild(args):
    """Rebuild database from cache CSVs."""
    import_script = os.path.join(SCRIPT_DIR, 'import_cache.py')

    print("=" * 60)
    print("  从 Cache 重建数据库")
    print("=" * 60)

    # Check cache dir exists and has files
    if not os.path.isdir(CACHE_DIR):
        print(f"\n  ❌ Cache目录不存在: {CACHE_DIR}")
        print("  请先运行 init 或 daily 以填充缓存")
        return

    cache_files = [f for f in os.listdir(CACHE_DIR)
                   if f.endswith('.csv') or os.path.isdir(os.path.join(CACHE_DIR, f))]
    if not cache_files:
        print(f"\n  ❌ Cache目录为空")
        return

    print(f"\n  Cache中发现: {', '.join(sorted(cache_files))}")

    extra = ['--cache-dir', CACHE_DIR, '--db', args.db or DB_PATH]
    if args.table:
        extra.extend(['--table', args.table])
    if args.sql_only:
        extra.append('--sql-only')

    run_script_path(import_script, extra, '从Cache CSV重建DB')


def run_script_path(script_path, args=None, description=None):
    """Run a script by full path."""
    cmd = [PYTHON, script_path] + (args or [])
    label = description or os.path.basename(script_path)
    print(f"\n{'─' * 50}")
    print(f"  ▸ {label}")
    print(f"    {' '.join(cmd)}")
    print(f"{'─' * 50}")
    subprocess.run(cmd, cwd=WORKSPACE)


# ═══════════════════ CACHE-STATUS: 缓存检查 ═══════════════════

def cmd_cache_status(args):
    """Show cache directory status."""
    print("=" * 60)
    print("  Cache 缓存状态")
    print("=" * 60)

    if not os.path.isdir(CACHE_DIR):
        print(f"\n  ❌ Cache目录不存在: {CACHE_DIR}")
        print("  运行 init/daily/weekly 后自动创建")
        return

    expected = [
        ('stocks.csv',        '股票列表'),
        ('listing_dates.csv', '上市日期'),
        ('bonds_full.csv',    '转债完整数据'),
        ('bonds_market.csv',  '转债行情'),
        ('fundamentals.csv',  '年度财务'),
        ('klines.csv',        'K线历史'),
        ('shareholders.csv',  '股东人数'),
        ('revise_history.csv','下修历史'),
        ('bond_putback_dates.csv', '回售起始日'),
    ]

    print()
    for fname, desc in expected:
        fpath = os.path.join(CACHE_DIR, fname)
        if os.path.exists(fpath):
            size = os.path.getsize(fpath)
            mtime = time.strftime('%Y-%m-%d %H:%M', time.localtime(os.path.getmtime(fpath)))
            if size > 1024 * 1024:
                size_str = f"{size / 1024 / 1024:.1f}MB"
            else:
                size_str = f"{size / 1024:.0f}KB"
            print(f"  ✅ {fname:25s} {size_str:>8s}  {mtime}  {desc}")
        else:
            print(f"  ❌ {fname:25s} {'—':>8s}  {'—':14s}  {desc}")

    # Daily market snapshots
    dm_dir = os.path.join(CACHE_DIR, 'daily_market')
    if os.path.isdir(dm_dir):
        dm_files = sorted(glob.glob(os.path.join(dm_dir, '*.csv')))
        if dm_files:
            print(f"\n  📅 日行情快照: {len(dm_files)} 天")
            print(f"     最早: {os.path.basename(dm_files[0])}")
            print(f"     最新: {os.path.basename(dm_files[-1])}")
    else:
        print(f"\n  ❌ 无日行情快照 (daily_market/)")

    # Cache completeness
    present = sum(1 for f, _ in expected if os.path.exists(os.path.join(CACHE_DIR, f)))
    print(f"\n  完整度: {present}/{len(expected)} 核心文件")
    if present == len(expected):
        print(f"  ✅ Cache完整，可随时 rebuild")
    else:
        missing = [f for f, _ in expected if not os.path.exists(os.path.join(CACHE_DIR, f))]
        print(f"  ⚠️ 缺失: {', '.join(missing)}")


# ═══════════════════ Main ═══════════════════

def main():
    parser = argparse.ArgumentParser(
        description='A股数据库管理 — 建库 + 日常维护',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
命令:
  init          从零建库（schema→股票→转债→财务→K线→股东→下修→分析）
  daily         每日更新（盘后15:30，~2分钟）
  weekly        每周维护（股东人数+下修历史，~15分钟）
  status        数据库健康检查
  rebuild       从Cache CSV重建数据库（离线/灾难恢复）
  cache-status  查看缓存状态

示例:
  python manage.py init                      # 完整建库
  python manage.py init --step klines        # 仅导入K线
  python manage.py daily                     # 每日完整更新
  python manage.py daily --step bonds        # 仅更新转债
  python manage.py weekly                    # 每周维护
  python manage.py weekly --step revise      # 仅刷新下修历史
  python manage.py status                    # 健康检查
  python manage.py rebuild                   # 从Cache完整重建DB
  python manage.py rebuild --table bonds     # 仅重建转债表
  python manage.py rebuild --sql-only        # DB锁定时生成SQL
  python manage.py cache-status              # 查看缓存状态
""")

    sub = parser.add_subparsers(dest='command')

    # init
    p_init = sub.add_parser('init', help='从零建库')
    p_init.add_argument('--step', choices=INIT_ORDER, help='仅运行指定步骤')
    p_init.add_argument('--force', action='store_true', help='覆盖已有数据库')

    # daily
    p_daily = sub.add_parser('daily', help='每日更新')
    p_daily.add_argument('--step', choices=['stocks', 'bonds', 'analysis'], help='仅运行指定步骤')
    p_daily.add_argument('--date', type=str, help='指定交易日期 (YYYY-MM-DD)')
    p_daily.add_argument('--dry-run', action='store_true', help='仅获取CSV不导入')

    # weekly
    p_weekly = sub.add_parser('weekly', help='每周维护')
    p_weekly.add_argument('--step', choices=WEEKLY_ORDER, help='仅运行指定步骤')

    # status
    sub.add_parser('status', help='数据库健康检查')

    # rebuild
    p_rebuild = sub.add_parser('rebuild', help='从Cache CSV重建DB')
    p_rebuild.add_argument('--table',
                           choices=['stocks', 'listing', 'bonds', 'bonds_market',
                                    'putback', 'fundamentals', 'klines', 'shareholders',
                                    'revise_history', 'analysis'],
                           help='仅重建指定表')
    p_rebuild.add_argument('--db', type=str, help='数据库路径')
    p_rebuild.add_argument('--sql-only', action='store_true',
                           help='仅生成SQL文件不执行（DB锁定时用）')

    # cache-status
    sub.add_parser('cache-status', help='查看缓存状态')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    os.chdir(WORKSPACE)  # Ensure CWD is workspace root

    if args.command == 'init':
        cmd_init(args)
    elif args.command == 'daily':
        cmd_daily(args)
    elif args.command == 'weekly':
        cmd_weekly(args)
    elif args.command == 'status':
        cmd_status(args)
    elif args.command == 'rebuild':
        cmd_rebuild(args)
    elif args.command == 'cache-status':
        cmd_cache_status(args)


if __name__ == '__main__':
    main()
