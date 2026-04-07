#!/usr/bin/env python3
"""Search for any DuckDB backup that has complete data (klines, bonds, etc)."""
import os
import glob
import subprocess

# 1. Search for a-share.db files
print("=== Searching for a-share.db files ===")
for pattern in [
    '/Users/rjwang/**/a-share.db*',
    '/tmp/**/a-share*',
    '/Users/rjwang/fun/a-share/**/*.db',
    '/Users/rjwang/fun/a-share/**/*.parquet',
]:
    for f in glob.glob(pattern, recursive=True):
        if '.venv' in f or 'node_modules' in f or 'camoufox' in f or 'patchright' in f:
            continue
        size = os.path.getsize(f)
        print(f"  {f}  ({size/1024/1024:.1f} MB)")

# 2. Check Time Machine
print("\n=== Time Machine ===")
try:
    result = subprocess.run(['tmutil', 'listbackups'], capture_output=True, text=True, timeout=5)
    backups = result.stdout.strip().split('\n')
    if backups and backups[0]:
        print(f"  Found {len(backups)} Time Machine backups")
        print(f"  Latest: {backups[-1]}")
        # Check if our DB exists in latest backup
        tm_path = os.path.join(backups[-1], 'Users/rjwang/fun/a-share/data/a-share.db')
        if os.path.exists(tm_path):
            size = os.path.getsize(tm_path)
            print(f"  DB in latest backup: {size/1024/1024:.1f} MB")
            # Check WAL too
            wal_path = tm_path + '.wal'
            if os.path.exists(wal_path):
                wsize = os.path.getsize(wal_path)
                print(f"  WAL in latest backup: {wsize/1024/1024:.1f} MB")
        else:
            print(f"  DB not found at: {tm_path}")
            # Try other backups (most recent first)
            for bp in reversed(backups[-5:]):
                tp = os.path.join(bp, 'Users/rjwang/fun/a-share/data/a-share.db')
                if os.path.exists(tp):
                    size = os.path.getsize(tp)
                    print(f"  Found in {bp}: {size/1024/1024:.1f} MB")
                    break
    else:
        print("  No Time Machine backups found")
except Exception as e:
    print(f"  Time Machine check failed: {e}")

# 3. Try to read each .db file and check for klines table
print("\n=== Checking DB contents ===")
import duckdb
for dbfile in [
    'data/a-share.db',
    'data/a-share.db.backup',
    'data/a-share.db.backup_20260317',
]:
    path = os.path.join('/Users/rjwang/fun/a-share', dbfile)
    if not os.path.exists(path):
        continue
    try:
        con = duckdb.connect(path, read_only=True)
        tables = [t[0] for t in con.execute('SHOW TABLES').fetchall()]
        has_klines = 'klines' in tables
        has_bonds = 'bonds' in tables
        klines_cnt = con.execute('SELECT COUNT(*) FROM klines').fetchone()[0] if has_klines else 0
        bonds_cnt = con.execute('SELECT COUNT(*) FROM bonds').fetchone()[0] if has_bonds else 0
        con.close()
        size = os.path.getsize(path)
        print(f"  {dbfile} ({size/1024/1024:.1f} MB): tables={tables}, klines={klines_cnt}, bonds={bonds_cnt}")
    except Exception as e:
        print(f"  {dbfile}: ERROR - {str(e)[:100]}")

# 4. List all SQL files that could rebuild data
print("\n=== Available SQL/CSV for rebuild ===")
data_dir = '/Users/rjwang/fun/a-share/data'
for f in sorted(os.listdir(data_dir)):
    if f.endswith(('.csv', '.sql')):
        fp = os.path.join(data_dir, f)
        size = os.path.getsize(fp)
        if size > 1000:
            print(f"  {f}  ({size/1024:.0f} KB)")
