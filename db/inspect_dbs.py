#!/usr/bin/env python3
"""Deep inspect all .db files."""
import duckdb, os, sys

files = [
    '/Users/rjwang/fun/a-share/data/a-share.db',
    '/Users/rjwang/fun/a-share/data/a-share.db.backup',
    '/Users/rjwang/fun/a-share/data/a-share.db.backup_20260317',
]

for f in files:
    if not os.path.exists(f):
        continue
    sz = os.path.getsize(f) / 1024 / 1024
    print(f"\n{'='*60}")
    print(f"FILE: {os.path.basename(f)}  ({sz:.1f} MB)")
    print(f"{'='*60}")
    
    # Check for WAL
    wal = f + '.wal'
    if os.path.exists(wal):
        wsz = os.path.getsize(wal) / 1024 / 1024
        print(f"  WAL: {wsz:.1f} MB")
    else:
        print(f"  WAL: none")
    
    try:
        con = duckdb.connect(f, read_only=True)
        tables = [t[0] for t in con.execute('SHOW TABLES').fetchall()]
        print(f"  Tables: {tables}")
        for t in tables:
            cnt = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            print(f"    {t}: {cnt} rows")
        con.close()
    except Exception as e:
        print(f"  ERROR: {str(e)[:200]}")

with open('/tmp/db_inspect.txt', 'w') as out:
    out.write("done\n")
