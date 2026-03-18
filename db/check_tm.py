#!/usr/bin/env python3
"""Search for Time Machine backups of a-share.db"""
import os, subprocess, sys

# Time Machine
try:
    r = subprocess.run(['tmutil', 'listbackups'], capture_output=True, text=True, timeout=10)
    lines = [l.strip() for l in r.stdout.strip().split('\n') if l.strip()]
    print(f"Time Machine backups: {len(lines)}")
    if lines:
        print(f"Latest: {lines[-1]}")
        # Check last 5 for our DB
        for bp in reversed(lines[-5:]):
            db_path = os.path.join(bp, 'Users/rjwang/fun/a-share/data/a-share.db')
            wal_path = db_path + '.wal'
            if os.path.exists(db_path):
                sz = os.path.getsize(db_path) / 1024 / 1024
                wal_sz = os.path.getsize(wal_path) / 1024 / 1024 if os.path.exists(wal_path) else 0
                print(f"  {bp}: db={sz:.1f}MB wal={wal_sz:.1f}MB")
            else:
                print(f"  {bp}: NOT FOUND")
except Exception as e:
    print(f"TM error: {e}")
