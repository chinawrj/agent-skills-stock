#!/usr/bin/env python3
"""Export remaining bond posts (after first 300) into validation batches."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "posts.db"
OUTPUT_DIR = Path(__file__).resolve().parent / "batches"
OUTPUT_DIR.mkdir(exist_ok=True)

BATCH_SIZE = 100  # larger batches for validation

conn = sqlite3.connect(str(DB_PATH))
c = conn.cursor()

# Get all bond posts AFTER the first 300 (older posts)
rows = c.execute("""
    SELECT time_text, full_text
    FROM posts
    WHERE post_type = 'bond'
    ORDER BY time_text DESC
    LIMIT -1 OFFSET 300
""").fetchall()

print(f"Remaining bond posts for validation: {len(rows)}")

batch_num = 0
for i in range(0, len(rows), BATCH_SIZE):
    batch_num += 1
    batch = rows[i:i + BATCH_SIZE]
    
    out_path = OUTPUT_DIR / f"validate_{batch_num}.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"# Validation Batch {batch_num}: Posts {300+i+1}-{300+i+len(batch)}\n")
        f.write(f"# Time range: {batch[-1][0]} to {batch[0][0]}\n\n")
        for idx, (time_text, full_text) in enumerate(batch, i + 1):
            f.write(f"--- Post #{300+idx} [{time_text}] ---\n")
            f.write(full_text.strip())
            f.write("\n\n")
    
    chars = sum(len(t) for _, t in batch)
    print(f"Validate batch {batch_num}: {len(batch)} posts, {chars} chars, {batch[-1][0]} ~ {batch[0][0]}")

conn.close()
