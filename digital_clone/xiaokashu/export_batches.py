#!/usr/bin/env python3
"""Export top 300 bond posts into 6 batch files for Map-Reduce analysis."""
import sqlite3
import json
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "posts.db"
OUTPUT_DIR = Path(__file__).resolve().parent / "batches"
OUTPUT_DIR.mkdir(exist_ok=True)

BATCH_SIZE = 50
NUM_BATCHES = 6

conn = sqlite3.connect(str(DB_PATH))
c = conn.cursor()

rows = c.execute("""
    SELECT time_text, full_text
    FROM posts
    WHERE post_type = 'bond'
    ORDER BY time_text DESC
    LIMIT ?
""", (BATCH_SIZE * NUM_BATCHES,)).fetchall()

print(f"Fetched {len(rows)} bond posts")

for i in range(NUM_BATCHES):
    start = i * BATCH_SIZE
    end = start + BATCH_SIZE
    batch = rows[start:end]
    
    # Write as readable text file
    out_path = OUTPUT_DIR / f"batch_{i+1}.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"# Batch {i+1}: Posts {start+1}-{end} (newest first)\n")
        f.write(f"# Time range: {batch[-1][0]} to {batch[0][0]}\n\n")
        for idx, (time_text, full_text) in enumerate(batch, start + 1):
            f.write(f"--- Post #{idx} [{time_text}] ---\n")
            f.write(full_text.strip())
            f.write("\n\n")
    
    chars = sum(len(t) for _, t in batch)
    print(f"Batch {i+1}: posts {start+1}-{end}, {chars} chars, time {batch[-1][0]} ~ {batch[0][0]}")

conn.close()
print(f"\nOutput: {OUTPUT_DIR}/batch_*.txt")
