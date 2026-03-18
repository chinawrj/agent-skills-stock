#!/usr/bin/env python3
"""Verify full text quality of the latest fetch."""
import json, glob, os

# Find latest run file
files = sorted(glob.glob("digital_clone/xiaokashu/runs/xueqiu_api_2*.json"))
latest = files[-1]
print(f"Checking: {latest}")

data = json.load(open(latest))
posts = data["posts"]
print(f"Total posts: {len(posts)}")

lengths = [len(p.get("detail_full_text", "")) for p in posts]
lengths.sort()
print(f"\nText length stats:")
print(f"  Min: {min(lengths)}, Max: {max(lengths)}, Avg: {sum(lengths)//len(lengths)}")
print(f"  Median: {lengths[len(lengths)//2]}")
print(f"  >500 chars: {sum(1 for l in lengths if l > 500)}")
print(f"  >1000 chars: {sum(1 for l in lengths if l > 1000)}")
print(f"  >2000 chars: {sum(1 for l in lengths if l > 2000)}")

# Truncation check
truncated = [p for p in posts if p.get("detail_full_text", "").endswith("...")]
print(f"\nPosts ending with '...': {len(truncated)} / {len(posts)}")

# Check truncated_detected flag
flagged = [p for p in posts if p.get("truncated_detected")]
print(f"Posts flagged truncated: {len(flagged)}")

# Sample a long post
by_len = sorted(posts, key=lambda p: len(p.get("detail_full_text", "")), reverse=True)
print(f"\nTop 5 longest posts:")
for p in by_len[:5]:
    t = p["detail_full_text"]
    print(f"  ID={p.get('status_id','?')}, len={len(t)}, truncated={p.get('truncated_detected')}")
    print(f"    Start: {t[:80]}...")
    print(f"    End: ...{t[-80:]}")
