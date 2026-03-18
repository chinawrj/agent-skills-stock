#!/usr/bin/env python3
"""Analyze posts for truncation signals."""
import json

data = json.load(open("digital_clone/xiaokashu/runs/xueqiu_api_all_posts.json"))
posts = data["posts"]

lengths = sorted(len(p.get("detail_full_text", "")) for p in posts)
print(f"Text length stats ({len(posts)} posts):")
print(f"  Min: {min(lengths)}, Max: {max(lengths)}, Avg: {sum(lengths)//len(lengths)}")
print(f"  Median: {lengths[len(lengths)//2]}")
print(f"  >500 chars: {sum(1 for l in lengths if l > 500)}")
print(f"  >1000 chars: {sum(1 for l in lengths if l > 1000)}")
print(f"  >2000 chars: {sum(1 for l in lengths if l > 2000)}")

# Check for truncation patterns
truncated = []
for p in posts:
    text = p.get("detail_full_text", "")
    if text.endswith("...") or text.endswith("…") or "展开全文" in text:
        truncated.append(p)

print(f"\nPosts ending with .../…/展开全文: {len(truncated)}")
for p in truncated[:5]:
    text = p["detail_full_text"]
    print(f"\n  ID={p.get('status_id','?')}, len={len(text)}")
    print(f"  Last 120 chars: ...{text[-120:]}")

# Also check: is there a max length ceiling suggesting API truncation?
print(f"\nTop 10 longest posts:")
by_len = sorted(posts, key=lambda p: len(p.get("detail_full_text", "")), reverse=True)
for p in by_len[:10]:
    text = p["detail_full_text"]
    print(f"  ID={p.get('status_id','?')}, len={len(text)}, ends_with='{text[-10:]}'")
