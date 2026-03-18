#!/usr/bin/env python3
"""Fetch all posts from Xueqiu user 小卡叔 via browser API and save to JSON."""
import asyncio
import json
import random
import re
import sys
from functools import partial
from pathlib import Path
from playwright.async_api import async_playwright

# Force unbuffered print
print = partial(print, flush=True)

CDP_URL = "http://localhost:9222"
OUTPUT_DIR = Path(__file__).resolve().parent / "runs"
OUTPUT_DIR.mkdir(exist_ok=True)

# We'll discover the user ID dynamically
KNOWN_UIDS = {
    "小卡叔": "9508203182",
}


async def fetch_all_posts(page, uid: str, max_pages: int = 200):
    """Fetch all posts using Xueqiu v4 statuses API via browser fetch."""
    all_posts = []
    current_page = 1
    consecutive_errors = 0
    MAX_RETRIES = 3

    while current_page <= max_pages:
        result = await page.evaluate(f'''async () => {{
            const url = "https://xueqiu.com/v4/statuses/user_timeline.json?user_id={uid}&page={current_page}&type=0";
            const resp = await fetch(url, {{credentials: "include"}});
            if (!resp.ok) return {{error: resp.status, text: await resp.text()}};
            const data = await resp.json();
            return {{
                maxPage: data.maxPage,
                total: data.total,
                count: data.statuses ? data.statuses.length : 0,
                statuses: data.statuses || []
            }};
        }}''')

        if "error" in result:
            consecutive_errors += 1
            err_code = result.get("error", "?")
            print(f"  API error on page {current_page} (attempt {consecutive_errors}): HTTP {err_code}")
            if consecutive_errors >= MAX_RETRIES:
                print(f"  Too many consecutive errors, stopping.")
                break
            backoff = 20 * consecutive_errors + random.uniform(10, 30)
            print(f"  Backing off {backoff:.1f}s before retry...")
            await asyncio.sleep(backoff)
            continue

        consecutive_errors = 0
        statuses = result.get("statuses", [])
        if not statuses:
            print(f"  No more statuses on page {current_page}, done.")
            break

        for s in statuses:
            # Use 'text' field (full content) over 'description' (truncated)
            full_html = s.get("text", "") or s.get("description", "")
            desc_html = s.get("description", "")
            title = s.get("title", "")
            created_at = s.get("created_at", 0)
            status_id = s.get("id", "")
            target = s.get("target", "")
            retweet = s.get("retweeted_status")

            all_posts.append({
                "status_id": status_id,
                "title": title,
                "full_html": full_html,
                "description_html": desc_html,
                "created_at_ms": created_at,
                "target_url": target,
                "is_retweet": retweet is not None,
                "retweet_id": retweet.get("id") if retweet else None,
                "raw": s,
            })

        max_page = result.get("maxPage", 1)
        print(f"  Page {current_page}/{max_page}: got {len(statuses)} posts (total so far: {len(all_posts)})")

        if current_page >= max_page:
            break
        current_page += 1
        delay = random.uniform(5.0, 10.0)
        # Every 10 pages, take a longer break
        if current_page % 10 == 0:
            delay = random.uniform(15.0, 25.0)
        print(f"  Waiting {delay:.1f}s...")
        await asyncio.sleep(delay)

    return all_posts


def strip_html(html: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r'<br\s*/?>', '\n', html)
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
    text = text.replace('&#10;', '\n')
    return text.strip()


async def main():
    uid = KNOWN_UIDS["小卡叔"]
    print(f"Fetching all posts for 小卡叔 (uid={uid})...")

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(CDP_URL)
        ctx = browser.contexts[0]
        page = ctx.pages[0]
        print(f"Connected to browser. Current page: {page.url}")

        raw_posts = await fetch_all_posts(page, uid)

    print(f"\nTotal raw posts fetched: {len(raw_posts)}")

    # Convert to ingest format
    ingest_posts = []
    skipped_video = 0
    for rp in raw_posts:
        full_html = rp.get("full_html", "") or rp.get("description_html", "")
        desc_html = rp.get("description_html", "")
        full_text = strip_html(full_html)
        desc_text = strip_html(desc_html)
        is_truncated = desc_text.endswith("...") and len(full_text) > len(desc_text)
        title = strip_html(rp.get("title", ""))

        # Skip video posts
        raw = rp.get("raw", {})
        if raw.get("type") in ("video", "reel"):
            skipped_video += 1
            continue
        # Check for video markers in text
        lower_text = (full_text + title).lower()
        if any(m in lower_text for m in ["[视频]", "视频"]):
            # Only skip if it's primarily a video post (very short text with video marker)
            if len(full_text) < 20:
                skipped_video += 1
                continue

        if not full_text:
            continue

        created_ms = rp.get("created_at_ms", 0)
        from datetime import datetime, timezone
        if created_ms:
            dt = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
            time_text = dt.strftime("%Y-%m-%d %H:%M")
        else:
            time_text = ""

        ingest_posts.append({
            "author": "小卡叔",
            "time_text": time_text,
            "list_snippet": desc_text[:120] + ("..." if len(desc_text) > 120 else ""),
            "truncated_detected": is_truncated,
            "detail_full_text": full_text,
            "likes_or_comments_text": f"like:{raw.get('like_count',0)} reply:{raw.get('reply_count',0)} retweet:{raw.get('retweet_count',0)}",
            "source_tab": "全部",
            "is_video": False,
            "post_type": "retweet" if rp.get("is_retweet") else "original",
            "status_id": str(rp.get("status_id", "")),
            "title": title,
        })

    payload = {
        "status": "success",
        "posts": ingest_posts,
        "notes": f"Fetched via Xueqiu API. Total raw: {len(raw_posts)}, video skipped: {skipped_video}, final: {len(ingest_posts)}"
    }

    from datetime import datetime as _dt
    ts = _dt.now().strftime("%Y%m%d_%H%M%S")
    out_file = OUTPUT_DIR / f"xueqiu_api_{ts}.json"
    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved {len(ingest_posts)} posts to {out_file}")
    print(f"Skipped {skipped_video} video posts")


if __name__ == "__main__":
    asyncio.run(main())
