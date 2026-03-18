#!/usr/bin/env python3
import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "posts.db"
SCHEMA_PATH = BASE_DIR / "schema.sql"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_text(text: str) -> str:
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def content_hash(full_text: str) -> str:
    normalized = normalize_text(full_text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def is_video_post(post: dict[str, Any]) -> bool:
    source_tab = str(post.get("source_tab") or "").strip().lower()
    post_type = str(post.get("post_type") or "").strip().lower()
    is_video_flag = bool(post.get("is_video"))
    list_snippet = str(post.get("list_snippet") or "").strip().lower()
    full_text = str(post.get("detail_full_text") or "").strip().lower()

    if source_tab in {"视频", "video"}:
        return True
    if post_type in {"视频", "video", "short_video", "reel"}:
        return True
    if is_video_flag:
        return True

    video_markers = [
        "[视频]",
        "视频",
        "video",
        "reel",
        "播放",
    ]
    if any(marker in list_snippet for marker in video_markers):
        return True
    if any(marker in full_text for marker in video_markers):
        return True

    for key, value in post.items():
        key_lower = str(key).strip().lower()
        if "video" not in key_lower:
            continue

        if isinstance(value, bool) and value:
            return True
        if isinstance(value, (int, float)) and value > 0:
            return True
        if isinstance(value, str) and value.strip():
            return True

    return False


def is_from_allowed_tab(post: dict[str, Any], allowed_tab: str) -> bool:
    tab = str(post.get("source_tab") or "").strip()
    return tab == allowed_tab


def init_db(conn: sqlite3.Connection) -> None:
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(schema)


def start_run(conn: sqlite3.Connection, requested_limit: int | None, notes: str | None) -> str:
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    conn.execute(
        """
        INSERT INTO crawl_runs (run_id, started_at, status, requested_limit, notes)
        VALUES (?, ?, 'running', ?, ?)
        """,
        (run_id, now_iso(), requested_limit, notes),
    )
    return run_id


def finish_run(conn: sqlite3.Connection, run_id: str, status: str, collected_count: int) -> None:
    conn.execute(
        """
        UPDATE crawl_runs
        SET ended_at = ?, status = ?, collected_count = ?
        WHERE run_id = ?
        """,
        (now_iso(), status, collected_count, run_id),
    )


def upsert_post(conn: sqlite3.Connection, run_id: str, post: dict[str, Any], source_screen: str) -> tuple[bool, str, bool]:
    full_text = (post.get("detail_full_text") or "").strip()
    if not full_text:
        return False, "", False

    post_hash = content_hash(full_text)
    current_time = now_iso()
    existing = conn.execute(
        "SELECT 1 FROM posts WHERE content_hash = ? LIMIT 1",
        (post_hash,),
    ).fetchone()
    is_new_insert = existing is None

    conn.execute(
        """
        INSERT INTO posts (
            content_hash, author, time_text, list_snippet, full_text,
            truncated_detected, likes_or_comments_text, source_screen,
            first_seen, last_seen, run_id, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(content_hash) DO UPDATE SET
            author=excluded.author,
            time_text=excluded.time_text,
            list_snippet=excluded.list_snippet,
            truncated_detected=excluded.truncated_detected,
            likes_or_comments_text=excluded.likes_or_comments_text,
            source_screen=excluded.source_screen,
            last_seen=excluded.last_seen,
            run_id=excluded.run_id,
            raw_json=excluded.raw_json
        """,
        (
            post_hash,
            post.get("author"),
            post.get("time_text"),
            post.get("list_snippet"),
            full_text,
            1 if post.get("truncated_detected") else 0,
            post.get("likes_or_comments_text"),
            source_screen,
            current_time,
            current_time,
            run_id,
            json.dumps(post, ensure_ascii=False),
        ),
    )

    return True, post_hash, is_new_insert


def ingest(
    payload: dict[str, Any],
    source_screen: str,
    notes: str | None = None,
    disallow_video: bool = True,
    allowed_tab: str = "全部",
) -> dict[str, int | str]:
    posts = payload.get("posts") or []
    requested_limit = len(posts)

    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)
        run_id = start_run(conn, requested_limit=requested_limit, notes=notes)

        processed = 0
        inserted_new = 0
        updated_existing = 0
        skipped = 0
        skipped_video = 0
        skipped_non_all_tab = 0

        for post in posts:
            if not is_from_allowed_tab(post, allowed_tab=allowed_tab):
                skipped += 1
                skipped_non_all_tab += 1
                continue

            if disallow_video and is_video_post(post):
                skipped += 1
                skipped_video += 1
                continue

            ok, _, is_new_insert = upsert_post(conn, run_id=run_id, post=post, source_screen=source_screen)
            if ok:
                processed += 1
                if is_new_insert:
                    inserted_new += 1
                else:
                    updated_existing += 1
            else:
                skipped += 1

        finish_run(conn, run_id, status="completed", collected_count=processed)
        conn.commit()

        return {
            "run_id": run_id,
            "total_input": requested_limit,
            "processed": processed,
            "inserted_new": inserted_new,
            "updated_existing": updated_existing,
            "skipped": skipped,
            "skipped_video": skipped_video,
            "skipped_non_all_tab": skipped_non_all_tab,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Xiaokashu posts into SQLite")
    parser.add_argument("--input", required=True, help="Path to JSON payload with posts[]")
    parser.add_argument("--source-screen", default="xueqiu_xiaokashu", help="Source tag")
    parser.add_argument("--notes", default=None, help="Optional notes for this crawl run")
    parser.add_argument(
        "--allowed-tab",
        default="全部",
        help="Only ingest posts whose source_tab exactly matches this value (default: 全部)",
    )
    parser.add_argument(
        "--allow-video",
        action="store_true",
        help="Allow video posts to be ingested (default: filtered out)",
    )
    args = parser.parse_args()

    input_path = Path(args.input).resolve()
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    result = ingest(
        payload,
        source_screen=args.source_screen,
        notes=args.notes,
        disallow_video=not args.allow_video,
        allowed_tab=args.allowed_tab,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
