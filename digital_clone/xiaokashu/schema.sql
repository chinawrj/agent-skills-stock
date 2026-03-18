PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS crawl_runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL,
    requested_limit INTEGER,
    collected_count INTEGER DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_hash TEXT NOT NULL UNIQUE,
    author TEXT,
    time_text TEXT,
    list_snippet TEXT,
    full_text TEXT NOT NULL,
    truncated_detected INTEGER NOT NULL DEFAULT 0,
    likes_or_comments_text TEXT,
    source_screen TEXT,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    run_id TEXT,
    raw_json TEXT,
    post_type TEXT,  -- 'bond', 'investment', 'life'
    FOREIGN KEY (run_id) REFERENCES crawl_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author);
CREATE INDEX IF NOT EXISTS idx_posts_time_text ON posts(time_text);
CREATE INDEX IF NOT EXISTS idx_posts_last_seen ON posts(last_seen);

CREATE TABLE IF NOT EXISTS post_chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_hash TEXT NOT NULL,
    chunk_order INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,
    run_id TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(content_hash, chunk_order),
    FOREIGN KEY (content_hash) REFERENCES posts(content_hash),
    FOREIGN KEY (run_id) REFERENCES crawl_runs(run_id)
);
