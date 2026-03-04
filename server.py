import os
import sqlite3
import hashlib
from datetime import datetime, timezone
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder="static")

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DB_DIR, "scrollxxx.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    os.makedirs(DB_DIR, exist_ok=True)
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS posts (
            id          TEXT PRIMARY KEY,
            title       TEXT NOT NULL,
            video_url   TEXT NOT NULL,
            subreddit   TEXT NOT NULL,
            upvotes     INTEGER DEFAULT 0,
            created_utc TIMESTAMP,
            scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_posts_subreddit ON posts(subreddit);
        CREATE INDEX IF NOT EXISTS idx_posts_upvotes ON posts(upvotes);
    """)

    # FTS5 virtual table for fast title search
    # Check if it exists first (CREATE IF NOT EXISTS doesn't work for virtual tables)
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='posts_fts'"
    ).fetchone()
    if not row:
        conn.execute("""
            CREATE VIRTUAL TABLE posts_fts USING fts5(
                title,
                content='posts',
                content_rowid='rowid'
            )
        """)
        # Triggers to keep FTS index in sync with posts table
        conn.executescript("""
            CREATE TRIGGER IF NOT EXISTS posts_ai AFTER INSERT ON posts BEGIN
                INSERT INTO posts_fts(rowid, title) VALUES (new.rowid, new.title);
            END;
            CREATE TRIGGER IF NOT EXISTS posts_ad AFTER DELETE ON posts BEGIN
                INSERT INTO posts_fts(posts_fts, rowid, title)
                    VALUES('delete', old.rowid, old.title);
            END;
            CREATE TRIGGER IF NOT EXISTS posts_au AFTER UPDATE ON posts BEGIN
                INSERT INTO posts_fts(posts_fts, rowid, title)
                    VALUES('delete', old.rowid, old.title);
                INSERT INTO posts_fts(rowid, title) VALUES (new.rowid, new.title);
            END;
        """)

    conn.commit()
    conn.close()


# ── API ──────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/videos")
def api_videos():
    page = max(1, request.args.get("page", 1, type=int))
    limit = min(50, max(1, request.args.get("limit", 20, type=int)))
    query = request.args.get("q", "").strip()
    subs_param = request.args.get("subs", "").strip()
    # Seed for randomization — same seed = same order (stable pagination)
    # Different seed = different shuffle (new visit = new order)
    seed = request.args.get("seed", "0", type=str)
    offset = (page - 1) * limit

    # Parse subreddit filter (comma-separated, alphanumeric only)
    sub_filter = []
    if subs_param:
        sub_filter = [s.strip().lower() for s in subs_param.split(",") if s.strip().isalnum()]

    conn = get_db()
    try:
        if query:
            safe_query = " ".join(
                word + "*" for word in query.split() if word.isalnum()
            )
            if not safe_query:
                return jsonify({"videos": [], "page": page, "has_more": False})

            if sub_filter:
                placeholders = ",".join("?" * len(sub_filter))
                rows = conn.execute(f"""
                    SELECT p.id, p.title, p.video_url, p.subreddit, p.upvotes
                    FROM posts p
                    JOIN posts_fts fts ON p.rowid = fts.rowid
                    WHERE posts_fts MATCH ? AND p.subreddit IN ({placeholders})
                    ORDER BY fts.rank
                    LIMIT ? OFFSET ?
                """, (safe_query, *sub_filter, limit + 1, offset)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT p.id, p.title, p.video_url, p.subreddit, p.upvotes
                    FROM posts p
                    JOIN posts_fts fts ON p.rowid = fts.rowid
                    WHERE posts_fts MATCH ?
                    ORDER BY fts.rank
                    LIMIT ? OFFSET ?
                """, (safe_query, limit + 1, offset)).fetchall()
        else:
            # Seeded shuffle using rowid (unique per row) for true randomization.
            # (rowid * A + B) % P gives a deterministic permutation per seed.
            # Same seed = stable pagination. New seed = completely different order.
            seed_hash = hashlib.md5(seed.encode()).hexdigest()
            seed_a = int(seed_hash[:8], 16) | 1  # ensure odd (coprime with P)
            seed_b = int(seed_hash[8:16], 16)

            if sub_filter:
                placeholders = ",".join("?" * len(sub_filter))
                rows = conn.execute(f"""
                    SELECT id, title, video_url, subreddit, upvotes
                    FROM posts
                    WHERE subreddit IN ({placeholders})
                    ORDER BY (rowid * ? + ?) % 999983
                    LIMIT ? OFFSET ?
                """, (*sub_filter, seed_a, seed_b, limit + 1, offset)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT id, title, video_url, subreddit, upvotes
                    FROM posts
                    ORDER BY (rowid * ? + ?) % 999983
                    LIMIT ? OFFSET ?
                """, (seed_a, seed_b, limit + 1, offset)).fetchall()

        has_more = len(rows) > limit
        videos = [dict(r) for r in rows[:limit]]
        return jsonify({"videos": videos, "page": page, "has_more": has_more})
    finally:
        conn.close()


@app.route("/api/stats")
def api_stats():
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        subs = conn.execute(
            "SELECT DISTINCT subreddit FROM posts ORDER BY subreddit"
        ).fetchall()
        return jsonify({
            "total_videos": total,
            "subreddits": [r[0] for r in subs],
        })
    finally:
        conn.close()


# ── Startup ──────────────────────────────────────────────────────────────────

init_db()

if __name__ == "__main__":
    print("ScrollXXX server running at http://localhost:5000")
    app.run(debug=True, port=5000)
