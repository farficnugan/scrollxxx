import os
import time
import sqlite3
import hashlib
from collections import defaultdict
from datetime import datetime, timezone
from functools import wraps
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder="static")

# Secret key for session security (generate a real one for production)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(32))

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DB_DIR, "scrollxxx.db")


# ── Security headers ────────────────────────────────────────────────────────

@app.after_request
def set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    # Allow Reddit video CDN for video sources
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "media-src 'self' https://v.redd.it; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "frame-ancestors 'none'"
    )
    return response


# ── Rate limiting (in-memory, per-IP) ───────────────────────────────────────

_rate_store = defaultdict(list)
RATE_LIMIT = 60        # requests per window
RATE_WINDOW = 60       # seconds


def rate_limit(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        ip = request.headers.get("X-Forwarded-For", request.remote_addr)
        if ip:
            ip = ip.split(",")[0].strip()
        now = time.time()
        # Clean old entries
        _rate_store[ip] = [t for t in _rate_store[ip] if now - t < RATE_WINDOW]
        if len(_rate_store[ip]) >= RATE_LIMIT:
            return jsonify({"error": "Rate limit exceeded. Try again later."}), 429
        _rate_store[ip].append(now)
        return f(*args, **kwargs)
    return decorated


# ── Database ────────────────────────────────────────────────────────────────

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
@rate_limit
def api_videos():
    page = max(1, request.args.get("page", 1, type=int))
    limit = min(50, max(1, request.args.get("limit", 20, type=int)))
    query = request.args.get("q", "").strip()[:200]  # Cap query length
    subs_param = request.args.get("subs", "").strip()
    seed = request.args.get("seed", "0", type=str)[:50]  # Cap seed length
    offset = (page - 1) * limit

    # Cap max offset to prevent deep pagination abuse
    if offset > 5000:
        return jsonify({"videos": [], "page": page, "has_more": False})

    # Parse subreddit filter (comma-separated, alphanumeric only, max 20)
    sub_filter = []
    if subs_param:
        sub_filter = [s.strip().lower() for s in subs_param.split(",")
                      if s.strip().isalnum()][:20]

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
            seed_hash = hashlib.md5(seed.encode()).hexdigest()
            seed_a = int(seed_hash[:8], 16) | 1
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
@rate_limit
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
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    print(f"ScrollXXX server running at http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=debug)
