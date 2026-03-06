import os
import time
import sqlite3
import hashlib
from collections import defaultdict
from functools import wraps
from flask import Flask, jsonify, request, send_from_directory, redirect, Response

app = Flask(__name__, static_folder="static")

app.secret_key = os.environ.get("SECRET_KEY", os.urandom(32))

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DB_DIR, "scrollxxx.db")


# ── Security headers ────────────────────────────────────────────────────────

@app.after_request
def set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' *.googletagmanager.com *.google-analytics.com *.adsterra.com *.adsterratech.com; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "media-src *; "
        "img-src * data:; "
        "connect-src 'self' *.google-analytics.com *.analytics.google.com *.googletagmanager.com *.adsterra.com *.adsterratech.com; "
        "frame-src *.adsterra.com *.adsterratech.com; "
        "frame-ancestors 'none'"
    )
    return response


# ── Rate limiting (in-memory, per-IP) ───────────────────────────────────────

_rate_store = defaultdict(list)
RATE_LIMIT = 60
RATE_WINDOW = 60


def rate_limit(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        ip = request.headers.get("X-Forwarded-For", request.remote_addr)
        if ip:
            ip = ip.split(",")[0].strip()
        now = time.time()
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
            scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            media_type  TEXT DEFAULT 'video',
            category    TEXT DEFAULT 'Porn'
        );

        CREATE INDEX IF NOT EXISTS idx_posts_subreddit ON posts(subreddit);
        CREATE INDEX IF NOT EXISTS idx_posts_upvotes ON posts(upvotes);
    """)

    # Migration: add columns if they don't exist (for databases created before this version)
    columns = [row[1] for row in conn.execute("PRAGMA table_info(posts)").fetchall()]
    if "media_type" not in columns:
        conn.execute("ALTER TABLE posts ADD COLUMN media_type TEXT DEFAULT 'video'")
    if "category" not in columns:
        conn.execute("ALTER TABLE posts ADD COLUMN category TEXT DEFAULT 'Porn'")
    conn.commit()

    # Now create indexes on the new columns (safe after migration)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_media_type ON posts(media_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_category ON posts(category)")

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


@app.route("/favicon.png")
def favicon():
    return send_from_directory("static", "favicon.png")


# ── SEO Category Landing Pages ──────────────────────────────────────────────

CATEGORY_SEO = {
    "Porn": {
        "title": "Free Porn Videos",
        "desc": "Watch the hottest free porn videos in an endless scroll feed. New content added daily from top sources.",
        "keywords": "free porn, porn videos, watch porn online, best porn",
    },
    "Goth/Emo": {
        "title": "Goth & Emo Girls",
        "desc": "Goth girls, emo babes, and alt chicks. Scroll through the best goth and emo adult content for free.",
        "keywords": "goth girls, emo porn, alt girls, goth sluts, big tiddy goth gf",
    },
    "Latina": {
        "title": "Latina Porn Videos",
        "desc": "Hot Latina videos and photos. The best Latina adult content in one endless scroll feed.",
        "keywords": "latina porn, hot latinas, latina videos, latina girls",
    },
    "Ebony": {
        "title": "Ebony Porn Videos",
        "desc": "Beautiful ebony women in the hottest videos and photos. Scroll through the best ebony adult content.",
        "keywords": "ebony porn, ebony videos, black girls, ebony amateur",
    },
    "White Girl": {
        "title": "White Girl Videos",
        "desc": "Thick white girls, PAWGs, and more. The best white girl adult content in an endless feed.",
        "keywords": "white girls, pawg, thick white girls, white girl porn",
    },
    "Indian": {
        "title": "Indian Porn Videos",
        "desc": "Indian beauties and desi content. Scroll through the best Indian adult videos and photos.",
        "keywords": "indian porn, desi porn, indian girls, brown hotties",
    },
}


def build_landing_page(category, seo):
    """Generate an SEO-optimized landing page that redirects to the main feed."""
    cat_slug = category.lower().replace("/", "-").replace(" ", "-")
    full_title = f"{seo['title']} - ScrollXXX"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="referrer" content="no-referrer">
<title>{full_title}</title>
<meta name="description" content="{seo['desc']}">
<meta name="keywords" content="{seo['keywords']}">
<link rel="canonical" href="https://scrollxxx.vip/{cat_slug}">
<meta property="og:type" content="website">
<meta property="og:url" content="https://scrollxxx.vip/{cat_slug}">
<meta property="og:title" content="{full_title}">
<meta property="og:description" content="{seo['desc']}">
<meta property="og:site_name" content="ScrollXXX">
<meta name="twitter:card" content="summary">
<meta name="twitter:title" content="{full_title}">
<meta name="twitter:description" content="{seo['desc']}">
<link rel="icon" type="image/png" href="/favicon.png">
<script async src="https://www.googletagmanager.com/gtag/js?id=G-GWEV0G41KG"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{dataLayer.push(arguments);}}
  gtag('js', new Date());
  gtag('config', 'G-GWEV0G41KG');
</script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    background: #000; color: #fff; min-height: 100vh;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    display: flex; flex-direction: column; align-items: center; justify-content: center;
    padding: 40px 20px; text-align: center;
  }}
  h1 {{ font-size: 42px; margin-bottom: 16px; font-family: 'Bebas Neue', sans-serif; letter-spacing: 2px; }}
  p {{ font-size: 16px; color: rgba(255,255,255,0.7); max-width: 500px; line-height: 1.6; margin-bottom: 30px; }}
  .enter-btn {{
    display: inline-block; padding: 14px 40px;
    background: #e53935; color: #fff; text-decoration: none;
    border-radius: 8px; font-size: 18px; font-weight: 600;
    transition: background 0.2s;
  }}
  .enter-btn:hover {{ background: #c62828; }}
  .cats {{ display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin-top: 40px; max-width: 600px; }}
  .cats a {{
    padding: 8px 16px; background: rgba(255,255,255,0.06); color: rgba(255,255,255,0.6);
    text-decoration: none; border-radius: 6px; font-size: 13px; transition: background 0.2s;
  }}
  .cats a:hover {{ background: rgba(255,255,255,0.12); color: #fff; }}
  .cats a.active {{ background: #e53935; color: #fff; }}
</style>
</head>
<body>
<h1>{seo['title']}</h1>
<p>{seo['desc']}</p>
<a href="/?category={category}" class="enter-btn">Watch Now</a>
<nav class="cats">
{"".join(f'<a href="/{c.lower().replace("/", "-").replace(" ", "-")}"' + (' class="active"' if c == category else '') + f'>{c}</a>' for c in CATEGORY_SEO)}
</nav>
</body>
</html>"""


@app.route("/<slug>")
def category_landing(slug):
    # Map slug back to category name
    for cat in CATEGORY_SEO:
        cat_slug = cat.lower().replace("/", "-").replace(" ", "-")
        if slug == cat_slug:
            html = build_landing_page(cat, CATEGORY_SEO[cat])
            return Response(html, content_type="text/html")
    # Not a category — 404
    return "Not found", 404


@app.route("/api/videos")
@rate_limit
def api_videos():
    page = max(1, request.args.get("page", 1, type=int))
    limit = min(50, max(1, request.args.get("limit", 20, type=int)))
    query = request.args.get("q", "").strip()[:200]
    media_type = request.args.get("type", "video").strip().lower()
    cat_param = request.args.get("category", "").strip()
    seed = request.args.get("seed", "0", type=str)[:50]
    offset = (page - 1) * limit

    if offset > 5000:
        return jsonify({"videos": [], "page": page, "has_more": False})

    # Validate media_type
    if media_type not in ("video", "image"):
        media_type = "video"

    # Parse category filter (comma-separated)
    cat_filter = []
    if cat_param:
        cat_filter = [c.strip() for c in cat_param.split(",") if c.strip()][:10]

    conn = get_db()
    try:
        if query:
            safe_query = " ".join(
                word + "*" for word in query.split() if word.isalnum()
            )
            if not safe_query:
                return jsonify({"videos": [], "page": page, "has_more": False})

            if cat_filter:
                cat_placeholders = ",".join("?" * len(cat_filter))
                rows = conn.execute(f"""
                    SELECT p.id, p.title, p.video_url, p.subreddit, p.upvotes, p.media_type, p.category
                    FROM posts p
                    JOIN posts_fts fts ON p.rowid = fts.rowid
                    WHERE posts_fts MATCH ? AND p.media_type = ? AND p.category IN ({cat_placeholders})
                    ORDER BY fts.rank
                    LIMIT ? OFFSET ?
                """, (safe_query, media_type, *cat_filter, limit + 1, offset)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT p.id, p.title, p.video_url, p.subreddit, p.upvotes, p.media_type, p.category
                    FROM posts p
                    JOIN posts_fts fts ON p.rowid = fts.rowid
                    WHERE posts_fts MATCH ? AND p.media_type = ?
                    ORDER BY fts.rank
                    LIMIT ? OFFSET ?
                """, (safe_query, media_type, limit + 1, offset)).fetchall()
        else:
            seed_hash = hashlib.md5(seed.encode()).hexdigest()
            seed_a = int(seed_hash[:8], 16) | 1
            seed_b = int(seed_hash[8:16], 16)

            if cat_filter:
                cat_placeholders = ",".join("?" * len(cat_filter))
                rows = conn.execute(f"""
                    SELECT id, title, video_url, subreddit, upvotes, media_type, category
                    FROM posts
                    WHERE media_type = ? AND category IN ({cat_placeholders})
                    ORDER BY (rowid * ? + ?) % 999983
                    LIMIT ? OFFSET ?
                """, (media_type, *cat_filter, seed_a, seed_b, limit + 1, offset)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT id, title, video_url, subreddit, upvotes, media_type, category
                    FROM posts
                    WHERE media_type = ?
                    ORDER BY (rowid * ? + ?) % 999983
                    LIMIT ? OFFSET ?
                """, (media_type, seed_a, seed_b, limit + 1, offset)).fetchall()

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
        total_videos = conn.execute("SELECT COUNT(*) FROM posts WHERE media_type='video'").fetchone()[0]
        total_images = conn.execute("SELECT COUNT(*) FROM posts WHERE media_type='image'").fetchone()[0]
        cats = conn.execute(
            "SELECT DISTINCT category FROM posts ORDER BY category"
        ).fetchall()
        return jsonify({
            "total_videos": total_videos,
            "total_images": total_images,
            "total": total,
            "categories": [r[0] for r in cats],
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
