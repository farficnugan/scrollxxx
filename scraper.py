"""
ScrollXXX Reddit Video & Image Scraper

Scrapes video and image posts from configured subreddits and stores them in the database.

Usage:
    python scraper.py                  # scrape once
    python scraper.py --loop           # scrape every 3 hours (run in background)
    python scraper.py --loop --interval 1  # scrape every 1 hour
"""

import os
import sys
import time
import sqlite3
from datetime import datetime, timezone

import requests

# ── Config ───────────────────────────────────────────────────────────────────

CATEGORIES = {
    "Porn": [
        "porn",
        "bestporningalaxy",
        "girlswatchingporn",
        "stepsisters_porn",
        "pornid",
        "long_porn",
        "porn_incest",
        "porn_with_sounds",
        "pornism",
        "homemadeporntub",
        "realhomeporn",
        "toocuteforporn",
    ],
    "Goth/Emo": [
        "gothsluts",
        "gothwhoress",
        "bigtiddygothgf",
        "thickgothgirls",
        "gothgirlsgw",
        "emogirlsfuck",
        "gothblowjobs",
        "emogirlsfucking",
    ],
    "Latina": [
        "Latinas",
        "latinateensgonewild",
        "latinchickswhitedicks",
        "latinasbj",
        "hotlatinaporn",
    ],
    "Ebony": [
        "ebonyamateurs",
        "ebony",
        "ebonyqueenstakingdick",
        "blackchickswhitedicks",
        "bestebonyporn",
        "ebonycumfaces",
    ],
    "White Girl": [
        "thickwhitegirls",
        "pawg",
        "whitegirlsnsfw",
    ],
    "Indian": [
        "indianporn_nsfw",
        "indiangoddess",
        "brownhotties",
        "indianinstabaddies",
        "brownchickswhitedicks",
        "indiansgonewild",
    ],
}

# Build a flat lookup: subreddit_name (lowercase) -> category
SUB_TO_CATEGORY = {}
ALL_SUBREDDITS = []
for cat, subs in CATEGORIES.items():
    for sub in subs:
        SUB_TO_CATEGORY[sub.lower()] = cat
        ALL_SUBREDDITS.append(sub)

# Each entry is (sort, time_filter_or_None, pages_to_fetch)
SCRAPE_MODES = [
    ("hot",  None,    3),
    ("new",  None,    3),
    ("top",  "all",   3),
    ("top",  "year",  3),
    ("top",  "month", 2),
    ("top",  "week",  2),
    ("top",  "day",   1),
]
POSTS_PER_REQUEST = 100   # Reddit max is 100

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DB_DIR, "scrollxxx.db")

HEADERS = {
    "User-Agent": "ScrollXXX-Scraper/2.0",
}

IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".webp")
VIDEO_DOMAINS = ("redgifs.com", "v.redd.it")

# Redgifs API token (fetched once per session)
_redgifs_token = None


def get_redgifs_token():
    """Get a temporary auth token from Redgifs API."""
    global _redgifs_token
    if _redgifs_token:
        return _redgifs_token
    try:
        resp = requests.get("https://api.redgifs.com/v2/auth/temporary", timeout=10)
        resp.raise_for_status()
        _redgifs_token = resp.json().get("token")
        return _redgifs_token
    except Exception as e:
        print(f"  [!] Failed to get Redgifs token: {e}")
        return None


def resolve_redgifs_url(url):
    """Resolve a redgifs.com link to a direct video URL via their API."""
    # Extract the GIF ID from URLs like https://redgifs.com/watch/someid
    # or https://www.redgifs.com/watch/someid
    parts = url.rstrip("/").split("/")
    gif_id = parts[-1].split("?")[0].split("#")[0]
    if not gif_id:
        return None

    token = get_redgifs_token()
    if not token:
        return None

    try:
        resp = requests.get(
            f"https://api.redgifs.com/v2/gifs/{gif_id.lower()}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        resp.raise_for_status()
        urls = resp.json().get("gif", {}).get("urls", {})
        # Prefer HD, fall back to SD
        return urls.get("hd") or urls.get("sd")
    except Exception:
        return None


# ── Database ─────────────────────────────────────────────────────────────────

def get_db():
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    # Ensure tables exist (scraper may run before server)
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
        CREATE INDEX IF NOT EXISTS idx_posts_media_type ON posts(media_type);
        CREATE INDEX IF NOT EXISTS idx_posts_category ON posts(category);
    """)

    # Migration: add columns if they don't exist (for existing databases)
    columns = [row[1] for row in conn.execute("PRAGMA table_info(posts)").fetchall()]
    if "media_type" not in columns:
        conn.execute("ALTER TABLE posts ADD COLUMN media_type TEXT DEFAULT 'video'")
    if "category" not in columns:
        conn.execute("ALTER TABLE posts ADD COLUMN category TEXT DEFAULT 'Porn'")
    conn.commit()

    # FTS5 table for search
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='posts_fts'"
    ).fetchone()
    if not row:
        conn.execute("""
            CREATE VIRTUAL TABLE posts_fts USING fts5(
                title, content='posts', content_rowid='rowid'
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
    return conn


def insert_post(conn, post):
    """Insert a post, ignoring duplicates (ON CONFLICT DO NOTHING)."""
    conn.execute("""
        INSERT OR IGNORE INTO posts (id, title, video_url, subreddit, upvotes, created_utc, scraped_at, media_type, category)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        post["id"],
        post["title"],
        post["video_url"],
        post["subreddit"],
        post["upvotes"],
        post["created_utc"],
        datetime.now(timezone.utc).isoformat(),
        post["media_type"],
        post["category"],
    ))


# ── Reddit Scraping ─────────────────────────────────────────────────────────

def scrape_subreddit(subreddit, category, sort="hot", time_filter=None, limit=POSTS_PER_REQUEST, pages=3):
    """Scrape video and image posts from a subreddit."""
    posts = []
    after = None

    for page in range(pages):
        url = f"https://www.reddit.com/r/{subreddit}/{sort}.json"
        params = {"limit": limit, "raw_json": 1}
        if after:
            params["after"] = after
        if time_filter:
            params["t"] = time_filter

        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [!] Request failed (page {page + 1}): {e}")
            break

        data = resp.json().get("data", {})
        after = data.get("after")
        children = data.get("children", [])

        if not children:
            break

        for child in children:
            post = child.get("data", {})
            created = datetime.fromtimestamp(
                post.get("created_utc", 0), tz=timezone.utc
            ).isoformat()

            post_url = post.get("url", "")
            post_url_lower = post_url.lower()

            # ── VIDEO: Reddit native (v.redd.it) ──
            if post.get("is_video"):
                reddit_video = (post.get("media") or {}).get("reddit_video")
                if reddit_video:
                    fallback = reddit_video.get("fallback_url", "")
                    if fallback:
                        video_url = fallback.split("?")[0]
                        posts.append({
                            "id": post["id"],
                            "title": post.get("title", ""),
                            "video_url": video_url,
                            "subreddit": subreddit,
                            "upvotes": post.get("ups", 0),
                            "created_utc": created,
                            "media_type": "video",
                            "category": category,
                        })
                continue

            # ── VIDEO: Redgifs links ──
            if "redgifs.com" in post_url_lower:
                video_url = resolve_redgifs_url(post_url)
                if video_url:
                    posts.append({
                        "id": post["id"],
                        "title": post.get("title", ""),
                        "video_url": video_url,
                        "subreddit": subreddit,
                        "upvotes": post.get("ups", 0),
                        "created_utc": created,
                        "media_type": "video",
                        "category": category,
                    })
                continue

            # ── VIDEO: Direct .mp4/.webm links (e.g. from Imgur) ──
            if post_url_lower.endswith(".mp4") or post_url_lower.endswith(".webm"):
                posts.append({
                    "id": post["id"],
                    "title": post.get("title", ""),
                    "video_url": post_url,
                    "subreddit": subreddit,
                    "upvotes": post.get("ups", 0),
                    "created_utc": created,
                    "media_type": "video",
                    "category": category,
                })
                continue

            # ── VIDEO: Embedded media from other sources (gfycat, etc.) ──
            preview = post.get("preview") or {}
            reddit_video_preview = preview.get("reddit_video_preview")
            if reddit_video_preview:
                fallback = reddit_video_preview.get("fallback_url", "")
                if fallback:
                    video_url = fallback.split("?")[0]
                    posts.append({
                        "id": post["id"],
                        "title": post.get("title", ""),
                        "video_url": video_url,
                        "subreddit": subreddit,
                        "upvotes": post.get("ups", 0),
                        "created_utc": created,
                        "media_type": "video",
                        "category": category,
                    })
                    continue

            # ── IMAGE: Direct image link ──
            if any(post_url_lower.endswith(ext) for ext in IMAGE_EXTENSIONS):
                posts.append({
                    "id": post["id"],
                    "title": post.get("title", ""),
                    "video_url": post_url,
                    "subreddit": subreddit,
                    "upvotes": post.get("ups", 0),
                    "created_utc": created,
                    "media_type": "image",
                    "category": category,
                })
                continue

            # ── IMAGE: Reddit-hosted (i.redd.it) ──
            if "i.redd.it" in post_url:
                posts.append({
                    "id": post["id"],
                    "title": post.get("title", ""),
                    "video_url": post_url,
                    "subreddit": subreddit,
                    "upvotes": post.get("ups", 0),
                    "created_utc": created,
                    "media_type": "image",
                    "category": category,
                })
                continue

            # ── IMAGE: Reddit gallery — grab first image ──
            if post.get("is_gallery"):
                media_metadata = post.get("media_metadata") or {}
                for key, meta in media_metadata.items():
                    if meta.get("status") == "valid" and meta.get("m", "").startswith("image/"):
                        source = (meta.get("s") or {}).get("u", "")
                        if source:
                            source = source.replace("&amp;", "&")
                            posts.append({
                                "id": post["id"] + "_" + key,
                                "title": post.get("title", ""),
                                "video_url": source,
                                "subreddit": subreddit,
                                "upvotes": post.get("ups", 0),
                                "created_utc": created,
                                "media_type": "image",
                                "category": category,
                            })
                            break

        if not after:
            break

        # Be polite to Reddit's servers
        time.sleep(1.5)

    return posts


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    conn = get_db()
    total_new = 0

    for category, subs in CATEGORIES.items():
        print(f"\n{'='*50}")
        print(f"Category: {category} ({len(subs)} subreddits)")
        print(f"{'='*50}")

        for sub in subs:
            print(f"\n  Scraping r/{sub}")
            sub_total = 0
            for sort, tf, pages in SCRAPE_MODES:
                label = f"{sort}/{tf}" if tf else sort
                print(f"    [{label}] fetching...", end=" ", flush=True)
                posts = scrape_subreddit(sub, category, sort=sort, time_filter=tf, pages=pages)
                vids = sum(1 for p in posts if p["media_type"] == "video")
                imgs = sum(1 for p in posts if p["media_type"] == "image")
                print(f"found {vids} videos, {imgs} images")

                before = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
                for p in posts:
                    insert_post(conn, p)
                conn.commit()
                after_count = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]

                new = after_count - before
                sub_total += new
                if new:
                    print(f"           +{new} new posts added")

                time.sleep(1)

            total_new += sub_total
            print(f"    Total new from r/{sub}: {sub_total}")

    total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    total_vids = conn.execute("SELECT COUNT(*) FROM posts WHERE media_type='video'").fetchone()[0]
    total_imgs = conn.execute("SELECT COUNT(*) FROM posts WHERE media_type='image'").fetchone()[0]
    conn.close()

    print(f"\n{'='*50}")
    print(f"Done! +{total_new} new posts added.")
    print(f"Total in database: {total} ({total_vids} videos, {total_imgs} images)")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ScrollXXX Reddit Scraper")
    parser.add_argument("--loop", action="store_true", help="Run continuously on a schedule")
    parser.add_argument("--interval", type=float, default=3, help="Hours between scrapes (default: 3)")
    args = parser.parse_args()

    if args.loop:
        interval_sec = args.interval * 3600
        print(f"ScrollXXX Scraper — loop mode (every {args.interval}h)")
        print(f"Press Ctrl+C to stop.\n")
        while True:
            try:
                main()
                print(f"\nSleeping {args.interval}h until next scrape...\n")
                time.sleep(interval_sec)
            except KeyboardInterrupt:
                print("\nStopped.")
                break
    else:
        main()
