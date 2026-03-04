"""
ScrollXXX Reddit Video Scraper

Scrapes video posts from configured subreddits and stores them in the database.

Usage:
    python scraper.py                  # scrape once
    python scraper.py --loop           # scrape every 3 hours (run in background)
    python scraper.py --loop --interval 1  # scrape every 1 hour
    python scraper.py funny memes      # scrape specific subreddits once
"""

import os
import sys
import time
import sqlite3
from datetime import datetime, timezone

import requests

# ── Config ───────────────────────────────────────────────────────────────────

SUBREDDITS = [
    "funny",
    "cats",
    "memes",
    "dogs",
    "animals",
    "cute",
]

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
    "User-Agent": "ScrollXXX-Scraper/1.0 (educational project)",
}


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
            scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_posts_subreddit ON posts(subreddit);
        CREATE INDEX IF NOT EXISTS idx_posts_upvotes ON posts(upvotes);
    """)
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
        INSERT OR IGNORE INTO posts (id, title, video_url, subreddit, upvotes, created_utc, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        post["id"],
        post["title"],
        post["video_url"],
        post["subreddit"],
        post["upvotes"],
        post["created_utc"],
        datetime.now(timezone.utc).isoformat(),
    ))


# ── Reddit Scraping ─────────────────────────────────────────────────────────

def scrape_subreddit(subreddit, sort="hot", time_filter=None, limit=POSTS_PER_REQUEST, pages=3):
    """Scrape video posts from a subreddit."""
    videos = []
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
            if not post.get("is_video"):
                continue
            reddit_video = (post.get("media") or {}).get("reddit_video")
            if not reddit_video:
                continue

            fallback = reddit_video.get("fallback_url", "")
            if not fallback:
                continue

            video_url = fallback.split("?")[0]
            created = datetime.fromtimestamp(
                post.get("created_utc", 0), tz=timezone.utc
            ).isoformat()

            videos.append({
                "id": post["id"],
                "title": post.get("title", ""),
                "video_url": video_url,
                "subreddit": subreddit,
                "upvotes": post.get("ups", 0),
                "created_utc": created,
            })

        if not after:
            break

        # Be polite to Reddit's servers
        time.sleep(1.5)

    return videos


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    # Allow overriding subreddits via CLI args
    subs = sys.argv[1:] if len(sys.argv) > 1 else SUBREDDITS

    conn = get_db()
    total_new = 0

    for sub in subs:
        print(f"\n{'='*50}")
        print(f"Scraping r/{sub}")
        print(f"{'='*50}")

        sub_total = 0
        for sort, tf, pages in SCRAPE_MODES:
            label = f"{sort}/{tf}" if tf else sort
            print(f"  [{label}] fetching...", end=" ", flush=True)
            videos = scrape_subreddit(sub, sort=sort, time_filter=tf, pages=pages)
            print(f"found {len(videos)} videos")

            before = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
            for v in videos:
                insert_post(conn, v)
            conn.commit()
            after_count = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]

            new = after_count - before
            sub_total += new
            if new:
                print(f"        +{new} new posts added")

            time.sleep(1)

        total_new += sub_total
        print(f"  Total new from r/{sub}: {sub_total}")

    total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    conn.close()

    print(f"\n{'='*50}")
    print(f"Done! +{total_new} new videos added. {total} total in database.")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ScrollXXX Reddit Scraper")
    parser.add_argument("subreddits", nargs="*", help="Subreddits to scrape (default: config list)")
    parser.add_argument("--loop", action="store_true", help="Run continuously on a schedule")
    parser.add_argument("--interval", type=float, default=3, help="Hours between scrapes (default: 3)")
    args = parser.parse_args()

    # Override sys.argv for main() compatibility
    if args.subreddits:
        sys.argv = [sys.argv[0]] + args.subreddits

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
