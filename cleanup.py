"""
ScrollXXX Broken Content Cleanup

Checks all media URLs in the database and removes entries that return errors
(403, 404, timeouts, etc). Keeps the feed clean of dead links.

Usage:
    python cleanup.py                       # dry run (shows what would be removed)
    python cleanup.py --delete              # actually remove broken entries
    python cleanup.py --delete --loop       # run every 48 hours automatically
    python cleanup.py --delete --loop --interval 24  # custom interval in hours
"""

import os
import sqlite3
import argparse
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DB_DIR, "scrollxxx.db")

# Only send a HEAD request — don't download the whole file
TIMEOUT = 15
MAX_WORKERS = 20  # concurrent checks

# Some CDNs block HEAD, so fall back to a range GET
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Range": "bytes=0-0",
}


def check_url(row):
    """Check if a URL is still alive. Returns (id, url, status, ok)."""
    post_id, url, media_type = row
    try:
        # Try HEAD first (fastest)
        r = requests.head(url, timeout=TIMEOUT, allow_redirects=True, headers={
            "User-Agent": HEADERS["User-Agent"]
        })
        if r.status_code == 405:
            # HEAD not allowed, try GET with range
            r = requests.get(url, timeout=TIMEOUT, allow_redirects=True, headers=HEADERS, stream=True)
            r.close()

        ok = r.status_code < 400
        return (post_id, url, media_type, r.status_code, ok)
    except requests.exceptions.Timeout:
        return (post_id, url, media_type, 0, False)
    except requests.exceptions.ConnectionError:
        return (post_id, url, media_type, 0, False)
    except Exception:
        return (post_id, url, media_type, -1, False)


def run_cleanup(delete=False, batch_size=200):
    """Run one cleanup pass. Returns (checked, broken_count)."""
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return 0, 0

    conn = sqlite3.connect(DB_PATH)
    total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting cleanup — {total:,} entries in database")

    if not delete:
        print("** DRY RUN — use --delete to actually remove broken entries **\n")

    broken = []
    checked = 0
    offset = 0
    start = time.time()

    while offset < total:
        rows = conn.execute(
            "SELECT id, video_url, media_type FROM posts LIMIT ? OFFSET ?",
            (batch_size, offset)
        ).fetchall()
        if not rows:
            break

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(check_url, row): row for row in rows}
            for future in as_completed(futures):
                result = future.result()
                post_id, url, media_type, status, ok = result
                checked += 1

                if not ok:
                    broken.append(post_id)
                    short_url = url[:80] + "..." if len(url) > 80 else url
                    print(f"  BROKEN [{status}] ({media_type}) {short_url}")

                if checked % 500 == 0:
                    elapsed = time.time() - start
                    rate = checked / elapsed if elapsed > 0 else 0
                    print(f"  ... checked {checked:,}/{total:,} ({rate:.0f}/sec) — {len(broken)} broken so far")

        offset += batch_size

    elapsed = time.time() - start
    print(f"\nDone. Checked {checked:,} entries in {elapsed:.1f}s")
    print(f"Found {len(broken):,} broken entries ({len(broken)/max(checked,1)*100:.1f}%)")

    if broken and delete:
        del_batch = 500
        for i in range(0, len(broken), del_batch):
            batch = broken[i:i+del_batch]
            placeholders = ",".join("?" * len(batch))
            conn.execute(f"DELETE FROM posts WHERE id IN ({placeholders})", batch)
        conn.execute("DELETE FROM posts_fts WHERE rowid NOT IN (SELECT rowid FROM posts)")
        conn.commit()
        print(f"Deleted {len(broken):,} broken entries from database.")

        conn.execute("VACUUM")
        print("Database vacuumed.")
    elif broken and not delete:
        print("\nRun with --delete to remove these entries.")

    conn.close()
    return checked, len(broken)


def main():
    parser = argparse.ArgumentParser(description="Clean up broken media URLs from the database")
    parser.add_argument("--delete", action="store_true", help="Actually delete broken entries (default is dry run)")
    parser.add_argument("--batch", type=int, default=200, help="Batch size for checking (default: 200)")
    parser.add_argument("--loop", action="store_true", help="Run continuously on an interval")
    parser.add_argument("--interval", type=int, default=48, help="Hours between cleanup runs (default: 48)")
    args = parser.parse_args()

    if args.loop:
        print(f"ScrollXXX Cleanup — running every {args.interval} hours (Ctrl+C to stop)")
        while True:
            try:
                run_cleanup(delete=args.delete, batch_size=args.batch)
                print(f"\nNext cleanup in {args.interval} hours...")
                time.sleep(args.interval * 3600)
            except KeyboardInterrupt:
                print("\nStopped.")
                break
    else:
        run_cleanup(delete=args.delete, batch_size=args.batch)


if __name__ == "__main__":
    main()
