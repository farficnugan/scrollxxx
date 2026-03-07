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
    if "thumbnail" not in columns:
        conn.execute("ALTER TABLE posts ADD COLUMN thumbnail TEXT DEFAULT ''")
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


@app.route("/sitemap.xml")
def dynamic_sitemap():
    """Generate a sitemap with all static pages + individual watch pages."""
    # Static pages
    static_urls = [
        ("https://scrollxxx.vip/", "daily", "1.0"),
        ("https://scrollxxx.vip/discover", "daily", "0.9"),
        ("https://scrollxxx.vip/info", "monthly", "0.3"),
    ]

    urls_xml = ""
    for loc, freq, priority in static_urls:
        urls_xml += f"""  <url>
    <loc>{loc}</loc>
    <changefreq>{freq}</changefreq>
    <priority>{priority}</priority>
  </url>\n"""

    # Category pages
    for cat in CATEGORY_SEO:
        slug = cat.lower().replace("/", "-").replace(" ", "-")
        urls_xml += f"""  <url>
    <loc>https://scrollxxx.vip/{slug}</loc>
    <changefreq>daily</changefreq>
    <priority>0.8</priority>
  </url>\n"""

    # Individual watch pages (up to 50,000 — sitemap limit)
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id FROM posts ORDER BY upvotes DESC LIMIT 49000"
        ).fetchall()
    finally:
        conn.close()

    for row in rows:
        urls_xml += f"""  <url>
    <loc>https://scrollxxx.vip/watch/{row["id"]}</loc>
    <changefreq>weekly</changefreq>
    <priority>0.5</priority>
  </url>\n"""

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{urls_xml}</urlset>"""
    return Response(xml, content_type="application/xml")


# ── SEO Category Landing Pages ──────────────────────────────────────────────

CATEGORY_SEO = {
    "Porn": {
        "title": "Free Porn Videos",
        "desc": "Watch the hottest free porn videos in an endless scroll feed. New content added daily from top sources.",
        "keywords": "free porn, porn videos, watch porn online, best porn",
        "emoji": "🔥",
    },
    "Goth/Emo": {
        "title": "Goth & Emo Girls",
        "desc": "Goth girls, emo babes, and alt chicks. Scroll through the best goth and emo adult content for free.",
        "keywords": "goth girls, emo porn, alt girls, goth sluts, big tiddy goth gf",
        "emoji": "🖤",
    },
    "Latina": {
        "title": "Latina Porn Videos",
        "desc": "Hot Latina videos and photos. The best Latina adult content in one endless scroll feed.",
        "keywords": "latina porn, hot latinas, latina videos, latina girls",
        "emoji": "🌶️",
    },
    "Ebony": {
        "title": "Ebony Porn Videos",
        "desc": "Beautiful ebony women in the hottest videos and photos. Scroll through the best ebony adult content.",
        "keywords": "ebony porn, ebony videos, black girls, ebony amateur",
        "emoji": "👑",
    },
    "White Girl": {
        "title": "White Girl Videos",
        "desc": "Thick white girls, PAWGs, and more. The best white girl adult content in an endless feed.",
        "keywords": "white girls, pawg, thick white girls, white girl porn",
        "emoji": "🍑",
    },
    "Indian": {
        "title": "Indian Porn Videos",
        "desc": "Indian beauties and desi content. Scroll through the best Indian adult videos and photos.",
        "keywords": "indian porn, desi porn, indian girls, brown hotties",
        "emoji": "💎",
    },
    "Asian": {
        "title": "Asian Porn Videos",
        "desc": "The hottest Asian girls in videos and photos. Japanese, Korean, Chinese and more in an endless feed.",
        "keywords": "asian porn, japanese porn, korean porn, asian girls",
        "emoji": "🌸",
    },
    "MILF": {
        "title": "MILF Porn Videos",
        "desc": "Hot MILFs and cougars in the best adult videos. Mature women who know what they're doing.",
        "keywords": "milf porn, milf videos, mature porn, cougar porn, hot moms",
        "emoji": "💋",
    },
    "Teen": {
        "title": "Teen Porn Videos (18+)",
        "desc": "Young adult (18+) content. Petite teens and college girls in an endless scroll feed.",
        "keywords": "teen porn 18+, college girls, young adult porn, petite girls",
        "emoji": "🎀",
    },
    "Amateur": {
        "title": "Amateur Porn Videos",
        "desc": "Real amateur content from real couples. Homemade videos and photos, no scripts, no studios.",
        "keywords": "amateur porn, homemade porn, real couples, amateur videos",
        "emoji": "📹",
    },
    "Anal": {
        "title": "Anal Porn Videos",
        "desc": "The best anal content in an endless scroll feed. Videos and photos updated daily.",
        "keywords": "anal porn, anal videos, anal sex, best anal",
        "emoji": "🍑",
    },
    "Lesbian": {
        "title": "Lesbian Porn Videos",
        "desc": "Girl on girl action. The hottest lesbian content in videos and photos, completely free.",
        "keywords": "lesbian porn, girl on girl, lesbian videos, lesbian sex",
        "emoji": "👩‍❤️‍👩",
    },
    "Big Tits": {
        "title": "Big Tits Porn Videos",
        "desc": "Busty babes with big tits. The best big boob content in an endless scroll feed.",
        "keywords": "big tits, big boobs, busty, big tits porn",
        "emoji": "🍈",
    },
    "Big Ass": {
        "title": "Big Ass Porn Videos",
        "desc": "Thick girls with big asses. PAWGs, booties, and the best big ass content online.",
        "keywords": "big ass, thick girls, pawg, big booty, big ass porn",
        "emoji": "🍑",
    },
    "Blowjob": {
        "title": "Blowjob Porn Videos",
        "desc": "The best blowjob and oral content. Deepthroat, sloppy, and more in an endless feed.",
        "keywords": "blowjob porn, oral sex, deepthroat, blowjob videos",
        "emoji": "👅",
    },
    "Creampie": {
        "title": "Creampie Porn Videos",
        "desc": "The best creampie content in videos and photos. Updated daily with new content.",
        "keywords": "creampie porn, creampie videos, internal cumshot",
        "emoji": "💦",
    },
    "Redhead": {
        "title": "Redhead Porn Videos",
        "desc": "Fiery redheads in the hottest adult content. Ginger girls in an endless scroll feed.",
        "keywords": "redhead porn, ginger girls, redhead videos, red hair porn",
        "emoji": "🔥",
    },
    "Threesome": {
        "title": "Threesome Porn Videos",
        "desc": "The best threesome and group content. FFM, MMF, and more in an endless feed.",
        "keywords": "threesome porn, group sex, ffm, mmf, threesome videos",
        "emoji": "👥",
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
<meta name="robots" content="index, follow">
<meta name="rating" content="adult">
<script type="application/ld+json">
{{
  "@context": "https://schema.org",
  "@type": "CollectionPage",
  "name": "{seo['title']}",
  "url": "https://scrollxxx.vip/{cat_slug}",
  "description": "{seo['desc']}",
  "isPartOf": {{
    "@type": "WebSite",
    "name": "ScrollXXX",
    "url": "https://scrollxxx.vip/"
  }}
}}
</script>
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


@app.route("/discover")
def discover_page():
    conn = get_db()
    try:
        counts = {}
        for row in conn.execute("SELECT category, COUNT(*) FROM posts GROUP BY category").fetchall():
            counts[row[0]] = row[1]
    finally:
        conn.close()

    cards_html = ""
    for cat, seo in CATEGORY_SEO.items():
        cat_slug = cat.lower().replace("/", "-").replace(" ", "-")
        count = counts.get(cat, 0)
        emoji = seo.get("emoji", "🔥")
        cards_html += f'''
        <a href="/{cat_slug}" class="cat-card">
          <div class="cat-emoji">{emoji}</div>
          <div class="cat-name">{cat}</div>
          <div class="cat-count">{count:,} videos</div>
        </a>'''

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="referrer" content="no-referrer">
<title>Discover Categories - ScrollXXX</title>
<meta name="description" content="Browse all categories on ScrollXXX. Find your favorite niche — porn, latina, ebony, goth, asian, MILF, amateur and more. Free endless scroll adult content.">
<meta name="keywords" content="porn categories, browse porn, porn niches, free porn categories, scrollxxx discover">
<link rel="canonical" href="https://scrollxxx.vip/discover">
<meta property="og:type" content="website">
<meta property="og:url" content="https://scrollxxx.vip/discover">
<meta property="og:title" content="Discover Categories - ScrollXXX">
<meta property="og:description" content="Browse all categories on ScrollXXX. Find your favorite niche.">
<meta property="og:site_name" content="ScrollXXX">
<link rel="icon" type="image/png" href="/favicon.png">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&display=swap" rel="stylesheet">
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
    padding: 0 0 60px;
  }}
  .header {{
    text-align: center; padding: 40px 20px 10px;
  }}
  .header .logo {{
    font-family: 'Bebas Neue', sans-serif; font-size: 36px;
    letter-spacing: 3px; color: #fff; text-decoration: none;
  }}
  .header .logo span {{ color: #e53935; }}
  .header p {{
    color: rgba(255,255,255,0.5); font-size: 14px; margin-top: 8px;
  }}
  .grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 14px; padding: 30px 20px;
    max-width: 900px; margin: 0 auto;
  }}
  .cat-card {{
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 12px; padding: 24px 16px;
    text-align: center; text-decoration: none;
    color: #fff; transition: all 0.2s;
    display: flex; flex-direction: column;
    align-items: center; gap: 8px;
  }}
  .cat-card:hover {{
    background: rgba(255,255,255,0.08);
    border-color: rgba(255,255,255,0.12);
    transform: translateY(-2px);
  }}
  .cat-emoji {{ font-size: 32px; }}
  .cat-name {{ font-size: 15px; font-weight: 600; }}
  .cat-count {{ font-size: 12px; color: rgba(255,255,255,0.4); }}
  .back-btn {{
    display: inline-block; margin: 30px auto 0; padding: 12px 30px;
    background: #e53935; color: #fff; text-decoration: none;
    border-radius: 8px; font-size: 15px; font-weight: 600;
    text-align: center;
  }}
  .back-btn:hover {{ background: #c62828; }}
  .back-wrap {{ text-align: center; }}
  @media (max-width: 500px) {{
    .grid {{ grid-template-columns: repeat(2, 1fr); gap: 10px; padding: 20px 14px; }}
    .cat-card {{ padding: 18px 10px; }}
    .cat-emoji {{ font-size: 28px; }}
  }}
</style>
</head>
<body>
<div class="header">
  <a href="/" class="logo">SCROLL<span>XXX</span></a>
  <p>Browse all categories</p>
</div>
<div class="grid">
  {cards_html}
</div>
<div class="back-wrap">
  <a href="/" class="back-btn">Back to Feed</a>
</div>
</body>
</html>"""
    return Response(html, content_type="text/html")


@app.route("/info")
def info_page():
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Legal Information - ScrollXXX</title>
<meta name="description" content="ScrollXXX legal information including Terms of Service, Privacy Policy, DMCA takedown requests, and 18 USC 2257 compliance.">
<link rel="canonical" href="https://scrollxxx.vip/info">
<link rel="icon" type="image/png" href="/favicon.png">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&display=swap" rel="stylesheet">
<script async src="https://www.googletagmanager.com/gtag/js?id=G-GWEV0G41KG"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());
  gtag('config', 'G-GWEV0G41KG');
</script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    background: #000; color: rgba(255,255,255,0.85); min-height: 100vh;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    padding: 0 0 60px; line-height: 1.7;
  }
  .header {
    text-align: center; padding: 40px 20px 10px;
  }
  .header .logo {
    font-family: 'Bebas Neue', sans-serif; font-size: 36px;
    letter-spacing: 3px; color: #fff; text-decoration: none;
  }
  .header .logo span { color: #e53935; }
  .header p {
    color: rgba(255,255,255,0.5); font-size: 14px; margin-top: 8px;
  }
  .content {
    max-width: 720px; margin: 0 auto; padding: 30px 20px;
  }
  h2 {
    color: #fff; font-size: 22px; margin: 40px 0 12px;
    padding-bottom: 8px; border-bottom: 1px solid rgba(255,255,255,0.1);
  }
  h2:first-child { margin-top: 0; }
  h3 { color: #e53935; font-size: 16px; margin: 20px 0 8px; }
  p, li { font-size: 14px; margin-bottom: 10px; }
  ul { padding-left: 20px; }
  a { color: #e53935; }
  .email { color: #e53935; font-weight: 600; }
  .back-wrap { text-align: center; margin-top: 40px; }
  .back-btn {
    display: inline-block; padding: 12px 30px;
    background: #e53935; color: #fff; text-decoration: none;
    border-radius: 8px; font-size: 15px; font-weight: 600;
  }
  .back-btn:hover { background: #c62828; }
  .updated { color: rgba(255,255,255,0.3); font-size: 12px; margin-top: 40px; text-align: center; }
</style>
</head>
<body>
<div class="header">
  <a href="/" class="logo">SCROLL<span>XXX</span></a>
  <p>Legal Information</p>
</div>
<div class="content">

<h2>Terms of Service</h2>
<h3>1. Eligibility</h3>
<p>You must be at least 18 years of age (or the age of majority in your jurisdiction) to access ScrollXXX. By using this site, you confirm that you meet this requirement.</p>

<h3>2. Content</h3>
<p>ScrollXXX is a content aggregation platform. We do not host or produce any content. All media is sourced from third-party platforms and linked directly. We do not claim ownership of any content displayed on this site.</p>

<h3>3. User Conduct</h3>
<p>You agree not to:</p>
<ul>
  <li>Use this site if you are under 18 years of age</li>
  <li>Attempt to scrape, copy, or redistribute content from this site</li>
  <li>Use automated tools to access the site in a way that degrades service for others</li>
  <li>Attempt to circumvent any security measures</li>
</ul>

<h3>4. Disclaimer</h3>
<p>ScrollXXX is provided "as is" without warranties of any kind. We are not responsible for the accuracy, legality, or content of any third-party material linked on this site. Use this site at your own risk.</p>

<h3>5. Changes</h3>
<p>We reserve the right to modify these terms at any time. Continued use of the site constitutes acceptance of updated terms.</p>

<h2>Privacy Policy</h2>
<h3>Information We Collect</h3>
<p>ScrollXXX collects minimal data. We use Google Analytics to collect anonymous usage data such as page views, session duration, and general location (country level). We do not collect personal information, emails, or login credentials. No accounts are required to use this site.</p>

<h3>Cookies</h3>
<p>We use cookies only for Google Analytics and local preferences (such as mute settings). No advertising tracking cookies are used.</p>

<h3>Third Parties</h3>
<p>Content displayed on ScrollXXX is loaded directly from third-party CDNs (Reddit, Redgifs, etc.). These services may have their own privacy policies and may collect data when their content is loaded in your browser.</p>

<h3>Data Retention</h3>
<p>We do not store any personally identifiable information. Analytics data is retained by Google per their standard retention policies.</p>

<h3>Contact</h3>
<p>For privacy-related inquiries, contact us at <span class="email">contact@scrollxxx.vip</span>.</p>

<h2>DMCA Takedown Request</h2>
<p>ScrollXXX respects intellectual property rights. If you believe content linked on this site infringes your copyright, please send a DMCA takedown notice to:</p>
<p><span class="email">contact@scrollxxx.vip</span></p>
<p>Your notice must include:</p>
<ul>
  <li>Identification of the copyrighted work you claim has been infringed</li>
  <li>The URL(s) on ScrollXXX where the infringing content appears</li>
  <li>Your contact information (name, email, phone number)</li>
  <li>A statement that you have a good faith belief that the use is not authorized by the copyright owner</li>
  <li>A statement, under penalty of perjury, that the information in your notice is accurate and that you are the copyright owner or authorized to act on behalf of the owner</li>
  <li>Your physical or electronic signature</li>
</ul>
<p>We will review and respond to valid DMCA requests promptly. Infringing links will be removed within 48 hours of receiving a valid notice.</p>

<h2>18 U.S.C. 2257 Compliance</h2>
<p>ScrollXXX is not a producer (primary or secondary) of any visual content displayed on this website. All content is sourced from and hosted by third-party platforms. The operators of this site are not the custodians of records for any content that appears on this website.</p>
<p>All content linked or embedded on ScrollXXX originates from third-party sources that are responsible for compliance with 18 U.S.C. 2257 and related regulations. Each third-party content provider is solely responsible for maintaining records as required under 18 U.S.C. 2257.</p>
<p>Any inquiries regarding 18 U.S.C. 2257 compliance for specific content should be directed to the original content producers or the hosting platforms where the content resides.</p>
<p>For questions about this statement, contact: <span class="email">contact@scrollxxx.vip</span></p>

<div class="updated">Last updated: March 2026</div>
</div>
<div class="back-wrap">
  <a href="/" class="back-btn">Back to Feed</a>
</div>
</body>
</html>"""
    return Response(html, content_type="text/html")


@app.route("/watch/<post_id>")
def watch_page(post_id):
    """Individual video/image page — indexable by Google."""
    # Sanitize post_id
    safe_id = post_id[:60].strip()
    if not safe_id:
        return "Not found", 404

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT id, title, video_url, subreddit, upvotes, media_type, category, thumbnail FROM posts WHERE id = ?",
            (safe_id,)
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return "Not found", 404

    post = dict(row)
    title = post["title"] or "Untitled"
    safe_title = title.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
    desc = f"Watch {safe_title} on ScrollXXX. Free {post['category']} content — endless scroll, no ads."
    if len(desc) > 160:
        desc = desc[:157] + "..."
    thumb = post.get("thumbnail", "") or ""
    is_video = post["media_type"] == "video"
    cat_slug = post["category"].lower().replace("/", "-").replace(" ", "-")

    if is_video:
        media_html = f'''<video src="{post["video_url"]}" controls autoplay muted loop playsinline
            referrerpolicy="no-referrer" preload="auto"
            {"poster=" + chr(34) + thumb + chr(34) if thumb else ""}
            style="width:100%;max-height:80vh;border-radius:12px;background:#111;"></video>'''
        schema_type = "VideoObject"
        schema_extra = f'"contentUrl": "{post["video_url"]}",'
        if thumb:
            schema_extra += f'\n    "thumbnailUrl": "{thumb}",'
    else:
        media_html = f'<img src="{post["video_url"]}" referrerpolicy="no-referrer" alt="{safe_title}" style="width:100%;max-height:80vh;border-radius:12px;object-fit:contain;background:#111;">'
        schema_type = "ImageObject"
        schema_extra = f'"contentUrl": "{post["video_url"]}",'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="referrer" content="no-referrer">
<title>{safe_title} - ScrollXXX</title>
<meta name="description" content="{desc}">
<meta name="robots" content="index, follow">
<meta name="rating" content="adult">
<link rel="canonical" href="https://scrollxxx.vip/watch/{safe_id}">
<meta property="og:type" content="{"video.other" if is_video else "article"}">
<meta property="og:url" content="https://scrollxxx.vip/watch/{safe_id}">
<meta property="og:title" content="{safe_title} - ScrollXXX">
<meta property="og:description" content="{desc}">
<meta property="og:site_name" content="ScrollXXX">
{"<meta property=" + chr(34) + "og:image" + chr(34) + " content=" + chr(34) + thumb + chr(34) + ">" if thumb else ""}
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{safe_title} - ScrollXXX">
<meta name="twitter:description" content="{desc}">
<script type="application/ld+json">
{{
  "@context": "https://schema.org",
  "@type": "{schema_type}",
  "name": "{safe_title}",
  "description": "{desc}",
  {schema_extra}
  "uploadDate": "{post.get("created_utc", "")}",
  "publisher": {{
    "@type": "Organization",
    "name": "ScrollXXX",
    "url": "https://scrollxxx.vip/"
  }}
}}
</script>
<link rel="icon" type="image/png" href="/favicon.png">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&display=swap" rel="stylesheet">
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
    padding: 0 0 60px;
  }}
  .header {{
    text-align: center; padding: 30px 20px 10px;
  }}
  .header .logo {{
    font-family: 'Bebas Neue', sans-serif; font-size: 32px;
    letter-spacing: 3px; color: #fff; text-decoration: none;
  }}
  .header .logo span {{ color: #e53935; }}
  .watch-container {{
    max-width: 640px; margin: 0 auto; padding: 20px 16px;
  }}
  .media-wrap {{
    width: 100%; margin-bottom: 16px;
  }}
  h1 {{
    font-size: 18px; font-weight: 600; line-height: 1.4;
    margin-bottom: 8px;
  }}
  .meta {{
    font-size: 13px; color: rgba(255,255,255,0.4);
    margin-bottom: 20px;
  }}
  .meta a {{ color: #e53935; text-decoration: none; }}
  .meta a:hover {{ text-decoration: underline; }}
  .cta {{
    display: inline-block; padding: 14px 36px;
    background: #e53935; color: #fff; text-decoration: none;
    border-radius: 8px; font-size: 16px; font-weight: 600;
    transition: background 0.2s; margin-bottom: 30px;
  }}
  .cta:hover {{ background: #c62828; }}
  .more-heading {{
    font-size: 14px; color: rgba(255,255,255,0.5);
    margin-bottom: 12px; text-transform: uppercase; letter-spacing: 1px;
  }}
  .cats {{
    display: flex; flex-wrap: wrap; gap: 8px;
  }}
  .cats a {{
    padding: 6px 14px; background: rgba(255,255,255,0.06);
    color: rgba(255,255,255,0.6); text-decoration: none;
    border-radius: 6px; font-size: 13px; transition: background 0.2s;
  }}
  .cats a:hover {{ background: rgba(255,255,255,0.12); color: #fff; }}
  .cats a.active {{ background: #e53935; color: #fff; }}
</style>
</head>
<body>
<div class="header">
  <a href="/" class="logo">SCROLL<span>XXX</span></a>
</div>
<div class="watch-container">
  <div class="media-wrap">{media_html}</div>
  <h1>{safe_title}</h1>
  <div class="meta">
    <a href="/{cat_slug}">{post["category"]}</a> &middot; r/{post["subreddit"]} &middot; {post["upvotes"]:,} upvotes
  </div>
  <a href="/?category={post["category"]}" class="cta">Browse More {post["category"]}</a>
  <div class="more-heading">Categories</div>
  <nav class="cats">
    {"".join(f'<a href="/{c.lower().replace("/", "-").replace(" ", "-")}"' + (' class="active"' if c == post["category"] else '') + f'>{c}</a>' for c in CATEGORY_SEO)}
  </nav>
</div>
</body>
</html>"""
    return Response(html, content_type="text/html")


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
                    SELECT p.id, p.title, p.video_url, p.subreddit, p.upvotes, p.media_type, p.category, p.thumbnail
                    FROM posts p
                    JOIN posts_fts fts ON p.rowid = fts.rowid
                    WHERE posts_fts MATCH ? AND p.media_type = ? AND p.category IN ({cat_placeholders})
                    ORDER BY fts.rank
                    LIMIT ? OFFSET ?
                """, (safe_query, media_type, *cat_filter, limit + 1, offset)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT p.id, p.title, p.video_url, p.subreddit, p.upvotes, p.media_type, p.category, p.thumbnail
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
                    SELECT id, title, video_url, subreddit, upvotes, media_type, category, thumbnail
                    FROM posts
                    WHERE media_type = ? AND category IN ({cat_placeholders})
                    ORDER BY (rowid * ? + ?) % 999983
                    LIMIT ? OFFSET ?
                """, (media_type, *cat_filter, seed_a, seed_b, limit + 1, offset)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT id, title, video_url, subreddit, upvotes, media_type, category, thumbnail
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
