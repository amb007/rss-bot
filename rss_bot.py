#!/usr/bin/env python3
"""
RSS → LLM → Telegram bot

Requirements:
    pip install feedparser python-telegram-bot apscheduler httpx python-dotenv
"""

import os, json, hashlib, logging, asyncio, re, email.utils, signal, time, threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse, urljoin

import feedparser
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    MessageReactionHandler, ContextTypes, filters, CallbackQueryHandler
)
from telegram.request import HTTPXRequest
import sqlite3

# ── bootstrap ─────────────────────────────────────────────────────────────────
# RSS_DIR: per-user data directory (set in launchd plist). Defaults to the
# directory containing this script, so an untampered copy stays self-contained.

RSS_DIR = Path(os.environ.get("RSS_DIR", str(Path(__file__).resolve().parent)))

from dotenv import load_dotenv
load_dotenv(RSS_DIR / ".env")

def _resolve_env_refs() -> None:
    """Values written as `env:VAR` in .env inherit VAR from the process
    environment (fixpoint to support chained refs). Never prints values."""
    for _ in range(8):   # fixpoint; warns below on unresolvable refs
        unresolved = False
        for key in list(os.environ):
            val = os.environ[key]
            if val.startswith("env:"):
                ref = val[4:].strip()
                target = os.environ.get(ref)
                if target is not None and not target.startswith("env:"):
                    os.environ[key] = target
                else:
                    unresolved = True
        if not unresolved:
            break
    for key in list(os.environ):   # leftover env: refs → warn (names only)
        val = os.environ[key]
        if val.startswith("env:"):
            logging.warning(f"{key}: referenced env var {val[4:].strip()!r} is not set; value left unset")
            os.environ[key] = ""

_resolve_env_refs()

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
LLM_BACKEND      = os.environ.get("LLM_BACKEND", "llamacpp")
LLM_BASE_URL     = os.environ.get("LLM_BASE_URL", "http://localhost:8080/v1")  # legacy alias
LLM_MODEL        = os.environ.get("LLM_MODEL", "local")                         # legacy alias
LLM_API_KEY      = os.environ.get("LLM_API_KEY", "")                            # legacy alias
SEARXNG_URL      = os.environ.get("SEARXNG_URL", "http://host.local:8888")
FEEDS_FILE       = Path(os.environ.get("FEEDS_FILE", str(RSS_DIR / "feeds.txt")))
ENV_FILE = Path(os.environ.get("ENV_FILE", str(RSS_DIR / ".env")))

def instance_phase_offset_seconds() -> int:
    env_dirs = sorted(
        str(p)
        for p in Path(RSS_DIR).parent.iterdir()
        if p.is_dir() and (p / ".env").is_file()
    )
    idx = env_dirs.index(str(RSS_DIR))
    assert 0 <= idx < len(env_dirs), f"phase index out of range: {idx}"
    return idx * 15  # 0s, 15s, 30s, … — unique per sorted instance

SETTING_DEFAULTS = {
    "DIGEST_HOUR":          "8",
    "DIGEST_TOP":           "8",
    "IGNORE_AFTER_H":       "12",
    "TOP_N":                "8",
    "MIN_SCORE":            "10",
    "PROFILE_EXAMPLES":     "30",
    "SCORE_BATCH":          "10",
    "ARTICLE_MAX_AGE_DAYS": "7",
    "IGNORE_SCORE_FLOOR":   "80",
    "SEARCH_BATCH":         "8",
    "MODELS_PER_PAGE":      "10",
    "LLM_MIN_INTERVAL":     "3",
    "LLM_MAX_RETRIES":      "4",
    "AUTOMODEL_ENABLED":    "1",
    "SWE_REFRESH_DAYS":     "7",
    "MODEL_PROBE_REFRESH_HOURS": "6",   # "ok" probe older than this is stale
    "MODEL_PROBE_INTERVAL_H": "24",    # how often to proactively re-probe
    "PROBE_BATCH":               "5",   # models probed per proactive refresh
}

# URL rewrite: feed servers that return wrong article links.
# Key = source domain prefix, value = target domain.
# If a URL matches a source domain, the entry summary is scanned for a
# link to the target domain (preferring the real article URL). Falls back
# to simple domain replacement.
URL_REWRITE = {
    "feeds.harvardbusiness.org": "hbr.org",
}

def rewrite_url(url: str, entry) -> str:
    for src, dst in URL_REWRITE.items():
        if src not in url:
            continue
        summary = entry.get("summary", "") or ""
        links = re.findall(rf"https://{re.escape(dst)}[^\s\"'<>]+", summary)
        if links:
            return links[0]
        return url.replace(src, dst)
    return url

# ── sqlite ────────────────────────────────────────────────────────────────────

DB_PATH = RSS_DIR / "rss_bot.db"

_db_conn: "sqlite3.Connection | None" = None
_db_lock = threading.RLock()

def _connect_db():
    global _db_conn
    for attempt in range(3):
        try:
            c = sqlite3.connect(DB_PATH, timeout=5, check_same_thread=False)
            c.row_factory = sqlite3.Row
            _db_conn = c
            return c
        except sqlite3.OperationalError:
            if attempt < 2:
                time.sleep(0.5)
            continue
    # stale WAL/SHM — recover and retry once more
    try:
        with sqlite3.connect(DB_PATH, timeout=1) as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        for f in [DB_PATH.with_suffix(f"{DB_PATH.suffix}-wal"),
                  DB_PATH.with_suffix(f"{DB_PATH.suffix}-shm")]:
            f.unlink(missing_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=5, check_same_thread=False)
    c.row_factory = sqlite3.Row
    _db_conn = c
    return c

class _db_ctx:
    """Reuse a single persistent connection, serialized by a lock.

    The context manager commits on clean exit (or rolls back on error),
    matching the previous `with sqlite3.connect(...) as c:` behavior —
    but without opening/closing a connection per block, which churns
    the WAL -shm file and causes transient disk I/O errors on macOS.
    """
    def __init__(self):
        self.c: "sqlite3.Connection | None" = None
    def __enter__(self) -> sqlite3.Connection:
        global _db_conn
        if _db_conn is None:
            _connect_db()
        _db_lock.acquire()
        assert _db_conn is not None
        self.c = _db_conn
        return self.c
    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None and self.c is not None:
                self.c.commit()
        except Exception:
            pass
        finally:
            self.c = None
            _db_lock.release()
        return False

def db():
    return _db_ctx()

def init_db():
    # recover stale WAL/SHM from a crash
    try:
        with sqlite3.connect(DB_PATH, timeout=1) as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        # can't checkpoint — remove stale WAL artifacts
        for f in [DB_PATH.with_suffix(f"{DB_PATH.suffix}-wal"),
                  DB_PATH.with_suffix(f"{DB_PATH.suffix}-shm")]:
            f.unlink(missing_ok=True)
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS articles (
            id          TEXT PRIMARY KEY,
            url         TEXT NOT NULL,
            title       TEXT NOT NULL,
            source      TEXT,
            published   TEXT,
            summary     TEXT,
            score       REAL,
            fetched_at  TEXT,
            sent_at     TEXT,
            opened_at   TEXT,
            liked_at    TEXT,
            ignored_at  TEXT,
            tg_msg_id   INTEGER
        );
        CREATE TABLE IF NOT EXISTS taste_profile (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            profile     TEXT,
            updated_at  TEXT
        );
        INSERT OR IGNORE INTO taste_profile VALUES (1, '', '');
        CREATE TABLE IF NOT EXISTS feeds (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            url         TEXT UNIQUE NOT NULL,
            title       TEXT,
            added_at    TEXT,
            deleted     INTEGER DEFAULT 0,
            deleted_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS swe_scores (
            model_family TEXT PRIMARY KEY,
            resolved_pct REAL,
            fetched_at   TEXT
        );
        CREATE TABLE IF NOT EXISTS model_health (
            backend     TEXT NOT NULL,
            model_id    TEXT NOT NULL,
            last_status TEXT,
            last_seen   TEXT,
            PRIMARY KEY (backend, model_id)
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
            title, summary, content='articles', content_rowid='rowid',
            tokenize='unicode61'
        );
        CREATE TRIGGER IF NOT EXISTS articles_fts_ai AFTER INSERT ON articles BEGIN
            INSERT INTO articles_fts(rowid, title, summary) VALUES (new.rowid, new.title, new.summary);
        END;
        CREATE TRIGGER IF NOT EXISTS articles_fts_ad AFTER DELETE ON articles BEGIN
            INSERT INTO articles_fts(articles_fts, rowid, title, summary) VALUES
                ('delete', old.rowid, old.title, old.summary);
        END;
        CREATE TRIGGER IF NOT EXISTS articles_fts_au AFTER UPDATE ON articles BEGIN
            INSERT INTO articles_fts(articles_fts, rowid, title, summary) VALUES
                ('delete', old.rowid, old.title, old.summary);
            INSERT INTO articles_fts(rowid, title, summary) VALUES (new.rowid, new.title, new.summary);
        END;
        """)
    # backfill FTS index (idempotent — rebuild on every start)
    with db() as c:
        c.execute("INSERT INTO articles_fts(articles_fts) VALUES ('rebuild')")
    c.execute("PRAGMA journal_mode = WAL")
    c.execute("PRAGMA synchronous = NORMAL")
    c.execute("PRAGMA journal_size_limit = 67108864")
    # migrations
    for col, typedef in [("tg_msg_id", "INTEGER"), ("feed_url", "TEXT"), ("feed_id", "INTEGER"), ("disliked_at", "TEXT"), ("hn_points", "INTEGER"), ("hn_comments", "INTEGER"), ("confidence", "TEXT")]:
        try:
            with db() as c:
                c.execute(f"ALTER TABLE articles ADD COLUMN {col} {typedef}")
        except Exception:
            pass
    # rename legacy shared_at → liked_at (idempotent, only if old column exists)
    try:
        with db() as c:
            cols = {r["name"] for r in c.execute("PRAGMA table_info(articles)").fetchall()}
        if "shared_at" in cols and "liked_at" not in cols:
            with db() as c:
                c.execute("ALTER TABLE articles RENAME COLUMN shared_at TO liked_at")
    except Exception:
        pass

    # sync feeds.txt ↔ feeds table
    _sync_feeds_with_file()
    # backfill feed_id for articles
    with db() as c:
        # backfill via feed_url
        orphans = c.execute("SELECT DISTINCT feed_url FROM articles WHERE feed_id IS NULL AND feed_url IS NOT NULL").fetchall()
        for row in orphans:
            url = row["feed_url"]
            existing = c.execute("SELECT id FROM feeds WHERE url=?", (url,)).fetchone()
            if not existing:
                c.execute("INSERT OR IGNORE INTO feeds (url, added_at) VALUES (?,?)",
                          (url, datetime.now(timezone.utc).isoformat()))
                feed_id = c.execute("SELECT id FROM feeds WHERE url=?", (url,)).fetchone()[0]
            else:
                feed_id = existing["id"]
            c.execute("UPDATE articles SET feed_id=? WHERE feed_url=? AND feed_id IS NULL", (feed_id, url))
        # backfill via domain matching
        still_null = [dict(r) for r in c.execute("SELECT id, url FROM articles WHERE feed_id IS NULL")]
        if still_null:
            all_feeds = [dict(r) for r in c.execute("SELECT id, url FROM feeds")]
            matched = 0
            for a in still_null:
                adom = urlparse(a["url"]).netloc
                if not adom:
                    continue
                fid = None
                for f in all_feeds:
                    fdom = urlparse(f["url"]).netloc
                    if adom == fdom or adom.endswith("." + fdom) or fdom.endswith("." + adom):
                        fid = f["id"]
                        break
                if fid:
                    c.execute("UPDATE articles SET feed_id=? WHERE id=?", (fid, a["id"]))
                    matched += 1
            if matched:
                logging.info(f"Backfilled feed_id for {matched} articles via domain matching")

# ── settings ──────────────────────────────────────────────────────────────────

def load_settings() -> dict:
    result = {}
    with db() as c:
        db_rows = {r["key"]: r["value"] for r in c.execute("SELECT key, value FROM settings")}
    for key, default in SETTING_DEFAULTS.items():
        env_val = os.environ.get(key)
        val = env_val if env_val is not None else db_rows.get(key, default)
        try:
            result[key] = float(val) if key == "MIN_SCORE" else int(val)
        except (ValueError, TypeError) as e:
            db_val = db_rows.get(key) if key in db_rows else None
            try:
                result[key] = float(db_val) if key == "MIN_SCORE" else int(db_val)  # type: ignore[arg-type]
                logging.error(f"Settings: invalid {key}={val!r}, using db value: {e}")
            except Exception:
                result[key] = float(default) if key == "MIN_SCORE" else int(default)
                logging.error(f"Settings: invalid {key}={val!r}, using default: {e}")
    with db() as c:
        for key, val in result.items():
            c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(val)))
    return result

def get_setting(key: str):
    with db() as c:
        row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    val = row["value"] if row else SETTING_DEFAULTS[key]
    return float(val) if key == "MIN_SCORE" else int(val)

def set_setting(key: str, value) -> bool:
    if key not in SETTING_DEFAULTS:
        return False
    with db() as c:
        c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
    try:
        _write_env_line(ENV_FILE, key, str(value))
        print(f"Writing .env to: {ENV_FILE}", flush=True)
    except Exception as e:
        logging.error(f"Failed to write .env: {e}")
        return False
    return True

def S(key): return get_setting(key)

def current_llm_backend() -> str:
    with db() as c:
        row = c.execute("SELECT value FROM settings WHERE key='llm_backend'").fetchone()
    val = row["value"] if row else ""
    return val or os.environ.get("LLM_BACKEND", "llamacpp")

def llm_base_url() -> str:
    b = current_llm_backend().upper()
    return (os.environ.get(f"{b}_BASE_URL")
            or LLM_BASE_URL
            or "http://localhost:8080/v1")

def llm_api_key() -> str:
    b = current_llm_backend().upper()
    return os.environ.get(f"{b}_API_KEY") or LLM_API_KEY

def known_backends() -> list[str]:
    """Backends that are configured in the instance .env (have a <B>_BASE_URL)."""
    env_path = ENV_FILE if ENV_FILE.is_absolute() else Path.cwd() / ENV_FILE
    known = []
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = re.match(r"^([A-Z0-9_]+)_BASE_URL\s*=", line)
            if m:
                known.append(m.group(1).lower())
    return known

def current_llm_model() -> str:
    with db() as c:
        row = c.execute("SELECT value FROM settings WHERE key='llm_model'").fetchone()
    if row and row["value"]:
        return row["value"]
    b = current_llm_backend().upper()
    return os.environ.get(f"{b}_MODEL") or LLM_MODEL

def _write_env_line(env_path: Path, key: str, value: str) -> None:
    """Replace an existing 'key=' line in .env (or append) and persist to disk."""
    env_path = env_path if env_path.is_absolute() else Path.cwd() / env_path
    if env_path.exists():
        lines = env_path.read_text().splitlines()
        found = False
        for i, line in enumerate(lines):
            if line.startswith(f"{key}=") or line.startswith(f"{key} ="):
                lines[i] = f"{key}={value}"
                found = True
                break
        if not found:
            lines.append(f"{key}={value}")
        env_path.write_text("\n".join(lines) + "\n")
    else:
        env_path.write_text(f"{key}={value}\n")

def set_llm_model(name: str) -> bool:
    name = name.strip()
    if not name:
        return False
    backend = current_llm_backend().upper()
    model_key = f"{backend}_MODEL"
    with db() as c:
        c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('llm_model', ?)", (name,))
    try:
        _write_env_line(ENV_FILE, model_key, name)
    except Exception as e:
        logging.error(f"Failed to write .env: {e}")
        return False
    os.environ[model_key] = name
    return True

def set_llm_backend(name: str) -> bool:
    """Switch LLM backend. Only accepted if <NAME>_BASE_URL is configured in .env."""
    name = name.strip().lower()
    if not name or name not in known_backends():
        return False
    with db() as c:
        c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('llm_backend', ?)", (name,))
    try:
        _write_env_line(ENV_FILE, "LLM_BACKEND", name)
    except Exception as e:
        logging.error(f"Failed to write .env: {e}")
        return False
    os.environ["LLM_BACKEND"] = name
    # reset model override so we fall back to the new backend's configured model
    with db() as c:
        c.execute("DELETE FROM settings WHERE key='llm_model'")
    return True

# ── helpers ───────────────────────────────────────────────────────────────────

def article_id(url: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()[:16]

def parse_published(entry) -> str:
    for field in ("published", "updated"):
        struct = entry.get(f"{field}_parsed")
        if struct:
            try:
                return datetime(*struct[:6], tzinfo=timezone.utc).isoformat()
            except Exception:
                pass
        val = entry.get(field, "")
        if not val:
            continue
        try:
            return email.utils.parsedate_to_datetime(val).astimezone(timezone.utc).isoformat()
        except Exception:
            pass
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()

def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def age_cutoff() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=S("ARTICLE_MAX_AGE_DAYS"))).isoformat()

# ── feed fetching ─────────────────────────────────────────────────────────────

def _load_feeds_from_file() -> list[str]:
    if not FEEDS_FILE.exists():
        FEEDS_FILE.write_text("# one feed URL per line\n")
        return []
    return [l.strip() for l in FEEDS_FILE.read_text().splitlines()
            if l.strip() and not l.startswith("#")]

def _sync_feeds_with_file():
    if not FEEDS_FILE.exists():
        return
    now = datetime.now(timezone.utc).isoformat()
    lines = FEEDS_FILE.read_text().splitlines()
    with db() as c:
        all_db = {r["url"]: r for r in c.execute("SELECT url, deleted FROM feeds").fetchall()}
        active_urls = set()
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("#"):
                url = stripped.lstrip("#").strip()
                if url in all_db and not all_db[url]["deleted"]:
                    c.execute("UPDATE feeds SET deleted=1, deleted_at=? WHERE url=?", (now, url))
                    logging.info(f"Feed commented out in file, soft-deleted: {url}")
            else:
                active_urls.add(stripped)
        for url in active_urls:
            existing = c.execute("SELECT deleted FROM feeds WHERE url=?", (url,)).fetchone()
            if not existing:
                c.execute("INSERT INTO feeds (url, added_at) VALUES (?,?)", (url, now))
            elif existing["deleted"]:
                c.execute("UPDATE feeds SET deleted=0, deleted_at=NULL WHERE url=?", (url,))

def load_feeds() -> list[dict]:
    with db() as c:
        rows = c.execute("SELECT id, url FROM feeds WHERE deleted=0 ORDER BY id").fetchall()
        return [dict(r) for r in rows]

FEED_UA = "rss-bot/1.0"

def is_feed_url(url: str) -> bool:
    """True if the URL itself parses as a feed (has entries or a feed version)."""
    try:
        parsed = feedparser.parse(url, request_headers={"User-Agent": FEED_UA})  # type: ignore[no-untyped-call]
    except Exception:
        return False
    return bool(parsed.entries) or bool(getattr(parsed, "version", None))

async def discover_feed_url(url: str) -> str | None:
    """Resolve a page URL to its real feed URL via <link rel=alternate>.

    Returns the input URL if it already parses as a feed, the discovered feed
    URL if a usable one is found in the page, or None.
    """
    if is_feed_url(url):
        return url
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            r = await client.get(url, headers={"User-Agent": FEED_UA})
            r.raise_for_status()
            html = r.text
    except Exception as e:
        logging.warning(f"Feed discovery: fetch failed {url}: {e}")
        return None
    m = re.search(
        r'<link[^>]+rel=["\']?(?:alternate|alternate\s+")["\']?[^>]+type=["\']'
        r'(?:application/rss\+xml|application/atom\+xml)["\'][^>]+href=["\']([^"\']+)["\']',
        html, re.IGNORECASE,
    ) or re.search(
        r'<link[^>]+type=["\'](?:application/rss\+xml|application/atom\+xml)["\'][^>]+rel=["\']alternate["\'][^>]+href=["\']([^"\']+)["\']',
        html, re.IGNORECASE,
    )
    if not m:
        return None
    feed_url = urljoin(url, m.group(1))
    return feed_url if is_feed_url(feed_url) else None

def fetch_feeds():
    feeds = load_feeds()
    new_count = 0
    now_iso = datetime.now(timezone.utc).isoformat()
    cutoff = age_cutoff()
    _last_progress_log = 0.0
    for feed in feeds:
        feed_id, feed_url = feed["id"], feed["url"]
        try:
            parsed = feedparser.parse(feed_url, request_headers={"User-Agent": "rss-bot/1.0"})  # type: ignore[no-untyped-call]
            source = parsed.feed.get("title", feed_url)  # type: ignore[union-attr]  
            # update feed title
            with db() as c:
                c.execute("UPDATE feeds SET title=? WHERE id=?", (source, feed_id))
            for entry in parsed.entries:
                url = entry.get("link", "")  # type: ignore[union-attr]
                if "ycombinator.com" in feed_url:
                    url = entry.get("comments", "") or url  # type: ignore[union-attr]
                if not url:
                    continue
                url = rewrite_url(url, entry)  # type: ignore[arg-type]
                aid = article_id(url)  # type: ignore[arg-type]
                title = entry.get("title", "").strip()  # type: ignore[union-attr]
                summary = entry.get("summary", "")[:500]  # type: ignore[union-attr]  
                pub = parse_published(entry)
                if pub < cutoff:
                    continue
                with db() as c:
                    if not c.execute("SELECT 1 FROM articles WHERE id=?", (aid,)).fetchone():
                        # dedup by title within the age window (HN resubmits, [Linkpost] variants, etc.)
                        # but allow through if URLs differ (e.g. weekly roundup series)
                        clean = re.sub(r"^\[.*?\]\s*", "", title).strip()
                        dup = c.execute("SELECT url FROM articles WHERE published>=? AND (title=? OR title=?)",
                                        (cutoff, title, clean)).fetchone()
                        if dup and dup["url"] != url:
                            # different URL — could be a new edition of a series; still dedup if hostname matches
                            new_host = urlparse(url).netloc
                            old_host = urlparse(dup["url"]).netloc
                            if new_host == old_host:
                                dup = None
                        if dup:
                            logging.info(f"Dedup skipped: '{title}' (matches existing article within {S('ARTICLE_MAX_AGE_DAYS')}d)")
                            continue
                        hn_pts = entry.get("hnpoints") or entry.get("points") or entry.get("score") or 0  # type: ignore[union-attr]
                        hn_cmts = entry.get("hncomments") or entry.get("comments_count") or 0  # type: ignore[union-attr]
                        try:
                            hn_pts = int(hn_pts)  # type: ignore[arg-type]
                        except (TypeError, ValueError):
                            m = re.search(r'\((\d+)\s*points?\)', title)
                            hn_pts = int(m.group(1)) if m else 0
                        try:
                            hn_cmts = int(hn_cmts)  # type: ignore[arg-type]
                        except (TypeError, ValueError):
                            m = re.search(r'(\d+)\s*comments?', title)
                            hn_cmts = int(m.group(1)) if m else 0
                        c.execute("""
                            INSERT INTO articles (id,url,title,source,published,summary,fetched_at,feed_url,feed_id,hn_points,hn_comments)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)
                        """, (aid, url, title, source, pub, summary, now_iso, feed_url, feed_id, hn_pts, hn_cmts))
                        new_count += 1
        except Exception as e:
            logging.warning(f"Feed error {feed_url}: {e}")
        now = datetime.now(timezone.utc).timestamp()
        if now - _last_progress_log >= 60:
            logging.info(f"Fetch progress: {new_count} new articles so far ({len(feeds)} feeds processed)")
            _last_progress_log = now
    now_iso = datetime.now(timezone.utc).isoformat()
    with db() as c:
        c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_fetch', ?)", (now_iso,))
    logging.info(f"Fetched feeds: {new_count} new articles")
    return new_count

# ── llm client ────────────────────────────────────────────────────────────────

_llm_lock = asyncio.Lock()   # serialize LLM calls (429-friendly pacing)
_last_llm_at = 0.0

async def _llm_throttle():
    """Space LLM requests at least LLM_MIN_INTERVAL seconds apart (global)."""
    global _last_llm_at
    async with _llm_lock:
        min_interval = max(0, S("LLM_MIN_INTERVAL"))
        now = asyncio.get_event_loop().time()
        wait = min_interval - (now - _last_llm_at)
        if wait > 0:
            await asyncio.sleep(wait)
            now = asyncio.get_event_loop().time()
        _last_llm_at = now

def _retry_wait(attempt: int, response=None) -> float:
    """Backoff in seconds, honoring Retry-After on 429."""
    if response is not None and response.status_code == 429:
        ra = response.headers.get("Retry-After")
        if ra:
            try:
                return float(ra)
            except ValueError:
                pass
        # no header: linear backoff, gentler than the 2^N default
        return 5 * (attempt + 1)
    return 2 ** attempt

_DEAD_STATUSES = {401, 403, 404, 405, 410}   # model gone / not-authorized → no point retrying
_429_STORM_LIMIT = 3   # consecutive retry-exhausted 429 calls before we treat
                       # a rate-limited model as degraded and fall back
_consec_429_storms = 0

async def _llm_request(messages: list[dict], max_tokens: int) -> str:
    headers = {"Content-Type": "application/json"}
    if llm_api_key():
        headers["Authorization"] = f"Bearer {llm_api_key()}"
    payload = {
        "model": current_llm_model(), "messages": messages, "max_tokens": max_tokens,
    }
    if current_llm_backend().lower() == "nim":
        # NIM-specific params: disable thinking tokens
        payload["chat_template_kwargs"] = {"enable_thinking": False}
        payload["thinking"] = False
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(f"{llm_base_url()}/chat/completions", headers=headers, json=payload)
        r.raise_for_status()
        msg = r.json()["choices"][0]["message"]
        content   = (msg.get("content") or "").strip()
        reasoning = (msg.get("reasoning_content") or "").strip()
        text = content if content else reasoning
        text = re.sub(r" thinking.*? response", "", text, flags=re.DOTALL).strip()
        text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
        text = re.sub(r"\*(.*?)\*",     r"\1", text)
        text = re.sub(r"`(.*?)`",       r"\1", text)
        return text

async def notify_owner(text: str) -> None:
    """Best-effort push message to the owner chat via the raw Bot API.
    Used from deep call paths (e.g. automodel switch) that have no bot handle."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text})
    except Exception as e:
        logging.warning(f"notify_owner failed: {e}")

async def _fallback_and_switch(status: int | None) -> bool:
    """On a dead model, discover+probe a replacement and switch to it (once)."""
    if status is not None and status not in _DEAD_STATUSES and status < 500:
        return False  # 400/422 = payload bug, not a dead model — don't mask it
    if not S("AUTOMODEL_ENABLED"):
        return False
    try:
        selection = await select_fallback(current_llm_model())
    except Exception as e:
        logging.warning(f"Fallback failed: {e}")
        return False
    if not selection:
        logging.warning("No working fallback model found.")
        return False
    new_backend, new_model = selection
    if new_backend != current_llm_backend():
        set_llm_backend(new_backend)
    set_llm_model(new_model)
    logging.warning(f"Auto-switched LLM → {new_model} (backend {new_backend})")
    await notify_owner(
        f"⚠️ LLM auto-switched → {new_model} ({new_backend}) — old model died."
        " Use /model to override.")
    return True

async def llm_chat(messages: list[dict], max_tokens=512, retries=None) -> str:
    if retries is None:
        retries = int(S("LLM_MAX_RETRIES"))
    global _consec_429_storms
    for attempt in range(retries):
        await _llm_throttle()
        try:
            result = await _llm_request(messages, max_tokens)
            _consec_429_storms = 0
            return result
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # dead model → no point retrying, go straight to fallback
            if status in _DEAD_STATUSES:
                if await _fallback_and_switch(status):
                    await _llm_throttle()
                    return await _llm_request(messages, max_tokens)
                raise
            if attempt >= retries - 1:
                # a sticky 429 means the model is rate-limited *persistently* —
                # after N consecutive storms treat it as degraded and fall back
                if status == 429:
                    _consec_429_storms += 1
                    if _consec_429_storms >= _429_STORM_LIMIT:
                        logging.warning(f"429 storm x{_consec_429_storms} — treating model as degraded")
                        _consec_429_storms = 0
                        if await _fallback_and_switch(None):
                            await _llm_throttle()
                            return await _llm_request(messages, max_tokens)
                elif await _fallback_and_switch(status):
                    await _llm_throttle()
                    return await _llm_request(messages, max_tokens)
                raise
            wait = _retry_wait(attempt, e.response)
            logging.warning(f"LLM call failed ({status}), retrying in {wait}s...")
            await asyncio.sleep(wait)
        except Exception as e:
            if attempt >= retries - 1:
                if await _fallback_and_switch(None):
                    await _llm_throttle()
                    return await _llm_request(messages, max_tokens)
                raise
            wait = _retry_wait(attempt)
            logging.warning(f"LLM call failed ({e}), retrying in {wait}s...")
            await asyncio.sleep(wait)
    return ""  # type: ignore[return]

async def llm_startup_check():
    """Background LLM liveness probe: one exchange, non-blocking, log on error."""
    try:
        reply = await llm_chat([{"role": "user", "content": "ping"}], max_tokens=64)
        snippet = " ".join((reply or "").split())[:80]
        logging.info(f"LLM startup check OK: {snippet!r}")
    except asyncio.TimeoutError:
        logging.error("LLM startup check FAILED: timeout — "
                      f"backend={current_llm_backend()} url={llm_base_url()!r} model={current_llm_model()!r}")
    except Exception as e:
        logging.error(f"LLM startup check FAILED: {e!r} — "
                      f"backend={current_llm_backend()} url={llm_base_url()!r} model={current_llm_model()!r}")

# ── smart model fallback ───────────────────────────────────────────────────────
# Discovers live free model candidates from the configured backends, probes them
# to verify they actually serve chat completions, and ranks them by SWE-bench
# coding skill (fetched live from swebench.com) × same-backend/same-family
# stickiness. No static model list — availability is always verified on demand.

SWEBENCH_URL = "https://www.swebench.com/"
PROBE_TIMEOUT = 12
PROBE_MAX_CANDIDATES = 25

def _model_base_url(backend: str) -> str:
    b = backend.upper()
    return (os.environ.get(f"{b}_BASE_URL") or LLM_BASE_URL
            or "http://localhost:8080/v1")

def _model_api_key(backend: str) -> str:
    b = backend.upper()
    return os.environ.get(f"{b}_API_KEY") or LLM_API_KEY

def normalize_model_name(name: str) -> str:
    """'Gemini 3 Flash (high)' → 'gemini-3-flash'; 'models/gemini-3.5-flash' →
    'gemini-3-5-flash'; 'moonshotai/kimi-k2.6' → 'moonshotai-kimi-k2-6'."""
    s = name.lower()
    s = re.sub(r"\s*\([^)]*\)", "", s)          # drop "(high)" etc.
    s = re.sub(r"^models?/", "", s)             # google "models/" prefix
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")

def _family_token(model_id: str) -> str:
    """Heuristic family key: last path segment with size/version/params stripped.

    'moonshotai/kimi-k2.6' → 'kimi-k2'   (patch version dropped)
    'nvidia/nemotron-3-ultra-550b-a55b' → 'nemotron-3-ultra'   (MoE sizes dropped)

    Used only as a same-family tiebreak in fallback ranking."""
    seg = model_id.rsplit("/", 1)[-1].lower()
    # trailing MoE active-params token: -a55b, -a3b
    seg = re.sub(r"-a\d+[a-z]?$", "", seg)
    # trailing size/version token: -550b, -30b, -3.5, -v2, .6
    seg = re.sub(r"[-_.]\d+(\.\d+)*[a-z]*$", "", seg)
    return seg.strip("-_. ")

def _swe_score_for(model_id: str, swe: dict) -> float:
    """Best-effort SWE score: exact normalized match, else family substring."""
    key = normalize_model_name(model_id)
    if key in swe:
        return swe[key]
    fam = _family_token(model_id)
    best = None
    for k, v in swe.items():
        if fam and (fam in k or k in fam) and len(fam) >= 4:
            if best is None or v > best:
                best = v
    return best if best is not None else 50.0

async def _fetch_swe_scores() -> dict[str, float]:
    """Fetch SWE-bench leaderboard from swebench.com (inline JSON) → {family: %}.

    Returns {} on any failure; callers treat unknown models as 50 (neutral)."""
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            r = await client.get(SWEBENCH_URL, headers={"User-Agent": FEED_UA})
            r.raise_for_status()
        scripts = re.findall(r"<script[^>]*>(.*?)</script>", r.text, re.DOTALL)
        big = next((s for s in scripts if s.strip().startswith("[")), None)
        if not big:
            return {}
        data = json.loads(big)
    except Exception as e:
        logging.warning(f"SWE fetch failed: {e}")
        return {}
    best: dict[str, float] = {}
    for bench in data:
        for res in bench.get("results", []):
            name = res.get("name", "").strip()
            resolved = res.get("resolved")
            if not name or resolved is None:
                continue
            key = normalize_model_name(name)
            if key and (key not in best or resolved > best[key]):
                best[key] = float(resolved)
    return best

def _store_swe_scores(swe: dict) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with db() as c:
        c.executemany(
            "INSERT OR REPLACE INTO swe_scores (model_family, resolved_pct, fetched_at) "
            "VALUES (?,?,?)",
            [(k, v, now) for k, v in swe.items()])

def _load_swe_scores() -> dict[str, float]:
    with db() as c:
        rows = c.execute("SELECT model_family, resolved_pct FROM swe_scores").fetchall()
    return {r["model_family"]: r["resolved_pct"] for r in rows}

async def refresh_swe_scores() -> dict[str, float]:
    """Fetch fresh SWE scores (or return cached on failure) and persist."""
    fresh = await _fetch_swe_scores()
    if fresh:
        _store_swe_scores(fresh)
        logging.info(f"SWE scores refreshed: {len(fresh)} families")
        return fresh
    cached = _load_swe_scores()
    if cached:
        logging.info(f"SWE fetch failed — using {len(cached)} cached families")
    return cached

async def discover_models(backend: str) -> list[str]:
    """List model IDs exposed by a backend's /models endpoint."""
    base = _model_base_url(backend)
    key = _model_api_key(backend)
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(f"{base}/models", headers=headers)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        logging.warning(f"discover_models({backend}) failed: {e}")
        return []
    ids = [m.get("id", "") for m in data.get("data", [])]
    if backend.lower() == "goo":
        ids = [i[len("models/"):] if i.startswith("models/") else i for i in ids]
    return [i for i in ids if i]

async def probe_model(backend: str, model_id: str) -> str:
    """Probe a model's /chat/completions with max_tokens=1.

    Returns 'ok' | 'rate_limited' | 'gone' | 'down' | 'timeout' | 'unknown'."""
    base = _model_base_url(backend)
    key = _model_api_key(backend)
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = f"Bearer {key}"
    payload = {"model": model_id, "messages": [{"role": "user", "content": "hi"}],
               "max_tokens": 1}
    if backend.lower() == "nim":
        payload["chat_template_kwargs"] = {"enable_thinking": False}
        payload["thinking"] = False
    try:
        async with httpx.AsyncClient(timeout=PROBE_TIMEOUT) as client:
            r = await client.post(f"{base}/chat/completions", headers=headers, json=payload)
    except Exception:
        return "timeout"
    s = r.status_code
    if s == 200:
        return "ok"
    if s == 429:
        return "rate_limited"
    if s in (401, 403, 404, 405, 410):
        return "gone"
    if s >= 500:
        return "down"
    return "unknown"   # 400, 422, … → not a chat model / unsupported params

def _record_health(backend: str, model_id: str, status: str) -> None:
    with db() as c:
        c.execute("INSERT OR REPLACE INTO model_health (backend, model_id, last_status, last_seen) "
                  "VALUES (?,?,?,?)",
                  (backend, model_id, status, datetime.now(timezone.utc).isoformat()))

_NON_TEXT_MODEL_RE = re.compile(
    r"image|imagen|tts|audio|video|whisper|embed|rerank|dall|veo|sora", re.I)

def _is_text_model(model_id: str) -> bool:
    """Filter out models that can't serve text chat (image/tts/embedding/…), so
    a lucky probe never lets the fallback pick e.g. an image model for scoring."""
    return not _NON_TEXT_MODEL_RE.search(model_id)

def _health_fresh(backend: str, model_id: str) -> str | None:
    """Return last_status if it's fresh (probed within MODEL_PROBE_REFRESH_HOURS),
    else None (stale / unknown / missing)."""
    fresh_hours = float(S("MODEL_PROBE_REFRESH_HOURS"))
    with db() as c:
        row = c.execute(
            "SELECT last_status, last_seen FROM model_health WHERE backend=? AND model_id=?",
            (backend, model_id)).fetchone()
    if not row:
        return None
    status, seen = row["last_status"], row["last_seen"]
    try:
        seen_dt = datetime.fromisoformat(seen)
    except (TypeError, ValueError):
        return None
    if datetime.now(timezone.utc) - seen_dt > timedelta(hours=fresh_hours):
        return None  # stale
    return status

async def probe_models_job() -> None:
    """Proactively re-probe models so health data stays fresh, instead of only
    probing during fallback. Refreshes stale/failed candidates first, then a
    rotating sample of known-good ones."""
    batch = max(1, int(S("PROBE_BATCH")))
    with db() as c:
        # 1) stale or failed entries — most important to refresh
        rows = c.execute("""
            SELECT backend, model_id, last_status, last_seen
            FROM model_health
            WHERE last_status != 'ok'
            ORDER BY last_seen ASC
        """).fetchall()
    for row in rows[:batch]:
        status = await probe_model(row["backend"], row["model_id"])
        _record_health(row["backend"], row["model_id"], status)
        logging.info(f"probe: {row['backend']}/{row['model_id']} → {status}")

    # 2) rotate through fresh "ok" models to keep them verified
    with db() as c:
        rows = c.execute("""
            SELECT backend, model_id FROM model_health WHERE last_status = 'ok'
            ORDER BY last_seen ASC
        """).fetchall()
    if rows:
        import hashlib as _hl
        # offset the rotation so different instances don't collide
        off = int(_hl.sha256(str(DB_PATH).encode()).hexdigest(), 16) % len(rows)
        for row in (rows[off:] + rows[:off])[:batch]:
            status = await probe_model(row["backend"], row["model_id"])
            _record_health(row["backend"], row["model_id"], status)
            logging.info(f"probe (rotate): {row['backend']}/{row['model_id']} → {status}")
    logging.info(f"probe_models_job done: {batch} stale + {batch} rotating probed")

async def select_fallback(current_model: str) -> tuple[str, str] | None:
    """Find the best working free model across configured backends.

    Returns (backend, model_id) or None. Prefers models with a recent "ok"
    probe (fresh health), then same family, then highest SWE score. Stale
    "ok" entries are re-probed before being trusted."""
    swe = _load_swe_scores() or await refresh_swe_scores()
    current_backend = current_llm_backend()
    # (priority, score, backend, model) — lower priority probed first
    candidates: list[tuple[int, float, str, str]] = []
    for b in known_backends():
        models = await discover_models(b)
        for mid in models:
            if b == current_backend and mid == current_model:
                continue  # skip the model that just failed
            if not _is_text_model(mid):
                continue  # image/tts/embedding models can't score text
            score = _swe_score_for(mid, swe)
            if b == current_backend:
                score += 3
            if _family_token(mid) and _family_token(mid) == _family_token(current_model):
                score += 5
            # priority 0 = fresh ok (recent known-good), 1 = everything else
            health = _health_fresh(b, mid)
            priority = 0 if health == "ok" else 1
            candidates.append((priority, score, b, mid))
    candidates.sort(key=lambda x: (x[0], -x[1]))
    # probe in order: fresh-known-good first, then re-verify others
    for priority, score, b, mid in candidates[:PROBE_MAX_CANDIDATES]:
        health = _health_fresh(b, mid)
        # trust a fresh "ok" only if the backend/key look right — re-probe anyway
        # for a cheap liveness confirm (we're in a crisis; be safe)
        status = await probe_model(b, mid)
        _record_health(b, mid, status)
        if status in ("ok", "rate_limited"):
            logging.info(f"Fallback selected: {mid} ({b}) — score {score:.0f} (was {health})")
            return b, mid
    return None

def get_taste_profile() -> str:
    with db() as c:
        row = c.execute("SELECT profile FROM taste_profile WHERE id=1").fetchone()
        return row["profile"] if row else ""

# ── web tools ─────────────────────────────────────────────────────────────────

async def fetch_url_text(url: str, retries=3) -> str:
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                r.raise_for_status()
                text = re.sub(r"<style[^>]*>.*?</style>", "", r.text, flags=re.DOTALL)
                text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
                text = re.sub(r"<[^>]+>", " ", text)
                return re.sub(r"\s+", " ", text).strip()[:6000]
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                logging.warning(f"fetch_url_text failed ({e}), retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                raise
    return ""  # type: ignore[return]

async def searxng_search(query: str, retries=3) -> str:
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(f"{SEARXNG_URL}/search", params={"q": query, "format": "json"})
                r.raise_for_status()
                data = r.json()
            results = data.get("results", [])[:5]
            if not results:
                return "No results found."
            return "\n\n".join(
                f"- {res.get('title','')}: {res.get('url','')}\n  {res.get('content','')[:200]}"
                for res in results
            )
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                logging.warning(f"searxng_search failed ({e}), retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                raise
    return ""  # type: ignore[return]

async def classify_message(text: str) -> str:
    reply = await llm_chat([
        {"role": "system", "content":
            "Classify the user message into exactly one of these categories:\n"
            "- 'search' if they want to find information on the web\n"
            "- 'summarize' if they want a summary of an article or URL\n"
            "- 'preference' if they are expressing a news content preference\n"
            "- 'chat' for anything else\n"
            "Reply with only the category word."},
        {"role": "user", "content": text}
    ], max_tokens=10)
    return reply.strip().lower()

# ── scoring ───────────────────────────────────────────────────────────────────

async def score_unscored():
    cutoff = age_cutoff()
    while True:
        with db() as c:
            rows = c.execute("""
                SELECT id, title, source, summary, published, hn_points, hn_comments FROM articles
                WHERE score IS NULL AND ignored_at IS NULL
                  AND disliked_at IS NULL AND published >= ?
                ORDER BY fetched_at DESC LIMIT ?
            """, (cutoff, S("SCORE_BATCH"))).fetchall()
        if not rows:
            break
        profile = get_taste_profile()
        with db() as c:
            positives = c.execute("""
                SELECT title, source, score FROM articles
                WHERE liked_at IS NOT NULL ORDER BY liked_at DESC LIMIT ?
            """, (S("PROFILE_EXAMPLES"),)).fetchall()
            disliked = c.execute("""
                SELECT title, source, score FROM articles
                WHERE disliked_at IS NOT NULL
                ORDER BY disliked_at DESC LIMIT ?
            """, (S("PROFILE_EXAMPLES"),)).fetchall()
            auto_ignored_count = c.execute("""
                SELECT COUNT(*) FROM articles
                WHERE ignored_at IS NOT NULL AND disliked_at IS NULL
            """).fetchone()[0]
        pos_text = "\n".join(f"- [{int(r['score'] or 0)}] {r['title']} ({r['source']})" for r in positives) or "none yet"
        dis_text = "\n".join(f"- [{int(r['score'] or 0)}] {r['title']} ({r['source']})" for r in disliked) or "none yet"
        system = f"""You are a personal news relevance scorer.
Taste profile: {profile or 'not established yet'}
Articles user shared (liked) — higher original score = stronger signal:
{pos_text}
Articles user explicitly disliked:
{dis_text}
({auto_ignored_count} articles were sent but the user never reacted — weak negative signal)

For each article, score how well it matches the user's taste profile:
  90-100 = perfect match, must-read
  70-89  = strong match, very likely to interest
  50-69  = moderate match
  20-49  = weak match
  1-19   = poor match
  0      = about something the user explicitly dislikes

Be critical — high scores should be rare. Use the full range. Defaulting to 50 is discouraged.
Some articles include hn_points and hn_comments (from Hacker News). Higher points = strong community interest (positive signal). Comments indicate engagement but can mean controversy — use as a mild positive.
Reply ONLY with a JSON array: [{{"id":"...","score":N,"confidence":"high|medium|low"}}, ...]"""
        def truncate_words(text, n=50):
            words = text.split()
            return " ".join(words[:n]) + ("..." if len(words) > n else "")
        articles_text = "\n".join(
            _format_article_for_scoring(r, truncate_words)
            for r in rows
        )
        raw = ""
        try:
            raw = await asyncio.wait_for(llm_chat([
                {"role": "system", "content": system},
                {"role": "user",   "content": f"Score these articles:\n{articles_text}"}
            ], max_tokens=8192), timeout=300)
            raw = raw.strip().lstrip("```json").lstrip("```").rstrip("```")
            try:
                scores = json.loads(raw)
                if isinstance(scores, dict):
                    scores = next((v for v in scores.values() if isinstance(v, list)), [scores])
            except json.JSONDecodeError:
                scores = [{"id": m[0], "score": int(m[1])}
                          for m in re.findall(r'"id"\s*:\s*"([^"]+)"\s*,\s*"score"\s*:\s*(\d+)', raw)]
                logging.warning(f"Recovered {len(scores)} scores from truncated JSON")
            with db() as c:
                for item in scores:
                    confidence = item.get("confidence") if isinstance(item, dict) else None
                    c.execute("UPDATE articles SET score=?, confidence=? WHERE id=?",
                              (item["score"], confidence, item["id"]))
                    if confidence == "low":
                        logging.info(f"Low confidence on {item['id']} (score={item['score']})")
            logging.info(f"Scored {len(scores)} articles")
        except asyncio.TimeoutError:
            logging.warning(f"LLM batch timed out after 300s — stopping scoring loop")
            break
        except Exception as e:
            logging.error(f"Scoring error: {e}\nRaw: {raw}")
            raise

def _format_article_for_scoring(r, truncate_words):
    summary = truncate_words(r["summary"], 50) if r["summary"] else ""
    extra = []
    if "hn_points" in r.keys() and (r["hn_points"] or r["hn_comments"]):
        pts = int(r["hn_points"])
        cmts = int(r["hn_comments"]) if r["hn_comments"] else 0
        pub = r["published"]
        if pub:
            t = (datetime.now(timezone.utc) - datetime.fromisoformat(pub)).total_seconds() / 3600
            g = 1.8
            hn_score = max(0, (pts - 1 + 0.5 * cmts)) / ((t + 2) ** g) if t >= 0 else pts
            extra.append(f"hn_score={hn_score:.1f}")
        extra.append(f"hn_points={pts}")
        extra.append(f"hn_comments={cmts}")
    hn = f" | {' '.join(extra)}" if extra else ""
    return f'id:{r["id"]} | {r["title"]} | {r["source"]} | {summary}{hn}'

# ── ignore detection ──────────────────────────────────────────────────────────

def mark_ignored():
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=S("IGNORE_AFTER_H"))).isoformat()
    floor = S("IGNORE_SCORE_FLOOR")
    with db() as c:
        c.execute("""
            UPDATE articles SET ignored_at = ?
            WHERE sent_at IS NOT NULL AND sent_at < ?
              AND opened_at IS NULL AND liked_at IS NULL
              AND ignored_at IS NULL AND disliked_at IS NULL
              AND (score IS NULL OR score < ?)
        """, (datetime.now(timezone.utc).isoformat(), cutoff, floor))

# ── taste profile ─────────────────────────────────────────────────────────────

async def update_taste_profile(user_instruction: str = ""):
    with db() as c:
        pos = c.execute("""
            SELECT title, source, summary FROM articles
            WHERE liked_at IS NOT NULL ORDER BY liked_at DESC LIMIT ?
        """, (S("PROFILE_EXAMPLES"),)).fetchall()
        liked_sources = {r["source"] for r in pos}
        disliked = c.execute("""
            SELECT title, source, summary FROM articles
            WHERE disliked_at IS NOT NULL AND source NOT IN ({})
            ORDER BY disliked_at DESC LIMIT ?
        """.format(",".join("?" for _ in liked_sources)), (*liked_sources, S("PROFILE_EXAMPLES"))).fetchall()
        auto_ignored_count = c.execute("""
            SELECT COUNT(*) FROM articles
            WHERE ignored_at IS NOT NULL AND disliked_at IS NULL
        """).fetchone()[0]
    pos_text = "\n".join(f"- {r['title']} ({r['source']}): {r['summary'][:150]}" for r in pos) or "none yet"
    dis_text = "\n".join(f"- {r['title']} ({r['source']}): {r['summary'][:150]}" for r in disliked) or "none yet"
    current  = get_taste_profile()
    instruction_block = f"\nNew explicit instruction from user: {user_instruction}" if user_instruction else ""
    prompt = f"""Rewrite this user's news taste profile as a single coherent paragraph (~150 words).
Consolidate everything — resolve contradictions, fold in new instructions, reflect implicit signals from liked/disliked/ignored articles.

Current profile: {current or 'none'}
{instruction_block}
Liked articles: {pos_text}
Explicitly disliked articles: {dis_text}
({auto_ignored_count} articles were sent but the user never reacted — weak negative signal)

Reply with ONLY the new profile text, no preamble, no headers."""
    try:
        profile = await llm_chat([{"role": "user", "content": prompt}], max_tokens=1024)
        print("RAW LLM profile:", repr(profile[:100]), flush=True)
        if not profile.strip():
            profile = user_instruction
        with db() as c:
            c.execute("UPDATE taste_profile SET profile=?, updated_at=? WHERE id=1",
                      (profile, datetime.now(timezone.utc).isoformat()))
        logging.info("Taste profile updated")
        return profile
    except Exception as e:
        import traceback
        print("PROFILE EXCEPTION:", traceback.format_exc(), flush=True)
        logging.error(f"Profile update error: {e}")
        return None

# ── digest ────────────────────────────────────────────────────────────────────

def digest_snapshot() -> list[dict]:
    """Full ordered list of ready (unread) articles as dicts — used for /feed
    pagination so Prev/Next navigate a stable snapshot instead of re-querying
    (which would skip articles already marked sent_at)."""
    mark_ignored()
    cutoff = age_cutoff()
    with db() as c:
        rows = c.execute("""
            SELECT id, title, source, score, url, hn_points, hn_comments,
                   confidence, liked_at, disliked_at
            FROM articles
            WHERE score >= ? AND sent_at IS NULL AND published >= ?
            ORDER BY score DESC
        """, (S("MIN_SCORE"), cutoff)).fetchall()
    return [dict(r) for r in rows]

def make_article_msg(r) -> str:
    score = r['score']
    score_str = f"{int(score)}" if score is not None else "–"
    conf = ""
    if "confidence" in r.keys() and r["confidence"]:
        c = r["confidence"]
        if c == "medium":
            conf = "~"
        elif c == "low":
            conf = "?"
    title  = escape_html(r['title'] or r['url'])
    source = escape_html(r['source'] or "")
    hn = ""
    if "hn_points" in r.keys():
        pts = int(r["hn_points"]) if r["hn_points"] else 0
        cmts = int(r["hn_comments"]) if r["hn_comments"] else 0
        parts = []
        if pts:
            parts.append(f"{pts} pts")
        if cmts:
            parts.append(f"{cmts} comments")
        hn = f" ({', '.join(parts)})" if parts else ""
    return f"{score_str}{conf} · <a href=\"{r['url']}\">{title}</a> — {source}{hn}"

async def tg_send(send_fn, *args, retries=3, **kwargs):
    for attempt in range(retries):
        try:
            return await send_fn(*args, **kwargs)
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                logging.warning(f"Telegram send failed ({e}), retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                raise

def digest_pool() -> list[dict]:
    """Articles eligible for a digest: new (published after the last digest)
    and not yet shown anywhere (sent_at IS NULL, i.e. not read via /feed and
    not in a prior digest)."""
    last = None
    with db() as c:
        row = c.execute("SELECT value FROM settings WHERE key='last_digest'").fetchone()
        if row:
            last = row["value"]
    since = last if last else age_cutoff()
    with db() as c:
        rows = c.execute("""
            SELECT id, title, source, score, url, hn_points, hn_comments
            FROM articles
            WHERE score >= ? AND sent_at IS NULL AND published > ?
            ORDER BY score DESC
        """, (S("MIN_SCORE"), since)).fetchall()
    return [dict(r) for r in rows]

def _digest_hn(r) -> int:
    return (r.get("hn_points") or 0) + (r.get("hn_comments") or 0)

def group_digest(pool: list[dict], budget: int = 8):
    """Split the digest pool into (top_picks, discussed, skim) sections.
    Top picks = highest score; discussed = most HN-active; the rest skim.
    Never returns more than `budget` rows total."""
    top = pool[:4]
    top_ids = {r["id"] for r in top}
    rest = [r for r in pool if r["id"] not in top_ids]
    with_hn = [r for r in rest if _digest_hn(r) > 0]
    discussed = sorted(with_hn, key=_digest_hn, reverse=True)[:3]
    disc_ids = {r["id"] for r in discussed}
    skim = [r for r in rest if r["id"] not in disc_ids][:max(0, budget - len(top) - len(discussed))]
    return top, discussed, skim

def render_digest_message(top, discussed, skim, total: int) -> str:
    day = datetime.now(timezone.utc).strftime("%a %d %b")
    lines = [f"📰 Daily Digest · {day}", ""]
    def _item(r, num):
        title = escape_html(r["title"] or r["url"])
        source = escape_html(r["source"] or "")
        hn = ""
        if _digest_hn(r):
            parts = [p for p in ((f"{r['hn_points']} pts" if r.get("hn_points") else None),
                                 (f"{r['hn_comments']} cmts" if r.get("hn_comments") else None)) if p]
            hn = f" <i>({', '.join(parts)})</i>"
        return f"{num}. <a href=\"{r['url']}\">{title}</a> — {source}{hn}"
    num = 0
    for label, rows in (("🔥 <b>Top picks</b>", top),
                        ("💬 <b>Most discussed</b>", discussed),
                        ("📚 <b>Worth a skim</b>", skim)):
        if not rows:
            continue
        lines.append(label)
        for r in rows:
            num += 1
            lines.append(_item(r, num))
        lines.append("")
    shown = len(top) + len(discussed) + len(skim)
    tail = f"· {total} new article{'s' if total != 1 else ''}"
    tail += " · 👍/👎 on /feed refines your profile"
    lines.append(tail)
    return "\n".join(lines)

async def send_digest(bot, chat_id: int) -> int:
    """Send one compact grouped digest message and advance the digest window.
    Articles are NOT marked sent_at, so they stay in /feed where the user can
    react to them. Returns how many articles were shown (0 = nothing new)."""
    pool = digest_pool()
    if not pool:
        return 0
    top, discussed, skim = group_digest(pool, budget=int(S("DIGEST_TOP")))
    shown = top + discussed + skim
    await tg_send(bot.send_message, chat_id=chat_id,
                  text=render_digest_message(top, discussed, skim, len(pool)),
                  parse_mode="HTML", disable_web_page_preview=True)
    now = datetime.now(timezone.utc).isoformat()
    with db() as c:
        c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_digest', ?)", (now,))
    return len(shown)

# ── search ────────────────────────────────────────────────────────────────────

# token → (query, offset, batch, [nav_msg_id, *card_msg_ids])
_search_state: dict[str, tuple[str, int, int, list[int]]] = {}
_search_token: int = 0

# token → (snapshot, page_size, offset, [nav_msg_id, *card_msg_ids])
_feed_state: dict[str, tuple[list[dict], int, int, list[int]]] = {}
_feed_token: int = 0

def search_articles(query: str, limit: int, offset: int = 0) -> list[sqlite3.Row]:
    """FTS5 bm25-ranked search over title (2x weight) + summary."""
    with db() as c:
        return c.execute("""
            SELECT a.id, a.title, a.source, a.score, a.url, a.published,
                   a.hn_points, a.hn_comments, a.confidence, a.liked_at, a.disliked_at
            FROM articles_fts f JOIN articles a ON a.rowid = f.rowid
            WHERE articles_fts MATCH ?
            ORDER BY bm25(articles_fts, 2.0, 1.0)
            LIMIT ? OFFSET ?
        """, (query, limit, offset)).fetchall()

def search_total(query: str) -> int:
    with db() as c:
        row = c.execute("""
            SELECT COUNT(*) FROM articles_fts f JOIN articles a ON a.rowid = f.rowid
            WHERE articles_fts MATCH ?
        """, (query,)).fetchone()
    return row[0] if row else 0

def normalize_search_query(query: str) -> str:
    """Validate/rewrite a raw /search query into valid FTS5.

    - Parses as-is → returned unchanged.
    - Ends with '*' → phrase-prefix: quote the base and keep '*' *outside* the
      quotes, so `e-graph*` matches e-graph, e-graphs, e-grapher, …
    - Otherwise → wrapped as a quoted phrase (never errors out).
    """
    try:
        search_articles(query, 1, 0)
        return query
    except Exception:
        if query.rstrip().endswith("*"):
            base = query.rstrip()[:-1].strip()
            return f'"{base}"*'
        return f'"{query}"'

async def _delete_msgs(bot, chat_id, ids) -> None:
    for mid in (ids or []):
        try:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass

async def _send_cards(bot, chat_id, rows) -> list[int]:
    """Send article rows as individual reactable cards; record tg_msg_id and
    re-apply the liked/disliked state as a real Telegram reaction (read fresh
    from the DB so Prev/Next navigation reflects the latest vote)."""
    ids = []
    for r in rows:
        try:
            sent = await bot.send_message(chat_id=chat_id, text=make_article_msg(r),
                                          parse_mode="HTML", disable_web_page_preview=True)
        except Exception as e:
            logging.warning(f"card send failed: {e}")
            continue
        ids.append(sent.message_id)
        aid = r["id"]
        with db() as c:
            c.execute("UPDATE articles SET tg_msg_id=? WHERE id=?", (sent.message_id, aid))
            st = c.execute("SELECT liked_at, disliked_at FROM articles WHERE id=?",
                           (aid,)).fetchone()
        emoji = None
        if st is not None:
            if st["liked_at"]:
                emoji = "👍"
            elif st["disliked_at"]:
                emoji = "👎"
        if emoji:
            try:
                await bot.set_message_reaction(chat_id=chat_id,
                                               message_id=sent.message_id, reaction=[emoji])
            except Exception as e:
                logging.warning(f"reaction re-apply failed: {e}")
    return ids

async def render_search_page(update, ctx, query: str, offset: int, batch: int) -> None:
    """Render one page of /search results as reactable cards + a nav message.

    `_search_state[token] = (query, offset, batch, [nav_id, *card_ids])` tracks
    the messages so the callback can delete them before rendering the next page."""
    global _search_token
    chat_id = update.effective_chat.id
    total = search_total(query)
    rows = search_articles(query, batch, offset)
    if not rows:
        await ctx.bot.send_message(chat_id=chat_id, text="No matches.")
        return
    card_ids = await _send_cards(ctx.bot, chat_id, rows)

    total_pages = max(1, (total + batch - 1) // batch)
    cur_page = (offset // batch) + 1
    kb_row = []
    prev_token = next_token = None
    if offset > 0:
        _search_token += 1
        prev_token = _search_token
        kb_row.append(InlineKeyboardButton("← Prev", callback_data=f"ps:{prev_token}"))
    kb_row.append(InlineKeyboardButton(f"{cur_page}/{total_pages}", callback_data="none"))
    if offset + len(rows) < total:
        _search_token += 1
        next_token = _search_token
        kb_row.append(InlineKeyboardButton("Next →", callback_data=f"ns:{next_token}"))
    kb = InlineKeyboardMarkup([kb_row]) if len(kb_row) > 1 else None
    header = (f"🔎 {total} match{'es' if total != 1 else ''} for "
              f"<code>{escape_html(query)}</code> — page {cur_page}/{total_pages}")
    nav = await ctx.bot.send_message(chat_id=chat_id, text=header, reply_markup=kb,
                                     parse_mode="HTML")
    page_msg_ids = [nav.message_id] + card_ids
    if prev_token:
        _search_state[f"ps:{prev_token}"] = (query, offset - batch, batch, page_msg_ids)
    if next_token:
        _search_state[f"ns:{next_token}"] = (query, offset + batch, batch, page_msg_ids)

async def cmd_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()  # type: ignore[union-attr]
    if len(args) < 2:
        await update.message.reply_text("/search <query> [batch size]")  # type: ignore[union-attr]
        return
    if len(args) >= 3 and args[-1].isdigit():
        batch = int(args[-1])
        args = args[:-1]
    else:
        batch = int(S("SEARCH_BATCH"))
    query = " ".join(args[1:])
    query = normalize_search_query(query)
    await render_search_page(update, ctx, query, 0, batch)

async def handle_search_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or update.effective_chat.id != TELEGRAM_CHAT_ID:
        if q:
            await q.answer("Unauthorized")
        return
    if not (q.data.startswith("ps:") or q.data.startswith("ns:")):
        return
    state = _search_state.get(q.data)
    if not state:
        await q.answer("Search session expired — run /search again.")
        return
    query, new_offset, batch, old_ids = state
    _search_state.pop(q.data, None)
    await _delete_msgs(ctx.bot, update.effective_chat.id, old_ids)
    await render_search_page(update, ctx, query, new_offset, batch)
    await q.answer()

async def _handle_page_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q and update.effective_chat.id == TELEGRAM_CHAT_ID:
        await q.answer()

# ── scheduled jobs ────────────────────────────────────────────────────────────

async def hourly_job(app):
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, fetch_feeds)
        try:
            await asyncio.wait_for(score_unscored(), timeout=600)
        except asyncio.TimeoutError:
            logging.warning("score_unscored() timed out after 10 minutes — skipping remainder")
    except Exception as e:
        logging.error(f"Hourly job error: {e}")
        print(f"HOURLY JOB ERROR: {e}", flush=True)
        if "429" not in str(e) and "404" not in str(e):
            try:
                await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                                           text=f"Hourly job error (LLM down?): {e}")
            except Exception as e2:
                print(f"FAILED TO SEND ERROR: {e2}", flush=True)

async def daily_digest_job(app):
    mark_ignored()
    await update_taste_profile()
    await send_digest(app.bot, TELEGRAM_CHAT_ID)

async def swe_refresh_job(app):
    try:
        await refresh_swe_scores()
    except Exception as e:
        logging.warning(f"SWE refresh job error: {e}")

# ── command handlers ──────────────────────────────────────────────────────────

async def cmd_feed(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    n = int(ctx.args[0]) if ctx.args and ctx.args[0].isdigit() else int(S("TOP_N"))
    cutoff = age_cutoff()
    with db() as c:
        unscored = c.execute("""
            SELECT COUNT(*) FROM articles
            WHERE score IS NULL AND ignored_at IS NULL
              AND disliked_at IS NULL AND published >= ?
        """, (cutoff,)).fetchone()[0]

    snapshot = digest_snapshot()

    if snapshot and unscored:
        # show now, score in background for next time
        async def _score():
            try:
                await score_unscored()
            except Exception as e:
                if "429" not in str(e) and "404" not in str(e):
                    await ctx.bot.send_message(chat_id=update.effective_chat.id,  # type: ignore[union-attr]
                                               text=f"Scoring error: {e}")
        asyncio.create_task(_score())
    elif not snapshot and unscored:
        # nothing to show yet — wait for scoring
        await tg_send(update.message.reply_text,  # type: ignore[union-attr]
                             f"Scoring {unscored} articles (batches of {S('SCORE_BATCH')})...")
        try:
            await score_unscored()
        except Exception as e:
            if "429" not in str(e):
                await update.message.reply_text(f"Scoring error: {e}")  # type: ignore[union-attr]
        snapshot = digest_snapshot()

    if not snapshot:
        await update.message.reply_text("Nothing new above your threshold.")  # type: ignore[union-attr]
        return

    await render_feed_page(ctx.bot, update.effective_chat.id, snapshot, 0, n)

async def cmd_digest(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sent = await send_digest(ctx.bot, update.effective_chat.id)  # type: ignore[union-attr]
    if not sent:
        await update.message.reply_text("Nothing new since your last digest.")  # type: ignore[union-attr]

async def render_feed_page(bot, chat_id: int, snapshot: list[dict], offset: int, n: int) -> None:
    """Send one page of /feed articles as reactable cards + a nav message.

    Sets sent_at on the shown page so it's consumed; Prev/Next navigate the
    stable snapshot (not a re-query). Shared by /feed and the daily digest."""
    global _feed_token
    total = len(snapshot)
    page = snapshot[offset:offset + n]
    if not page:
        await bot.send_message(chat_id=chat_id, text="No more articles.")
        return

    card_ids = await _send_cards(bot, chat_id, page)

    now = datetime.now(timezone.utc).isoformat()
    with db() as c:
        c.executemany("UPDATE articles SET sent_at=? WHERE id=?",
                      [(now, r["id"]) for r in page])

    total_pages = max(1, (total + n - 1) // n)
    cur_page = (offset // n) + 1
    kb_row = []
    prev_token = next_token = None
    if offset > 0:
        _feed_token += 1
        prev_token = _feed_token
        kb_row.append(InlineKeyboardButton("← Prev", callback_data=f"fp:{prev_token}"))
    kb_row.append(InlineKeyboardButton(f"{cur_page}/{total_pages}", callback_data="none"))
    if offset + n < total:
        _feed_token += 1
        next_token = _feed_token
        kb_row.append(InlineKeyboardButton("Next →", callback_data=f"fn:{next_token}"))
    kb = InlineKeyboardMarkup([kb_row]) if len(kb_row) > 1 else None
    header = f"📰 Your feed — {total} articles (page {cur_page}/{total_pages})"
    nav = await bot.send_message(chat_id=chat_id, text=header, reply_markup=kb)
    page_msg_ids = [nav.message_id] + card_ids
    if prev_token:
        _feed_state[f"fp:{prev_token}"] = (snapshot, n, offset - n, page_msg_ids)
    if next_token:
        _feed_state[f"fn:{next_token}"] = (snapshot, n, offset + n, page_msg_ids)

async def handle_feed_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or update.effective_chat.id != TELEGRAM_CHAT_ID:
        if q:
            await q.answer("Unauthorized")
        return
    if not (q.data.startswith("fp:") or q.data.startswith("fn:")):
        return
    state = _feed_state.get(q.data)
    if not state:
        await q.answer("Feed session expired — run /feed again.")
        return
    snapshot, n, new_offset, old_ids = state
    _feed_state.pop(q.data, None)
    await _delete_msgs(ctx.bot, update.effective_chat.id, old_ids)
    await render_feed_page(ctx.bot, update.effective_chat.id, snapshot, new_offset, n)
    await q.answer()

async def cmd_fetch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Fetching feeds in background...")  # type: ignore[union-attr]
    async def _fetch():
        try:
            loop = asyncio.get_event_loop()
            n = await loop.run_in_executor(None, fetch_feeds)
            await ctx.bot.send_message(chat_id=update.effective_chat.id,  # type: ignore[union-attr]
                                       text=f"Done. {n or 0} new articles.")
        except Exception as e:
            await ctx.bot.send_message(chat_id=update.effective_chat.id,  # type: ignore[union-attr]
                                       text=f"Fetch error: {e}")
    asyncio.create_task(_fetch())

async def cmd_profile(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    p = get_taste_profile()
    await update.message.reply_text(p or "No taste profile yet. Share some articles or tell me your interests.")  # type: ignore[union-attr]

async def cmd_remember(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    instruction = " ".join(ctx.args)  # type: ignore[arg-type]
    if not instruction:
        await update.message.reply_text("Usage: /remember I don't want crypto news")  # type: ignore[union-attr]
        return
    await update.message.reply_text("Updating your profile...")  # type: ignore[union-attr]
    profile = await update_taste_profile(user_instruction=instruction)
    if profile:
        await update.message.reply_text(f"Profile updated:\n\n{profile[:3800]}")  # type: ignore[union-attr]
    else:
        await update.message.reply_text("Something went wrong updating the profile.")  # type: ignore[union-attr]

async def cmd_get(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    extra = {
        "LLM_BACKEND": current_llm_backend(),
        "LLM_MODEL":   current_llm_model(),
        "LLM_BASE_URL": llm_base_url(),
    }
    if ctx.args:
        key = ctx.args[0].upper()
        if key in extra:
            await update.message.reply_text(f"{key}={extra[key]}")  # type: ignore[union-attr]
            return
        if key not in SETTING_DEFAULTS:
            await update.message.reply_text(f"Unknown setting: {key}\nAvailable: {', '.join(SETTING_DEFAULTS)}")  # type: ignore[union-attr]
            return
        await update.message.reply_text(f"{key}={get_setting(key)}")  # type: ignore[union-attr]
    else:
        lines = [f"{k}={get_setting(k)}" for k in SETTING_DEFAULTS]
        lines += [f"{k}={v}" for k, v in extra.items()]
        await update.message.reply_text("Settings:\n" + "\n".join(lines))  # type: ignore[union-attr]

async def cmd_set(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) != 2:  # type: ignore[arg-type]
        await update.message.reply_text(f"Usage: /set KEY VALUE\nAvailable: {', '.join(SETTING_DEFAULTS)}")  # type: ignore[union-attr]
        return
    key, val = ctx.args[0].upper(), ctx.args[1]  # type: ignore[index]
    if key not in SETTING_DEFAULTS:
        await update.message.reply_text(f"Unknown setting: {key}")  # type: ignore[union-attr]
        return
    try:
        parsed = float(val) if key == "MIN_SCORE" else int(val)
    except ValueError:
        await update.message.reply_text(f"Invalid value: {val}")  # type: ignore[union-attr]
        return
    if set_setting(key, parsed):
        await update.message.reply_text(f"Set {key}={parsed} (db + .env updated)")  # type: ignore[union-attr]
    else:
        await update.message.reply_text(f"Set {key}={parsed} in db, but failed to update .env")  # type: ignore[union-attr]

async def cmd_model(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = " ".join(ctx.args).strip()  # type: ignore[arg-type]
    if not args:
        await update.message.reply_text(  # type: ignore[union-attr]
            f"Usage: /model <name>\nCurrent: {current_llm_model()} (backend: {current_llm_backend()}, base: {llm_base_url()})")
        return
    if set_llm_model(args):
        await update.message.reply_text(f"Model set to: {args}")  # type: ignore[union-attr]
    else:
        await update.message.reply_text("Failed to set model.")  # type: ignore[union-attr]

async def cmd_backend(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = " ".join(ctx.args).strip()  # type: ignore[arg-type]
    known = ", ".join(known_backends()) or "(none — set <BACKEND>_BASE_URL in .env)"
    if not args:
        await update.message.reply_text(  # type: ignore[union-attr]
            f"Usage: /backend <name>\nCurrent: {current_llm_backend()}\nKnown: {known}")
        return
    if args.lower() not in known_backends():
        await update.message.reply_text(  # type: ignore[union-attr]
            f"Unknown backend: {args}\nKnown: {known}")  # type: ignore[union-attr]
        return
    if set_llm_backend(args):
        await update.message.reply_text(  # type: ignore[union-attr]
            f"Backend set to: {args}\nNow model: {current_llm_model()} (base: {llm_base_url()})")
    else:
        await update.message.reply_text("Failed to set backend.")  # type: ignore[union-attr]

def _looks_free(backend: str, model_id: str) -> bool | None:
    """Heuristic free/paid signal. True=likely free, False=likely paid,
    None=unknown. Zen has a reliable '-free' suffix; Goo's free tier is the
    flash/embedding/gemma family on AI Studio; NIM exposes no field."""
    m = model_id.lower()
    if backend.lower() == "zen":
        return m.endswith("-free")
    if backend.lower() == "goo":
        # Paid / rate-limited-only: pro, image/video/audio gen, robotics, etc.
        if any(k in m for k in ("pro", "image", "tts", "audio", "video", "veo",
                                "lyria", "nano", "robotics", "computer-use",
                                "transcribe", "live", "antigravity",
                                "deep-research", "aqa", "omni")):
            return False
        # AI Studio free tier: flash / flash-lite / embedding / gemma.
        if any(k in m for k in ("flash-lite", "flash")):
            return True
        if any(k in m for k in ("embedding", "gemma")):
            return True
        return None
    return None

# /models browsing state.
# nav token → (providers, provider_idx, model_offset, page_size, [nav_msg_id, content_msg_id])
# where providers = list of (backend, [model_id, ...]) — one sorted model list per backend.
_models_state: dict[str, tuple[list[tuple[str, list[str]]], int, int, int, list[int]]] = {}
# model-pick token → (backend, model_id)
_models_pick: dict[str, tuple[str, str]] = {}
_models_token: int = 0

def _prov_short(backend: str) -> str:
    """3-letter provider shortcut for nav buttons."""
    return {"llamacpp": "LCP", "nim": "NIM", "goo": "GOO", "zen": "ZEN"}.get(
        backend, backend.upper()[:3])

def _model_status_mark(backend: str, model_id: str) -> str:
    """Status marker for a model: ● alive, · unknown, x dead (monochrome)."""
    with db() as c:
        row = c.execute("SELECT last_status FROM model_health WHERE backend=? AND model_id=?",
                        (backend, model_id)).fetchone()
    status = row["last_status"] if row else None
    if status in ("ok", "rate_limited"):
        return "●"
    if status == "unknown":
        return "·"
    return "x"

def _model_button_text(backend: str, model_id: str, swe: dict) -> str:
    """Plain-text label for a model row (Telegram buttons don't take HTML)."""
    score = _swe_score_for(model_id, swe)
    fr = _looks_free(backend, model_id)
    cur = "✓ " if (backend == current_llm_backend() and model_id == current_llm_model()) else ""

    parts = [cur] if cur else []
    parts.append(_model_status_mark(backend, model_id))
    parts.append("\u262e\ufe0e" if fr is True else ("$" if fr is False else "?"))
    return f"{' '.join(parts)} {int(score)}% {model_id}"

async def render_models_page(bot, chat_id: int, providers: list[tuple[str, list[str]]],
                             pidx: int, moff: int, n: int, swe: dict) -> None:
    """Send one provider's model page: a content message whose keyboard is the
    models (tap = /model), plus a nav message with provider + model paging."""
    global _models_token
    total_p = len(providers)
    if pidx < 0:
        pidx = total_p - 1
    elif pidx >= total_p:
        pidx = 0
    backend, models = providers[pidx]
    n_models = len(models)
    total_pages = max(1, (n_models + n - 1) // n)
    cur_page = (moff // n) + 1
    page = models[moff:moff + n]

    # content message: one button per model row → select that model
    kb = []
    for mid in page:
        _models_token += 1
        tok = f"ms:{_models_token}"
        _models_pick[tok] = (backend, mid)
        kb.append([InlineKeyboardButton(_model_button_text(backend, mid, swe),
                                        callback_data=tok)])

    # content: model buttons with legend header
    content = await bot.send_message(chat_id=chat_id,
                                     text="● alive · unknown x dead | ☮ free $ paid ? unknown | SWE score — tap a model to use it",
                                     reply_markup=InlineKeyboardMarkup(kb) if kb else None)
    nav_row = []
    prev_p = next_p = prev_m = next_m = None
    if total_p > 1:
        _models_token += 1
        prev_p = f"mp:{_models_token}"
        nav_row.append(InlineKeyboardButton(
            f"← {_prov_short(providers[(pidx-1) % total_p][0])}", callback_data=prev_p))
    if moff > 0:
        _models_token += 1
        prev_m = f"ml:{_models_token}"
        nav_row.append(InlineKeyboardButton("← Prev", callback_data=prev_m))
    nav_row.append(InlineKeyboardButton(f"{cur_page}/{total_pages}", callback_data="none"))
    if moff + n < n_models:
        _models_token += 1
        next_m = f"mr:{_models_token}"
        nav_row.append(InlineKeyboardButton("Next →", callback_data=next_m))
    if total_p > 1:
        _models_token += 1
        next_p = f"mn:{_models_token}"
        nav_row.append(InlineKeyboardButton(
            f"{_prov_short(providers[(pidx+1) % total_p][0])} →", callback_data=next_p))
    nav_text = f"<b>{backend.upper()}</b> — {n_models} models (page {cur_page}/{total_pages})"
    nav = await bot.send_message(chat_id=chat_id, text=nav_text, parse_mode="HTML",
                                 reply_markup=InlineKeyboardMarkup([nav_row]))
    page_msg_ids = [nav.message_id, content.message_id]
    if prev_p:
        _models_state[prev_p] = (providers, pidx - 1, 0, n, page_msg_ids)
    if next_p:
        _models_state[next_p] = (providers, pidx + 1, 0, n, page_msg_ids)
    if prev_m:
        _models_state[prev_m] = (providers, pidx, moff - n, n, page_msg_ids)
    if next_m:
        _models_state[next_m] = (providers, pidx, moff + n, n, page_msg_ids)

async def handle_models_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or update.effective_chat.id != TELEGRAM_CHAT_ID:
        if q:
            await q.answer("Unauthorized")
        return
    data = q.data
    # model selection → equivalent to /model <id> (switch backend too)
    if data.startswith("ms:"):
        pick = _models_pick.get(data)
        if not pick:
            await q.answer("Expired — run /models again.")
            return
        backend, mid = pick
        if backend != current_llm_backend():
            set_llm_backend(backend)
        set_llm_model(mid)
        logging.info(f"/models pick → {mid} ({backend})")
        await q.answer(f"Using {mid} ({backend})")
        return
    if not (data.startswith("mp:") or data.startswith("mn:") or
            data.startswith("ml:") or data.startswith("mr:")):
        return
    state = _models_state.get(data)
    if not state:
        await q.answer("Models list expired — run /models again.")
        return
    providers, new_pidx, new_moff, n, old_ids = state
    _models_state.pop(data, None)
    swe = _load_swe_scores() or {}
    await _delete_msgs(ctx.bot, update.effective_chat.id, old_ids)
    await render_models_page(ctx.bot, update.effective_chat.id, providers, new_pidx, new_moff, n, swe)
    await q.answer()

async def cmd_models(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = " ".join(ctx.args).strip().lower()  # type: ignore[arg-type]
    force = args == "refresh"
    if force:
        await update.message.reply_text("Discovering + probing models… (may take ~1 min)")  # type: ignore[union-attr]

    swe = _load_swe_scores()
    if not swe:
        swe = await refresh_swe_scores()

    providers: list[tuple[str, list[str]]] = []
    for b in known_backends():
        ids = await discover_models(b)
        # sort: current first, alive, free, unknown, dead, SWE desc, name
        def _sort_key(mid: str, b=b):
            with db() as c:
                row = c.execute("SELECT last_status FROM model_health WHERE backend=? AND model_id=?",
                                (b, mid)).fetchone()
            status = row["last_status"] if row else None
            status_cat = 0 if status in ("ok", "rate_limited") else \
                         (1 if status == "unknown" else 2)
            cur = 0 if (b == current_llm_backend() and mid == current_llm_model()) else 1
            free = _looks_free(b, mid) is True
            return (cur, status_cat, not free, -_swe_score_for(mid, swe), mid.lower())
        ids_sorted = sorted(ids, key=_sort_key)
        if force:
            to_probe = [m for m in ids_sorted if _looks_free(b, m) is not False]
            sem = asyncio.Semaphore(8)
            async def probe_one(mid):
                async with sem:
                    st = await probe_model(b, mid)
                    _record_health(b, mid, st)
            await asyncio.gather(*(probe_one(m) for m in to_probe))
        providers.append((b, ids_sorted))
    if not providers:
        await update.message.reply_text("No configured backends — set <BACKEND>_BASE_URL in .env")  # type: ignore[union-attr]
        return
    await render_models_page(ctx.bot, update.effective_chat.id, providers, 0, 0,
                             int(S("MODELS_PER_PAGE")), swe)

async def cmd_automodel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = " ".join(ctx.args).strip().lower()  # type: ignore[arg-type]
    cur = S("AUTOMODEL_ENABLED")
    if not args:
        await update.message.reply_text(f"Automodel: {'on' if cur else 'off'}. Usage: /automodel on|off")  # type: ignore[union-attr]
        return
    if args in ("on", "1", "true", "yes"):
        set_setting("AUTOMODEL_ENABLED", 1)
        await update.message.reply_text("Automodel on — dead models auto-switch to a live fallback.")  # type: ignore[union-attr]
    elif args in ("off", "0", "false", "no"):
        set_setting("AUTOMODEL_ENABLED", 0)
        await update.message.reply_text("Automodel off — manual /model only.")  # type: ignore[union-attr]
    else:
        await update.message.reply_text("Usage: /automodel on|off")  # type: ignore[union-attr]

async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cutoff = age_cutoff()
    with db() as c:
        total     = c.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        too_old   = c.execute("SELECT COUNT(*) FROM articles WHERE published < ?", (cutoff,)).fetchone()[0]
        recent    = total - too_old
        r_shared  = c.execute("SELECT COUNT(*) FROM articles WHERE published >= ? AND liked_at IS NOT NULL", (cutoff,)).fetchone()[0]
        r_disliked= c.execute("SELECT COUNT(*) FROM articles WHERE published >= ? AND disliked_at IS NOT NULL", (cutoff,)).fetchone()[0]
        r_ignored = c.execute("SELECT COUNT(*) FROM articles WHERE published >= ? AND ignored_at IS NOT NULL AND disliked_at IS NULL", (cutoff,)).fetchone()[0]
        r_sent    = c.execute("SELECT COUNT(*) FROM articles WHERE published >= ? AND sent_at IS NOT NULL AND liked_at IS NULL AND ignored_at IS NULL AND disliked_at IS NULL", (cutoff,)).fetchone()[0]
        r_ready   = c.execute("SELECT COUNT(*) FROM articles WHERE published >= ? AND sent_at IS NULL AND score >= ? AND ignored_at IS NULL AND disliked_at IS NULL", (cutoff, S("MIN_SCORE"))).fetchone()[0]
        r_low     = c.execute("SELECT COUNT(*) FROM articles WHERE published >= ? AND sent_at IS NULL AND score IS NOT NULL AND score > 0 AND score < ? AND ignored_at IS NULL AND disliked_at IS NULL", (cutoff, S("MIN_SCORE"))).fetchone()[0]
        r_unscored= c.execute("SELECT COUNT(*) FROM articles WHERE published >= ? AND score IS NULL AND ignored_at IS NULL AND disliked_at IS NULL", (cutoff,)).fetchone()[0]

        buckets = c.execute("""
            SELECT MIN(CAST(score/10 AS INTEGER), 9) * 10 AS bucket,
                   SUM(CASE WHEN liked_at IS NOT NULL THEN 1 ELSE 0 END) AS liked,
                   SUM(CASE WHEN disliked_at IS NOT NULL THEN 1 ELSE 0 END) AS disliked
            FROM articles
            WHERE score IS NOT NULL AND published >= ?
            GROUP BY bucket ORDER BY bucket
        """, (cutoff,)).fetchall()

        last_fetch_raw = c.execute("SELECT value FROM settings WHERE key='last_fetch'").fetchone()

    last_fetch = ""
    if last_fetch_raw:
        dt = datetime.fromisoformat(last_fetch_raw[0])
        delta = datetime.now(timezone.utc) - dt
        mins = int(delta.total_seconds() // 60)
        if mins < 60:
            last_fetch = f" ({mins}m ago)"
        else:
            last_fetch = f" ({mins // 60}h {mins % 60}m ago)"

    fn_row = [0] * 10
    fp_row = [0] * 10
    for r in buckets:
        i = r["bucket"] // 10
        fn_row[i] = r["liked"]
        fp_row[i] = r["disliked"]

    labels   = "SC" + "".join(f"{b:>4}" for b in [10,20,30,40,50,60,70,80,90,100])
    fn_line  = "FN" + "".join(f"{n:>4}" for n in fn_row)
    fp_line  = "FP" + "".join(f"{n:>4}" for n in fp_row)

    table = f"<pre>{labels}\n{fn_line}\n{fp_line}</pre>"
    await tg_send(update.message.reply_text,  # type: ignore[union-attr]
        f"📊 Stats\n"
        f"\n"
        f"Total in DB: {total}\n"
        f"  Too old to show: {too_old}\n"
        f"  Recent ({S('ARTICLE_MAX_AGE_DAYS')}d{last_fetch}): {recent}\n"
        f"\n"
        f"Recent breakdown:\n"
        f"  ✓ Liked: {r_shared}\n"
        f"  ✗ Explicitly disliked: {r_disliked}\n"
        f"  ➖ Auto-ignored (no reaction): {r_ignored}\n"
        f"  Seen (sent, no vote): {r_sent}\n"
        f"  Ready for /feed: {r_ready}\n"
        f"  Below MIN_SCORE ({S('MIN_SCORE')}): {r_low}\n"
        f"  Unscored: {r_unscored}\n"
        f"\n"
        f"{table}\n"
        f"\n"
        f"Reactions: 👍❤️🔥 = like · 👎🤮💤😐 = dislike",
        parse_mode="HTML"
    )

async def cmd_commands(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(  # type: ignore[union-attr]
        f"/feed [n]    — scored digest (default {S('TOP_N')})\n"
        f"/digest      — compact daily digest (new + unseen)\n"
        "/fetch       — fetch new articles from feeds\n"
        "/scoreall    — score all unscored articles\n"
        "/model NAME   — switch LLM model (persists to .env)\n"
        "/backend NAME — switch LLM backend (must be configured in .env)\n"
        "/models       — list discovered models + SWE% + health\n"
        "/automodel on — auto-switch to a live model when current dies\n"
        "/profile     — show your taste profile\n"
        "/remember    — update profile explicitly\n"
        "/get [KEY]   — show setting(s)\n"
        "/set KEY VAL — change a setting\n"
        "/stats       — show DB stats\n"
        "/addfeed     — add an RSS feed\n"
        "/removefeed  — remove a feed (reply or pass URL)\n"
        "/search QUERY— search saved articles (FTS5)\n"
        "/commands    — show this list\n"
        "\n"
        "Reply to an article message to summarize it or remove its feed.\n"
        "Just send a URL to search for that article.\n"
        "Emoji reactions: 👍❤️🔥 = like · 👎🤮💤😐 = dislike"
    )

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await cmd_commands(update, ctx)

async def cmd_scoreall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cutoff = age_cutoff()
    with db() as c:
        total = c.execute("""
            SELECT COUNT(*) FROM articles
            WHERE score IS NULL AND ignored_at IS NULL
              AND disliked_at IS NULL AND published >= ?
        """, (cutoff,)).fetchone()[0]
    if not total:
        await update.message.reply_text("Nothing to score.")  # type: ignore[union-attr]
        return
    await update.message.reply_text(f"Scoring {total} articles in background, {S('SCORE_BATCH')} at a time...")  # type: ignore[union-attr]
    async def run():
        batches = 0
        while True:
            with db() as c:
                remaining = c.execute("""
                    SELECT COUNT(*) FROM articles
                    WHERE score IS NULL AND ignored_at IS NULL
                      AND disliked_at IS NULL AND published >= ?
                """, (cutoff,)).fetchone()[0]
            if not remaining:
                break
            await score_unscored()
            batches += 1
        await ctx.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                                   text=f"Done scoring. {batches} batches processed.")
    asyncio.create_task(run())

# ── message handler ───────────────────────────────────────────────────────────

async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    print("HANDLE_MESSAGE CALLED", flush=True)
    text = update.message.text.strip()  # type: ignore[union-attr]
    try:
        await _handle_message_inner(update, ctx, text)
    except Exception as e:
        import traceback
        print("HANDLE_MESSAGE ERROR:", traceback.format_exc(), flush=True)
        await update.message.reply_text(f"Error (LLM down?): {e}")  # type: ignore[union-attr]

async def _handle_message_inner(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    # unknown command
    if text.startswith("/"):
        await update.message.reply_text("Unknown command. Try /commands for the list.")  # type: ignore[union-attr]
        return

    # forwarded message → positive signal
    if update.message.forward_origin:  # type: ignore[union-attr]
        for word in text.split():
            if word.startswith("http"):
                aid = article_id(word)
                with db() as c:
                    c.execute("UPDATE articles SET liked_at=? WHERE id=?",
                              (datetime.now(timezone.utc).isoformat(), aid))
                await update.message.reply_text("✓ Liked")  # type: ignore[union-attr]
                return

    # reply to a bot message → extract URL and summarize
    replied = update.message.reply_to_message  # type: ignore[union-attr]
    article_url = None
    if replied and replied.from_user and replied.from_user.is_bot:
        if replied.entities:
            for ent in replied.entities:
                if ent.type.name in ("URL", "TEXT_LINK"):  # type: ignore[union-attr]
                    article_url = ent.url if ent.url else replied.text[ent.offset:ent.offset+ent.length]  # type: ignore[index]
                    break
        if not article_url and replied.text:
            for word in replied.text.split():
                if word.startswith("http"):
                    article_url = word
                    break

    if article_url:
        await update.message.reply_text("Fetching article...")  # type: ignore[union-attr]
        async def _summarize(chat_id, reply_to):
            try:
                content = await fetch_url_text(article_url)
                reply = await llm_chat([
                    {"role": "system", "content": "You are a helpful assistant. Summarize the article content concisely."},
                    {"role": "user", "content": f"User request: {text}\n\nArticle content:\n{content}"}
                ], max_tokens=1500)
                await ctx.bot.send_message(chat_id=chat_id, text=reply[:4000] if reply else "Could not summarize.",
                                           reply_to_message_id=reply_to, parse_mode=None)
            except Exception as e:
                await ctx.bot.send_message(chat_id=chat_id, text=f"Could not fetch article: {e}")
        ctx.application.create_task(_summarize(update.effective_chat.id, update.message.message_id))
        return

    # classify intent
    intent = await classify_message(text)
    print("INTENT:", intent, flush=True)

    if intent == "preference":
        await update.message.reply_text("Updating your profile...")  # type: ignore[union-attr]
        profile = await update_taste_profile(user_instruction=text)
        if profile:
            await update.message.reply_text(f"Profile updated:\n\n{profile[:3800]}")  # type: ignore[union-attr]
        else:
            await update.message.reply_text("Something went wrong updating the profile.")  # type: ignore[union-attr]
        return

    if intent == "search":
        await update.message.reply_text("Searching...")  # type: ignore[union-attr]
        try:
            results = await searxng_search(text)
            reply = await llm_chat([
                {"role": "system", "content": "You are a helpful assistant. Answer based on these search results."},
                {"role": "user",   "content": f"Query: {text}\n\nSearch results:\n{results}"}
            ], max_tokens=600)
            await update.message.reply_text(reply[:4000] if reply else "No answer found.", parse_mode=None)  # type: ignore[union-attr]
        except Exception as e:
            await update.message.reply_text(f"Search error: {e}")  # type: ignore[union-attr]
        return

    if intent == "summarize":
        url = next((w for w in text.split() if w.startswith("http")), None)
        if url:
            await update.message.reply_text("Fetching article...")  # type: ignore[union-attr]
            async def _summarize_url(chat_id, reply_to):
                try:
                    content = await fetch_url_text(url)
                    reply = await llm_chat([
                        {"role": "system", "content": "Summarize this article concisely."},
                        {"role": "user",   "content": content}
                    ], max_tokens=1500)
                    await ctx.bot.send_message(chat_id=chat_id, text=reply[:4000] if reply else "Could not summarize.",
                                               reply_to_message_id=reply_to, parse_mode=None)
                except Exception as e:
                    await ctx.bot.send_message(chat_id=chat_id, text=f"Could not fetch: {e}")
            ctx.application.create_task(_summarize_url(update.effective_chat.id, update.message.message_id))
        else:
            await update.message.reply_text("Please include a URL or reply to an article message.")  # type: ignore[union-attr]
        return

    # plain chat
    profile = get_taste_profile()
    with db() as c:
        recent = c.execute("""
            SELECT title, source, score FROM articles
            WHERE sent_at IS NOT NULL ORDER BY score DESC LIMIT 20
        """).fetchall()
    context = "\n".join(f"- {r['title']} ({r['source']}, score {int(r['score'])})" for r in recent)
    reply = await llm_chat([
        {"role": "system", "content":
            f"You are the user's personal news assistant.\n"
            f"Taste profile: {profile}\n"
            f"Recent top articles:\n{context}"},
        {"role": "user", "content": text}
    ], max_tokens=800)
    print("CHAT REPLY:", repr(reply[:100]), flush=True)
    if not reply:
        await update.message.reply_text("The model returned an empty response. Try again.")  # type: ignore[union-attr]
        return
    await update.message.reply_text(reply[:4000], parse_mode=None)  # type: ignore[union-attr]

# ── reaction handler ──────────────────────────────────────────────────────────

async def handle_reaction(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    reaction = update.message_reaction
    if not reaction:
        return
    msg_id = reaction.message_id

    like_emojis    = {"👍", "❤️", "❤", "🔥"}
    dislike_emojis = {"👎", "🤮", "💤", "😐"}

    # reaction removed
    if reaction.old_reaction and not reaction.new_reaction:
        emoji = reaction.old_reaction[0].emoji if hasattr(reaction.old_reaction[0], 'emoji') else str(reaction.old_reaction[0])  # type: ignore[union-attr]
        with db() as c:
            row = c.execute("SELECT id, title FROM articles WHERE tg_msg_id=?", (msg_id,)).fetchone()
        if row:
            with db() as c:
                if emoji in like_emojis:
                    c.execute("UPDATE articles SET liked_at=NULL WHERE id=?", (row["id"],))
                elif emoji in dislike_emojis:
                    c.execute("UPDATE articles SET disliked_at=NULL WHERE id=?", (row["id"],))
            print(f"REACTION REMOVED: {emoji} on '{row['title']}'", flush=True)
            try:
                await ctx.bot.set_message_reaction(
                    chat_id=reaction.chat.id, message_id=msg_id, reaction=[])
            except Exception as e:
                print(f"REACTION: clear reaction error: {e}", flush=True)
        return

    # reaction added
    if not reaction.new_reaction:
        return
    emoji = reaction.new_reaction[0].emoji if hasattr(reaction.new_reaction[0], 'emoji') else str(reaction.new_reaction[0])  # type: ignore[union-attr]
    print(f"REACTION: {emoji} on message {msg_id}", flush=True)

    if emoji not in like_emojis and emoji not in dislike_emojis:
        return

    with db() as c:
        row = c.execute("SELECT id, title FROM articles WHERE tg_msg_id=?", (msg_id,)).fetchone()
    if not row:
        print(f"REACTION: no article found for message {msg_id}", flush=True)
        return

    now = datetime.now(timezone.utc).isoformat()
    label = ""
    with db() as c:
        if emoji in like_emojis:
            c.execute("UPDATE articles SET liked_at=?, disliked_at=NULL WHERE id=?", (now, row["id"]))
            label = "Liked"
        else:
            c.execute("UPDATE articles SET disliked_at=?, liked_at=NULL WHERE id=?", (now, row["id"]))
            label = "Disliked"
    print(f"REACTION: {emoji} → {label} '{row['title']}'", flush=True)
    try:
        await ctx.bot.set_message_reaction(
            chat_id=reaction.chat.id, message_id=msg_id,
            reaction=list(reaction.new_reaction))
    except Exception as e:
        print(f"REACTION: confirm reaction error: {e}", flush=True)

def _match_feed_by_domain(article_url: str) -> int | None:
    adom = urlparse(article_url).netloc
    if not adom:
        return None
    with db() as c:
        for f in c.execute("SELECT id, url FROM feeds WHERE deleted=0"):
            fdom = urlparse(f["url"]).netloc
            if adom == fdom or adom.endswith("." + fdom) or fdom.endswith("." + adom):
                return f["id"]
    return None

# ── feed management ─────────────────────────────────────────────────────────

def _sync_feeds_to_file():
    with db() as c:
        active  = [r["url"] for r in c.execute("SELECT url FROM feeds WHERE deleted=0 ORDER BY id")]
        deleted = [f"# {r['url']}" for r in c.execute("SELECT url FROM feeds WHERE deleted=1 ORDER BY id")]
    all_lines = active + [""] + deleted if deleted else active
    FEEDS_FILE.write_text("\n".join(all_lines) + "\n")

def remove_feed(feed_id: int) -> bool:
    with db() as c:
        row = c.execute("SELECT url, deleted FROM feeds WHERE id=?", (feed_id,)).fetchone()
        if not row or row["deleted"]:
            return False
        now = datetime.now(timezone.utc).isoformat()
        c.execute("UPDATE feeds SET deleted=1, deleted_at=? WHERE id=?", (now, feed_id))
    _sync_feeds_to_file()
    return True

async def cmd_addfeed(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    url = " ".join(ctx.args).strip()  # type: ignore[arg-type]
    if not url or not url.startswith("http"):
        await update.message.reply_text("Usage: /addfeed https://example.com/feed.xml")  # type: ignore[union-attr]
        return
    await update.message.reply_text("Checking feed...")  # type: ignore[union-attr]
    discovered = await discover_feed_url(url)
    if not discovered:
        await update.message.reply_text(  # type: ignore[union-attr]
            f"Couldn't find a feed at {url} — it's neither a feed nor a page with a feed link.")
        return
    was_resolved = url != discovered
    original_url = url
    url = discovered
    already_there = False
    with db() as c:
        try:
            now = datetime.now(timezone.utc).isoformat()
            c.execute("INSERT INTO feeds (url, added_at) VALUES (?,?)", (url, now))
        except sqlite3.IntegrityError:
            existing = c.execute("SELECT deleted FROM feeds WHERE url=?", (url,)).fetchone()
            if existing and existing["deleted"]:
                c.execute("UPDATE feeds SET deleted=0, deleted_at=NULL WHERE url=?", (url,))
            else:
                already_there = True
    _sync_feeds_to_file()
    if already_there:
        await update.message.reply_text("Feed already in list.")  # type: ignore[union-attr]
        return
    suffix = f"\n(auto-detected from {original_url})" if was_resolved else ""
    await update.message.reply_text(f"✓ Added feed:\n{url}{suffix}")  # type: ignore[union-attr]

async def cmd_removefeed(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    url_arg = " ".join(ctx.args).strip()  # type: ignore[arg-type]
    feed_id = None
    # via reply to an article message
    reply = update.message.reply_to_message  # type: ignore[union-attr]
    if reply:
        with db() as c:
            row = c.execute("SELECT id, feed_id, feed_url, url FROM articles WHERE tg_msg_id=?", (reply.message_id,)).fetchone()
            if row:
                if row["feed_id"]:
                    feed_id = row["feed_id"]
                elif row["feed_url"]:
                    f = c.execute("SELECT id FROM feeds WHERE url=?", (row["feed_url"],)).fetchone()
                    if f:
                        feed_id = f["id"]
                if not feed_id:
                    feed_id = _match_feed_by_domain(row["url"])
    # via URL argument
    if not feed_id and url_arg:
        with db() as c:
            # try as feed URL directly
            row = c.execute("SELECT id FROM feeds WHERE url=?", (url_arg,)).fetchone()
            if row:
                feed_id = row["id"]
            else:
                # try as article URL → get feed_url via articles table
                a = c.execute("SELECT feed_url FROM articles WHERE url=?", (url_arg,)).fetchone()
                if a and a["feed_url"]:
                    f = c.execute("SELECT id FROM feeds WHERE url=?", (a["feed_url"],)).fetchone()
                    if f:
                        feed_id = f["id"]
                if not feed_id:
                    feed_id = _match_feed_by_domain(url_arg)
    if not feed_id:
        await update.message.reply_text("Reply to an article message or pass a feed URL.")  # type: ignore[union-attr]
        return
    if remove_feed(feed_id):
        print(f"FEED REMOVED via /removefeed: feed_id={feed_id}", flush=True)
        await update.message.reply_text("✓ Feed removed.")  # type: ignore[union-attr]
    else:
        await update.message.reply_text("Feed already removed.")  # type: ignore[union-attr]

async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    import traceback
    logging.error(f"Telegram error: {ctx.error}")
    logging.error(traceback.format_exception(type(ctx.error), ctx.error, ctx.error.__traceback__))  # type: ignore[arg-type]
    print("TELEGRAM ERROR UPDATE:", update, flush=True)

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    init_db()
    load_settings()

    request = HTTPXRequest(read_timeout=15, connect_timeout=10, pool_timeout=15)
    app = Application.builder().token(TELEGRAM_TOKEN).request(request).concurrent_updates(True).build()

    owner = filters.Chat(chat_id=TELEGRAM_CHAT_ID)
    app.add_handler(CommandHandler("start",    cmd_start,     filters=owner))
    app.add_handler(CommandHandler("feed",     cmd_feed,      filters=owner))
    app.add_handler(CommandHandler("digest",   cmd_digest,    filters=owner))
    app.add_handler(CommandHandler("fetch",    cmd_fetch,     filters=owner))
    app.add_handler(CommandHandler("profile",  cmd_profile,   filters=owner))
    app.add_handler(CommandHandler("remember", cmd_remember,  filters=owner))
    app.add_handler(CommandHandler("get",      cmd_get,       filters=owner))
    app.add_handler(CommandHandler("set",      cmd_set,       filters=owner))
    app.add_handler(CommandHandler("stats",    cmd_stats,     filters=owner))
    app.add_handler(CommandHandler("commands", cmd_commands,  filters=owner))
    app.add_handler(CommandHandler("scoreall", cmd_scoreall,  filters=owner))
    app.add_handler(CommandHandler("model",    cmd_model,     filters=owner))
    app.add_handler(CommandHandler("backend",  cmd_backend,   filters=owner))
    app.add_handler(CommandHandler("models",   cmd_models,    filters=owner))
    app.add_handler(CommandHandler("automodel",cmd_automodel, filters=owner))
    app.add_handler(CommandHandler("addfeed",     cmd_addfeed,    filters=owner))
    app.add_handler(CommandHandler("removefeed",  cmd_removefeed, filters=owner))
    app.add_handler(CommandHandler("search",      cmd_search,     filters=owner))
    app.add_handler(CallbackQueryHandler(handle_search_callback, pattern=r"^(ps|ns):"))
    app.add_handler(CallbackQueryHandler(handle_feed_callback, pattern=r"^(fp|fn):"))
    app.add_handler(CallbackQueryHandler(handle_models_callback, pattern=r"^(ms|mp|mn|ml|mr):"))
    app.add_handler(CallbackQueryHandler(_handle_page_cb, pattern=r"^none$"))
    app.add_handler(MessageReactionHandler(handle_reaction, chat_id=TELEGRAM_CHAT_ID))
    app.add_handler(MessageHandler(filters.ALL & owner, handle_message))
    app.add_error_handler(error_handler)

    scheduler_holder: dict = {}

    async def start_scheduler(application):
        scheduler = AsyncIOScheduler()
        scheduler_holder["job"] = scheduler
        phase = instance_phase_offset_seconds()
        scheduler.add_job(hourly_job, "interval", minutes=30, args=[application],
                          next_run_time=datetime.now(timezone.utc) + timedelta(seconds=phase))
        scheduler.add_job(daily_digest_job, "cron", hour=S("DIGEST_HOUR"), args=[application])
        scheduler.add_job(swe_refresh_job, "interval", days=max(1, S("SWE_REFRESH_DAYS")),
                          args=[application], next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30))
        scheduler.add_job(probe_models_job, "interval", hours=max(1, S("MODEL_PROBE_INTERVAL_H")),
                          next_run_time=datetime.now(timezone.utc) + timedelta(seconds=60))
        scheduler.start()
        logging.info(f"Scheduler started (phase offset: {phase}s)")
        asyncio.create_task(llm_startup_check())

    async def stop_scheduler(application):
        scheduler = scheduler_holder.get("job")
        if scheduler:
            scheduler.shutdown(wait=False)

    app.post_init = start_scheduler
    app.post_shutdown = stop_scheduler

    logging.info("Bot running. Polling Telegram...")
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES,
                        stop_signals=[signal.SIGTERM])
    finally:
        logging.info("Bot shut down gracefully.")

if __name__ == "__main__":
    main()
