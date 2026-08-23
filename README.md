# rss-bot — personal RSS digest bot for Telegram

A Python/Telegram bot that fetches RSS/Atom feeds, scores articles with an LLM
against your interests, and pushes a daily digest to your Telegram chat. The
**whole service is per-user**: each bot instance gets its own data directory,
token, chat, feeds, database, and log.

It is designed to host any number of instances (one directory each, see
below); adding a new one — for yourself or a friend — is a ~10-minute
copy-paste job. This file walks you through the whole thing.

---

## Layout

```
~/.rss-bot/
├── rss_bot.py              # single shared bot implementation
├── README.md
├── alice/                  # instance 1
│   ├── .env                # token, chat id, LLM backend, tuning knobs
│   ├── feeds.txt            # one feed URL per line
│   └── rss_bot.db           # sqlite: articles, profile, settings, ...
└── bob/                    # instance 2 (a friend's bot)
    ├── .env
    ├── feeds.txt
    └── rss_bot.db
```

There is **no per-instance copy of the code** — every instance runs the same
`rss_bot.py`. Each process reads its own `.env` from the `RSS_DIR` it was
started with, and that single env variable is the whole isolation boundary.

---

## Instance isolation (the RSS_DIR trick)

`rss_bot.py` resolves its data directory at startup:

```python
RSS_DIR = Path(os.environ.get("RSS_DIR", str(Path(__file__).resolve().parent)))
load_dotenv(RSS_DIR / ".env")

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
...
FEEDS_FILE = Path(os.environ.get("FEEDS_FILE", str(RSS_DIR / "feeds.txt")))
DB_PATH    = RSS_DIR / "rss_bot.db"
```

- If `RSS_DIR` is set (by launchd/nix), the instance is fully self-contained in
  that directory (feeds file, database, settings).
- If it is unset, everything defaults to the script's own directory, so a
  plain copy of the script stays a working standalone bot.

### Security: the bot only talks to its owner

Every Telegram handler is gated on the owner's chat:

```python
owner = filters.Chat(chat_id=TELEGRAM_CHAT_ID)
app.add_handler(CommandHandler("feed", cmd_feed, filters=owner))
...
app.add_handler(MessageHandler(filters.ALL & owner, handle_message))
app.add_handler(MessageReactionHandler(handle_reaction, chat_id=TELEGRAM_CHAT_ID))
```

- `TELEGRAM_CHAT_ID` is the **push target** for scheduled sends (daily digest,
  hourly-error alerts, `/scoreall` completion).
- It is also the **only chat that is ever answered**. Commands, messages and
  reactions from anyone else are silently dropped, so strangers cannot read your
  feed or mark articles as seen.
- Interactive replies to the owner go to `update.effective_chat.id`, so if you
  ever use the bot from another chat of yours it still routes correctly.

---

## Environment variables (.env)

| Variable | Example | Purpose |
|---|---|---|
| `TELEGRAM_TOKEN` | `123456:ABC...` | Bot token from @BotFather (one per bot, do not share) |
| `TELEGRAM_CHAT_ID` | `42` | Numeric id of the owner chat; **the only authorized chat** |
| `LLM_BACKEND` | `llamacpp` / `nim` / `zen` | Active LLM backend used for scoring (switched with `/backend`) |
| `<BACKEND>_BASE_URL` | `zen` → `https://opencode.ai/zen/v1` | OpenAI-compatible endpoint for that backend (e.g. `ZEN_BASE_URL`, `NIM_BASE_URL`, `LLAMACPP_BASE_URL`) |
| `<BACKEND>_MODEL` | `zen` → `deepseek-v4-flash-free` | Model name for that backend; `/model` writes to the active backend (e.g. `ZEN_MODEL`) |
| `<BACKEND>_API_KEY` | `ZEN_API_KEY=sk-...` | Key for that backend (e.g. `ZEN_API_KEY`, `NIM_API_KEY`) |
| `SEARXNG_URL` | `http://host.local:8888` | Local SearXNG instance used for `/search` |
| `DIGEST_HOUR` | `8` | Hour of day to push the daily digest (24h) |
| `IGNORE_AFTER_H` | `12` | Article age (h) before it's ignored by the digest |
| `TOP_N` | `30` | Max articles in the daily digest |
| `MIN_SCORE` | `10` | Minimum LLM score for a "keep" article |
| `PROFILE_EXAMPLES` | `30` | Number of saved-article examples used to derive the interest profile |
| `SCORE_BATCH` | `10` | Articles scored per LLM batch |
| `ARTICLE_MAX_AGE_DAYS` | `7` | Scrape/articles older than this are dropped |
| `LLM_MIN_INTERVAL` | `3` | Minimum seconds between LLM API calls (spaces out scoring under rate limits) |
| `LLM_MAX_RETRIES` | `4` | Retries per LLM request on failure/429, with backoff |

Settings come from `.env` (env vars win), then the `settings` table in the
instance DB, then `SETTING_DEFAULTS`.

`feeds.txt` is one feed URL per line, e.g. `http://arxiv.org/rss/cs.SE`.

---

## Fetch → score → digest pipeline

Runs on a scheduler inside the same process (APScheduler):

1. `hourly_job` — every 30 min: fetch all feeds in `feeds.txt`, insert unseen
   articles, then `score_unscored()` (LLM-scored, batched per `SCORE_BATCH`).
2. `daily_digest_job` — at `DIGEST_HOUR`: select fresh articles above
   `MIN_SCORE`, dedupe against an interest profile, and push to
   `TELEGRAM_CHAT_ID` through `send_digest()`.
3. Errors during the hourly job are pushed straight to the owner's chat.

The scraper, scoring, and search are all local — no cloud LLM needed if you
point `<BACKEND>_BASE_URL` (e.g. `LLAMACPP_BASE_URL`) at a local
`llama.cpp`/Ollama server.

---

## Floor commands (owner-only)

| Command | What it does |
|---|---|
| `/start` | Hello + bot commands |
| `/feed` | Get the current pending feed (fresh scored articles) |
| `/fetch` | Force a feed fetch + scoring cycle |
| `/profile` | Show the current saved-articles interest profile |
| `/remember` / `/get` / `/set` | Save / look up / change settings and profile |
| `/model NAME` | Switch the active backend's LLM model |
| `/backend NAME` | Switch LLM backend (must have `<NAME>_BASE_URL` in `.env`) |
| `/stats` | Bot statistics |
| `/commands` | List commands |
| `/scoreall` | Force-scoring all unscored articles |
| `/addfeed <url>` | Add a feed (auto-detects from a page/URL — accepts a story or homepage URL too) |
| `/removefeed <url>` | Remove a feed |
| `/search <q>` | Free-text search over the local DB (with pagination buttons) |

Reacting 👍/👌/etc. to an article marks it liked; other reactions mark it
ignored.

### `/search` syntax (SQLite FTS5)

The `/search` command uses SQLite's built-in FTS5 full-text search over article
**title** (2× weight) and **summary**. Supported operators:

| Syntax | Example | Meaning |
|---|---|---|
| simple term | `python` | contains "python" |
| exact phrase | `"machine learning"` | exact phrase match |
| prefix / wildcard | `program*` | words starting with "program" |
| boolean OR | `rust OR go` | either term |
| boolean NOT | `python NOT snake` | contains "python" but not "snake" |
| NEAR | `NEAR(ai model, 5)` | terms within 5 words |
| column-specific | `title:rust summary:async` | "rust" in title, "async" in summary |

Invalid FTS5 syntax is automatically wrapped as a quoted phrase (so it always
returns results instead of erroring).

---

## Running under nix-darwin (launchd)

Each instance is declared as a launchd user agent in your nix-darwin module
(e.g. `~/.config/nix-darwin/modules/system.nix`):

```nix
launchd.user.agents.rssbot-alice = {
  serviceConfig = {
    Label            = "com.rssbot-alice";
    ProgramArguments = [ "${python}"
                         "${home}/.rss-bot/rss_bot.py" ];
    WorkingDirectory = "${home}/.rss-bot/alice";
    EnvironmentVariables = { RSS_DIR = "${home}/.rss-bot/alice"; };
    RunAtLoad        = true;
    KeepAlive        = true;
    StandardOutPath  = "/tmp/rssbot-alice.log";
    StandardErrorPath = "/tmp/rssbot-alice.log";
  };
};

# (replace ${python} with your interpreter path, ${home} with your home dir)
```

Adding an instance is: add the `.env` + `feeds.txt` + agent block (with its own
`Label`, `RSS_DIR`, and log), then re-apply:

```
sudo darwin-rebuild switch
```

(Nix-darwin regenerates the plist in `~/Library/LaunchAgents/`.)

### Manual control (without rebuilding nix)

The plists live at `~/Library/LaunchAgents/com.rssbot-<name>.plist`,
label `com.rssbot-<name>`.

```
# check state
launchctl print gui/$(id -u)/com.rssbot-alice | grep -E "state|pid"

# restart a bot after a code change (picks up new rss_bot.py)
launchctl kickstart -k gui/$(id -u)/com.rssbot-alice

# stop / start
launchctl bootout   gui/$(id -u)/com.rssbot-alice
launchctl bootstrap gui/$(id -u)/com.rssbot-alice ~/Library/LaunchAgents/com.rssbot-alice.plist
```

---

## Current instances

| Instance | Data dir | `RSS_DIR` | Log |
|---|---|---|---|
| alice | `~/.rss-bot/alice` | `.../alice` | `/tmp/rssbot-alice.log` |
| bob | `~/.rss-bot/bob` | `.../bob` | `/tmp/rssbot-bob.log` |

> **Gotcha:** each bot pushes only to its own `TELEGRAM_CHAT_ID`. If a friend
> is messaging *your* bot in a shared admin chat, they talk to your feed and
> your reactions. The right bot is locked to its own chat id — double-check the
> username the other person started.

---

## Quick start (no nix)

```bash
pip install -r requirements.txt
mkdir myinstance && cp .env.example myinstance/.env   # fill in token, chat id, LLM backend
echo "https://example.org/feed" > myinstance/feeds.txt
RSS_DIR=$PWD/myinstance python rss_bot.py
```

---

## Debug / ops notes

- Logs: `/tmp/rssbot-<name>.log` (the same file for stdout and stderr).
- Database: sqlite3, `~/.rss-bot/<name>/rss_bot.db` (created on first run).
  Handy quick checks:
  ```
  sqlite3 ~/.rss-bot/<name>/rss_bot.db "SELECT COUNT(*) FROM articles"
  ```
- The scraper uses concurrent workers, so don't set a huge poll interval.
- `rss_bot.py` has no comments; keep changes minimal and spellcheck the logs —
  the bot only ever answers the owner, so debug by tailing the log, not by
  talking to it from another account.
