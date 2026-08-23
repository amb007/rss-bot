# Using your RSS bot in Telegram — a user's guide

This bot keeps a personal news digest and sends it to your Telegram chat once a
day. It learns what you like, so the more you interact with it, the better its
recommendations get.

This guide is written for the person **talking to the bot in Telegram**. If you
are the person who set up the bot, you only need the short version — everything
here works for you too.

---

## First contact

Find the bot in Telegram and press **Start** (or send `/start`).

`/start` just prints the command list (same as `/commands`).

> If the bot does not answer you, you are talking to the **wrong bot**. Each
> bot only replies to exactly one Telegram account (its owner's). Check the
> bot's username in the chat header against the one you were given.

---

## What you get each day

Once a day the bot pushes your **daily digest** — a batch of article links,
sorted by how well they match your taste:

```
85 · <link> Title of an article — The Source
78 · <link> Another headline — Different Site
...
📰 Your feed — 12 articles
```

Each line is `score · link title — source`. A `~` after the score means the bot
is only *fairly* sure it matches you; a `?` means it is guessing. Higher is
better.

The bot is quiet the rest of the day unless something breaks — then it sends
you a short error note.

---

## Teaching the bot what you like 👍 / 👎

Reacting to any article message is the single most powerful thing you can do.
The bot reads reactions on the articles it sends you.

| Reaction | Meaning |
|---|---|
| 👍 ❤️ 🔥 | I like this — similar articles should score higher |
| 👎 🤮 💤 😐 | Not for me — similar articles should score lower |

That's it — reactions are recorded in the background and feed your **taste
profile**, which the bot uses when scoring new articles.

---

## Commands

| Command | What it does |
|---|---|
| `/feed` | Show your current best articles right now (instead of waiting for the daily digest). Add a number to change the count, e.g. `/feed 5`. |
| `/fetch` | Pull the newest articles from your subscribed feeds now (usually you don't need to — the bot does this itself every 30 minutes) |
| `/scoreall` | Force-score everything that isn't scored yet (e.g. right after setup). Usually not needed. |
| `/profile` | See your current taste profile — the plain-text description of "what you're into" (with the 👍/👎 build up) |
| `/remember ...` | Tell the bot directly: `/remember I don't want crypto news` or `/remember More deep-work essays`. Updates your profile. |
| `/get [KEY]` | Show a setting. Without a KEY it lists all of them. |
| `/set KEY VALUE` | Change a setting (e.g. `/set TOP_N 20`, `/set MIN_SCORE 5`). See the table below. |
| `/model [NAME]` | Change or view the model used for scoring. Without a NAME it shows the current one. Persists to `.env` as the active backend's `<BACKEND>_MODEL`. |
| `/backend [NAME]` | Switch which LLM backend does the scoring (switches the model with it). Only backends configured in `.env` are accepted; without a NAME it lists them. |
| `/stats` | The bot's stats — total articles, how many are new, scored, ignored... Good for checking what's going on. |
| `/addfeed https://...` | Subscribe to a new RSS/Atom feed. You can also paste an article or homepage URL — the bot finds the real feed automatically |
| `/removefeed [url]` | Unsubscribe. Two ways: reply to any article message (the bot finds its source feed) or pass the URL. |
| `/search QUERY` | Full-text search over everything the bot has stored (try `/search openai`, `/search nba`) |
| `/commands` | Show this list |

### Search tips (FTS5 syntax)

The `/search` command uses SQLite FTS5 over **title** (2× weight) and **summary**.
It understands:

| Query | Meaning |
|---|---|
| `openai` | contains "openai" |
| `"large language model"` | exact phrase |
| `program*` | prefix: "program", "programming", etc. |
| `rust OR go` | either term |
| `ai NOT art` | "ai" but not "art" |
| `NEAR(machine learning, 3)` | terms within 3 words |
| `title:python summary:async` | column-scoped |

If the query fails FTS5 syntax, it's wrapped as a phrase (so you always get
results instead of an error).

---

### Settings you can tune

| KEY | Meaning | Default |
|---|---|---|
| `TOP_N` | How many articles appear in a `/feed` or daily digest | `30` |
| `MIN_SCORE` | Minimum score for an article to make the digest (raise if you get too much fluff, lower if too little) | `10` |
| `DIGEST_HOUR` | Hour (0–23) when the daily digest is pushed | `8` |
| `IGNORE_AFTER_H` | Ignore articles older than this many hours | `12` |
| `SCORE_BATCH` | Number of articles LLM-scored per batch (affects how fast `/scoreall` finishes) | `10` |
| `PROFILE_EXAMPLES` | How many liked articles to derive the taste profile from | `30` |
| `ARTICLE_MAX_AGE_DAYS` | Drop articles older than this many days | `7` |
| `IGNORE_SCORE_FLOOR` | Score threshold to auto-ignore while learning | `80` |
| `LLM_MIN_INTERVAL` | Minimum seconds between LLM calls (raise if you hit "Too many requests") | `3` |
| `LLM_MAX_RETRIES` | Retries per LLM request on failure (429 included), with backoff | `4` |

`/set` always works for the owner; if it turns out the bot ignores your
changes, check with `/get` whether the value was accepted.

---

## Getting the most out of it

- **React to things.** The bot's scoring is driven by your reactions; if you
  never react, recommendations stay broad.
- **Use /remember for preferences you can't express with a reaction alone**:
  `/remember I follow the NBA but skip the NFL`.
- **Use /feed to trigger** the next batch instantly if you're craving news
  before the daily digest.
- **Feed hygiene.** `/addfeed` the sites you actually want; `/removefeed` the
  ones that just add noise.

---

## Common troubleshooting (from the user side)

- **"No answer."** → Wrong bot. You must talk to *your* bot — the one you were
  sent. Each bot ignores everyone except its owner.
- **"The bot is quiet today."** → It only speaks when there's a digest slot or
  a problem. Run `/stats` to see if new articles are being picked up, and
  `/feed` for an on-demand view.
- **"Nothing matches me yet."** → The profile is still empty. `/remember` your
  interests and keep reaction- 👍 articles; it learns fast.
- **"I don't want that feed anymore."** → Reply to an article from it and send
  `/removefeed` hands-free, or `/removefeed https://the.feed.url`.
- **"Too much noise."** → Raise `MIN_SCORE` and/or shrink `TOP_N`:
  `/set MIN_SCORE 15`, `/set TOP_N 15`.

---

## If this is one of several bots in the family

This repo runs several bots, one per person, each with its own feeds and its
own chat. As a Telegram user that means exactly one rule:

> Talk to **your** bot, not someone else's. Your bot knows your feeds and your
> profile. Someone else's bot ignores you completely and never shows your
> feed.