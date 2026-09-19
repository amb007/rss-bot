import os
import sys
import tempfile
import importlib
from pathlib import Path

os.environ["TELEGRAM_TOKEN"] = "dummy"
os.environ["TELEGRAM_CHAT_ID"] = "1"

tmp = Path(tempfile.mkdtemp())
env = tmp / ".env"
env.write_text(
    "LLM_BACKEND=zen\n"
    "ZEN_BASE_URL=https://opencode.ai/zen/v1\n"
    "ZEN_MODEL=deepseek-v4-flash-free\n"
    "ZEN_API_KEY=sk-test\n"
    "# NIM (if LLM_BACKEND=nim):\n"
    "NIM_BASE_URL=https://integrate.api.nvidia.com/v1\n"
    "NIM_MODEL=meta/llama-3.1-8b-instruct\n"
)
os.environ["RSS_DIR"] = str(tmp)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import rss_bot as r

r.init_db()
r.load_settings()

assert r.current_llm_backend() == "zen", r.current_llm_backend()
assert r.llm_base_url() == "https://opencode.ai/zen/v1", r.llm_base_url()
assert r.llm_api_key() == "sk-test", r.llm_api_key()
assert r.current_llm_model() == "deepseek-v4-flash-free", r.current_llm_model()
assert sorted(r.known_backends()) == ["nim", "zen"], r.known_backends()
print("accessors OK")

# /model on zen -> writes ZEN_MODEL
assert r.set_llm_model("gpt-test")
assert r.current_llm_model() == "gpt-test"
assert "ZEN_MODEL=gpt-test" in (tmp / ".env").read_text()
print("set_llm_model OK:", r.current_llm_model())

# /backend to known backend
assert r.set_llm_backend("nim")
assert r.current_llm_backend() == "nim"
assert r.llm_base_url() == "https://integrate.api.nvidia.com/v1"
# model override reset -> falls back to NIM_MODEL
assert r.current_llm_model() == "meta/llama-3.1-8b-instruct", r.current_llm_model()
assert "LLM_BACKEND=nim" in (tmp / ".env").read_text()
print("set_llm_backend OK:", r.current_llm_backend())

# /backend to unknown backend -> rejected
assert not r.set_llm_backend("bogus")
assert r.current_llm_backend() == "nim"
print("unknown backend rejected OK")

# model after backend switch writes to NIM_MODEL
assert r.set_llm_model("nim-test")
assert "NIM_MODEL=nim-test" in (tmp / ".env").read_text()
assert r.current_llm_model() == "nim-test"
print("set_llm_model on nim OK")

# set_setting -> _write_env_line rewrites/inserts .env (refactor dedup)
assert r.set_setting("TOP_N", 15)
assert "TOP_N=15" in (tmp / ".env").read_text()
assert r.get_setting("TOP_N") == 15
env_before = (tmp / ".env").read_text()
assert r.set_setting("TOP_N", 20)
assert "TOP_N=20" in (tmp / ".env").read_text()
assert (tmp / ".env").read_text().count("TOP_N=") == 1, "set_setting must rewrite, not duplicate"
assert "# NIM (if LLM_BACKEND=nim):" in (tmp / ".env").read_text(), "comments preserved"
assert r.get_setting("TOP_N") == 20
print("set_setting -> _write_env_line OK")

# invalid setting key rejected
assert not r.set_setting("BOGUS", 5)
print("set_setting unknown key OK")

# set_llm_backend only accepts backends present in .env (known_backends)
assert r.set_llm_backend("zen")
assert r.current_llm_backend() == "zen"
assert not r.set_llm_backend("llamacpp")  # LLAMACPP_BASE_URL not in test .env
assert r.current_llm_backend() == "zen"
print("set_llm_backend known-only OK")

# discover_feed_url: page URL auto-resolves to its <link rel=alternate> feed
orig_client, orig_is_feed = r.httpx.AsyncClient, r.is_feed_url

class _FakeResp:
    text = '<html><head><link rel="alternate" type="application/rss+xml" href="https://wired.com/feed/rss"></head></html>'
    def raise_for_status(self): pass

class _FakeAsyncClient:
    def __init__(self, *a, **k): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *a, **k): pass
    async def get(self, url, headers=None): return _FakeResp()

r.httpx.AsyncClient = _FakeAsyncClient
r.is_feed_url = lambda url: url == "https://wired.com/feed/rss"
import asyncio as _asyncio

async def _check_discover():
    assert await r.discover_feed_url("https://wired.com/story/some-article") == "https://wired.com/feed/rss"
    # already a feed -> returned unchanged
    assert await r.discover_feed_url("https://wired.com/feed/rss") == "https://wired.com/feed/rss"

_asyncio.run(_check_discover())
r.httpx.AsyncClient, r.is_feed_url = orig_client, orig_is_feed
print("discover_feed_url OK")

# llm_chat payload: NIM-only fields only for nim backend
import httpx as _httpx

class _FakeJson:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body
    def json(self):
        return self._body
    def raise_for_status(self):
        if self.status_code >= 400:
            raise _httpx.HTTPStatusError("boom", request=_httpx.Request("POST", "http://x"), response=self)

async def _capture_payload(messages, max_tokens):
    captured = {}

    class _C(_httpx.AsyncClient):
        async def post(self, url, headers=None, json=None):
            captured["json"] = json
            return _FakeJson(200, {"choices": [{"message": {"content": "hi"}}]})

    orig = _httpx.AsyncClient
    _httpx.AsyncClient = _C
    try:
        await r.llm_chat([{"role": "user", "content": "ping"}], max_tokens=64)
    finally:
        _httpx.AsyncClient = orig
    return captured["json"]

import asyncio as _asyncio

async def _check_payload():
    r.set_llm_backend("nim")
    p_nim = await _capture_payload([], 64)
    assert p_nim.get("chat_template_kwargs") == {"enable_thinking": False}, p_nim
    assert p_nim.get("thinking") is False, p_nim
    r.set_llm_backend("zen")
    p_zen = await _capture_payload([], 64)
    assert "chat_template_kwargs" not in p_zen, p_zen
    assert "thinking" not in p_zen, p_zen

_asyncio.run(_check_payload())
print("llm_chat payload gating OK")

# instance_phase_offset_seconds: unique offsets per sorted instance dir
# (self-contained: temp sibling dirs, each holding a .env)
phase_parent = Path(tempfile.mkdtemp())
for name in ("alpha", "beta", "gamma"):
    (phase_parent / name).mkdir()
    (phase_parent / name / ".env").write_text("TELEGRAM_TOKEN=t\nTELEGRAM_CHAT_ID=1\n")
offsets = {}
for name in ("alpha", "beta", "gamma"):
    os.environ["RSS_DIR"] = str(phase_parent / name)
    import importlib
    import rss_bot as _r
    importlib.reload(_r)
    offsets[name] = _r.instance_phase_offset_seconds()

# restore env for other tests
os.environ["RSS_DIR"] = str(tmp)
importlib.reload(r)

assert [offsets[n] for n in ("alpha", "beta", "gamma")] == [0, 15, 30], offsets
print("instance_phase_offset_seconds OK:", offsets)

# --- smart fallback helpers ---
assert r.normalize_model_name("Gemini 3 Flash (high)") == "gemini-3-flash"
assert r.normalize_model_name("models/gemini-3.5-flash") == "gemini-3-5-flash"
assert r.normalize_model_name("moonshotai/kimi-k2.6") == "moonshotai-kimi-k2-6"
assert r._family_token("moonshotai/kimi-k2.6") == "kimi-k2", r._family_token("moonshotai/kimi-k2.6")
assert r._family_token("nvidia/nemotron-3-ultra-550b-a55b") == "nemotron-3-ultra"
print("normalize_model_name / _family_token OK")

# _swe_score_for: exact normalized match, else family substring, else neutral 50
swe = {"gemini-3-flash": 75.8, "kimi-k2-5": 70.8}
assert r._swe_score_for("gemini-3-flash", swe) == 75.8
assert r._swe_score_for("moonshotai/kimi-k2.6", swe) == 70.8  # family "kimi-k2" matches
assert r._swe_score_for("nvidia/nemotron-3-ultra-550b-a55b", swe) == 50.0
print("_swe_score_for OK")

# probe_model: classify by HTTP status
class _ProbeResp:
    def __init__(self, code):
        self.status_code = code

orig_client = _httpx.AsyncClient
async def _check_probe():
    results = {}
    class _P(_httpx.AsyncClient):
        def __init__(self, *a, **k): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a, **k): pass
        async def post(self, url, headers=None, json=None):
            return _ProbeResp(results["code"])
    _httpx.AsyncClient = _P
    try:
        for code, want in [(200, "ok"), (429, "rate_limited"), (404, "gone"),
                           (401, "gone"), (503, "down"), (422, "unknown")]:
            results["code"] = code
            got = await r.probe_model("nim", "x/y")
            assert got == want, (code, got, want)
    finally:
        _httpx.AsyncClient = orig_client
_asyncio.run(_check_probe())
print("probe_model OK")

# _looks_free heuristic: Zen -free suffix is the only reliable signal
assert r._looks_free("zen", "nemotron-3-ultra-free") is True
assert r._looks_free("zen", "claude-opus-4-7") is False
assert r._looks_free("zen", "gpt-5.1") is False
assert r._looks_free("nim", "nvidia/nemotron-3-ultra-550b-a55b") is None
print("_looks_free OK")

# --- /search query normalization (FTS5 prefix on hyphenated terms) ---
# valid single-token / prefix queries pass through unchanged
assert r.normalize_search_query("python") == "python"
assert r.normalize_search_query("program*") == "program*"
assert r.normalize_search_query("rust OR go") == "rust OR go"
# hyphen makes it invalid FTS5 → wrapped as a quoted phrase
assert r.normalize_search_query("e-graph") == '"e-graph"'
# trailing '*' → phrase-prefix (star *outside* quotes), not swallowed
assert r.normalize_search_query("e-graph*") == '"e-graph"*'
print("normalize_search_query OK")

# end-to-end: phrase-prefix actually matches plural + derived tokens
with r.db() as c:
    c.execute("DELETE FROM articles_fts")  # clear FTS index (rebuild next)
    for i, t in enumerate(["e-graph equality saturation",
                           "e-graphs for compilers",
                           "e-grapher tool",
                           "equality saturation only"]):
        c.execute("INSERT INTO articles (id,url,title,source) VALUES (?,?,?,?)",
                  (f"t{i}", f"http://x/{i}", t, "sx"))
        c.execute("INSERT INTO articles_fts(rowid, title) VALUES (last_insert_rowid(), ?)", (t,))

assert [r.search_articles('"e-graph"*', 10, 0)]  # ensure it runs
assert len(r.search_articles('"e-graph"*', 10, 0)) == 3  # e-graph, e-graphs, e-grapher
assert len(r.search_articles('"e-graph"', 10, 0)) == 1   # exact phrase only
print("phrase-prefix matching OK")

# make_article_msg renders the card (no text reaction — reactions are applied
# via set_message_reaction in _send_cards instead)
base = {"score": 42, "confidence": None, "title": "T", "source": "S",
        "url": "http://x", "hn_points": None, "hn_comments": None,
        "liked_at": None, "disliked_at": None}
assert r.make_article_msg(base).startswith("42")
assert "<a href=\"http://x\">T</a>" in r.make_article_msg(base)
print("make_article_msg OK")

# _is_text_model excludes image/tts/embedding/etc from fallback candidates
assert r._is_text_model("nvidia/nemotron-3-ultra-550b-a55b")
assert r._is_text_model("gemini-3-flash-preview")
assert not r._is_text_model("gemini-3-pro-image-preview")
assert not r._is_text_model("gemini-3.1-flash-image")
assert not r._is_text_model("gemini-2.5-pro-preview-tts")
assert not r._is_text_model("text-embedding-004")
print("_is_text_model OK")

# 429-storm fallback: consecutive exhausted 429s trigger _fallback_and_switch
_429_err = _httpx.HTTPStatusError("too many requests", request=_httpx.Request("POST", "http://x"),
                                  response=_httpx.Response(429, headers={}))

async def _always_429(messages, max_tokens):
    raise _429_err

orig_throttle = r._llm_throttle
async def _no_throttle():
    return
r._llm_throttle = _no_throttle   # speed up: no real throttle in tests

orig_request = r._llm_request
switches = []
async def _fake_fallback(status, prefer_other_provider=False):
    switches.append(status)
    assert prefer_other_provider is True   # storm must ask for a different provider
    return False   # no fallback selected → llm_chat must raise
r._llm_request = _always_429
try:
    orig_fb = r._fallback_and_switch
    r._fallback_and_switch = _fake_fallback
    r._consec_429_storms = 0
    try:
        for i in range(r._429_STORM_LIMIT):
            try:
                _asyncio.run(r.llm_chat([{"role": "user", "content": "x"}], retries=1))
                assert False, "should have raised"
            except _httpx.HTTPStatusError:
                pass
    finally:
        r._fallback_and_switch = orig_fb
    # storms 1..limit-1 → plain re-raise (no fallback attempted);
    # storm #limit → _fallback_and_switch(None) (model treated as degraded)
    assert switches == [None], switches
    print("429-storm fallback OK — switches:", switches)
finally:
    r._llm_request = orig_request
    r._llm_throttle = orig_throttle
    r._consec_429_storms = 0

# notify_owner is best-effort and must never raise
_asyncio.run(r.notify_owner("test notification"))
print("notify_owner OK")

# env: prefix resolution in .env
_os = __import__("os")
_os.environ["TEST_REAL"] = "s3c"
_os.environ["TEST_ALIAS"] = "env:TEST_REAL"
_os.environ["TEST_CHAIN"] = "env:TEST_ALIAS"
_os.environ["TEST_MISSING"] = "env:NOT_SET"
r._resolve_env_refs()
assert _os.environ["TEST_ALIAS"] == "s3c", "single env: ref failed"
assert _os.environ["TEST_CHAIN"] == "s3c", "chained env: ref failed"
assert _os.environ["TEST_MISSING"] == "", "missing ref not cleared"
# cleanup
for k in ["TEST_REAL", "TEST_ALIAS", "TEST_CHAIN", "TEST_MISSING"]:
    _os.environ.pop(k, None)
print("env: prefix resolution OK")

# _health_fresh: fresh "ok" is trusted, stale "ok" is not
_dt = __import__("datetime").datetime
_tz = __import__("datetime").timezone
_td = __import__("datetime").timedelta
from pathlib import Path as _P
import sqlite3 as _sq
_conn = _sq.connect(str(tmp / "rss_bot.db"))
_conn.execute("DELETE FROM model_health WHERE backend='test'")
_conn.commit()
# fresh entry (now)
_conn.execute("INSERT INTO model_health (backend, model_id, last_status, last_seen) VALUES (?,?,?,?)",
              ("test", "fresh-model", "ok", _dt.now(_tz.utc).isoformat()))
# stale entry (30h ago)
_conn.execute("INSERT INTO model_health (backend, model_id, last_status, last_seen) VALUES (?,?,?,?)",
              ("test", "stale-model", "ok", (_dt.now(_tz.utc) - _td(hours=30)).isoformat()))
_conn.commit()
_conn.close()
assert r._health_fresh("test", "fresh-model") == "ok", "fresh ok not trusted"
assert r._health_fresh("test", "stale-model") is None, "stale ok wrongly trusted"
assert r._health_fresh("test", "missing-model") is None, "missing should be None"
print("_health_fresh staleness OK")

# /models provider shortcut + model button text
assert r._prov_short("nim") == "NIM"
assert r._prov_short("goo") == "GOO"
assert r._prov_short("zen") == "ZEN"
assert r._prov_short("llamacpp") == "LCP"
assert r._prov_short("weird") == "WEI"
print("_prov_short OK")

btn = r._model_button_text("nim", "meta/llama-3.1-8b-instruct", {})
assert "meta/llama-3.1-8b-instruct" in btn
assert btn.count("%") == 1
# unknown free/paid → "?" placeholder (nim has no free flag)
assert " ? " in btn or btn.startswith("? ") or btn.endswith("? ") or " ?" in btn
# starts with a leading mark (✓/●/·/x/?)
assert btn.split(" ")[0] in ("✓", "●", "·", "x", "?", "free", "$") or btn.startswith("✓ ")

# zen -free suffix → ☮ (peace symbol) free marker
btnz = r._model_button_text("zen", "deepseek-v4-flash-free", {})
assert "\u262e" in btnz
assert "$" not in btnz

# goo: flash-lite free, pro paid, flash-image paid
assert r._looks_free("goo", "gemini-3.1-flash-lite-preview") is True
assert r._looks_free("goo", "gemini-flash-latest") is True
assert r._looks_free("goo", "gemini-3.1-pro-preview") is False
assert r._looks_free("goo", "gemini-2.5-flash-image") is False
assert r._looks_free("nim", "meta/llama-3.1-8b-instruct") is None
print("_model_button_text OK:", btn[:40], "| zen:", btnz[:40])

# --- digest: group_digest splits into sections within budget ---
def _mk(rid, score, hnpts=0, hncmts=0):
    return {"id": rid, "title": f"t{rid}", "source": "s", "score": score,
            "url": f"u{rid}", "hn_points": hnpts, "hn_comments": hncmts}

# 12 items: top 4 by score = id descending (score 12..1), rest have HN signal
pool = [_mk(i, score=i, hnpts=i, hncmts=i) for i in range(12, 0, -1)]
top, disc, skim = r.group_digest(pool, budget=8)
assert [x['id'] for x in top] == [12, 11, 10, 9], [x['id'] for x in top]
assert [x['id'] for x in disc] == [8, 7, 6], [x['id'] for x in disc]
assert [x['id'] for x in skim] == [5], [x['id'] for x in skim]
assert len(top) + len(disc) + len(skim) <= 8
print("group_digest split OK")

# discussed section only picks the most HN-active among the non-top rest
mixed = [_mk(1, score=1, hnpts=99, hncmts=99), _mk(2, score=2, hnpts=0, hncmts=0),
         _mk(3, score=3, hnpts=5, hncmts=0), _mk(4, score=4, hnpts=0, hncmts=0),
         _mk(5, score=5, hnpts=10, hncmts=0), _mk(6, score=6, hnpts=1, hncmts=0)]
mixed.sort(key=lambda x: x['score'], reverse=True)  # digest_pool orders score-desc
top2, disc2, skim2 = r.group_digest(mixed, budget=8)
assert [x['id'] for x in top2] == [6, 5, 4, 3]
assert [x['id'] for x in disc2] == [1], [x['id'] for x in disc2]  # only HN-active, most = id1
print("group_digest discussed (HN-ranked only) OK")

# no-HN pool → discussed empty, skim fills the rest
plain = [_mk(i, score=i, hnpts=0, hncmts=0) for i in range(10, 0, -1)]
top3, disc3, skim3 = r.group_digest(plain, budget=8)
assert disc3 == []
assert len(top3) + len(skim3) == 8
print("group_digest no-HN OK")

# render_digest_message: sections + numbering + footer + HTML escaped
msg = r.render_digest_message(top, disc, skim, total=12)
assert "🔥 <b>Top picks</b>" in msg
assert "💬 <b>Most discussed</b>" in msg
assert "📚 <b>Worth a skim</b>" in msg
assert "1. " in msg and "5. " in msg
assert "more in /feed" not in msg
# digest message no longer boasts about unshown ones (shown==total here)

# escaped title with markup
escaped_pool = [_mk("x", 10, 0, 0)]
escaped_pool[0]["title"] = "A <b>bold</b> & ampersand"
t, d, s = r.group_digest(escaped_pool, budget=8)
emsg = r.render_digest_message(t, d, s, total=1)
assert "<b>bold</b>" not in emsg.replace("<b>Top picks</b>", ""), "title not escaped"
# line-count sanity: single section, header + label + 1 item + blank + footer ~5 lines
assert emsg.count("\n") <= 8, emsg
print("render_digest_message OK")

# digest_pool: only new + unseen; respects previous last_digest; survives missing key
import datetime as _dtm

def _ins(rid, published, score=50, sent=None):
    with r.db() as c:
        c.execute("INSERT INTO articles (id,url,title,source,published,score,sent_at)"
                  " VALUES (?,?,?,?,?,?,?)", (rid, f"http://x/{rid}", f"t{rid}", "s", published, score, sent))

_ins("d1", _dtm.datetime.now(_dtm.timezone.utc).isoformat(), 50, None)  # new, in window → eligible
_ins("d2", r.age_cutoff(), 50, "2026-01-01T00:00:00")  # already sent → excluded
_ins("d3", "1990-01-01T00:00:00", 50, None)    # too old (beyond age_cutoff) → excluded
_ins("d4", r.age_cutoff(), 1, None)             # below MIN_SCORE → excluded
# no last_digest set → digest_pool must NOT KeyError (stored-only key, absent)
p = r.digest_pool()
assert {x['id'] for x in p} == {"d1"}, {x['id'] for x in p}
assert p and p[0]['id'] == 'd1'
# after recording a last_digest, older-but-unseen remain eligible only if newer than it
with r.db() as c:
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ("last_digest", "2100-01-01T00:00:00"))
p2 = r.digest_pool()
assert p2 == [], p2
print("digest_pool new+unseen + missing-key OK")


# --- select_fallback: rate_limited NOT accepted, tried pairs remembered ---
orig_disc, orig_probe, orig_known = r.discover_models, r.probe_model, r.known_backends
orig_text, orig_health = r._is_text_model, r._health_fresh
orig_swe = r._load_swe_scores
orig_family = r._family_token
r._load_swe_scores = lambda: {}
plan = {}   # backend -> (seq, statuses) consumed in order
probed = []
r.known_backends = lambda: ["goo", "nim"]
r._is_text_model = lambda m: True
r._health_fresh = lambda b, m: None
r._family_token = lambda m: ""
async def _fake_discover(b):
    return [f"{b}-m1", f"{b}-m2"]
async def _fake_probe(b, m):
    probed.append((b, m))
    seq, statuses = plan[b]
    plan[b] = (seq + 1, statuses)
    return statuses[seq]
r.discover_models = _fake_discover
r.probe_model = _fake_probe

try:
    # (1) default path: goo is preferred (same-backend bonus) but its probes come
    # back rate_limited → rejected → keep probing → pick the genuinely-ok nim.
    # A rate_limited model is never the final fallback answer.
    r._fallback_tried.clear()
    probed.clear()
    plan = {"goo": (0, ["rate_limited"]*50), "nim": (0, ["ok", "ok"])}
    got = _asyncio.run(r.select_fallback("goo-old"))
    assert got == ("nim", "nim-m1"), got
    assert ("goo", "goo-m1") in probed   # goo was probed & rejected
    print("select_fallback rejects rate_limited, jumps provider OK", got)

    # (2) without reset, the already-tried goo-m1/goo-m2 are skipped (not
    # re-probed); a fresh nim model is still eligible and picks.
    probed.clear()
    plan = {"goo": (0, ["ok"]*50), "nim": (0, ["ok", "ok"])}
    got = _asyncio.run(r.select_fallback("goo-old"))
    assert got == ("nim", "nim-m1"), got
    assert not any(b == "goo" for b, m in probed), probed   # goo skipped, not re-tried
    print("select_fallback skips already-tried OK")

    # (3) all-fail → None, and every failed pair is remembered
    r._fallback_tried.clear()
    probed.clear()
    plan = {"goo": (0, ["rate_limited"]*50), "nim": (0, ["down", "down"])}
    got = _asyncio.run(r.select_fallback("goo-old"))
    assert got is None, got
    assert any(b == "goo" for b, m in r._fallback_tried)
    assert any(b == "nim" for b, m in r._fallback_tried)
    print("select_fallback remembers failures + returns None OK")
finally:
    r.discover_models, r.probe_model, r.known_backends = orig_disc, orig_probe, orig_known
    r._is_text_model, r._health_fresh = orig_text, orig_health
    r._load_swe_scores = orig_swe
    r._family_token = orig_family
    r._fallback_tried.clear()


# --- hnrss.org scoring feed parsing + digest overlay + theme ---
hn_xml = ("<?xml version=\"1.0\"?><rss version=\"2.0\"><channel>"
          "<item><link>https://example.com/a</link>"
          "<comments>https://news.ycombinator.com/item?id=111</comments>"
          "<description><![CDATA[<p>Points: 42</p><p># Comments: 7</p>]]></description></item>"
          "<item><link>https://example.com/b</link>"
          "<comments>https://news.ycombinator.com/item?id=222</comments>"
          "<description><![CDATA[<p>Points: 1,234</p><p># Comments: 0</p>]]></description></item>"
          "</channel></rss>")
parsed = r.parse_hn_scoring_feed(hn_xml)
assert parsed["https://example.com/a"]["points"] == 42, parsed
assert parsed["https://example.com/a"]["comments"] == 7, parsed
assert parsed["https://example.com/b"]["points"] == 1234, parsed  # comma stripped
assert "https://news.ycombinator.com/item?id=111" in parsed  # indexed under item URL too
print("parse_hn_scoring_feed OK", parsed["https://example.com/a"])

# overlay matches pool rows and fills HN fields even when stored url is the item
pool = [{"url": "https://example.com/a", "hn_points": 0, "hn_comments": 0},
        {"url": "https://example.com/b/", "hn_points": 0, "hn_comments": 0},
        {"url": "https://nowhere.example", "hn_points": 0, "hn_comments": 0}]
r._overlay_hn_scores(pool, parsed)
assert pool[0]["hn_points"] == 42 and pool[0]["hn_comments"] == 7
assert pool[1]["hn_points"] == 1234, pool[1]
assert pool[2]["hn_points"] == 0  # unmatched stays 0
print("overlay_hn_scores OK")

# render includes the theme opener right under the header, before Top picks
_top = [_mk("t", 10)]
_t, _d, _s = r.group_digest(_top, budget=8)
_msg = r.render_digest_message(_t, _d, _s, 1, theme="Today is all about X.")
assert _msg.startswith("📰 Daily Digest"), _msg
assert "Today is all about X." in _msg
assert _msg.index("Today is all about X.") < _msg.index("Top picks"), _msg
print("render_digest_message with theme OK")

print("ALL TESTS PASSED")