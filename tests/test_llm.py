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
async def _fake_fallback(status):
    switches.append(status)
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

print("ALL TESTS PASSED")