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
print("ALL TESTS PASSED")