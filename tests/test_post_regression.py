"""Regression tests for market-monday-post.py (v17.4 fixes).

Covers: post_ids consistency, char-trim overflow, verify_posts error handling,
root_pid None check, load_token corruption.
"""
import json
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest

# Load post module
_POST_PATH = Path(__file__).parent.parent / "scripts" / "market-monday-post.py"
_spec = __import__("importlib").util.spec_from_file_location("mmp_post", _POST_PATH)
post = __import__("importlib").util.module_from_spec(_spec)
_spec.loader.exec_module(post)


# ─── Bug #1: post_ids mixed types (TypeError on sort) ───────────────────────

def test_post_ids_consistent_tuple_format_on_retry():
    """post_ids must contain (slide_idx, pid) tuples on BOTH initial and retry paths.

    Bug: retry path appended raw `pid` (str) instead of (slide_idx, pid) tuple.
    main() then crashed at `sorted(post_ids, key=lambda x: x[0])` with
    `'<' not supported between instances of 'int' and 'str'`.
    """
    fake_resp = mock.MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = {"id": "fake_id"}
    fake_resp.text = ""

    call_count = [0]
    def maybe_fail(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 2:  # second call (slide 1 first attempt) fails
            raise Exception("simulated network error")
        return fake_resp

    post._HTTP = mock.MagicMock()
    post._HTTP.post = mock.MagicMock(side_effect=maybe_fail)
    post._HTTP.get = mock.MagicMock(return_value=fake_resp)

    # Run post_thread
    slides = ["Slide 1 text " * 20, "Slide 2 text " * 20, "Slide 3 text " * 20]
    result = post.post_thread("uid", "tok", slides, None)

    # All entries must be tuples
    for entry in result:
        assert isinstance(entry, tuple), f"Expected tuple, got {type(entry).__name__}: {entry!r}"
        assert len(entry) == 2

    # Sorting must work without TypeError
    sorted_result = sorted(result, key=lambda x: x[0])
    assert len(sorted_result) == len(result)


# ─── Bug #2: char-trim produces 501 chars (Threads rejects >500) ───────────

def test_char_trim_keeps_within_500_limit():
    """The 500+1 ellipsis char-trim overflow bug.

    Bug: `trimmed.rstrip() + "…"` produced 501 chars. Threads API rejects >500.
    Fix: replace last char with ellipsis to stay at exactly 500.
    """
    long_text = "a" * 600
    trimmed = long_text[:500]
    text = trimmed[:-1].rstrip() + "…"
    assert len(text) <= 500, f"Trim must be ≤500, got {len(text)}"


# ─── Bug #3: verify_posts crashes on non-JSON response ─────────────────────

def test_verify_posts_handles_non_json_response():
    """verify_posts must not crash when Threads API returns HTML error page.

    Bug: `r.json().get("data", [])` raised JSONDecodeError on non-JSON response,
    crashing the entire --verify command.
    """
    class BadJsonResp:
        status_code = 500
        text = "<html>Server error</html>"
        def json(self):
            raise json.JSONDecodeError("bad", "x", 0)

    post._HTTP = mock.MagicMock()
    post._HTTP.get = mock.MagicMock(return_value=BadJsonResp())

    # Should not raise
    results = post.verify_posts("uid", "tok", 10)
    assert results == []


def test_verify_posts_handles_network_error():
    """verify_posts must handle httpx network errors gracefully."""
    import httpx
    post._HTTP = mock.MagicMock()
    post._HTTP.get = mock.MagicMock(side_effect=httpx.ConnectError("network down"))

    # Should not raise
    results = post.verify_posts("uid", "tok", 10)
    assert results == []


# ─── Bug #4: root_pid None check wrong condition ────────────────────────────

def test_post_thread_returns_early_when_root_fails():
    """When root (i=0) fails twice, post_thread must return immediately
    with empty list, NOT post subsequent slides as their own roots.

    Bug: previous check `if root_pid is None and i > 0` missed the case where
    the ROOT itself (i=0) failed — then all subsequent slides posted as roots.
    """
    fake_resp = mock.MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = {"id": "fake_id"}
    fake_resp.text = ""

    # Make ALL post calls fail
    post._HTTP = mock.MagicMock()
    post._HTTP.post = mock.MagicMock(side_effect=Exception("simulated fail"))

    slides = ["Slide 1 text " * 20, "Slide 2 text " * 20, "Slide 3 text " * 20]
    result = post.post_thread("uid", "tok", slides, None)

    # No slides should be posted (root failed, so nothing should attempt)
    assert result == [], f"Expected empty result, got {result}"


# ─── Bug #5: load_token crashes on corrupted file ──────────────────────────

def test_load_token_exits_cleanly_on_corruption():
    """load_token must exit gracefully on corrupted token file, not crash."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write("{corrupted json")
        tmp_path = Path(f.name)

    orig = post.TOKEN_FILE
    post.TOKEN_FILE = tmp_path
    try:
        with pytest.raises(SystemExit) as exc_info:
            post.load_token()
        assert exc_info.value.code == 1
    finally:
        post.TOKEN_FILE = orig
        tmp_path.unlink()


def test_load_token_exits_on_missing_file():
    """load_token must exit gracefully on missing token file."""
    fake_path = Path("/tmp/nonexistent_token_xyz_12345.json")
    if fake_path.exists():
        fake_path.unlink()
    orig = post.TOKEN_FILE
    post.TOKEN_FILE = fake_path
    try:
        with pytest.raises(SystemExit) as exc_info:
            post.load_token()
        assert exc_info.value.code == 1
    finally:
        post.TOKEN_FILE = orig


# ─── parse_slides sanity ───────────────────────────────────────────────────

def test_parse_slides_basic():
    """parse_slides splits on === separator."""
    text = "Slide 1\n\n===\n\nSlide 2\n\n===\n\nSlide 3"
    slides = post.parse_slides(text)
    assert len(slides) == 3
    assert "Slide 1" in slides[0]
    assert "Slide 3" in slides[2]


def test_parse_slides_filters_empty():
    """parse_slides filters empty entries."""
    text = "Slide 1\n\n===\n\n\n\n===\n\nSlide 3"
    slides = post.parse_slides(text)
    assert len(slides) == 2
