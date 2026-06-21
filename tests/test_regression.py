"""Regression tests for bugs fixed in v17.x.

Each test corresponds to a specific bug fix. Run with: pytest tests/test_regression.py -v
"""
import pytest
import importlib.util
from pathlib import Path

# Load pipeline module (it's not a package, file is in scripts/)
_PIPELINE_PATH = Path(__file__).parent.parent / "scripts" / "market-monday-pipeline.py"
_spec = importlib.util.spec_from_file_location("mmp", _PIPELINE_PATH)
mmp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mmp)


# ─── Bug #1: THREADS_SCRIPT path ────────────────────────────────────────────

def test_threads_script_path_exists():
    """THREADS_SCRIPT must point to the actual market-monday-post.py file.
    Bug: previously pointed to ~/.hermes/market-monday/... (didn't exist),
    causing post_to_threads to silently skip every post.
    """
    assert mmp.THREADS_SCRIPT.exists(), f"THREADS_SCRIPT not found: {mmp.THREADS_SCRIPT}"
    assert mmp.THREADS_SCRIPT.name == "market-monday-post.py"
    assert mmp.THREADS_SCRIPT.is_relative_to(Path(__file__).parent.parent / "scripts")


# ─── Bug #2: is_finance_niche loads env ──────────────────────────────────────

def test_is_finance_niche_calls_load_env():
    """is_finance_niche must call load_env() at the top so MISTRAL_API_KEY
    is available in cron/headless context. Without it, the function silently
    defaults to True and bypasses the finance filter.
    """
    import inspect
    src = inspect.getsource(mmp.is_finance_niche)
    # Find the function body up to the first 'if not article_content'
    body = src.split("def is_finance_niche")[1].split("if not article_content")[0]
    assert "load_env()" in body, "is_finance_niche must call load_env() before checking api_key"


# ─── Bug #3: AMBIGUOUS_EXCLUDES word-boundary for short tokens ───────────────

def test_exclude_no_false_positive_on_kemas():
    """'kemas' contains 'emas' as substring but should NOT trigger exclude.

    Bug: substring match on AMBIGUOUS_EXCLUDES caused 'emas' to match inside
    'kemas'/'lemas'/'kemeja' etc. Now uses word-boundary for ≤4-char tokens.
    """
    text = "Produk makanan dikemas dengan rapi untuk pasar tradisional"
    assert mmp.check_exclude_keywords(text) is None


def test_exclude_no_false_positive_on_memblokir():
    """'memblokir' contains 'blok' as substring but should NOT trigger exclude."""
    text = "Bank Indonesia memblokir transaksi mencurigakan dari luar negeri"
    assert mmp.check_exclude_keywords(text) is None


def test_exclude_still_flags_emas_without_finance_context():
    """Standalone 'emas' without nearby include keyword must still be flagged."""
    text = "Perhiasan emas antik ditemukan di situs arkeologi"
    # No "harga emas", "emas" with finance context within ±100 chars
    result = mmp.check_exclude_keywords(text)
    assert result is not None
    assert "emas" in result


def test_exclude_accepts_emas_with_finance_context():
    """'harga emas' with finance context (±100 chars) must NOT be flagged."""
    text = "Harga emas Antam hari ini naik menjadi Rp 1.500.000 per gram di pasar"
    assert mmp.check_exclude_keywords(text) is None


# ─── Bug #4: validate_grounding only iterates 6 slides ──────────────────────

def test_validate_grounding_uses_six_slides():
    """validate_grounding should iterate 1-6 (not 1-7, leftover from v15).

    Stale range(1, 8) was harmless (slide_7 returned empty) but the comment
    said 6 slides, so the loop bound was inconsistent with the spec.
    """
    import inspect
    src = inspect.getsource(mmp.validate_grounding)
    # Find the for loop
    assert "range(1, 6)" in src or "range(1, 7)" in src
    assert "range(1, 8)" not in src
    assert "range(1,8)" not in src


# ─── Bug #5: dead code removed ───────────────────────────────────────────────

def test_dead_boost_functions_removed():
    """apply_topic_boost and apply_time_boost were no-op stubs never called.
    They should be removed (or kept as inline no-ops if external callers).
    """
    # If kept, they should not be referenced by score_candidate
    import inspect
    score_src = inspect.getsource(mmp.score_candidate)
    assert "apply_topic_boost" not in score_src
    assert "apply_time_boost" not in score_src


# ─── Bug #6: redundant import removed ────────────────────────────────────────

def test_check_include_keywords_no_redundant_re_import():
    """The 'import re as _re' inside check_include_keywords is redundant —
    re is already imported at module top. Should use 're' directly.
    """
    import inspect
    src = inspect.getsource(mmp.check_include_keywords)
    assert "import re" not in src
    assert "_re" not in src


# ─── Stress test findings (v17.3) ────────────────────────────────────────────

def test_is_fresh_rejects_future_dated_articles():
    """is_fresh must return False for future-dated articles (clock skew / TZ mismatch).

    Bug: previously `age < 0 < hours*3600` was True, so future-dated articles
    were treated as fresh AND got max 15 recency points. Now rejects explicitly.
    """
    import datetime
    # 24 hours in the future
    future = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=24))
    future_str = future.strftime("%a, %d %b %Y %H:%M:%S +0000")
    assert mmp.is_fresh(future_str) is False, "Future-dated article must NOT be fresh"

    # Way in the future (1 year)
    far_future = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365))
    far_str = far_future.strftime("%a, %d %b %Y %H:%M:%S +0000")
    assert mmp.is_fresh(far_str) is False


def test_is_fresh_accepts_recent_articles():
    """is_fresh must still return True for articles within the window."""
    import datetime
    # 2 hours ago
    recent = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2))
    recent_str = recent.strftime("%a, %d %b %Y %H:%M:%S +0000")
    assert mmp.is_fresh(recent_str) is True


def test_is_fresh_rejects_old_articles():
    """is_fresh must return False for articles outside the 24h window."""
    import datetime
    # 48 hours ago
    old = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=48))
    old_str = old.strftime("%a, %d %b %Y %H:%M:%S +0000")
    assert mmp.is_fresh(old_str) is False


def test_format_slides_handles_string_slide():
    """format_slides must not crash when slide is a raw string (e.g., from
    plain-text format that bypassed normalize).

    Bug: previously `slide.get(...)` raised AttributeError. Now wraps string
    as a dict before accessing fields.
    """
    # slide_1 as string
    out = mmp.format_slides({"slide_1": "raw hook text", "slide_2": "raw body"})
    assert len(out) == 2
    assert out[0]["hook"] == "raw hook text"
    assert out[1]["content"] == "raw body"

    # None / non-dict skipped gracefully
    out = mmp.format_slides({"slide_1": None, "slide_2": 12345})
    assert len(out) == 2
    assert out[0]["hook"] == ""  # None → empty


def test_select_best_candidate_handles_none_articles():
    """select_best_candidate must skip None/non-dict articles without crashing.

    Bug: previously crashed with AttributeError on None. Now skips gracefully.
    """
    # None in list
    out = mmp.select_best_candidate([None, None], set(), {}, [], top_n=1)
    assert out == []

    # Mix of valid + None
    valid = {
        "title": "IHSG naik 5% ke level 7000",
        "url": "https://test.com/1",
        "source": "CNBC Indonesia",
        "description": "Pasar modal Indonesia",
        "published": "Sun, 21 Jun 2026 10:00:00 +0000",
    }
    out = mmp.select_best_candidate([None, valid], set(), {}, [], top_n=1)
    assert len(out) == 1
    assert out[0][1]["title"] == "IHSG naik 5% ke level 7000"

    # Empty title skipped
    empty_title = {"title": "", "url": "x", "source": "X", "published": "Sun, 21 Jun 2026 10:00:00 +0000"}
    out = mmp.select_best_candidate([empty_title], set(), {}, [], top_n=1)
    assert out == []


def test_score_candidate_robust_to_missing_fields():
    """score_candidate must handle articles with minimal fields (defensive)."""
    # All defaults — should not crash
    score = mmp.score_candidate({"title": "IHSG bitcoin", "url": "x"}, set(), {})
    assert isinstance(score, int) and score >= 0

    # Empty dict
    score = mmp.score_candidate({}, set(), {})
    assert isinstance(score, int) and score >= 0


# ─── Stress test ronde 3 findings (v17.4) ────────────────────────────────────

def test_post_to_threads_requires_both_root_and_permalink():
    """post_to_threads must return success=True ONLY when BOTH root_id AND
    permalink are present. Previously returned success=True with permalink=None,
    which caused update_analytics to store permalink=None for valid posts.
    """
    from unittest import mock
    import subprocess

    staging = {
        "title": "T", "url": "u", "slides": [
            {"hook": "H1", "content": ""},
            {"hook": "", "content": "C2"},
        ]
    }

    # Case 1: root_id only (no permalink) — should be FAIL
    with mock.patch('subprocess.run') as mr:
        mr.return_value.returncode = 0
        mr.return_value.stdout = "Root: abc123\n"
        mr.return_value.stderr = ""
        success, rid, link = mmp.post_to_threads(staging)
        assert success is False, "Root-only output should NOT be success"
        assert rid is None and link is None

    # Case 2: both root and permalink — should be SUCCESS
    with mock.patch('subprocess.run') as mr:
        mr.return_value.returncode = 0
        mr.return_value.stdout = "Root: abc123\nPost: https://threads.net/x\n"
        mr.return_value.stderr = ""
        success, rid, link = mmp.post_to_threads(staging)
        assert success is True
        assert rid == "abc123"
        assert link == "https://threads.net/x"

    # Case 3: permalink only (no root) — should be FAIL
    with mock.patch('subprocess.run') as mr:
        mr.return_value.returncode = 0
        mr.return_value.stdout = "Post: https://threads.net/x\n"
        mr.return_value.stderr = ""
        success, rid, link = mmp.post_to_threads(staging)
        assert success is False


def test_call_llm_preserves_partial_content_on_stream_die():
    """call_llm must return any partial content already buffered when the
    stream connection dies mid-flight (ChunkedEncodingError). Previously
    discarded all tokens, wasting API spend.
    """
    from unittest import mock
    import requests as r

    class FakeResponse:
        status_code = 200
        def iter_lines(self):
            yield b'data: {"choices": [{"delta": {"content": "Hello "}}]}'
            yield b'data: {"choices": [{"delta": {"content": "world"}}]}'
            # Stream dies
            raise r.exceptions.ChunkedEncodingError("conn broken")

    with mock.patch('requests.post') as mp:
        mp.return_value = FakeResponse()
        # MiniMax-M3 has reasoning_effort opt-in, M3 route
        content, reasoning = mmp.call_llm("sys", "user", "MiniMax-M3")
        # We should get the partial content, not None
        assert content is not None, "Partial content must be preserved"
        assert "Hello" in content and "world" in content


def test_update_analytics_serializes_concurrent_writes():
    """update_analytics must serialize concurrent writes via file lock to
    prevent data loss. Without lock, 10 threads writing 10 unique URLs lose
    ~70% of writes due to read-modify-write race.
    """
    import tempfile, shutil, threading
    from pathlib import Path

    tmpdir = Path(tempfile.mkdtemp())
    try:
        orig_data = mmp.DATA_DIR
        orig_posted = mmp.POSTED_FILE
        orig_cache = mmp.TITLE_CACHE_FILE
        mmp.DATA_DIR = tmpdir
        mmp.POSTED_FILE = tmpdir / "posted.json"
        mmp.TITLE_CACHE_FILE = tmpdir / "cache.json"

        def write(i):
            st = {"title": f"Article-{i}", "url": f"https://test.com/{i}",
                  "source": "X", "score": 60, "slides": [{"hook": "", "content": "x"}]}
            mmp.update_analytics(st, f"root-{i}", f"https://threads.net/{i}")

        threads = [threading.Thread(target=write, args=(i,)) for i in range(20)]
        for t in threads: t.start()
        for t in threads: t.join()

        posted = mmp.load_json(mmp.POSTED_FILE)
        # All 20 unique URLs must be persisted
        assert len(posted) == 20, f"Expected 20 posts, got {len(posted)} (lock failed)"
    finally:
        mmp.DATA_DIR = orig_data
        mmp.POSTED_FILE = orig_posted
        mmp.TITLE_CACHE_FILE = orig_cache
        shutil.rmtree(tmpdir, ignore_errors=True)
