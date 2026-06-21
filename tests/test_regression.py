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
