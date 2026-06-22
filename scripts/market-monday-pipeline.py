#!/usr/bin/env python3
"""
MARKET MONDAY Pipeline — Generation & Validation
Niche: Economics & Market for Indonesian Professionals
Target account: @ryanhadiii (Threads)

Modes:
  (default)      Scrape RSS → Score → Pick → Extract → LLM Generate → Validate → Stage
  --benchmark    Test RSS source quality (writes benchmark_results.json)
  --analytics    Fetch engagement → update market_feedback.json
  --dry-run      Generate without writing staging.json
  --model X      Force specific model (skip fallback chain)

Architecture: forked from Pressbox v7 pattern, fully standalone since v17.2
Author: Hadijayyy
Created: 17 Jun 2026
Updated: 22 Jun 2026 — v17.4 (renamed MISTRAL_API_KEY → PIPELINE_MISTRAL_KEY)
"""

import os
import sys
import json
import re
import html
import requests
import argparse
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from email.utils import parsedate_to_datetime

# Global import newspaper3k
try:
    import newspaper
    HAS_NEWSPAPER = True
except ImportError:
    HAS_NEWSPAPER = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────
DATA_DIR = Path.home() / ".hermes" / "market_monday"
SCRIPTS_DIR = Path.home() / ".hermes" / "scripts"
ENV_FILE = Path.home() / ".hermes" / ".env"
TOKEN_PATH = Path.home() / ".hermes" / "market_monday" / "threads_token.json"
# market-monday-post.py lives next to this script (same scripts/ dir).
# Use __file__-relative path so it works regardless of where the repo is cloned.
THREADS_SCRIPT = Path(__file__).parent / "market-monday-post.py"

STAGING_FILE = DATA_DIR / "staging.json"
POSTED_FILE = DATA_DIR / "posted_topics.json"
FEEDBACK_FILE = DATA_DIR / "market_feedback.json"
RAW_OUTPUT_FILE = DATA_DIR / "raw_llm_output.txt"
LATEST_FILE = DATA_DIR / "latest.md"
TITLE_CACHE_FILE = DATA_DIR / "title_cache.json"
BENCHMARK_FILE = DATA_DIR / "benchmark_results.json"
REPORT_FILE = DATA_DIR / "market_analytics_report.md"

# LLM CONFIG
# Model routes — each model maps to its own API URL + key env var
# Primary: mistral-large-latest (Mistral direct, fast, 8.5/10 ID+EN)
# Fallback: MiniMax-M3 (tokenrouter, free, slow ~2 min — bumped LLM_TIMEOUT to 180s)
MODEL_ROUTES = {
    "mistral-large-latest": ("https://api.mistral.ai/v1/chat/completions", "PIPELINE_MISTRAL_KEY"),
    "MiniMax-M3": ("https://api.tokenrouter.com/v1/chat/completions", "MINIMAX_API_KEY"),
}
# Primary → fallback chain (order matters — first success wins)
LLM_MODELS = ["mistral-large-latest", "MiniMax-M3"]
DRY_RUN = False
FORCE_MODEL = None
# Threads account handle for CTA "Follow @{handle}". Edit if account changes.
THREADS_HANDLE = "@ryanhadiii"
LLM_MAX_TOKENS = 16000  # bumped 10000→16000 — v15 prompt strict checks cause M3 over-planning, hits budget mid-thought
LLM_TIMEOUT = 240  # bumped 180→240s — M3 fallback takes 3-4 min with longer thinking

# SIMILARITY
SIMILARITY_THRESHOLD = 0.35

# WIB timezone
WIB = timezone(timedelta(hours=7))

# Global User-Agent
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# RSS SOURCES
RSS_SOURCES = [
    {"name": "CNBC Indonesia", "url": "https://www.cnbcindonesia.com/rss", "type": "rss"},
    {"name": "Detik Finance", "url": "https://finance.detik.com/rss", "type": "rss"},
    {"name": "IDX Channel", "url": "https://www.idxchannel.com/rss", "type": "rss"},
]

BENCHMARK_SOURCES = [
    {"name": "CNBC Indonesia", "url": "https://www.cnbcindonesia.com/rss"},
    {"name": "Detik Finance", "url": "https://finance.detik.com/rss"},
    {"name": "IDX Channel", "url": "https://www.idxchannel.com/rss"},

    {"name": "BBC Business", "url": "https://feeds.bbci.co.uk/news/business/rss.xml"},
]

# ─── KEYWORD & SCORING SYSTEM (v17 — 21 Jun 2026, per user spec) ─────────────
# Scope: Makro Indonesia + Saham/IHSG + Crypto/Web3
# Skor 0-100, threshold ≥60 untuk masuk pipeline

# === 1. INCLUDE KEYWORDS ===
# Direct substring match (case-insensitive) — keywords chosen to be unambiguous

INCLUDE_KEYWORDS = {
    # Makro Indonesia (BI, APBN, kurs, dll)
    "makro": [
        "rupiah", "nilai tukar", "kurs", "bi rate", "bi-rate", "suku bunga acuan",
        "bank indonesia", "inflasi", "deflasi", "pdb", "pertumbuhan ekonomi",
        "neraca dagang", "neraca perdagangan", "ekspor impor", "defisit anggaran",
        "apbn", "cadangan devisa", "utang luar negeri", "pmi manufaktur",
        "indeks keyakinan konsumen", "bps", "kemenkeu", "sri mulyani",
        "perry warjiyo", "capital outflow", "capital inflow", "yield obligasi",
        "sbn", "surat utang negara", "lelang sun", "credit rating", "moody's", "fitch", "s&p",
    ],
    # Saham / IHSG / Emiten
    "saham": [
        "ihsg", "indeks harga saham gabungan", "bei", "bursa efek indonesia",
        "saham blue chip", "market cap", "kapitalisasi pasar", "ipo",
        "laporan keuangan", "kuartal", "dividen", "right issue", "buyback",
        "saham gorengan", "foreign outflow", "foreign inflow", "net sell", "net buy",
        "lq45", "idx30", "sektor perbankan", "sektor energi", "sektor consumer",
        "emiten", "suspensi saham", "ara", "arb", "auto reject", "capital gain",
        "analis merekomendasikan", "target harga", "rating saham", "downgrade", "upgrade",
    ],
    # Crypto / Web3
    "crypto": [
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "altcoin", "stablecoin",
        "usdt", "usdc", "market cap crypto", "exchange crypto", "binance", "indodax",
        "bappebti", "regulasi kripto", "etf bitcoin", "etf crypto", "halving",
        "defi", "nft", "staking", "airdrop", "token listing", "whale movement",
        "on-chain data", "smart contract", "web3", "blockchain", "memecoin",
        "likuidasi", "leverage", "funding rate", "perpetual futures", "cex", "dex",
    ],
    # Cross-cutting (global market, geopolitik, commodity)
    "cross": [
        "the fed", "suku bunga the fed", "fomc", "jerome powell", "resesi",
        "volatilitas pasar", "sentimen pasar", "geopolitik", "harga minyak",
        "harga emas", "perang dagang", "tarif", "china-as", "krisis ekonomi",
    ],
}

# === 2. EXCLUDE KEYWORDS ===
# Strict: substring match → hard reject (-1)

EXCLUDE_KEYWORDS = {
    "noise": [
        "prediksi zodiak", "ramalan", "gosip", "artis", "selebriti",
        "giveaway", "kuis berhadiah", "undian", "kontes foto",
    ],
    "non_redaksional": [
        "advertorial", "press release", "lowongan kerja",
        "event promosi", "sponsored content",
    ],
}

# Ambiguous excludes — context-window check required (might be finance OR non-finance)
# Example: "saham mata" (non-finance) vs "saham BCA" (finance)
# Only flag if NO include keyword within ±100 chars
AMBIGUOUS_EXCLUDES = ["saham", "token", "blok", "emas"]

# === 3. SOURCE TIER (SUMBER KREDIBILITAS) ===

SOURCE_TIER_1 = ["kontan", "bisnis.com", "cnbc indonesia", "katadata", "investor daily"]
SOURCE_TIER_2 = ["detik finance", "idx channel", "kumparan", "tempo", "republika", "okezone"]

# === 4. HELPER FUNCTIONS ===

def compute_age_hours(pub_date_str):
    """Compute article age in hours from publish timestamp."""
    if not pub_date_str:
        return 999
    try:
        pub_date = parsedate_to_datetime(pub_date_str)
        now = datetime.now(timezone.utc)
        return (now - pub_date).total_seconds() / 3600
    except Exception:
        return 999

def source_tier(source):
    """Return tier (1/2/0) for source name."""
    s = (source or "").lower()
    for t in SOURCE_TIER_1:
        if t in s:
            return 1
    for t in SOURCE_TIER_2:
        if t in s:
            return 2
    return 0

def check_include_keywords(text):
    """Returns (matched_count, categories_set). Case-insensitive.
    Short tokens (≤4 chars) use word-boundary regex to avoid substring false
    positives (e.g. 'ara' inside 'Barat', 'ipo' inside any word).
    """
    text_lower = text.lower()
    matched = set()
    categories = set()
    for cat, keywords in INCLUDE_KEYWORDS.items():
        for kw in keywords:
            kw_lower = kw.lower()
            if len(kw_lower) <= 4:
                # Short token — require word boundary
                pattern = r"\b" + re.escape(kw_lower) + r"\b"
                if re.search(pattern, text_lower):
                    matched.add(kw)
                    categories.add(cat)
            else:
                if kw_lower in text_lower:
                    matched.add(kw)
                    categories.add(cat)
    return len(matched), categories

def check_exclude_keywords(text):
    """Check strict excludes + ambiguous excludes with context window.
    Returns matched exclude keyword (str) or None.
    """
    text_lower = text.lower()
    # Strict excludes — direct match
    for cat, keywords in EXCLUDE_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in text_lower:
                return kw
    # Ambiguous excludes — only flag if NO include keyword nearby (±100 chars).
    # Short tokens (≤4 chars: "blok", "emas") use word-boundary to avoid false
    # positives like "kemas"/"lemas" containing "emas" as a substring.
    include_kws_flat = [kw.lower() for kws in INCLUDE_KEYWORDS.values() for kw in kws]
    context_window = 100
    for kw in AMBIGUOUS_EXCLUDES:
        if len(kw) <= 4:
            # Word-boundary match for short tokens
            pattern = r"\b" + re.escape(kw) + r"\b"
            match = re.search(pattern, text_lower)
            if not match:
                continue
            idx = match.start()
        else:
            idx = text_lower.find(kw)
            if idx == -1:
                continue
        context = text_lower[max(0, idx-context_window):idx+len(kw)+context_window]
        has_include_nearby = any(inc in context for inc in include_kws_flat)
        if not has_include_nearby:
            return f"{kw} (no finance context)"
    return None

def has_specific_data(text):
    """Detect specific numbers (percentages, prices, index levels). Returns bool."""
    patterns = [
        r'\d+\.?\d*\s*(%|persen|percent)',         # percentages
        r'rp\s*\d+[\d.,]*',                         # Rp amounts
        r'\$\s*\d+[\d.,]*',                         # USD amounts
        r'us\$\s*\d+',                              # US$ prefix
        r'\d+\.?\d*\s*(poin|points|bps)',          # basis points
        r'(ihsg|idx|nikkei|nasdaq|dow|s&p|lq45|idx30)\s*[:\s\-]*\d',  # index levels
        r'\d{3,}\s*(triliun|miliar|juta|rb|tn|mn)',  # currency amounts
        r'(naik|turun)\s*\d+\.?\d*\s*%',           # movement percentages
    ]
    for p in patterns:
        if re.search(p, text, re.IGNORECASE):
            return True
    return False

# ─── HELPER FUNCTIONS ────────────────────────────────────────────────────────

def load_env():
    """Load environment variables from .env file."""
    if ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, value = line.partition('=')
                    os.environ[key.strip()] = value.strip().strip('"').strip("'")

def load_json(path, default=None):
    """Load JSON file safely with fallback."""
    try:
        if path.exists():
            with open(path) as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return default if default is not None else {}

def save_json(path, data):
    """Save JSON file cleanly."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def log(msg, level="INFO"):
    """Log to stderr with timestamp."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", file=sys.stderr)

def alert_telegram(msg):
    """Send alert to Telegram using native requests."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("ALERT_CHAT", "")
    if token and chat_id:
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": f"📈 Market Monday: {html.escape(msg)}",
                "parse_mode": "HTML"
            }
            requests.post(url, data=payload, timeout=10)
        except Exception as e:
            log(f"Failed to send Telegram alert: {e}", "WARN")

# ─── TITLE SIMILARITY DEDUP (Jaccard) ───────────────────────────────────────

STOPWORDS = frozenset([
    "the", "a", "an", "in", "on", "at", "to", "for", "of", "with", "by",
    "as", "is", "was", "are", "were", "be", "been", "has", "have", "had",
    "but", "or", "and", "not", "no", "so", "if", "it", "its", "this",
    "that", "these", "those", "from", "into", "about", "between", "through",
    "yang", "dan", "di", "ke", "dari", "ini", "itu", "untuk", "dengan",
    "pada", "adalah", "akan", "juga", "sudah", "tidak", "bisa", "lebih"
])

def clean_words(text):
    """Clean text for similarity comparison."""
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    words = text.split()
    return set(w for w in words if w not in STOPWORDS and len(w) > 1)

def is_similar(new_title, posted_titles, threshold=SIMILARITY_THRESHOLD):
    """Check if title is too similar to already posted content."""
    new_words = clean_words(new_title)
    if not new_words:
        return False

    for posted_title in posted_titles:
        posted_words = clean_words(posted_title)
        if not posted_words:
            continue

        intersection = len(new_words & posted_words)
        min_len = min(len(new_words), len(posted_words))

        if min_len > 0 and intersection / min_len >= threshold:
            log(f"[DEDUP] Similar to: '{posted_title[:50]}...' (similarity: {intersection/min_len:.2f})")
            return True

    return False

# ─── FEEDBACK LOOP ───────────────────────────────────────────────────────────

def load_feedback():
    """Load analytics feedback for topic/time boosts."""
    feedback = load_json(FEEDBACK_FILE, {})
    if not feedback:
        log("No feedback file found - running without boosts")
    return feedback

def extract_topics_from_title(title):
    """DEPRECATED in v17 — kept as stub for analytics backward compat."""
    return ["general"]

# Removed: apply_topic_boost() and apply_time_boost() (v17).
# They were no-op stubs (always returned score unchanged) and never called.
# Topic/time boosts are no longer part of the scoring formula per v17 spec.

# ─── IMAGE EXTRACTION ────────────────────────────────────────────────────────

def extract_image_from_html(html_content):
    """Extract image from HTML using 3-method regex matching."""
    m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html_content, re.IGNORECASE)
    if m:
        return m.group(1)

    m = re.search(r'<meta\s+(?:name|property)="twitter:image"\s+content="([^"]+)"', html_content, re.IGNORECASE)
    if m:
        return m.group(1)

    m = re.search(r'<article[^>]*>.*?<img[^>]+src="([^"]+)"', html_content, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1)

    return None

def extract_image(url):
    """Extract article image with native requests fallback chain."""
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        return extract_image_from_html(r.text)
    except Exception as e:
        log(f"[IMAGE] Native extraction failed for {url}: {e}", "WARN")
        return None

# ─── RSS SCRAPING ────────────────────────────────────────────────────────────

def scrape_rss(url, source_name):
    """Scrape RSS feed using native requests."""
    articles = []
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=15)
        if response.status_code != 200:
            log(f"RSS fetch failed: {source_name} (HTTP {response.status_code})", "WARN")
            return []

        content = response.text
        items = re.findall(r'<item>(.*?)</item>', content, re.DOTALL)

        for item in items[:10]:
            title_match = re.search(r'<title[^>]*>(.*?)</title>', item, re.DOTALL)
            link_match = re.search(r'<link[^>]*>(.*?)</link>', item, re.DOTALL)
            desc_match = re.search(r'<description[^>]*>(.*?)</description>', item, re.DOTALL)
            pub_match = re.search(r'<pubDate[^>]*>(.*?)</pubDate>', item, re.DOTALL)

            if title_match and link_match:
                title = html.unescape(title_match.group(1).strip())
                title = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', title)
                title = re.sub(r'<[^>]+>', '', title)

                link = link_match.group(1).strip()
                link = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', link)

                desc = desc_match.group(1).strip() if desc_match else ""
                desc = html.unescape(desc)
                desc = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', desc)
                desc = re.sub(r'<[^>]+>', '', desc)[:300]

                pub_date = pub_match.group(1).strip() if pub_match else ""

                articles.append({
                    "title": title,
                    "url": link,
                    "description": desc,
                    "source": source_name,
                    "published": pub_date
                })

        log(f"Scraped {len(articles)} articles from {source_name}")
    except Exception as e:
        log(f"RSS error: {source_name} - {e}", "WARN")

    return articles

def scrape_all_sources():
    """Scrape all RSS sources in parallel."""
    all_articles = []

    def fetch_source(source):
        return scrape_rss(source["url"], source["name"])

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_source, s): s for s in RSS_SOURCES}
        for future in as_completed(futures):
            try:
                articles = future.result()
                all_articles.extend(articles)
            except Exception as e:
                log(f"Scrape error: {e}", "WARN")

    return all_articles

# ─── SCORING ─────────────────────────────────────────────────────────────────

def is_fresh(pub_date_str, hours=24):
    """Check if article is within freshness window.

    Bug fix (v17.3): previously returned True for future-dated articles
    (clock skew, wrong timezone in RSS feed) because `age < 0 < hours*3600`.
    Now requires age to be non-negative AND within window.
    """
    if not pub_date_str:
        return True
    try:
        pub_date = parsedate_to_datetime(pub_date_str)
        now = datetime.now(timezone.utc)
        age = now - pub_date
        age_seconds = age.total_seconds()
        # Reject future-dated articles (clock skew / TZ mismatch) and old ones
        if age_seconds < 0:
            log(f"[FRESH] Future-dated article rejected (age={age_seconds/3600:.1f}h): {pub_date_str[:40]}", "WARN")
            return False
        return age_seconds < hours * 3600
    except Exception as e:
        log(f"Date parse error: {e}", "WARN")
        return True

def score_candidate(article, posted, feedback):
    """Score article 0-100 per v17 spec (Makro/Saham/Crypto pipeline).

    Components:
      1. Keyword Match  : +8 pts per unique include keyword (max 5 = 40 pts)
      2. Category Relev : 20 (Makro/Saham/Crypto) / 10 (cross only) / 0 (none)
      3. Recency        : 15 (<6h) / 10 (6-24h) / 5 (24-48h) / 0 (>48h)
      4. Data/Angka     : 15 (specific: %, Rp, index level) / 7 (vague digits) / 0
      5. Sumber Tier    : 10 (Tier 1) / 5 (Tier 2) / 0 (unknown)
      Penalti           : -1 hard reject if exclude keyword matched

    Returns:
      -1   → hard reject (posted URL or exclude match)
      0-100 → score (threshold ≥60 untuk pipeline)
    """
    title = article.get("title", "")
    desc = article.get("description", "")
    combined = f"{title} {desc}"

    # Hard reject: already posted
    if article.get("url") in posted:
        return -1

    # Hard reject: exclude keyword match (strict OR ambiguous w/o finance context)
    exclude_kw = check_exclude_keywords(combined)
    if exclude_kw:
        log(f"[SCORING] ❌ EXCLUDE matched ({exclude_kw}): {title[:60]}...", "WARN")
        return -1

    # 1. Keyword Match (max 40 pts)
    matched_count, categories = check_include_keywords(combined)
    keyword_pts = min(matched_count, 5) * 8

    # 2. Category Relevance (max 20 pts)
    if categories & {"makro", "saham", "crypto"}:
        cat_pts = 20
    elif categories & {"cross"}:
        cat_pts = 10
    else:
        cat_pts = 0

    # 3. Recency (max 15 pts)
    age_h = compute_age_hours(article.get("published", ""))
    if age_h < 6:
        recency_pts = 15
    elif age_h < 24:
        recency_pts = 10
    elif age_h < 48:
        recency_pts = 5
    else:
        recency_pts = 0

    # 4. Data/Angka Konkret (max 15 pts)
    if has_specific_data(combined):
        data_pts = 15
    elif re.search(r'\d+', combined):
        data_pts = 7
    else:
        data_pts = 0

    # 5. Sumber Kredibilitas (max 10 pts)
    tier = source_tier(article.get("source", ""))
    if tier == 1:
        source_pts = 10
    elif tier == 2:
        source_pts = 5
    else:
        source_pts = 0

    total = keyword_pts + cat_pts + recency_pts + data_pts + source_pts
    return total

def select_best_candidate(articles, posted, feedback, posted_titles=None, top_n=1):
    """Select top N articles by score, with title dedup. v17 threshold: ≥60."""
    scored = []
    skipped_similar = 0
    skipped_below_threshold = 0
    skipped_invalid = 0

    for article in articles:
        # Defensive: skip None or non-dict articles instead of crashing
        if not isinstance(article, dict):
            skipped_invalid += 1
            log(f"[SELECT] Skipped non-dict article: {type(article).__name__}", "WARN")
            continue
        if not article.get("title"):
            skipped_invalid += 1
            log(f"[SELECT] Skipped article with empty title", "WARN")
            continue

        if posted_titles and is_similar(article["title"], posted_titles):
            skipped_similar += 1
            continue

        score = score_candidate(article, posted, feedback)
        if score >= 60:  # v17 threshold (was: > 0)
            scored.append((score, article))
        elif score >= 0:
            skipped_below_threshold += 1

    if skipped_similar > 0:
        log(f"[DEDUP] Skipped {skipped_similar} similar titles")
    if skipped_below_threshold > 0:
        log(f"[SCORING] {skipped_below_threshold} articles below threshold (score <60)")
    if skipped_invalid > 0:
        log(f"[SELECT] {skipped_invalid} invalid articles skipped (None/non-dict/empty title)")

    if not scored:
        return []

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:top_n]
    if top:
        best_score, best_article = top[0]
        log(f"Best candidate: {best_article['title']} (score: {best_score:.1f})")
    return top


def is_finance_niche(article, article_content):
    """Quick LLM check: is this article in the finance niche? Uses FULL content.

    Returns True if finance, False otherwise. Default True on error (don't lose article).
    Cost: ~$0.0016 per call (mistral, max_tokens=5, ~800 input tokens).
    Latency: ~3-5s per call.
    """
    # Load .env so PIPELINE_MISTRAL_KEY is available when called from cron/headless context.
    # Without this, the function silently defaults to True and bypasses the filter.
    load_env()

    if not article_content or len(article_content) < 100:
        return False

    classify_prompt = f"""JUDUL: {article['title']}
SUMBER: {article['source']}

ARTIKEL (3000 char pertama):
{article_content[:3000]}

Niche apa artikel ini?

Pilih SATU:
- KEUANGAN: ekonomi makro, pasar modal, saham, IHSG, bank (regulasi/merger/fraud), fintech, kripto, inflasi, BI rate, properti komersial, industri, emiten
- NON-KEUANGAN: retail (promo/diskon/sale), gaya hidup, hiburan, K-pop/film, teknologi konsumen (smartphone/laptop), travel, kuliner, fashion

Jawab: KEUANGAN atau NON-KEUANGAN. Hanya 1 kata."""

    api_key = os.environ.get("PIPELINE_MISTRAL_KEY")
    if not api_key:
        return True  # Default yes if can't check

    try:
        resp = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": "mistral-large-latest",
                "messages": [{"role": "user", "content": classify_prompt}],
                "max_tokens": 5,
                "temperature": 0,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            log(f"[CLASSIFY] HTTP {resp.status_code}, defaulting to YES", "WARN")
            return True

        result = resp.json()["choices"][0]["message"]["content"].strip().upper()
        is_finance = "KEUANGAN" in result and "NON" not in result
        log(f"[CLASSIFY] {article['title'][:50]}... → {result} → {'KEUANGAN' if is_finance else 'NON'}")
        return is_finance
    except Exception as e:
        log(f"[CLASSIFY] Error: {e}, defaulting to YES", "WARN")
        return True

# ─── CONTENT EXTRACTION ──────────────────────────────────────────────────────

def extract_article_content(url):
    """Extract article content via newspaper3k fallback system."""
    if HAS_NEWSPAPER:
        try:
            article = newspaper.Article(url)
            article.download()
            article.parse()
            if len(article.text) > 500:
                log(f"[EXTRACT] newspaper3k: {len(article.text)} chars")
                return article.text[:5000]
        except Exception as e:
            log(f"[EXTRACT] newspaper3k failed: {e}", "WARN")

    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=10)
        html_content = response.text

        article_match = re.search(r'<article[^>]*>(.*?)</article>', html_content, re.DOTALL)
        if article_match:
            article_html = article_match.group(1)
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', article_html, re.DOTALL)
            text = ' '.join([re.sub(r'<[^>]+>', '', p).strip() for p in paragraphs if len(p) > 50])
            if len(text) > 500:
                log(f"[EXTRACT] native article tag: {len(text)} chars")
                return text[:5000]

        paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', html_content, re.DOTALL)
        text = ' '.join([re.sub(r'<[^>]+>', '', p).strip() for p in paragraphs if len(p) > 50])
        if len(text) > 500:
            log(f"[EXTRACT] native p tags: {len(text)} chars")
            return text[:5000]

        text = re.sub(r'<[^>]+>', ' ', html_content)
        text = re.sub(r'\s+', ' ', text).strip()
        log(f"[EXTRACT] native fallback: {len(text)} chars")
        return text[:5000]

    except Exception as e:
        log(f"[EXTRACT] Native extraction failed: {e}", "ERROR")
        return ""

# ─── LLM CALLS ───────────────────────────────────────────────────────────────

def call_llm(system_prompt, user_prompt, model):
    """Call LLM API with system + user prompt split. Routes per model via MODEL_ROUTES."""
    load_env()

    # Resolve route for this model
    if model not in MODEL_ROUTES:
        log(f"No route configured for model '{model}'", "ERROR")
        return None, None
    api_url, key_env = MODEL_ROUTES[model]

    api_key = os.environ.get(key_env, "")
    if not api_key:
        log(f"Missing {key_env} env var for model {model}", "ERROR")
        return None, None

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}" if api_key else ""
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": LLM_MAX_TOKENS,
        "temperature": 0.5,  # 0.8→0.5: less sampling randomness, fewer hallucinated numbers
        "reasoning_effort": "low",  # global default — reduces reasoning tokens ~35% per skill
        "stream": True
    }
    # Opt-out for models that don't support reasoning_effort (e.g. mistral direct)
    if model not in ("MiniMax-M3", "mimo-v2.5", "minimax-m2.5", "minimax-m2.7", "deepseek-v4-flash"):
        payload.pop("reasoning_effort", None)

    # Defensive: strip reasoning_content from assistant msgs (Mistral rejects extra fields, HTTP 422)
    for _m in payload.get("messages", []):
        _m.pop("reasoning_content", None)

    try:
        r = requests.post(api_url, headers=headers, json=payload, timeout=LLM_TIMEOUT, stream=True)

        if r.status_code != 200:
            log(f"LLM API error ({model}): HTTP {r.status_code}", "ERROR")
            return None, None

        content_parts = []
        reasoning_parts = []
        # Bug fix (v17.4): if stream dies mid-flight (ChunkedEncodingError, timeout,
        # connection reset), return any partial content we already buffered instead
        # of throwing away paid tokens. Only return None,None if NOTHING was received.
        try:
            for line in r.iter_lines():
                if not line:
                    continue
                line = line.decode("utf-8")
                if not line.startswith("data: "):
                    continue
                data_str = line[6:]
                if data_str.strip() == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                    choices = chunk.get("choices", [])
                    if not choices:
                        continue
                    delta = choices[0].get("delta", {})
                    if "content" in delta and delta["content"]:
                        content_parts.append(delta["content"])
                    if "reasoning_content" in delta and delta["reasoning_content"]:
                        reasoning_parts.append(delta["reasoning_content"])
                    if "reasoning" in delta and delta["reasoning"]:
                        reasoning_parts.append(delta["reasoning"])
                except json.JSONDecodeError:
                    continue
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError) as e:
            # Stream died mid-flight — salvage whatever we got
            log(f"[LLM] Stream interrupted ({model}): {e}; partial content kept", "WARN")

        content = "".join(content_parts).strip()
        reasoning = "".join(reasoning_parts).strip()

        if not content and not reasoning:
            log(f"Empty LLM response ({model})", "ERROR")
            return None, None

        log(f"[LLM] Response: content={len(content)}c, reasoning={len(reasoning)}c")
        return content, reasoning

    except Exception as e:
        log(f"LLM error ({model}): {e}", "ERROR")
        return None, None

def extract_plain_text_slides(content):
    """Parse '1/ ... 2/ ... 6/ ...' plain text format (v13+ new format).

    Returns: dict like {"slide_1": "...", "slide_2": "...", ..., "slide_6": "..."}
    or None if format not detected / not all 6 slides found.
    """
    if not content:
        return None

    # Strip code fences (in case model wrapped in markdown)
    content = re.sub(r'```\w*\s*', '', content)
    content = re.sub(r'```', '', content)
    content = content.strip()

    # Look for "N/ ..." patterns at line start
    # Each slide can span multiple lines until next "N/" prefix
    pattern = re.compile(r'^(\d)/[/\s]+(.*?)(?=^\d/|\Z)', re.DOTALL | re.MULTILINE)
    slides = {}
    for match in pattern.finditer(content):
        num = int(match.group(1))
        text = match.group(2).strip()
        slides[f"slide_{num}"] = text

    # Need all 6 slides
    if len(slides) == 6:
        return slides
    return None


def extract_json_from_content(content):
    """Extract JSON from LLM content (handles multiple formats).

    Reasoning models (M3, deepseek) often embed a draft JSON in their
    `` block before producing the final answer. Greedy regex
    would match a span containing BOTH objects and fail with 'Extra data'.
    Fix: enumerate all balanced {...} blocks, take the LAST valid one
    (final answer is always at the end of the response).
    """
    content = re.sub(r'```json\s*', '', content)
    content = re.sub(r'```\s*$', '', content)
    content = re.sub(r'```\w*\s*', '', content)
    content = content.strip()

    # Strategy 0: enumerate balanced {...} blocks, take LAST valid dict
    candidates = []
    search_start = 0
    while True:
        idx = content.find('{', search_start)
        if idx == -1:
            break
        depth = 0
        end = -1
        for i in range(idx, len(content)):
            if content[i] == '{':
                depth += 1
            elif content[i] == '}':
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end == -1:
            break
        try:
            obj = json.loads(content[idx:end + 1])
            if isinstance(obj, dict):
                candidates.append(obj)
        except json.JSONDecodeError:
            pass
        search_start = end + 1

    if candidates:
        data = candidates[-1]  # last valid = final answer, not thinking draft
    else:
        # Fallback: original 3-strategy (greedy regex → slide_1 marker → brace counter)
        json_match = re.search(r'\{[\s\S]*\}', content)

        if not json_match:
            json_match = re.search(r'\{[^{}]*"slide_1"[\s\S]*\}', content)

        if not json_match:
            start = content.find('{')
            if start != -1:
                depth = 0
                for i in range(start, len(content)):
                    if content[i] == '{':
                        depth += 1
                    elif content[i] == '}':
                        depth -= 1
                        if depth == 0:
                            json_match = re.search(r'\{[\s\S]*\}', content[start:i + 1])
                            break

        if not json_match:
            return None

        try:
            data = json.loads(json_match.group())
        except json.JSONDecodeError:
            json_str = json_match.group()
            json_str = re.sub(r',\s*}', '}', json_str)
            json_str = re.sub(r',\s*]', ']', json_str)
            try:
                data = json.loads(json_str)
            except Exception as e:
                log(f"JSON repair failed: {e}", "WARN")
                return None
    
    normalized = {}
    for i in range(1, 7):  # v16: 6 slides, not 7
        key = f"slide_{i}"
        slide_val = None

        # v16 reference format: nested under "slides" key
        if "slides" in data and isinstance(data["slides"], dict):
            slide_val = data["slides"].get(key)

        # v14/v15 fallback: flat top-level slide_N
        if slide_val is None and key in data:
            slide_val = data[key]

        if slide_val is None:
            continue

        if isinstance(slide_val, str):
            if i == 1:
                normalized[key] = {"type": "hook", "title": "HOOK", "hook": slide_val, "content": ""}
            else:
                normalized[key] = {"type": f"slide_{i}", "title": f"SLIDE {i}", "hook": "", "content": slide_val}
        elif isinstance(slide_val, dict):
            # v16 reference: {"type": "hook", "title": "...", "content": "...", "facts_used": [...], ...}
            # Ensure "content" exists (other metadata like facts_used, loops_to_slide_1 also preserved)
            if "content" not in slide_val and "text" in slide_val:
                slide_val["content"] = slide_val["text"]
            normalized[key] = slide_val

    if len(normalized) >= 6:
        log(f"   extract_json_from_content: found {len(normalized)} slides")
        return normalized
    return None

def extract_json_from_reasoning(reasoning, content=""):
    """Extract JSON from reasoning content (Strategy 1 + 2 + 3)."""
    if not reasoning:
        return None
    
    for marker in ["slide_1", "slides"]:
        idx = 0
        while idx < len(reasoning):
            start = reasoning.find('{', idx)
            if start == -1:
                break
            depth = 0
            end = -1
            for i in range(start, len(reasoning)):
                if reasoning[i] == '{':
                    depth += 1
                elif reasoning[i] == '}':
                    depth -= 1
                if depth == 0:
                    end = i
                    break
            if end == -1:
                break
            try:
                obj = json.loads(reasoning[start:end+1])
                if isinstance(obj, dict) and marker in obj:
                    sample = ""
                    for k in ["slide_1", "slide_2", "slides"]:
                        if k in obj:
                            v = obj[k]
                            if isinstance(v, dict):
                                sample = v.get("content", "") or v.get("hook", "")
                            elif isinstance(v, str):
                                sample = v
                            elif isinstance(v, list) and len(v) > 0:
                                sample = v[0] if isinstance(v[0], str) else str(v[0])
                            break
                    if len(sample) > 50:
                        log(f"   Strategy 1: Found JSON in reasoning ({len(reasoning[start:end+1])}c, key={marker})")
                        return extract_json_from_content(reasoning[start:end+1])
            except json.JSONDecodeError:
                pass
            idx = end + 1
    
    log("   Strategy 2: scanning for last valid JSON with content...")
    best_json = ""
    best_score = 0
    for i in range(len(reasoning) - 1, max(len(reasoning) - 50000, -1), -1):
        if reasoning[i] == '}':
            for j in range(i, max(i - 15000, -1), -1):
                if reasoning[j] == '{':
                    try:
                        obj = json.loads(reasoning[j:i+1])
                        if isinstance(obj, dict) and len(obj) >= 4:
                            total_content = 0
                            for k, v in obj.items():
                                if isinstance(v, dict) and "content" in v:
                                    total_content += len(v["content"])
                                elif isinstance(v, str):
                                    total_content += len(v)
                            if total_content > best_score:
                                best_score = total_content
                                best_json = reasoning[j:i+1]
                    except json.JSONDecodeError:
                        pass
            if best_json:
                break
    
    if best_json and best_score > 200:
        log(f"   Strategy 2: Found JSON ({len(best_json)}c, score={best_score})")
        return extract_json_from_content(best_json)
    elif best_json:
        log(f"   Strategy 2: Found JSON but low score ({best_score}), trying anyway...")
        return extract_json_from_content(best_json)

    # Strategy 3: try content+reasoning combined
    log("   Strategy 3: trying combined content+reasoning...")
    combined = (content if content else "") + "\n" + (reasoning if reasoning else "")
    combined = combined.strip()
    if combined:
        result = extract_json_from_content(combined)
        if result:
            log(f"   Strategy 3: Found JSON in combined ({len(combined)}c)")
            return result

    return None

def generate_content(article, article_content):
    """Generate Threads content via LLM with model fallback."""
    handle = THREADS_HANDLE
    system_prompt = """# ROLE
You are a senior Threads content writer for the finance niche. Your job: turn ONE article into a 6-slide carousel that feels like a story, not a summary.

# INPUT
- ARTICLE: [paste full article text below this prompt]
- NICHE: finance (crypto/stocks/macro/personal finance — match article)

# CORE RULES (NON-NEGOTIABLE)

1. **Anti-halusinasi:** Every fact in slides 1-4 MUST be verbatim or directly paraphrased from the article. If a fact isn't in the article, cut it.
2. **Verbatim grounding:** When citing numbers, dates, names, or quotes — copy from the article. Don't reword statistics.
3. **Slides 5-6:** Personal POV allowed. Mark clearly with "POV:" or in separate opinion section.
4. **No external knowledge:** Don't add context the article doesn't have.
5. **If article is thin/weak:** Output {"error": "article_too_thin", "missing": [...]} and stop. Don't pad with filler.
6. **Self-check before output:** All facts in slides 1-4 traceable to article? If unsure, drop the fact.

# STRUCTURE (6 SLIDES, STORY ARC)

[SLIDE 1: HOOK]          -> Emotional, relatable, NO facts yet
[SLIDE 2: SETUP]         -> What happened (context from article)
[SLIDE 3: COMPLICATION]  -> Why it matters / what's at stake
[SLIDE 4: INSIGHT]       -> Key finding/data point from article
[SLIDE 5: POV]           -> Your take — opinion allowed
[SLIDE 6: CTA]           -> Question that loops back to slide 1

## SLIDE 1 - HOOK (emotional/engaging, MUST be 2-3 sentences)
- WAJIB 2-3 kalimat (bukan 1, bukan 4+). Substantif, bukan cuma 1 pertanyaan pendek.
- Emotional trigger: fear, greed, surprise, FOMO, relief, regret
- NO facts yet (build curiosity)
- Should make reader stop scrolling
- Patterns (multi-sentence):
  - "Gue baru sadar selama ini [X]. [shocking twist]. [rhetorical question]"
  - "Kebanyakan orang nggak tau kalau [X]. Padahal tiap hari [Y] kejadian."
  - "Kalau lo [habit umum], berhenti sekarang. Lo bisa kena [Z]."
- CONTOH HOOK YANG BENAR (2 kalimat):
  "Lo pernah denger dividen? Tiap tahun, perusahaan bagi-bagi 'uang gratis' ke pemegang saham."
  "Uang lo raib dalam 1 jam? Padahal sistem udah dirancang buat nahan transaksi."
- CONTOH HOOK YANG SALAH (terlalu pendek, JANGAN):
  "Uang Gratis?" (1 kalimat saja, tidak cukup)
- Last word/topic must anchor to article subject

## SLIDE 2 - SETUP (nyambung dari slide 1)
- Opens with callback to slide 1's last word/topic
- 2-3 sentences
- Establishes: who, what, when, where
- All facts from article
- Pattern: "Jadi [article context]. [Article fact 1]. [Article fact 2]."

## SLIDE 3 - COMPLICATION (escalation)
- Opens connecting slide 2's ending
- 2-3 sentences
- Raises stakes: what's at risk, who's affected, what changes
- Facts from article (numbers, quotes, events)
- Pattern: "Tapi masalahnya [complication]. [Article stat]. [Article quote/observation]."

## SLIDE 4 - INSIGHT (the "aha")
- Opens connecting slide 3's tension
- 2-3 sentences
- The KEY takeaway — the data point or finding that changes everything
- This is the "value" slide
- Pattern: "Ternyata [article insight]. [Article number/quote]. Ini karena [article reasoning]."

## SLIDE 5 - POV (personal opinion)
- Opens connecting slide 4's insight
- 2-4 sentences
- Start with "POV gue:"
- Your interpretation: agree/disagree, what it means for reader, what to watch
- Connect to broader finance wisdom OK here
- Pattern: "POV gue: [opinion]. [Reasoning]. Buat lo yang [reader profile], [action]."

## SLIDE 6 - CTA (question, loops to slide 1)
- Opens with callback to slide 1's hook theme
- 1-2 sentences MAX
- Question form
- Should trigger comments
- Patterns:
  - "Lo masih [hook behavior dari slide 1]?"
  - "Menurut lo [topic dari slide 1] gimana?"
  - "Pernah ngalamin [hook situation] juga?"

# STORYTELLING CHECKLIST (verify before output)
- [ ] Slide 2's first sentence references slide 1's last word/topic
- [ ] Slide 3 references slide 2's conclusion
- [ ] Slide 4 references slide 3's tension
- [ ] Slide 5 references slide 4's insight
- [ ] Slide 6 references slide 1's hook explicitly
- [ ] No "dead-end" slides — each flows into next

# TONE & LENGTH
- Bahasa Indonesia casual + English finance terms OK (e.g., "rally", "ATH", "allocation")
- Use "lo/gue", contractions, short sentences
- No corporate jargon, no emoji spam (max 1-2 per slide)
- Numbers/stats: keep raw, don't round
- Max 4 sentences per slide (slide 1: max 2)
- Target: 200-400 chars/slide, ~1500-2400 chars total

# OUTPUT FORMAT (JSON STRICT)

{
  "source": "[article title/URL]",
  "niche": "finance",
  "narrative_arc": "[1 sentence: story flow dari hook ke CTA]",
  "slides": {
    "slide_1": {
      "type": "hook",
      "title": "[optional, max 5 words]",
      "content": "[1-2 kalimat emosional]"
    },
    "slide_2": {
      "type": "setup",
      "title": "[optional]",
      "content": "[2-3 kalimat]",
      "facts_used": ["[fact verbatim dari artikel]", "[fact 2]"]
    },
    "slide_3": {
      "type": "complication",
      "title": "[optional]",
      "content": "[2-3 kalimat]",
      "facts_used": ["..."]
    },
    "slide_4": {
      "type": "insight",
      "title": "[optional]",
      "content": "[2-3 kalimat]",
      "facts_used": ["..."]
    },
    "slide_5": {
      "type": "pov",
      "title": "[optional]",
      "content": "[2-4 kalimat, mulai dengan POV gue:]",
      "pov_marker": "POV"
    },
    "slide_6": {
      "type": "cta",
      "title": "[optional]",
      "content": "[1-2 kalimat, bentuk pertanyaan]",
      "loops_to_slide_1": "[elemen hook slide 1 yang di-reference]"
    }
  },
  "verification": {
    "all_facts_in_slides_1_4_from_article": true,
    "external_facts_added": [],
    "story_chain_complete": true,
    "slide_6_loops_to_slide_1": true
  }
}

# FINAL CHECK BEFORE DELIVERING
1. All facts in slides 1-4 traceable to article
2. Story flows slide 1->2->3->4->5->6
3. Slide 6 references slide 1's hook
4. POV only in slides 5-6
5. Each slide <=4 sentences
6. JSON valid"""

    fact_bank = extract_facts(article_content[:3000])

    user_prompt = f"""JUDUL: {article['title']}
SUMBER: {article['source']}
URL: {article['url']}

FAKTA DARI ARTIKEL (JANGAN sebut angka/nama yang tidak ada di sini):
{fact_bank if fact_bank else "(tidak ada angka/entitas yang bisa diekstrak — gunakan frasa umum artikel)"}

ARTIKEL:
{article_content[:3000]}"""

    models_to_try = [FORCE_MODEL] if FORCE_MODEL else LLM_MODELS
    MAX_HOOK_RETRIES = 2
    
    for model in models_to_try:
        for attempt in range(MAX_HOOK_RETRIES):
            log(f"[LLM] Trying model: {model} (attempt {attempt + 1})")
            content, reasoning = call_llm(system_prompt, user_prompt, model)

            if content or reasoning:
                save_json(RAW_OUTPUT_FILE, {
                    "content": content, 
                    "reasoning": reasoning[:2000] if reasoning else "", 
                    "model": model, 
                    "timestamp": datetime.now().isoformat()
                })

                slides_data = None
                if content:
                    # v13+ format: try plain text "1/ ... 2/ ... 6/ ..." first
                    slides_data = extract_plain_text_slides(content)
                    if not slides_data:
                        # Fallback: JSON format
                        slides_data = extract_json_from_content(content)
                if not slides_data and reasoning:
                    log("[LLM] Content empty, extracting from reasoning...")
                    slides_data = extract_json_from_reasoning(reasoning, content)

                if slides_data:
                    # Normalize to {slide_N: {"hook": ..., "content": ...}} structure
                    # for downstream validation/normalize code
                    normalized = {}
                    for k, v in slides_data.items():
                        if isinstance(v, str):
                            # Plain text format: all text in one field
                            normalized[k] = {"hook": v if k == "slide_1" else "", "content": v if k != "slide_1" else ""}
                        elif isinstance(v, dict):
                            normalized[k] = v
                    slides_data = normalized if normalized else slides_data
                    hook = slides_data.get("slide_1", {}).get("hook", "") or slides_data.get("slide_1", {}).get("content", "")
                    is_valid, issues = validate_hook(hook)
                    
                    if is_valid:
                        # Normalize sentence counts (trim to max instead of rejecting)
                        slides_data, norm_changes = normalize_slide_sentences(slides_data)
                        if norm_changes:
                            log(f"[LLM] ✂️ Normalized: {'; '.join(norm_changes)}", "INFO")

                        # Add \\n\\n between every sentence (mobile readability on Threads)
                        ws_changes = 0
                        for i in range(1, 7):
                            slide = slides_data.get(f"slide_{i}", {})
                            if isinstance(slide, dict):
                                if slide.get("hook"):
                                    new_h = add_smart_whitespace(slide["hook"])
                                    if new_h != slide["hook"]:
                                        slide["hook"] = new_h
                                        ws_changes += 1
                                if slide.get("content"):
                                    new_c = add_smart_whitespace(slide["content"])
                                    if new_c != slide["content"]:
                                        slide["content"] = new_c
                                        ws_changes += 1
                        if ws_changes:
                            log(f"[LLM] ␣ Whitespace applied to {ws_changes} slides", "INFO")

                        grounding_valid, grounding_issues = validate_grounding(slides_data, article_content)

                        if grounding_valid:
                            log(f"[LLM] ✅ Success with {model} - Hook valid: {hook[:50]}...")
                            return slides_data
                        else:
                            log(f"[LLM] ⚠️ Grounding issues: {', '.join(grounding_issues)}", "WARN")
                            continue
                    else:
                        log(f"[LLM] ⚠️ Hook invalid: {', '.join(issues)}", "WARN")
                        continue
                else:
                    log(f"[LLM] ❌ JSON parse failed for {model}", "WARN")
                    break
    
    log("[LLM] ❌ All models failed (hook validation failed or parse error)", "ERROR")
    return None

def extract_facts(content):
    """Extract numbers, named entities, percentages, currency from article for FACT BANK.

    Returns a compact string of facts the model is allowed to reference.
    Single-call anti-hallucination guard: by giving the model a pre-extracted
    fact list, it stops "concretizing" vague article phrases with made-up numbers.
    """
    if not content:
        return ""

    # Extract numbers (skip year-only and URL-like long sequences — 6+ digit usually article IDs/URLs)
    numbers = set()
    for m in re.finditer(r'\d[\d.,]*\d|\d+', content):
        n = m.group()
        if n in {str(y) for y in range(2020, 2031)}:
            continue
        if len(n.replace('.', '').replace(',', '')) >= 6:  # 6+ digit = URL/article ID, skip
            continue
        numbers.add(n)

    # Extract named entities (capitalized multi-letter words)
    skip_words = {'Yang', 'Untuk', 'Dari', 'Dengan', 'Atau', 'Ini', 'Itu', 'The', 'And', 'For'}
    entities = set()
    for m in re.finditer(r'\b[A-Z][a-zA-Z]{2,}(?:\s+[A-Z][a-zA-Z]{2,})*\b', content):
        e = m.group().strip()
        if len(e) >= 3 and e not in skip_words:
            entities.add(e)

    # Percentages and currency amounts as-is
    pct = re.findall(r'\d+(?:[.,]\d+)?\s*%', content)
    cur = re.findall(r'(?:Rp|US\$|USD|IDR|MYR|SGD)\s*[\d.,]+(?:\s*(?:juta|miliar|triliun|ribu|jt|m|b))?', content, re.IGNORECASE)

    lines = []
    if numbers:
        lines.append(f"- Angka spesifik: {', '.join(sorted(numbers)[:30])}")
    if entities:
        lines.append(f"- Nama/entitas: {', '.join(sorted(entities)[:20])}")
    if pct:
        lines.append(f"- Persentase: {', '.join(set(pct))}")
    if cur:
        lines.append(f"- Mata uang: {', '.join(sorted(set(cur))[:15])}")

    return "\n".join(lines)


def add_smart_whitespace(content):
    """Add \\n\\n between sentences, but NOT after abbreviations.

    Smart whitespace: protects abbreviations (English + Indonesian) from
    being treated as sentence endings. Indonesian content often has
    "dll.", "dsb.", "Bpk." at end of clauses that should NOT break the
    sentence into a new line.
    """
    if not content:
        return content
    abbreviations = [
        # English
        'No', 'Mr', 'Mrs', 'Dr', 'St', 'vs', 'etc',
        'Jan', 'Feb', 'Mar', 'Apr', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
        'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun',
        # Indonesian
        'dll', 'dsb', 'dkk', 'yth',
        'Bpk', 'Bp', 'Sdri', 'Ibu', 'Mba', 'Mas',
        # Indonesian academic/professional titles
        'Drs', 'Dra', 'dr', 'Drg', 'drh', 'Ir',
        'S.H', 'M.H', 'S.E', 'M.M', 'M.Si', 'S.T', 'S.P', 'S.Kom', 'S.KM', 'S.Pt', 'S.Hut',
    ]
    protected = content
    for abbr in abbreviations:
        # Protect "Abbr." pattern only (avoid matching "Abbr" mid-word)
        protected = re.sub(rf'\b{re.escape(abbr)}\.', f'{abbr}[[DOT]]', protected)

    # Split on .!? followed by whitespace and a letter (any case, per skill pitfall #32)
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Za-z])', protected)
    restored = [sent.replace('[[DOT]]', '.').strip() for sent in sentences]
    restored = [s for s in restored if s]
    return '\n\n'.join(restored)

def validate_hook(hook):
    """Validate that hook has substance (per v16.1 spec — HOOK tightened to 2-3 sent).

    Spec (21 Jun 2026 update):
      - 2-3 sentences (emotional trigger, NO facts yet, builds curiosity)
      - Must have at least 2 sentences
      - Must be at least 4 words
    """
    issues = []

    if not hook or len(hook.strip()) < 10:
        issues.append("hook too short or empty")
        return False, issues

    word_count = len(hook.split())
    if word_count < 4:
        issues.append("hook too short (<4 words)")

    # Sentence count check (v16.1: HOOK must be 2-3 sentences)
    sent_count = count_sentences(hook)
    if sent_count < 2:
        issues.append(f"hook too short ({sent_count} sent, need 2-3)")
    elif sent_count > 3:
        issues.append(f"hook too long ({sent_count} sent, max 3)")

    return len(issues) == 0, issues

def count_sentences(text):
    """Count sentences in text (skips short fragments < 5 chars)."""
    if not text:
        return 0
    sentences = re.split(r'(?<=[.!?])\s+', text)
    return len([s for s in sentences if s.strip() and len(s.strip()) > 5])

def normalize_slide_sentences(slides_data):
    """Normalize slide sentence counts to fit per-slide bounds (no reject — auto-fix).

    Per v16 spec (21 Jun 2026) — threads-finance-6slide reference:
      - slide_1 HOOK:         min 1, max 2
      - slide_2 SETUP:        min 2, max 3
      - slide_3 COMPLICATION: min 2, max 3
      - slide_4 INSIGHT:      min 2, max 3
      - slide_5 POV:          min 2, max 4
      - slide_6 CTA:          min 1, max 2

    Behavior:
      - Over max → trim to first N sentences (keep first, drop rest)
      - Under min → pass through, log warning (padding risks fabrication)
    Returns: (normalized_slides_data, list_of_changes)
    """
    # Per-slide bounds (v16.1 — 21 Jun, HOOK tightened 1-2 → 2-3 per user feedback)
    # HOOK 2-3, SETUP 2-3, COMPLICATION 2-3, INSIGHT 2-3, POV 2-4, CTA 1-2
    bounds = {
        1: (2, 3),   # HOOK (emotional, 2-3 sent — substantive)
        2: (2, 3),   # SETUP
        3: (2, 3),   # COMPLICATION
        4: (2, 3),   # INSIGHT
        5: (2, 4),   # POV
        6: (1, 2),   # CTA (question form, short)
    }

    changes = []

    def trim_text(text, max_n):
        if not text:
            return text
        sentences = re.split(r'(?<=[.!?])\s+', text)
        valid = [s.strip() for s in sentences if s.strip() and len(s.strip()) > 5]
        if len(valid) > max_n:
            changes.append(f"{len(valid)}→{max_n} sent")
            return ' '.join(valid[:max_n])
        return text

    for i in range(1, 7):
        slide = slides_data.get(f'slide_{i}', {})
        if not isinstance(slide, dict):
            continue
        text = slide.get('content', '') or slide.get('hook', '')
        if not text:
            continue
        min_n, max_n = bounds[i]
        s = count_sentences(text)
        if s > max_n:
            trimmed = trim_text(text, max_n)
            if 'content' in slide and slide['content']:
                slide['content'] = trimmed
            else:
                slide['hook'] = trimmed
        elif s < min_n:
            changes.append(f"slide_{i} under min ({s}<{min_n})")

    return slides_data, changes


def validate_slide_sentences(slides_data):
    """Validate sentence counts per slide (per-slide bounds, no tolerance).

    Per v16.1 spec (21 Jun 2026) — threads-finance-6slide reference, HOOK tightened:
      - slide_1 HOOK:         2-3 sentences (was 1-2, user feedback: too short)
      - slide_2 SETUP:        2-3 sentences
      - slide_3 COMPLICATION: 2-3 sentences
      - slide_4 INSIGHT:      2-3 sentences
      - slide_5 POV:          2-4 sentences
      - slide_6 CTA:          1-2 sentences
    """
    bounds = {
        1: (2, 3), 2: (2, 3), 3: (2, 3),
        4: (2, 3), 5: (2, 4), 6: (1, 2),
    }
    issues = []

    for i in range(1, 7):
        slide = slides_data.get(f'slide_{i}', {})
        if isinstance(slide, dict):
            text = slide.get('content', '') or slide.get('hook', '')
        else:
            text = str(slide)
        s_count = count_sentences(text)
        min_n, max_n = bounds[i]
        if not (min_n <= s_count <= max_n):
            issues.append(f"slide_{i}: {s_count} sentences (need {min_n}-{max_n})")

    return len(issues) == 0, issues

def validate_grounding(slides_data, article_text):
    """Validate that every factual claim in slides appears in the article.
    
    Very lenient mode: only flags obvious hallucinations.
    Excludes years, single digits, currency amounts, and common numbers.
    """
    issues = []
    
    # Extract numbers from article (more flexible regex)
    article_numbers = set()
    for match in re.finditer(r'\d[\d.,]*', article_text):
        article_numbers.add(match.group())
    
    # Also extract just the digits without formatting
    article_digits = set()
    for num in article_numbers:
        clean = num.replace('.', '').replace(',', '')
        article_digits.add(clean)
    
    # Year exclusion list (2020-2030)
    EXCLUDE_YEARS = {str(y) for y in range(2020, 2031)}
    
    # Common numbers that appear in content
    COMMON_NUMBERS = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '100', '1000'}
    
    for i in range(1, 7):  # v16: 6 slides only (was 1-7 in v15, leftover bug)
        slide = slides_data.get(f"slide_{i}", {})
        hook = slide.get('hook', '') if isinstance(slide, dict) else ''
        content = slide.get('content', '') if isinstance(slide, dict) else ''
        slide_text = (hook + ' ' + content)
        
        slide_numbers = set(re.findall(r'\d[\d.,]*', slide_text))
        for num in slide_numbers:
            clean_num = num.replace('.', '').replace(',', '')
            
            # Skip years
            if num in EXCLUDE_YEARS:
                continue
            
            # Skip single digits
            if len(clean_num) <= 1:
                continue
            
            # Skip common numbers
            if clean_num in COMMON_NUMBERS:
                continue
            
            # Skip long IDs (article IDs, etc.)
            if len(clean_num) >= 6:  # 6+ digit = URL/article ID, skip
                continue
            
            # Skip if exact match in article
            if num in article_numbers:
                continue
            
            # Skip if digits match (e.g., "15,2" matches "15")
            if clean_num in article_digits:
                continue
            
            # Skip if it's a currency amount (Rp, RM, $, etc.)
            if re.search(rf'{re.escape(num)}\s*(?:juta|miliar|triliun|ribu|jt)', slide_text, re.IGNORECASE):
                continue
            
            # Skip if number appears in a URL
            if re.search(rf'https?://[^\s]*{re.escape(num)}', slide_text, re.IGNORECASE):
                continue
            
            issues.append(f"slide_{i}: Number '{num}' not found in article")
    
    return len(issues) == 0, issues

def format_slides(slides_data):
    """Format slides data into storytelling format with whitespace.

    For slide_1 (HOOK): prefer `content` because LLM v16 spec puts the hook body
    in `content` and only a short header in `title`. Fall back to hook/title only
    if content is empty.
    """
    slides = []
    for i in range(1, 7):
        key = f"slide_{i}"
        if key in slides_data:
            slide = slides_data[key]
            # Defensive: if slide is a raw string (e.g., from plain-text format that
            # bypassed normalize), wrap it as a dict. This is a latent crash fix —
            # current call path normalizes strings → dicts, but future paths may not.
            if isinstance(slide, str):
                slide = {"content": slide} if i != 1 else {"hook": slide, "content": ""}
            if not isinstance(slide, dict):
                log(f"[FORMAT] slide_{i} unexpected type {type(slide).__name__}, skipping", "WARN")
                slides.append({"hook": "", "content": ""})
                continue
            if i == 1:
                # Hook body lives in `content` per v16 JSON spec, NOT `title` (which is just a header)
                hook = slide.get("content", "") or slide.get("hook", "") or slide.get("title", "")
                hook = hook.replace('—', ', ').replace('–', ', ')
                slides.append({"hook": hook, "content": ""})
            else:
                content = slide.get("content", "")
                content = content.replace('—', ', ').replace('–', ', ')
                content = add_smart_whitespace(content)
                slides.append({"hook": "", "content": content})
    return slides

# ─── THREADS POSTING ─────────────────────────────────────────────────────────

def post_to_threads(staging_data):
    """Post slides to Threads using market-monday-post.py."""
    import subprocess

    if not THREADS_SCRIPT.exists():
        log("[POST] market-monday-post.py not found - skipping auto-post", "WARN")
        return False, None, None

    md_content = ""
    for i, slide in enumerate(staging_data['slides'], 1):
        hook = slide.get('hook', '')
        content = slide.get('content', '')
        if i == 1 and hook:
            md_content += f"{hook}\n\n---\n\n"
        elif content:
            md_content += f"{content}\n\n---\n\n"

    temp_file = DATA_DIR / "latest.md"
    temp_file.write_text(md_content)

    try:
        cmd = ["python3", str(THREADS_SCRIPT), "--file", str(temp_file)]
        image_url = staging_data.get("image_url", "")
        if image_url:
            cmd.extend(["--image", image_url])
            log(f"[POST] 📷 Attaching image: {image_url[:60]}...")

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        output = result.stdout
        root_id, permalink = None, None

        for line in output.split('\n'):
            if line.startswith('Root:'):
                root_id = line.split('Root:')[1].strip()
            elif line.startswith('Post:'):
                permalink = line.split('Post:')[1].strip()

        # Bug fix (v17.4): require BOTH root_id AND permalink for success.
        # Previously returned success=True with permalink=None, which caused
        # update_analytics to store permalink=None for valid posts.
        if root_id and permalink:
            log(f"[POST] ✅ Posted to Threads: {permalink}")
            return True, root_id, permalink
        elif root_id and not permalink:
            log(f"[POST] ⚠️ Got root_id but no permalink (post may have failed): {root_id}", "WARN")
            return False, None, None
        else:
            log(f"[POST] ❌ No root post ID found. Output: {output[:200]}", "ERROR")
            return False, None, None

    except subprocess.TimeoutExpired:
        log("[POST] ⚠️ Timeout (120s) - Threads API may be slow", "WARN")
        return False, None, None
    except Exception as e:
        log(f"[POST] ❌ Error: {e}", "ERROR")
        return False, None, None

def update_analytics(staging_data, root_id=None, permalink=None):
    """Update analytics data store after a post execution.

    Bug fix (v17.4): added file lock to prevent race condition when multiple
    processes (cron overlap, parallel runs) write to POSTED_FILE/TITLE_CACHE_FILE
    simultaneously. Without lock, load → modify → save is non-atomic — concurrent
    writers lose each other's updates.
    """
    _safe_json_update(POSTED_FILE, _do_posted_update, staging_data, root_id, permalink)
    _safe_json_update(TITLE_CACHE_FILE, _do_title_cache_update, staging_data)
    log(f"[ANALYTICS] Updated cache for: {staging_data['title'][:50]}...")


def _safe_json_update(path, updater, *args):
    """Load → modify → save with file lock to prevent concurrent-write corruption.

    Uses fcntl.flock (POSIX). No-op on platforms without fcntl (e.g. Windows).
    Lock is per-process; multiple threads in same process still serialize via GIL.
    """
    import fcntl as _fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    try:
        with open(lock_path, "w") as lockf:
            try:
                _fcntl.flock(lockf.fileno(), _fcntl.LOCK_EX)
                data = load_json(path, {})
                data = updater(data, *args)
                save_json(path, data)
            finally:
                _fcntl.flock(lockf.fileno(), _fcntl.LOCK_UN)
    except (ImportError, AttributeError, OSError):
        # No flock available (Windows) or other lock error — best-effort without lock
        data = load_json(path, {})
        data = updater(data, *args)
        save_json(path, data)


def _do_posted_update(posted, staging_data, root_id, permalink):
    """Updater for POSTED_FILE — adds/overwrites a post entry."""
    posted[staging_data["url"]] = {
        "title": staging_data["title"],
        "url": staging_data["url"],
        "source": staging_data["source"],
        "score": staging_data.get("score", 0),
        "slides": len(staging_data.get("slides", [])),
        "posted_at": datetime.now().isoformat(),
        "root_id": root_id,
        "permalink": permalink,
        "engagement": {"likes": 0, "replies": 0, "shares": 0, "views": 0}
    }
    return posted


def _do_title_cache_update(cache, staging_data):
    """Updater for TITLE_CACHE_FILE — appends new title, trims to last 100."""
    if "titles" not in cache:
        cache["titles"] = []
    if staging_data["title"] not in cache["titles"]:
        cache["titles"].append(staging_data["title"])
        cache["titles"] = cache["titles"][-100:]
    return cache

# ══════════════════════════════════════════════════════════════════════════════
# MODE: --benchmark
# ══════════════════════════════════════════════════════════════════════════════

def benchmark_extract_full_text(url):
    """Extract full text via newspaper3k fallback system for benchmarking."""
    if HAS_NEWSPAPER:
        try:
            article = newspaper.Article(url)
            article.download()
            article.parse()
            return {
                "success": True,
                "length": len(article.text),
                "preview": article.text[:200] + "..." if len(article.text) > 200 else article.text,
                "has_content": len(article.text) > 500
            }
        except Exception as e:
            return {"success": False, "error": str(e), "length": 0, "preview": "", "has_content": False}
    return {"success": False, "error": "newspaper3k not available", "length": 0, "preview": "", "has_content": False}

def benchmark_extract_image(url):
    """Benchmark og:image URL extraction metrics natively."""
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=8)
        html_content = response.text
        patterns = [
            r'<meta\s+property="og:image"\s+content="([^"]+)"',
            r'<meta\s+(?:name|property)="twitter:image"\s+content="([^"]+)"',
        ]
        for pattern in patterns:
            m = re.search(pattern, html_content, re.IGNORECASE)
            if m:
                img_url = m.group(1)
                return {
                    "success": True,
                    "url": img_url[:100] + "...",
                    "full_url": img_url,
                    "is_hd": any(x in img_url for x in ["1024", "1200", "1920"])
                }
        return {"success": False, "error": "No image found", "url": "", "full_url": "", "is_hd": False}
    except Exception as e:
        return {"success": False, "error": str(e), "url": "", "full_url": "", "is_hd": False}

def benchmark_source(source):
    """Run benchmark cycle for a singular source context."""
    print(f"\n{'='*60}")
    print(f"📰 {source['name']}")
    print(f"{'='*60}")
    
    articles = scrape_rss(source['url'], source['name'])
    if not articles:
        print("  ❌ No articles found")
        return {"source": source['name'], "rss_ok": False, "articles": 0}

    print(f"  ✅ Found {len(articles)} articles")
    test_article = articles[0]
    text_result = benchmark_extract_full_text(test_article['url'])
    image_result = benchmark_extract_image(test_article['url'])

    return {
        "source": source['name'],
        "rss_ok": True,
        "articles": len(articles),
        "full_text": text_result,
        "image": image_result
    }

def run_benchmark():
    """Run system diagnostic benchmarks over target publishers."""
    print("\n" + "="*60)
    print("📊 MARKET MONDAY — Source Benchmark")
    print("="*60)
    
    results = [benchmark_source(s) for s in BENCHMARK_SOURCES]
    
    print("\n\n" + "="*60)
    print("📊 SUMMARY")
    print("="*60)
    
    print(f"\n{'Source':<20} {'RSS':<8} {'Articles':<10} {'Full Text':<12} {'Image':<10} {'HD':<6}")
    print("-"*70)
    
    for r in results:
        rss = "✅" if r.get('rss_ok') else "❌"
        articles = r.get('articles', 0)
        full_text = "✅" if r.get('full_text', {}).get('has_content') else "❌"
        image = "✅" if r.get('image', {}).get('success') else "❌"
        hd = "✅" if r.get('image', {}).get('is_hd') else "❌"
        print(f"{r['source']:<20} {rss:<8} {articles:<10} {full_text:<12} {image:<10} {hd:<6}")
    
    save_json(BENCHMARK_FILE, results)

# ══════════════════════════════════════════════════════════════════════════════
# MODE: --analytics
# ══════════════════════════════════════════════════════════════════════════════

def analytics_get_token():
    """Extract platform validation tokens from credentials map."""
    with open(TOKEN_PATH) as f:
        data = json.load(f)
    return data["access_token"], str(data["user_id"])

def analytics_fetch_recent_posts(tok, uid, limit=20):
    """Fetch user profile threads posts timeline via native standard requests."""
    try:
        url = f"https://graph.threads.net/v1.0/{uid}/threads"
        params = {"access_token": tok, "fields": "id,text,timestamp", "limit": limit}
        r = requests.get(url, params=params, timeout=15)
        return r.json().get("data", [])
    except Exception as e:
        print(f"Error fetching posts: {e}")
        return []

def analytics_fetch_engagement(tok, post_id):
    """Gather deep engagement performance insights data via native API calls."""
    try:
        url = f"https://graph.threads.net/v1.0/{post_id}/insights"
        params = {"access_token": tok, "metric": "likes,replies,reposts,views,quotes", "period": "lifetime"}
        r = requests.get(url, params=params, timeout=10)
        metrics = {"likes": 0, "replies": 0, "reposts": 0, "views": 0, "quotes": 0}
        for item in r.json().get("data", []):
            metrics[item["name"]] = item["values"][0]["value"]
        return metrics
    except Exception as e:
        log(f"Analytics insights fetch failed: {e}", "WARN")
        return {"likes": 0, "replies": 0, "reposts": 0, "views": 0, "quotes": 0}

def analytics_calc_score(m):
    """Weighted operational formula score mapping framework."""
    return m["likes"] + m["replies"] * 3 + m["reposts"] * 2 + m["quotes"] * 2

def analytics_to_wib_hour(ts):
    """Convert strict ISO timestamp boundaries to localized hours integers."""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(WIB).hour
    except Exception as e:
        log(f"WIB hour parse failed: {e}", "WARN")
        return 12

def run_analytics():
    """Execute metrics audit engine over current historical footprint."""
    print("📊 Market Monday Analytics — Starting...")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        tok, uid = analytics_get_token()
    except Exception as e:
        print(f"❌ Token error: {e}")
        return 1

    raw = analytics_fetch_recent_posts(tok, uid, limit=20)
    if not raw:
        print("⚠️ No posts found.")
        return 0

    enriched = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(analytics_fetch_engagement, tok, p["id"]): p for p in raw}
        for future in as_completed(futures):
            post = futures[future]
            metrics = future.result()
            wib_hour = analytics_to_wib_hour(post["timestamp"])
            enriched.append({
                "text": post.get("text", ""),
                "ts": post["timestamp"],
                "post_id": post["id"],
                "metrics": metrics,
                "score": analytics_calc_score(metrics),
                "wib_hour": wib_hour,
                "time_slot": "pagi (06-10)" if 6 <= wib_hour < 10 else "siang (10-14)" if 10 <= wib_hour < 14 else "sore (14-18)" if 14 <= wib_hour < 18 else "malam (18-22)" if 18 <= wib_hour < 22 else "dini hari (22-06)"
            })

    enriched.sort(key=lambda x: x["score"], reverse=True)
    avg_score = sum(p["score"] for p in enriched) / max(len(enriched), 1)

    topic_stats = defaultdict(lambda: {"count": 0, "total_score": 0})
    time_stats = defaultdict(lambda: {"count": 0, "total_score": 0})
    
    for p in enriched:
        for t in extract_topics_from_title(p["text"]):
            topic_stats[t]["count"] += 1
            topic_stats[t]["total_score"] += p["score"]
        slot = p["time_slot"]
        time_stats[slot]["count"] += 1
        time_stats[slot]["total_score"] += p["score"]

    feedback = {
        "generated_at": datetime.now().isoformat(),
        "total_posts_analyzed": len(enriched),
        "overall": {
            "avg_score": round(avg_score, 1),
            "max_score": enriched[0]["score"] if enriched else 0,
            "min_score": enriched[-1]["score"] if enriched else 0
        },
        "topic_boosts": {
            t: {
                "avg_score": round(v["total_score"]/v["count"], 1),
                "count": v["count"],
                "boost_pct": round(((v["total_score"]/v["count"] - avg_score)/avg_score)*100, 1) if avg_score > 0 else 0
            } for t, v in topic_stats.items()
        },
        "time_boosts": {
            s: {
                "avg_score": round(v["total_score"]/v["count"], 1),
                "count": v["count"],
                "boost_pct": round(((v["total_score"]/v["count"] - avg_score)/avg_score)*100, 1) if avg_score > 0 else 0
            } for s, v in time_stats.items()
        },
        "best_topics": [k for k, _ in sorted(topic_stats.items(), key=lambda x: x[1]["total_score"]/x[1]["count"], reverse=True)[:3]],
        "worst_topics": [k for k, _ in sorted(topic_stats.items(), key=lambda x: x[1]["total_score"]/x[1]["count"])[:3]],
        "best_times": [k for k, _ in sorted(time_stats.items(), key=lambda x: x[1]["total_score"]/x[1]["count"], reverse=True)[:2]],
        "worst_times": [k for k, _ in sorted(time_stats.items(), key=lambda x: x[1]["total_score"]/x[1]["count"])[:2]]
    }

    save_json(FEEDBACK_FILE, feedback)
    print(f"✅ Feedback saved: {FEEDBACK_FILE}")
    return 0

# ─── MAIN PIPELINE RUNNER ────────────────────────────────────────────────────

def run_pipeline():
    """Execute full linear content scheduling orchestration workflow."""
    log("=== Market Monday Pipeline Started ===")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    posted_urls = set(load_json(POSTED_FILE, {}).keys())
    posted_titles = load_json(TITLE_CACHE_FILE, {"titles": []}).get("titles", [])
    feedback = load_feedback()

    articles = scrape_all_sources()
    candidates = select_best_candidate(articles, posted_urls, feedback, posted_titles, top_n=3)

    if not candidates:
        log("No eligible fresh content matches scoring thresholds.", "WARN")
        return False

    # Try each candidate until one passes the finance niche check
    for i, (score, best) in enumerate(candidates, 1):
        log(f"[CANDIDATE {i}/{len(candidates)}] Trying: {best['title']} (score: {score:.1f})")

        article_content = extract_article_content(best["url"])
        if len(article_content) < 100:
            log(f"  Extraction too short, skipping", "WARN")
            continue

        is_finance = is_finance_niche(best, article_content)
        if not is_finance:
            log(f"  ❌ Not finance niche, trying next candidate", "WARN")
            continue

        log(f"  ✅ Confirmed finance niche, generating content...", "INFO")
        slides_data = generate_content(best, article_content)
        if not slides_data:
            log(f"  Generation failed, trying next candidate", "WARN")
            continue

        # Success — save and post
        slides = format_slides(slides_data)
        image_url = extract_image(best['url'])

        staging_data = {
            "title": best["title"],
            "url": best["url"],
            "source": best["source"],
            "score": score,
            "slides": slides,
            "image_url": image_url or "",
            "timestamp": datetime.now().isoformat()
        }
        save_json(STAGING_FILE, staging_data)

        if DRY_RUN:
            log("🏃 Dry run configured - processing skipped.")
            update_analytics(staging_data, "dry-run", "dry-run-mode")
        else:
            success, r_id, p_link = post_to_threads(staging_data)
            update_analytics(staging_data, r_id, p_link)
        return True

    log(f"All {len(candidates)} candidates failed finance niche check or generation", "ERROR")
    return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Market Monday Pipeline — All-in-One")
    parser.add_argument("--dry-run", action="store_true", help="Skip posting to Threads")
    parser.add_argument("--benchmark", action="store_true", help="Test RSS sources quality")
    parser.add_argument("--analytics", action="store_true", help="Fetch engagement, update feedback")
    parser.add_argument("--model", type=str, help="Force specific LLM model")
    args = parser.parse_args()
    
    DRY_RUN = args.dry_run
    FORCE_MODEL = args.model
    
    try:
        if args.benchmark:
            run_benchmark()
        elif args.analytics:
            run_analytics()
        else:
            run_pipeline()
    except KeyboardInterrupt:
        log("Interrupted by operator request.")
        sys.exit(1)
    except Exception as e:
        log(f"Fatal unhandled panic event: {e}", "ERROR")
        traceback.print_exc()
        sys.exit(1)
