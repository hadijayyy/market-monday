#!/usr/bin/env python3
"""
MARKET MONDAY Pipeline — All-in-One
Niche: Economics & Market for Indonesian Professionals

Modes:
  (default)      Scrape → Score → LLM → Post
  --benchmark    Test RSS sources quality
  --analytics    Fetch engagement, update feedback
  --dry-run      Skip posting to Threads
  --model X      Force specific model

Architecture: Pressbox v7 pattern
Author: Hadijayyy
Created: 17 Jun 2026
Updated: 18 Jun 2026 — Consolidated benchmark + analytics into 1 file
Patched: 18 Jun 2026 — Code review: 14 fixes (bugs, perf, error handling)
"""

import os
import sys
import json
import time
import re
import html
import subprocess
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ─── CONFIG ──────────────────────────────────────────────────────────────────
DATA_DIR = Path.home() / ".hermes" / "market_monday"
SCRIPTS_DIR = Path.home() / ".hermes" / "scripts"
ENV_FILE = Path.home() / ".hermes" / ".env"
TOKEN_PATH = Path.home() / ".hermes" / "threads_token.json"

STAGING_FILE = DATA_DIR / "staging.json"
POSTED_FILE = DATA_DIR / "posted_topics.json"
FEEDBACK_FILE = DATA_DIR / "market_feedback.json"
RAW_OUTPUT_FILE = DATA_DIR / "raw_llm_output.txt"
LATEST_FILE = DATA_DIR / "latest.md"
TITLE_CACHE_FILE = DATA_DIR / "title_cache.json"
BENCHMARK_FILE = DATA_DIR / "benchmark_results.json"
REPORT_FILE = DATA_DIR / "market_analytics_report.md"

# LLM CONFIG
LLM_API_URL = "https://opencode.ai/zen/go/v1/chat/completions"
LLM_MODELS = ["deepseek-v4-flash", "mimo-v2.5"]  # Removed claude-sonnet-4.6 (not available)
DRY_RUN = False
FORCE_MODEL = None  # --model override
LLM_MAX_TOKENS = 6000
LLM_TIMEOUT = 90

# SIMILARITY
SIMILARITY_THRESHOLD = 0.35

# THREADS
THREADS_SCRIPT = SCRIPTS_DIR / "pressbox-direct-post.py"

# WIB timezone
WIB = timezone(timedelta(hours=7))

# RSS SOURCES
RSS_SOURCES = [
    {"name": "CNBC Indonesia", "url": "https://www.cnbcindonesia.com/rss", "type": "rss"},
    {"name": "Detik Finance", "url": "https://finance.detik.com/rss", "type": "rss"},
    {"name": "IDX Channel", "url": "https://www.idxchannel.com/rss", "type": "rss"},
]

# BENCHMARK RSS SOURCES (for --benchmark mode)
BENCHMARK_SOURCES = [
    {"name": "CNBC Indonesia", "url": "https://www.cnbcindonesia.com/rss"},
    {"name": "Detik Finance", "url": "https://finance.detik.com/rss"},
    {"name": "IDX Channel", "url": "https://www.idxchannel.com/rss"},
    {"name": "Kontan", "url": "https://www.kontan.co.id/rss"},
    {"name": "Bisnis.com", "url": "https://www.bisnis.com/rss"},
    {"name": "BBC Business", "url": "https://feeds.bbci.co.uk/news/business/rss.xml"},
]

# ─── STOPWORDS (module-level, not recreated per call) ────────────────────────

STOPWORDS = frozenset([
    "the", "a", "an", "in", "on", "at", "to", "for", "of", "with", "by",
    "as", "is", "was", "are", "were", "be", "been", "has", "have", "had",
    "but", "or", "and", "not", "no", "so", "if", "it", "its", "this",
    "that", "these", "those", "from", "into", "about", "between", "through",
    "yang", "dan", "di", "ke", "dari", "ini", "itu", "untuk", "dengan",
    "pada", "adalah", "akan", "juga", "sudah", "tidak", "bisa", "lebih",
])

# ─── SCORING KEYWORDS ────────────────────────────────────────────────────────

IMPACT_CRASH = {
    "crash", "crisis", "recession", "collapse", "plunge", "default",
    "bankruptcy", "layoff", "layoffs", "unemployment", "emergency",
    "panic", "meltdown", "turmoil", "slump", "downturn", "insolvency", "implosion",
    "anjlok", "ambruk", "jatuh", "gagal", "bangkrut", "phk", "resesi",
    "krisis", "darurat", "panik", "merosot", "menurun",
}

IMPACT_SURGE = {
    "surge", "rally", "soar", "boom", "breakthrough", "record",
    "historic", "milestone", "all-time high", "first time", "skyrocket",
    "outperform", "beat expectations", "strongest",
    "kenaikan", "meningkat", "melambung", "meroket", "menembus",
    "tembus", "rekor", "tertinggi", "terbesar", "pertama", "berhasil",
    "positif", "optimistis", "pulih", "rebound",
}

IMPACT_NEGATIVE = {
    "warning", "downgrade", "cut", "reduce", "slowdown", "weaken",
    "decline", "drop", "fall", "tumble", "sink", "miss", "miss expectations",
    "peringatan", "potong", "kurang", "perlambatan", "melemah",
    "penurunan", "merosot", "gagal", "meleset",
}

URGENCY_HIGH = {
    "breaking", "just in", "alert", "emergency", "urgent", "flash",
    "terbaru", "baru", "mendesak", "darurat", "segera", "breaking news",
}

URGENCY_MEDIUM = {
    "today", "this week", "imminent", "announce", "reveals", "expects",
    "hari ini", "minggu ini", "akan datang", "mengumumkan", "menguak",
    "memprediksi", "estimasi", "proyeksi",
}

INDO_HIGH = {
    "rupiah", "idr", "bi rate", "bank indonesia", "suku bunga",
    "ihsg", "idx", "ojk", "beban", "emiten", "saham indonesia",
    "jakarta", "indonesia", "pemerintah", "kementerian",
}

INDO_MEDIUM = {
    "komoditas", "batu bara", "nikel", "cpo", "kelapa sawit",
    "tembaga", "emas", "minyak mentah", "lng", "palm oil",
}

INDO_LOW = {
    "asia", "asean", "singapore", "malaysia", "thailand", "vietnam",
    "philippines", "china", "jepang", "korea",
}

BORING_KEYWORDS = {
    "quarterly report", "earnings preview", "market open", "market close",
    "trading update", "dividend announcement", "annual report",
    "regulatory filing", "proxy statement",
}

OPINION_KEYWORDS = {
    "opinion", "analysis", "column", "editorial", "commentary",
    "perspective", "viewpoint", "says analyst", "expert says",
}

VIDEO_KEYWORDS = {
    "video", "watch", "nonton", "tonton", "footage", "clip",
    "livestream", "live streaming", "replay",
}

VIRAL_FACTORS = {
    "outrage_money": ["price", "cost", "debt", "money", "tax", "billion",
                      "trillion", "rp", "harga", "biaya", "utang", "pajak"],
    "human_story": ["worker", "family", "household", "consumer", "employee",
                    "pekerja", "keluarga", "rumah tangga", "konsumen"],
    "controversy": ["ban", "scandal", "fraud", "corruption", "protest",
                    "korupsi", "skandal", "penipuan"],
    "record_milestone": ["record", "history", "milestone", "first ever",
                         "highest", "terbesar", "tertinggi", "pertama"],
    "geopolitical": ["war", "conflict", "sanction", "tariff", "ban",
                     "perang", "konflik", "sanksi"],
}

TOPIC_PATTERNS = {
    "inflasi": ["inflasi", "inflation", "harga", "price", "cpi", "deflasi"],
    "suku_bunga": ["suku bunga", "interest rate", "bi rate", "rate hike", "rate cut", "moneter"],
    "global_market": ["wall street", "saham", "stock", "ihsg", "idx", "rally", "crash", "bear", "bull"],
    "currency": ["rupiah", "dollar", "yen", "eur", "forex", "nilai tukar", "exchange rate"],
    "komoditas": ["minyak", "oil", "emas", "gold", "batu bara", "coal", "commodity"],
    "property": ["properti", "property", "rumah", "apartemen", "kpr", "real estate"],
    "tech_biz": ["ai", "tech", "startup", "digital", "fintech", "e-commerce"],
    "kebijakan": ["pajak", "tax", "regulasi", "regulation", "kebijakan", "policy", "bi", "ojk"],
    "karir": ["karir", "career", "gaji", "salary", "phk", "layoff", "lowongan", "job"],
    "energi": ["energi", "energy", "listrik", "pln", "bbm", "subsidi"],
    "global_event": ["perang", "war", "konflik", "conflict", "sanction", "g7", "g20", "imf"],
}

# ─── ECONOMIC KEYWORDS (module-level, not recreated per score_candidate) ─────

ECONOMIC_KEYWORDS = [
    "harga", "saham", "ihsg", "idx", "rupiah", "dollar", "bi rate", "suku bunga",
    "inflasi", "ekonomi", "pasar", "investasi", "komoditas", "cpo", "sawit",
    "pertambangan", "energi", "listrik", "bbm", "pertamax", "solar", "industri",
    "manufaktur", "ekspor", "impor", "neraca", "defisit", "surplus", "utang",
    "kredit", "pinjaman", "bank", "ojk", "emiten", "dividen", "laba", "rugi",
    "phk", "pekerja", "gaji", "upah", "tunjangan", "perpajakan", "pajak",
    "reksadana", "obligasi", "deposito", "asuransi", "properti", "rumah", "kpr",
    "kripto", "bitcoin", "ethereum", "blockchain", "startup", "fintech", "digital",
    "pemerintah", "kementerian", "regulasi", "kebijakan", "apbn", "apbd",
    "cadangan devisa", "balance of payment", "gdp", "pdb", "pertumbuhan",
    "resesi", "stagnasi", "perlambatan", "pemulihan", "rebound", "rally",
    "anomali", "manipulasi", "korupsi", "skandal", "penipuan", "gelap",
]

# ─── HELPER FUNCTIONS ────────────────────────────────────────────────────────

def load_env():
    """Load environment variables from .env file."""
    if ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, value = line.partition('=')
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    os.environ[key] = value

def load_json(path, default=None):
    """Load JSON file with fallback."""
    try:
        if path.exists():
            with open(path) as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return default if default is not None else {}

def save_json(path, data):
    """Save JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def log(msg, level="INFO"):
    """Log to stderr."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", file=sys.stderr)

def get_time_slot(hour):
    """Convert hour (0-23) to WIB time slot name. Shared by scoring + analytics."""
    if 6 <= hour < 10:
        return "pagi (06-10)"
    elif 10 <= hour < 14:
        return "siang (10-14)"
    elif 14 <= hour < 18:
        return "sore (14-18)"
    elif 18 <= hour < 22:
        return "malam (18-22)"
    else:
        return "dini hari (22-06)"

def alert_telegram(msg):
    """Send alert to Telegram via requests (no subprocess, avoids exposing token in ps)."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("ALERT_CHAT", "")
    if not (token and chat_id):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": f"📈 Market Monday: {msg}", "parse_mode": "HTML"},
            timeout=10,
        )
    except requests.RequestException:
        pass

# ─── TITLE SIMILARITY DEDUP (Jaccard) ───────────────────────────────────────

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
    """Extract topics from article title for feedback matching."""
    title_lower = title.lower()
    topics = []
    for topic, patterns in TOPIC_PATTERNS.items():
        for pattern in patterns:
            if pattern in title_lower:
                topics.append(topic)
                break
    return topics if topics else ["general"]

def apply_topic_boost(score, title, feedback):
    """Apply topic boost from analytics feedback."""
    if not feedback or "topic_boosts" not in feedback:
        return score

    topics = extract_topics_from_title(title)
    total_boost = 0

    for topic in topics:
        if topic in feedback["topic_boosts"]:
            boost_pct = feedback["topic_boosts"][topic].get("boost_pct", 0)
            boost = min(boost_pct / 2, 50)
            total_boost += boost

    return score + total_boost

def apply_time_boost(score, feedback):
    """Apply time-of-day boost from analytics feedback. Uses WIB timezone."""
    if not feedback or "time_boosts" not in feedback:
        return score

    current_hour = datetime.now(WIB).hour  # FIX: was datetime.now().hour (server tz)
    slot = get_time_slot(current_hour)

    if slot in feedback["time_boosts"]:
        boost_pct = feedback["time_boosts"][slot].get("boost_pct", 0)
        boost = min(boost_pct / 2, 30)
        return score + boost

    return score

# ─── IMAGE EXTRACTION ────────────────────────────────────────────────────────

def extract_image_from_html(html_content):
    """Extract image from HTML using 3 methods."""
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
    """Extract article image with 3-method fallback chain."""
    try:
        r = subprocess.run(
            ["curl", "-sL", "--max-time", "8",
             "-A", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
             url],
            capture_output=True, text=True, timeout=12,
        )
        return extract_image_from_html(r.stdout)
    except (subprocess.TimeoutExpired, OSError):
        return None

# ─── RSS SCRAPING ────────────────────────────────────────────────────────────

def scrape_rss(url, source_name):
    """Scrape RSS feed and return list of articles."""
    articles = []
    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "15",
             "-A", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
             url],
            capture_output=True, text=True, timeout=20,
        )
        if result.returncode != 0:
            log(f"RSS fetch failed: {source_name}", "WARN")
            return []

        content = result.stdout
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
                    "published": pub_date,
                })

        log(f"Scraped {len(articles)} articles from {source_name}")
    except Exception as e:
        log(f"RSS error: {source_name} - {e}", "WARN")

    return articles

def scrape_all_sources():
    """Scrape all RSS sources in parallel."""
    import concurrent.futures

    all_articles = []

    def fetch_source(source):
        return scrape_rss(source["url"], source["name"])

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_source, s): s for s in RSS_SOURCES}
        for future in concurrent.futures.as_completed(futures):
            try:
                articles = future.result()
                all_articles.extend(articles)
            except Exception as e:
                log(f"Scrape error: {e}", "WARN")

    return all_articles

# ─── SCORING ─────────────────────────────────────────────────────────────────

def is_fresh(pub_date_str, hours=24):
    """Check if article is within freshness window."""
    if not pub_date_str:
        return True
    try:
        from email.utils import parsedate_to_datetime
        pub_date = parsedate_to_datetime(pub_date_str)
        now = datetime.now(timezone.utc)
        age = now - pub_date
        return age.total_seconds() < hours * 3600
    except (ValueError, TypeError, OverflowError):
        return True  # conservative: assume fresh if we can't parse

def score_candidate(article, posted, feedback):
    """Score article with 7-layer scoring system."""
    title = article["title"].lower()
    desc = article["description"].lower()
    combined = f"{title} {desc}"

    if article["url"] in posted:
        return -1000

    if not is_fresh(article.get("published", ""), hours=24):
        return -500

    has_economic_keyword = any(kw in combined for kw in ECONOMIC_KEYWORDS)
    if not has_economic_keyword:
        return -200

    score = 0

    for kw in IMPACT_CRASH:
        if kw in combined:
            score += 30
            break

    for kw in IMPACT_SURGE:
        if kw in combined:
            score += 25
            break

    for kw in IMPACT_NEGATIVE:
        if kw in combined:
            score += 20
            break

    for kw in URGENCY_HIGH:
        if kw in combined:
            score += 25
            break

    for kw in URGENCY_MEDIUM:
        if kw in combined:
            score += 15
            break

    for kw in INDO_HIGH:
        if kw in combined:
            score += 40
            break

    for kw in INDO_MEDIUM:
        if kw in combined:
            score += 25
            break

    for kw in INDO_LOW:
        if kw in combined:
            score += 15
            break

    for kw in BORING_KEYWORDS:
        if kw in combined:
            score -= 15
            break

    for kw in OPINION_KEYWORDS:
        if kw in combined:
            score -= 20
            break

    for kw in VIDEO_KEYWORDS:
        if kw in title:
            score -= 100
            break

    viral_count = 0
    for factor, keywords in VIRAL_FACTORS.items():
        for kw in keywords:
            if kw in combined:
                viral_count += 1
                score += 10
                break

    if viral_count >= 3:
        score += 50

    words = article["title"].split()
    if len(words) <= 8:
        score += 15
    elif len(words) > 15:
        score -= 10

    if re.search(r'\d+', article["title"]):
        score += 10

    score = apply_topic_boost(score, article["title"], feedback)
    score = apply_time_boost(score, feedback)

    return score

def select_best_candidate(articles, posted, feedback, posted_titles=None):
    """Select the best article with feedback boosts + title dedup.

    Returns (article, score) tuple to avoid re-scoring in pipeline.
    """
    scored = []
    skipped_similar = 0

    for article in articles:
        if posted_titles and is_similar(article["title"], posted_titles):
            skipped_similar += 1
            continue

        score = score_candidate(article, posted, feedback)
        if score > 0:
            scored.append((score, article))

    if skipped_similar > 0:
        log(f"[DEDUP] Skipped {skipped_similar} similar titles")

    if not scored:
        return None, 0

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_article = scored[0]
    log(f"Best candidate: {best_article['title']} (score: {best_score:.1f})")
    return best_article, best_score

# ─── CONTENT EXTRACTION ──────────────────────────────────────────────────────

def extract_article_content(url):
    """Extract article content using 3-method chain."""
    try:
        import newspaper
        article = newspaper.Article(url)
        article.download()
        article.parse()
        if len(article.text) > 500:
            log(f"[EXTRACT] newspaper3k: {len(article.text)} chars")
            return article.text[:5000]
    except ImportError:
        log("[EXTRACT] newspaper3k not installed, using curl", "WARN")
    except Exception as e:
        log(f"[EXTRACT] newspaper3k failed: {e}", "WARN")

    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "10",
             "-A", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
             url],
            capture_output=True, text=True, timeout=15,
        )
        html_content = result.stdout

        article_match = re.search(r'<article[^>]*>(.*?)</article>', html_content, re.DOTALL)
        if article_match:
            article_html = article_match.group(1)
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', article_html, re.DOTALL)
            text = ' '.join([re.sub(r'<[^>]+>', '', p).strip() for p in paragraphs if len(p) > 50])
            if len(text) > 500:
                log(f"[EXTRACT] curl article tag: {len(text)} chars")
                return text[:5000]

        paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', html_content, re.DOTALL)
        text = ' '.join([re.sub(r'<[^>]+>', '', p).strip() for p in paragraphs if len(p) > 50])
        if len(text) > 500:
            log(f"[EXTRACT] curl p tags: {len(text)} chars")
            return text[:5000]

        text = re.sub(r'<[^>]+>', ' ', html_content)
        text = re.sub(r'\s+', ' ', text).strip()
        log(f"[EXTRACT] curl fallback: {len(text)} chars")
        return text[:5000]

    except Exception as e:
        log(f"[EXTRACT] curl failed: {e}", "ERROR")
        return ""

# ─── LLM CALLS ───────────────────────────────────────────────────────────────

def call_llm(system_prompt, user_prompt, model):
    """Call LLM API with system + user prompt split."""
    api_key = os.environ.get("OPENCODE_GO_API_KEY", "")

    if not api_key:
        log("No API key found", "ERROR")
        return None, None

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": LLM_MAX_TOKENS,
        "temperature": 0.8,
        "reasoning_effort": "low",
        "stream": True,
    }

    try:
        r = requests.post(LLM_API_URL, headers=headers, json=payload, timeout=LLM_TIMEOUT, stream=True)

        if r.status_code != 200:
            log(f"LLM API error ({model}): HTTP {r.status_code}", "ERROR")
            return None, None

        content_parts = []
        reasoning_parts = []
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

def extract_json_from_content(content):
    """Extract JSON from LLM content (handles multiple formats)."""
    content = re.sub(r'```json\s*', '', content)
    content = re.sub(r'```\s*$', '', content)
    content = re.sub(r'```\w*\s*', '', content)
    content = content.strip()

    json_match = None

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
                    json_match = re.search(r'\{[\s\S]*\}', content[start:i+1])
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
        except (json.JSONDecodeError, ValueError):
            return None

    normalized = {}
    for i in range(1, 9):
        key = f"slide_{i}"
        if key in data:
            val = data[key]
            if isinstance(val, str):
                if i == 1:
                    normalized[key] = {"hook": val, "content": ""}
                else:
                    normalized[key] = {"hook": "", "content": val}
            elif isinstance(val, dict):
                normalized[key] = val

    if len(normalized) >= 6:
        return normalized
    return None

def extract_json_from_reasoning(reasoning):
    """Extract JSON from reasoning content (Strategy 1 + 2).

    FIX: Strategy 2 now limits to last 20 closing braces to prevent O(n²) hang.
    """
    if not reasoning:
        return None

    # Strategy 1: Find JSON with slide markers
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

    # Strategy 2: Scan backward for last valid JSON (capped at 20 closing braces)
    log("   Strategy 2: scanning for last valid JSON with content...")
    best_json = ""
    best_score = 0
    closing_braces_seen = 0
    MAX_BRACES = 20  # FIX: cap iterations to prevent O(n²) hang

    for i in range(len(reasoning) - 1, -1, -1):
        if reasoning[i] == '}':
            closing_braces_seen += 1
            if closing_braces_seen > MAX_BRACES:
                break
            for j in range(i, max(i - 15000, -1), -1):
                if reasoning[j] == '{':
                    try:
                        obj = json.loads(reasoning[j:i+1])
                        if isinstance(obj, dict) and len(obj) >= 8:
                            total_content = 0
                            for k, v in obj.items():
                                if isinstance(v, dict) and "content" in v:
                                    total_content += len(v["content"])
                                elif isinstance(v, str):
                                    total_content += len(v)
                            if total_content > best_score:
                                best_score = total_content
                                best_json = reasoning[j:i+1]
                    except (json.JSONDecodeError, ValueError):
                        pass
            if best_json:
                break

    if best_json and best_score > 500:
        log(f"   Strategy 2: Found JSON ({len(best_json)}c, score={best_score})")
        return extract_json_from_content(best_json)
    elif best_json:
        log(f"   Strategy 2: Found JSON but low score ({best_score}), trying anyway...")
        return extract_json_from_content(best_json)

    return None

def generate_content(article, article_content):
    """Generate Threads content via LLM with model fallback."""
    system_prompt = """# ROLE
Kamu adalah content writer ekonomi pasar Indonesia. Nada: langsung, jujur, empati ke orang kecil — bukan wartawan formal.

# CONTEXT
Kamu akan menerima satu artikel berita ekonomi Indonesia. Tugasmu adalah mengubah artikel itu menjadi 8 slide konten Threads yang informatif dan relatable.

Batasan ketat:
- Slide 1–7: HANYA gunakan fakta yang ada di artikel (nama, angka, tanggal, lokasi, kejadian).
- Slide 8: Boleh tambahkan opini tajam berbasis fakta + empati personal sebagai penulis.
- Slide 6: Boleh inferensi logis dari fakta artikel — tapi harus di-flag sebagai analisis, bukan fakta.

# TASK
Ikuti langkah ini secara berurutan:

## Langkah 1 — Ekstrak Fakta
Baca artikel. Catat HANYA fakta eksplisit: siapa, apa, kapan, berapa, di mana.
Jangan tambah informasi dari luar artikel.

## Langkah 2 — Tulis 8 Slide

Slide 1 — Hook (2–3 kalimat)
Angka spesifik dari artikel + konteks + urgensi. Buat orang berhenti scroll.

Slide 2 — Apa yang Terjadi (3–4 kalimat)
Fakta utama: siapa melakukan apa, kapan. Padat, tanpa basa-basi.

Slide 3 — Kenapa Ini Penting (3–4 kalimat)
Konteks: kenapa ini terjadi sekarang? Dukung dengan angka dari artikel.

Slide 4 — Siapa yang Terdampak (3–4 kalimat)
Fokus ke orang kecil: petani, pedagang, buruh, UMKM. Bukan korporasi.

Slide 5 — Fakta yang Kurang Diketahui (3–4 kalimat)
Satu fakta dari artikel yang jarang disorot media umum.

Slide 6 — Analisis Dampak Lanjutan (3–4 kalimat)
Inferensi logis dari fakta artikel. Wajib buka dengan: "Kalau tren ini berlanjut..." atau frasa serupa yang jelas ini analisis, bukan fakta artikel.

Slide 7 — Yang Masih Belum Jelas (3–4 kalimat)
Ketidakpastian nyata dari artikel. Apa yang masih menggantung atau belum dijawab?

Slide 8 — Opini + CTA (2–3 kalimat)
Satu pendapat tajam berbasis fakta. Boleh tambahkan empati personal sebagai penulis.
Tutup dengan: "Menurut lo, [pertanyaan spesifik]?"
Sertakan URL artikel di baris terakhir.

# OUTPUT
Kembalikan HANYA JSON valid. Mulai dengan {}. Tanpa teks sebelum atau sesudah JSON.

Format:
{"slide_1":"...","slide_2":"...","slide_3":"...","slide_4":"...","slide_5":"...","slide_6":"...","slide_7":"...","slide_8":"..."}

# RULES
- Gunakan HANYA fakta dari artikel untuk slide 1–5 dan 7.
- Slide 6: inferensi logis, wajib di-flag sebagai analisis.
- Slide 8: opini + empati personal dibolehkan.
- Bahasa: Indonesia gaul yang kredibel. "Lo/gue" boleh, tapi sparingly.
- Gunakan \\n\\n untuk line break antar kalimat dalam JSON string.
- Setiap kalimat harus dipisahkan dengan spasi ganda (\\n\\n) agar mudah dibaca.
- Dilarang: em dash ( — ), hashtag, frasa kosong seperti "hal ini menunjukkan bahwa".
- Jangan sebut kata "slide" di dalam konten.
- Jumlah kalimat adalah target — prioritaskan kualitas dan kejelasan."""

    user_prompt = f"""JUDUL: {article['title']}
SUMBER: {article['source']}
URL: {article['url']}

ARTIKEL:
{article_content[:3000]}"""

    # Determine models to try
    if FORCE_MODEL:
        models_to_try = [FORCE_MODEL]
    else:
        models_to_try = LLM_MODELS

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
                    "timestamp": datetime.now().isoformat(),
                })

                slides_data = None
                if content:
                    slides_data = extract_json_from_content(content)
                if not slides_data and reasoning:
                    log("[LLM] Content empty, extracting from reasoning...")
                    slides_data = extract_json_from_reasoning(reasoning)

                if slides_data:
                    hook = slides_data.get("slide_1", {}).get("hook", "") or slides_data.get("slide_1", {}).get("content", "")
                    is_valid, issues = validate_hook(hook)

                    if is_valid:
                        sentences_valid, sentence_issues = validate_slide_sentences(slides_data)

                        if sentences_valid:
                            grounding_valid, grounding_issues = validate_grounding(slides_data, article_content)

                            if grounding_valid:
                                log(f"[LLM] ✅ Success with {model} - Hook valid: {hook[:50]}...")
                                return slides_data
                            else:
                                log(f"[LLM] ⚠️ Grounding issues: {', '.join(grounding_issues)}", "WARN")
                                if attempt < MAX_HOOK_RETRIES - 1:
                                    log("[LLM] Retrying with same model...")
                                continue
                        else:
                            sentence_info = ", ".join(sentence_issues)
                            log(f"[LLM] ⚠️ Sentence count: {sentence_info}", "WARN")
                            if attempt < MAX_HOOK_RETRIES - 1:
                                log("[LLM] Retrying with same model...")
                            continue
                    else:
                        log(f"[LLM] ⚠️ Hook invalid: {', '.join(issues)}", "WARN")
                        if attempt < MAX_HOOK_RETRIES - 1:
                            log("[LLM] Retrying with same model...")
                        continue
                else:
                    log(f"[LLM] ❌ JSON parse failed for {model}", "WARN")
                    break

    log("[LLM] ❌ All models failed (hook validation failed or parse error)", "ERROR")
    return None

def add_smart_whitespace(content):
    """Add line breaks after sentences, but NOT after abbreviations."""
    abbreviations = ['No', 'Mr', 'Mrs', 'Dr', 'St', 'vs', 'etc', 'dll']
    protected = content
    for abbr in abbreviations:
        protected = protected.replace(f'{abbr}.', f'{abbr}[[DOT]]')

    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', protected)

    restored = [sent.replace('[[DOT]]', '.') for sent in sentences]

    return '\n\n'.join(restored)

def validate_hook(hook):
    """Validate that hook has all 3 required elements: ANGKA + KONTEKST + DRAMA."""
    issues = []

    has_angka = bool(re.search(r'\d+', hook))
    if not has_angka:
        issues.append("GAK ADA ANGKA SPESIFIK")

    konteks_words = ['gaji', 'harga', 'sembako', 'BBM', 'rumah', 'IHSG', 'saham', 'investasi',
                     'properti', 'KPR', 'cicilan', 'pangan', 'beras', 'minyak', 'energi',
                     'ekonomi', 'pasar', 'defisit', 'inflasi', 'suku bunga', 'BI rate',
                     'ekspor', 'impor', 'neraca', 'komoditas', 'kripto', 'dollar',
                     'air', 'listrik', 'transport', 'logistik', 'komoditas',
                     'amdk', 'sni', 'kemasan', 'pedagang', 'umkm', 'usaha',
                     'utang', 'bank', 'pinjam', 'kredit', 'aset', 'dana', 'modal',
                     'reksadana', 'obligasi', 'deposito', 'tabungan', 'kas',
                     'piutang', 'hutang', 'anggaran', 'belanja', 'pajak',
                     'rupiah', 'emiten', 'persero', 'pt', 'tbk',
                     'buruh', 'pekerja', 'karyawan', 'aryawan', 'tenaga kerja',
                     'lapangan kerja', 'phk', 'pemutusan', 'industri', 'pabrik',
                     'manufaktur', 'produksi', 'ekspor', 'impor',
                     'bitcoin', 'crypto', 'token', 'blockchain', 'ethereum',
                     'the fed', 'fed', 'the federal', 'moneter', 'rate',
                     'bantuan', 'kemensos', 'sosial', 'banpoin', 'penerima',
                     'korban', 'bencana', 'longsor', 'banjir', 'gempa']
    has_konteks = any(word.lower() in hook.lower() for word in konteks_words)
    if not has_konteks:
        issues.append("GAK ADA KONTEKST YANG JELAS")

    # FIX: removed leading spaces from 'lumpuh', 'kontroversi', 'sorotan'
    drama_words = ['naik', 'turun', 'anjlok', 'meledak', 'ambruk', 'jatuh', 'rally',
                   'kosong', 'langka', 'mahal', 'murah', 'sesak', 'miskin', 'kaya',
                   'phk', 'tutup', 'bangkrut', 'gagal', 'guncang', 'terancam',
                   'tapi', 'malah', 'justru', 'tetep', 'terus', 'makin',
                   'hilang', 'ditarik', 'tarik', 'belum', 'gigit', 'was-was',
                   'kaget', 'terkejut', 'miris', 'menyedihkan',
                   'darah', 'berdarah', 'runtuh', 'hancur',
                   'panik', 'ketakutan', 'gejolak', 'krisis', 'darurat',
                   'perang', 'konflik', 'sanksi', 'larang', 'blokir',
                   # EXTRA DRAMA
                   'merugi', 'rugi', 'anjlok', 'merosot', 'terpuruk', 'terperosok',
                   'sengsara', 'menderita', 'susah', 'kesulitan', 'ketergantungan',
                   'ancaman', 'bahaya', 'risiko', 'padam', 'mati', 'lumpuh',
                   'kolaps', 'gulung tikar', 'tutup operasi', 'hentikan',
                   'tinggalkan', 'kabur', 'melarikan', 'lari', 'menghilang',
                   'publik', 'heboh', 'viral', 'ramai', 'polemik', 'kontroversi',
                   'sorot', 'sorotan', 'perhatian', 'fokus', 'gebrakan', 'kejutan']
    has_drama = any(word.lower() in hook.lower() for word in drama_words)
    if not has_drama:
        issues.append("GAK ADA DRAMA/EMOSI")

    is_valid = len(issues) == 0
    return is_valid, issues

def count_sentences(text):
    """Count sentences in text (skips short fragments < 5 chars)."""
    if not text:
        return 0
    sentences = re.split(r'(?<=[.!?])\s+', text)
    sentences = [s for s in sentences if s.strip() and len(s.strip()) > 5]
    return len(sentences)

def validate_slide_sentences(slides_data):
    """Validate sentence counts per slide (+1 tolerance)."""
    issues = []

    slide1 = slides_data.get('slide_1', {})
    hook = slide1.get('hook', '') if isinstance(slide1, dict) else ''
    content = slide1.get('content', '') if isinstance(slide1, dict) else ''
    text = hook if hook else content
    sentences = count_sentences(text)
    if sentences < 2:
        issues.append(f"slide_1: {sentences} sentences (need 2-3)")
    elif sentences > 4:
        issues.append(f"slide_1: {sentences} sentences (need 2-3)")

    for i in range(2, 8):
        slide = slides_data.get(f'slide_{i}', {})
        content = slide.get('content', '') if isinstance(slide, dict) else ''
        hook = slide.get('hook', '') if isinstance(slide, dict) else ''
        text = content if content else hook
        sentences = count_sentences(text)
        if sentences < 3:
            issues.append(f"slide_{i}: {sentences} sentences (need 3-4)")
        elif sentences > 5:
            issues.append(f"slide_{i}: {sentences} sentences (need 3-4)")

    slide8 = slides_data.get('slide_8', {})
    content = slide8.get('content', '') if isinstance(slide8, dict) else ''
    hook = slide8.get('hook', '') if isinstance(slide8, dict) else ''
    text = content if content else hook
    sentences = count_sentences(text)
    if sentences < 2:
        issues.append(f"slide_8: {sentences} sentences (need 2-3)")
    elif sentences > 4:
        issues.append(f"slide_8: {sentences} sentences (need 2-3)")

    is_valid = len(issues) == 0
    return is_valid, issues

def validate_grounding(slides_data, article_text):
    """Validate that every factual claim in slides appears in the article."""
    issues = []

    article_numbers = set(re.findall(r'(?<![/\w])\d[\d.,]*\d(?![//\w])', article_text))

    for i in range(1, 9):
        slide_key = f"slide_{i}"
        slide = slides_data.get(slide_key, {})

        hook = slide.get('hook', '') if isinstance(slide, dict) else ''
        content = slide.get('content', '') if isinstance(slide, dict) else ''
        slide_text = (hook + ' ' + content).lower()

        slide_numbers = set(re.findall(r'(?<![/\w-])\d[\d.,]*\d(?![/\\w-])', slide_text))
        for num in slide_numbers:
            if num not in article_numbers and len(num) > 1:
                if len(num.replace('.', '').replace(',', '')) > 6:
                    continue
                try:
                    float(num.replace(',', '.'))
                    issues.append(f"slide_{i}: Number '{num}' not found in article")
                except (ValueError, TypeError):
                    pass

    passed = len(issues) == 0
    return passed, issues

def format_slides(slides_data):
    """Format slides data into storytelling format with whitespace."""
    slides = []
    for i in range(1, 9):
        key = f"slide_{i}"
        if key in slides_data:
            slide = slides_data[key]

            if i == 1:
                hook = slide.get("hook", "") or slide.get("title", "") or slide.get("content", "")
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
    """Post slides to Threads using Press Box direct-post.py."""
    if not THREADS_SCRIPT.exists():
        log("[POST] Press Box direct-post.py not found - skipping auto-post", "WARN")
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

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120,
        )

        output = result.stdout
        root_id = None
        permalink = None

        for line in output.split('\n'):
            if line.startswith('Root:'):
                root_id = line.split('Root:')[1].strip()
            elif line.startswith('Post:'):
                permalink = line.split('Post:')[1].strip()

        if root_id:
            log(f"[POST] ✅ Posted to Threads: {permalink}")
            return True, root_id, permalink
        else:
            log("[POST] ❌ No root post ID found", "ERROR")
            log(f"[POST] Output: {output[:200]}")
            return False, None, None

    except subprocess.TimeoutExpired:
        log("[POST] ⚠️ Timeout (120s) - Threads API may be slow", "WARN")
        return False, None, None
    except Exception as e:
        log(f"[POST] ❌ Error: {e}", "ERROR")
        return False, None, None

def update_analytics(staging_data, root_id=None, permalink=None):
    """Update analytics after posting."""
    posted = load_json(POSTED_FILE, {})

    entry = {
        "title": staging_data["title"],
        "url": staging_data["url"],
        "source": staging_data["source"],
        "score": staging_data.get("score", 0),
        "slides": len(staging_data.get("slides", [])),
        "posted_at": datetime.now().isoformat(),
        "root_id": root_id,
        "permalink": permalink,
        "engagement": {
            "likes": 0,
            "replies": 0,
            "shares": 0,
            "views": 0,
        },
    }

    posted[staging_data["url"]] = entry
    save_json(POSTED_FILE, posted)

    title_cache = load_json(TITLE_CACHE_FILE, {"titles": []})
    if staging_data["title"] not in title_cache["titles"]:
        title_cache["titles"].append(staging_data["title"])
        title_cache["titles"] = title_cache["titles"][-100:]
        save_json(TITLE_CACHE_FILE, title_cache)

    log(f"[ANALYTICS] Updated: {staging_data['title'][:50]}...")

# ══════════════════════════════════════════════════════════════════════════════
# MODE: --benchmark
# ══════════════════════════════════════════════════════════════════════════════

def benchmark_extract_full_text(url):
    """Extract full text via newspaper3k (for benchmark)."""
    try:
        import newspaper
        article = newspaper.Article(url)
        article.download()
        article.parse()
        text = article.text
        return {
            "success": True,
            "length": len(text),
            "preview": text[:200] + "..." if len(text) > 200 else text,
            "has_content": len(text) > 500,
        }
    except Exception as e:
        return {"success": False, "error": str(e), "length": 0, "preview": "", "has_content": False}

def benchmark_extract_image(url):
    """Extract og:image URL and check resolution (for benchmark)."""
    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "8",
             "-A", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
             url],
            capture_output=True, text=True, timeout=12,
        )
        html_content = result.stdout

        patterns = [
            r'<meta\s+property="og:image"\s+content="([^"]+)"',
            r'<meta\s+(?:name|property)="twitter:image"\s+content="([^"]+)"',
            r'<meta\s+property="og:image:secure_url"\s+content="([^"]+)"',
        ]

        for pattern in patterns:
            m = re.search(pattern, html_content, re.IGNORECASE)
            if m:
                image_url = m.group(1)
                if any(ext in image_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.webp', 'image']):
                    return {
                        "success": True,
                        "url": image_url[:100] + "..." if len(image_url) > 100 else image_url,
                        "full_url": image_url,
                        "is_hd": "1024" in image_url or "1200" in image_url or "1920" in image_url,
                    }

        return {"success": False, "error": "No image found", "url": "", "full_url": "", "is_hd": False}
    except Exception as e:
        return {"success": False, "error": str(e), "url": "", "full_url": "", "is_hd": False}

def benchmark_source(source):
    """Benchmark a single source."""
    print(f"\n{'='*60}")
    print(f"📰 {source['name']}")
    print(f"{'='*60}")

    print(f"\n1️⃣ Scraping RSS...")
    articles = scrape_rss(source['url'], source['name'])

    if not articles:
        print("  ❌ No articles found")
        return {"source": source['name'], "rss_ok": False, "articles": 0}

    print(f"  ✅ Found {len(articles)} articles")
    for i, art in enumerate(articles, 1):
        print(f"     {i}. {art['title'][:60]}...")

    test_article = articles[0]
    print(f"\n2️⃣ Testing: {test_article['title'][:50]}...")

    print(f"\n   📄 Full Text Extraction...")
    text_result = benchmark_extract_full_text(test_article['url'])
    if text_result['success']:
        status = "✅" if text_result['has_content'] else "⚠️"
        print(f"   {status} Length: {text_result['length']} chars")
        print(f"      Preview: {text_result['preview'][:100]}...")
    else:
        print(f"   ❌ Error: {text_result['error']}")

    print(f"\n   🖼️  Image Extraction...")
    image_result = benchmark_extract_image(test_article['url'])
    if image_result['success']:
        hd_status = "HD" if image_result['is_hd'] else "SD"
        print(f"   ✅ Found ({hd_status})")
        print(f"      URL: {image_result['url']}")
    else:
        print(f"   ❌ {image_result['error']}")

    return {
        "source": source['name'],
        "rss_ok": True,
        "articles": len(articles),
        "full_text": text_result,
        "image": image_result,
    }

def run_benchmark():
    """Run benchmark on all RSS sources."""
    print("\n" + "="*60)
    print("📊 MARKET MONDAY — Source Benchmark")
    print("="*60)

    results = []
    for source in BENCHMARK_SOURCES:
        result = benchmark_source(source)
        results.append(result)
        time.sleep(2)

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
    print(f"\n📁 Results saved: {BENCHMARK_FILE}")

# ══════════════════════════════════════════════════════════════════════════════
# MODE: --analytics
# ══════════════════════════════════════════════════════════════════════════════

def analytics_get_token():
    """Get Threads API token."""
    with open(TOKEN_PATH) as f:
        data = json.load(f)
    return data["access_token"], str(data["user_id"])

def analytics_fetch_recent_posts(tok, uid, limit=20):
    """Fetch recent posts from Threads API."""
    import httpx
    try:
        r = httpx.get(
            f"https://graph.threads.net/v1.0/{uid}/threads",
            params={"access_token": tok, "fields": "id,text,timestamp", "limit": limit},
            timeout=15,
        )
        return r.json().get("data", [])
    except Exception as e:
        print(f"Error fetching posts: {e}")
        return []

def analytics_fetch_engagement(tok, post_id):
    """Fetch engagement metrics for a post."""
    import httpx
    try:
        r = httpx.get(
            f"https://graph.threads.net/v1.0/{post_id}/insights",
            params={
                "access_token": tok,
                "metric": "likes,replies,reposts,views,quotes",
                "period": "lifetime",
            },
            timeout=10,
        )
        metrics = {"likes": 0, "replies": 0, "reposts": 0, "views": 0, "quotes": 0}
        for item in r.json().get("data", []):
            metrics[item["name"]] = item["values"][0]["value"]
        return metrics
    except (requests.RequestException, KeyError, IndexError):
        return {"likes": 0, "replies": 0, "reposts": 0, "views": 0, "quotes": 0}

def analytics_calc_score(m):
    """Calculate engagement score (weighted)."""
    return m["likes"] + m["replies"] * 3 + m["reposts"] * 2 + m["quotes"] * 2

def analytics_to_wib_hour(ts):
    """Convert timestamp to WIB hour."""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(WIB).hour
    except (ValueError, TypeError):
        return 12

def run_analytics():
    """Run analytics — fetch engagement, generate feedback + report."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from collections import defaultdict

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

    print(f"📊 Analyzing {len(raw)} posts...")

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
                "time_slot": get_time_slot(wib_hour),  # FIX: shared utility
            })

    enriched.sort(key=lambda x: x["score"], reverse=True)

    scores = [p["score"] for p in enriched]
    avg_score = sum(scores) / max(len(scores), 1)
    max_score = max(scores) if scores else 0
    min_score = min(scores) if scores else 0

    topic_stats = defaultdict(lambda: {"count": 0, "total_score": 0, "avg_score": 0})
    for post in enriched:
        topics = extract_topics_from_title(post["text"])
        for topic in topics:
            topic_stats[topic]["count"] += 1
            topic_stats[topic]["total_score"] += post["score"]

    for topic in topic_stats:
        stats = topic_stats[topic]
        stats["avg_score"] = stats["total_score"] / max(stats["count"], 1)

    sorted_topics = sorted(topic_stats.items(), key=lambda x: x[1]["avg_score"], reverse=True)

    time_stats = defaultdict(lambda: {"count": 0, "total_score": 0, "avg_score": 0})
    for post in enriched:
        slot = post["time_slot"]
        time_stats[slot]["count"] += 1
        time_stats[slot]["total_score"] += post["score"]

    for slot in time_stats:
        stats = time_stats[slot]
        stats["avg_score"] = stats["total_score"] / max(stats["count"], 1)

    sorted_times = sorted(time_stats.items(), key=lambda x: x[1]["avg_score"], reverse=True)

    top_posts = enriched[:5]
    bottom_posts = enriched[-5:] if len(enriched) >= 5 else enriched

    feedback = {
        "generated_at": datetime.now().isoformat(),
        "total_posts_analyzed": len(enriched),
        "overall": {
            "avg_score": round(avg_score, 1),
            "max_score": max_score,
            "min_score": min_score,
        },
        "topic_boosts": {},
        "time_boosts": {},
        "best_topics": [],
        "worst_topics": [],
        "best_times": [],
        "worst_times": [],
    }

    for topic, stats in sorted_topics:
        boost = 0
        if avg_score > 0:
            boost = ((stats["avg_score"] - avg_score) / avg_score) * 100
        feedback["topic_boosts"][topic] = {
            "avg_score": round(stats["avg_score"], 1),
            "count": stats["count"],
            "boost_pct": round(boost, 1),
        }

    for slot, stats in sorted_times:
        boost = 0
        if avg_score > 0:
            boost = ((stats["avg_score"] - avg_score) / avg_score) * 100
        feedback["time_boosts"][slot] = {
            "avg_score": round(stats["avg_score"], 1),
            "count": stats["count"],
            "boost_pct": round(boost, 1),
        }

    feedback["best_topics"] = [t[0] for t in sorted_topics[:3]]
    feedback["worst_topics"] = [t[0] for t in sorted_topics[-3:]]
    feedback["best_times"] = [t[0] for t in sorted_times[:2]]
    feedback["worst_times"] = [t[0] for t in sorted_times[-2:]]

    save_json(FEEDBACK_FILE, feedback)
    print(f"✅ Feedback saved: {FEEDBACK_FILE}")

    report = f"""# 📊 Market Monday Analytics Report
**Generated:** {datetime.now().strftime('%d %b %Y %H:%M WIB')}
**Posts Analyzed:** {len(enriched)}

---

## 📈 Overall Performance

| Metric | Value |
|--------|-------|
| Average Score | **{avg_score:.1f}** |
| Highest Score | **{max_score}** |
| Lowest Score | **{min_score}** |

---

## 🏆 Top Topics (by avg score)

| Topic | Avg Score | Count | Boost |
|-------|-----------|-------|-------|
"""
    for topic, stats in sorted_topics[:5]:
        boost = feedback["topic_boosts"][topic]["boost_pct"]
        emoji = "🟢" if boost > 0 else "🔴" if boost < 0 else "⚪"
        report += f"| {topic} | {stats['avg_score']:.1f} | {stats['count']} | {emoji} {boost:+.0f}% |\n"

    report += f"""
---

## ⏰ Best Posting Times (WIB)

| Time Slot | Avg Score | Count | Boost |
|-----------|-----------|-------|-------|
"""
    for slot, stats in sorted_times:
        boost = feedback["time_boosts"][slot]["boost_pct"]
        emoji = "🟢" if boost > 0 else "🔴" if boost < 0 else "⚪"
        report += f"| {slot} | {stats['avg_score']:.1f} | {stats['count']} | {emoji} {boost:+.0f}% |\n"

    report += f"""
---

## 🔥 Top 5 Posts

"""
    for i, post in enumerate(top_posts, 1):
        topics = extract_topics_from_title(post["text"])
        report += f"""**{i}. Score: {post['score']}** | Topics: {', '.join(topics)}
> {post['text'][:100]}...

"""

    report += f"""
---

## 📉 Bottom 5 Posts (Learning)

"""
    for i, post in enumerate(bottom_posts, 1):
        topics = extract_topics_from_title(post["text"])
        report += f"""**{i}. Score: {post['score']}** | Topics: {', '.join(topics)}
> {post['text'][:100]}...

"""

    report += f"""
---

## 💡 Recommendations

**DO MORE:**
"""
    for topic in feedback["best_topics"][:3]:
        report += f"- ✅ **{topic}** — performing above average\n"

    report += f"""
**DO LESS:**
"""
    for topic in feedback["worst_topics"][:3]:
        report += f"- ❌ **{topic}** — performing below average\n"

    report += f"""
**BEST TIMES:**
"""
    for time_slot in feedback["best_times"]:
        report += f"- ⏰ **{time_slot}** — higher engagement\n"

    with open(REPORT_FILE, 'w') as f:
        f.write(report)
    print(f"✅ Report saved: {REPORT_FILE}")

    print(f"\n📊 Summary:")
    print(f"   Posts analyzed: {len(enriched)}")
    print(f"   Avg score: {avg_score:.1f}")
    print(f"   Best topics: {', '.join(feedback['best_topics'][:3])}")
    print(f"   Best times: {', '.join(feedback['best_times'])}")

    return 0

# ══════════════════════════════════════════════════════════════════════════════
# MODE: (default) — Main Pipeline
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    """Main pipeline execution."""
    log("=== Market Monday Pipeline Started ===")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    posted = load_json(POSTED_FILE, {})
    posted_urls = set(posted.keys())

    title_cache = load_json(TITLE_CACHE_FILE, {"titles": []})
    posted_titles = title_cache.get("titles", [])

    feedback = load_feedback()

    log("Scraping RSS sources...")
    articles = scrape_all_sources()

    if not articles:
        log("No articles found", "WARN")
        print("No articles found")
        sys.exit(2)

    log(f"Total articles scraped: {len(articles)}")

    # FIX: select_best_candidate now returns (article, score) tuple
    best, best_score = select_best_candidate(articles, posted_urls, feedback, posted_titles)

    if not best:
        log("No suitable candidate found", "WARN")
        print("No suitable candidate")
        sys.exit(2)

    log(f"Extracting content: {best['url'][:60]}...")
    article_content = extract_article_content(best["url"])

    if not article_content or len(article_content) < 100:
        log("Article content too short or empty", "WARN")
        print("Article content too short")
        sys.exit(2)

    log("Generating content via LLM...")
    slides_data = generate_content(best, article_content)

    if not slides_data:
        log("LLM generation failed", "ERROR")
        alert_telegram("LLM generation failed")
        print("LLM failed")
        sys.exit(1)

    slides = format_slides(slides_data)

    if len(slides) < 8:
        log(f"Only {len(slides)} slides generated (need 8)", "WARN")

    image_url = extract_image(best['url'])
    if image_url:
        log(f"📷 Image: {image_url[:60]}...")
    else:
        log("No image found")

    staging_data = {
        "title": best["title"],
        "url": best["url"],
        "source": best["source"],
        "score": best_score,  # FIX: reuse score from selection, no re-computation
        "slides": slides,
        "image_url": image_url or "",
        "timestamp": datetime.now().isoformat(),
    }

    save_json(STAGING_FILE, staging_data)

    md_content = f"{best['title']}\n\nSumber: {best['source']}\nURL: {best['url']}\n\n---\n\n"
    for i, slide in enumerate(slides, 1):
        hook = slide.get('hook', '')
        content = slide.get('content', '')
        if i == 1 and hook:
            md_content += f"{hook}\n\n---\n\n"
        elif content:
            md_content += f"{content}\n\n---\n\n"

    with open(LATEST_FILE, 'w') as f:
        f.write(md_content)

    if DRY_RUN:
        log("🏃 DRY RUN - skipping post to Threads")
        success = True
        root_id = "dry-run"
        permalink = "dry-run-mode"
    else:
        log("Auto-posting to Threads...")
        success, root_id, permalink = post_to_threads(staging_data)

    update_analytics(staging_data, root_id, permalink)

    if success:
        log(f"✅ Pipeline complete! Posted to Threads: {permalink}")
        print(f"✅ Pipeline complete: {best['title']} ({len(slides)} slides)")
        print(f"🔗 {permalink}")
    else:
        log("⚠️ Pipeline complete (not posted to Threads)")
        print(f"⚠️ Pipeline complete (staging only): {best['title']} ({len(slides)} slides)")

    return True

# ─── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Market Monday Pipeline — All-in-One")
    parser.add_argument("--dry-run", action="store_true", help="Skip posting to Threads")
    parser.add_argument("--benchmark", action="store_true", help="Test RSS sources quality")
    parser.add_argument("--analytics", action="store_true", help="Fetch engagement, update feedback")
    parser.add_argument("--model", type=str, help="Force specific LLM model")
    args = parser.parse_args()

    DRY_RUN = args.dry_run
    FORCE_MODEL = args.model

    # FIX: load env once at startup, not on every LLM call
    load_env()

    if DRY_RUN:
        log("🏃 DRY RUN MODE - will NOT post to Threads")

    if FORCE_MODEL:
        log(f"🎯 FORCE MODEL: {FORCE_MODEL}")

    try:
        if args.benchmark:
            run_benchmark()
        elif args.analytics:
            run_analytics()
        else:
            success = run_pipeline()
    except KeyboardInterrupt:
        log("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        log(f"Fatal error: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)
