#!/usr/bin/env python3
"""
MARKET MONDAY Pipeline - Threads Personal Branding Automation
Niche: Economics & Market for Indonesian Professionals

Architecture reference: Press Box v7
Analytics feedback: market_feedback.json (iterative loop)

Flow:
1. Scrape RSS (BBC, CNBC)
2. Filter (URL dedup + Title similarity)
3. Score candidates (7-layer scoring system)
4. LLM generate 8 slides (with model fallback)
5. Auto-post to Threads
6. Track engagement

Author: Hadijayyy
Created: 17 Jun 2026
Updated: 17 Jun 2026 - 6 optimizations implemented
"""

import os
import sys
import json
import time
import re
import html
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ─── CONFIG ──────────────────────────────────────────────────────────────────
DATA_DIR = Path.home() / ".hermes" / "market_monday"
SCRIPTS_DIR = Path.home() / ".hermes" / "scripts"
ENV_FILE = Path.home() / ".hermes" / ".env"

STAGING_FILE = DATA_DIR / "staging.json"
POSTED_FILE = DATA_DIR / "posted_topics.json"
FEEDBACK_FILE = DATA_DIR / "market_feedback.json"
RAW_OUTPUT_FILE = DATA_DIR / "raw_llm_output.txt"
LATEST_FILE = DATA_DIR / "latest.md"
TITLE_CACHE_FILE = DATA_DIR / "title_cache.json"  # For Jaccard dedup

# LLM CONFIG - mimo-v2.5 primary (clean JSON), deepseek v4-flash fallback
LLM_API_URL = "https://opencode.ai/zen/go/v1/chat/completions"
LLM_MODELS = ["mimo-v2.5", "minimax-m3"]  # Primary → Fallback (reasoning model)
DRY_RUN = False  # Set to True via --dry-run flag
LLM_MAX_TOKENS = 6000
LLM_TIMEOUT = 60  # 60s per model (reduced from 90s)

# SIMILARITY CONFIG (from Press Box)
SIMILARITY_THRESHOLD = 0.35  # Jaccard similarity threshold for dedup

# THREADS CONFIG (for auto-post)
THREADS_SCRIPT = SCRIPTS_DIR / "pressbox-direct-post.py"

# RSS SOURCES - Indonesia only (4 sources tested & working)
RSS_SOURCES = [
    {"name": "CNBC Indonesia", "url": "https://www.cnbcindonesia.com/rss", "type": "rss"},
    {"name": "Detik Finance", "url": "https://finance.detik.com/rss", "type": "rss"},
    {"name": "IDX Channel", "url": "https://www.idxchannel.com/rss", "type": "rss"},
]

# ─── SCORING KEYWORDS ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
# SCORING SYSTEM - Adapted from Press Box (Football) → Market Monday (Finance)
# ══════════════════════════════════════════════════════════════════════════════

# ── LAYER 1: IMPACT KEYWORDS (market-moving news) ──────────────────────────
IMPACT_CRASH = {          # +30 - BAD news that moves markets
    "crash", "crisis", "recession", "collapse", "plunge", "default",
    "bankruptcy", "layoff", "layoffs", "unemployment", "emergency",
    "panic", "meltdown", "turmoil", "slump", "downturn", "insolvency", "implosion",
    # Indonesian
    "anjlok", "ambruk", "jatuh", "gagal", "bangkrut", "phk", "resesi",
    "krisis", "darurat", "panik", "merosot", "menurun", "turun"
}

IMPACT_SURGE = {          # +25 - GOOD news that moves markets
    "surge", "rally", "soar", "boom", "breakthrough", "record",
    "historic", "milestone", "all-time high", "first time", "skyrocket",
    "outperform", "beat expectations", "strongest",
    # Indonesian
    "naik", "kenaikan", "meningkat", "melambung", "meroket", "menembus",
    "tembus", "rekor", "tertinggi", "terbesar", "pertama", "berhasil",
    "positif", "optimistis", "pulih", " rebound"
}

IMPACT_NEGATIVE = {       # +20 - Negative but not crash-level
    "warning", "downgrade", "cut", "reduce", "slowdown", "weaken",
    "decline", "drop", "fall", "tumble", "sink", "miss", "miss expectations",
    # Indonesian
    "peringatan", "potong", "kurang", "perlambatan", "melemah",
    "penurunan", "turun", "merosot", "gagal", "meleset"
}

# ── LAYER 2: URGENCY SIGNALS (breaking > analysis) ─────────────────────────
URGENCY_HIGH = {          # +25 - Breaking/urgent
    "breaking", "just in", "alert", "emergency", "urgent", "flash",
    # Indonesian
    "terbaru", "baru", "mendesak", "darurat", "segera", "breaking news"
}

URGENCY_MEDIUM = {        # +15 - Timely
    "today", "this week", "imminent", "announce", "reveals", "expects",
    # Indonesian
    "hari ini", "minggu ini", "akan datang", "mengumumkan", "menguak",
    "memprediksi", "estimasi", "proyeksi"
}

# ── LAYER 3: INDONESIAN RELEVANCE (local impact) ───────────────────────────
INDO_HIGH = {             # +40 - Direct Indonesia impact
    "rupiah", "idr", "bi rate", "bank indonesia", "suku bunga",
    "ihsg", "idx", "ojk", "beban", "emiten", "saham indonesia",
    "jakarta", "indonesia", "pemerintah", "kementerian"
}

INDO_MEDIUM = {           # +25 - Commodity/resource Indonesia
    "komoditas", "batu bara", "nikel", "cpo", "kelapa sawit",
    "tembaga", "emas", "minyak mentah", "lng", "palm oil"
}

INDO_LOW = {              # +15 - Regional Asia
    "asia", "asean", "singapore", "malaysia", "thailand", "vietnam",
    "philippines", "china", "jepang", "korea"
}

# ── LAYER 4: BORING/PENALTY (noise, not market-moving) ──────────────────────
BORING_KEYWORDS = {       # -15 - Routine/dry reports
    "quarterly report", "earnings preview", "market open", "market close",
    "trading update", "dividend announcement", "annual report",
    "regulatory filing", "proxy statement"
}

OPINION_KEYWORDS = {      # -20 - Opinion/analysis (not factual news)
    "opinion", "analysis", "column", "editorial", "commentary",
    "perspective", "viewpoint", "says analyst", "expert says"
}

VIDEO_KEYWORDS = {        # -100 - Skip video-based articles
    "video", "watch", "nonton", "tonton", "footage", "clip",
    "livestream", "live streaming", "replay"
}

# ── LAYER 5: VIRAL FACTORS (engagement drivers) ────────────────────────────
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
                     "perang", "konflik", "sanksi"]
}

# ── LAYER 6: TOPIC PATTERNS (for analytics feedback boost) ─────────────────
TOPIC_PATTERNS = {
    "inflasi": ["inflasi", "inflation", "harga", "price", "cpi"],
    "suku_bunga": ["suku bunga", "interest rate", "bi rate", "rate hike", "rate cut"],
    "global_market": ["wall street", "saham", "stock", "ihsg", "idx", "rally", "crash"],
    "currency": ["rupiah", "dollar", "yen", "forex", "nilai tukar"],
    "komoditas": ["minyak", "oil", "emas", "gold", "batu bara", "commodity"],
    "property": ["properti", "property", "rumah", "apartemen", "kpr"],
    "tech_biz": ["ai", "tech", "startup", "digital", "fintech"],
    "kebijakan": ["pajak", "tax", "regulasi", "policy", "ojk"],
    "karir": ["karir", "career", "gaji", "salary", "phk", "layoff"],
    "energi": ["energi", "energy", "listrik", "bbm"],
    "global_event": ["perang", "war", "konflik", "sanction", "g7", "imf"],
}

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

def alert_telegram(msg):
    """Send alert to Telegram."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("ALERT_CHAT", "")
    if token and chat_id:
        try:
            import subprocess
            subprocess.run([
                "curl", "-s", "-X", "POST",
                f"https://api.telegram.org/bot{token}/sendMessage",
                "-d", f"chat_id={chat_id}",
                "-d", f"text=📈 Market Monday: {msg}",
                "-d", "parse_mode=HTML"
            ], timeout=10, capture_output=True)
        except:
            pass

# ─── FIX 1: TITLE SIMILARITY DEDUP (Jaccard 35%) ─────────────────────────────

def clean_words(text):
    """Clean text for similarity comparison - from Press Box."""
    STOPWORDS = frozenset([
        "the", "a", "an", "in", "on", "at", "to", "for", "of", "with", "by",
        "as", "is", "was", "are", "were", "be", "been", "has", "have", "had",
        "but", "or", "and", "not", "no", "so", "if", "it", "its", "this",
        "that", "these", "those", "from", "into", "about", "between", "through",
        "yang", "dan", "di", "ke", "dari", "ini", "itu", "untuk", "dengan",
        "pada", "adalah", "akan", "juga", "sudah", "tidak", "bisa", "lebih"
    ])
    text = text.lower()
    # Remove punctuation
    text = re.sub(r'[^\w\s]', ' ', text)
    words = text.split()
    # Filter stopwords
    return set(w for w in words if w not in STOPWORDS and len(w) > 1)

def is_similar(new_title, posted_titles, threshold=SIMILARITY_THRESHOLD):
    """Check if title is too similar to already posted content - Jaccard similarity.
    
    From Press Box: prevents posting same story from different angles.
    Example: "IHSG Naik 5%" vs "IHSG Rally 5%" = similarity 0.7 → SKIP
    """
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
    """Apply time-of-day boost from analytics feedback."""
    if not feedback or "time_boosts" not in feedback:
        return score

    current_hour = datetime.now().hour

    if 6 <= current_hour < 10:
        slot = "pagi (06-10)"
    elif 10 <= current_hour < 14:
        slot = "siang (10-14)"
    elif 14 <= current_hour < 18:
        slot = "sore (14-18)"
    elif 18 <= current_hour < 22:
        slot = "malam (18-22)"
    else:
        slot = "dini hari (22-06)"

    if slot in feedback["time_boosts"]:
        boost_pct = feedback["time_boosts"][slot].get("boost_pct", 0)
        boost = min(boost_pct / 2, 30)
        return score + boost

    return score

# ─── FIX 4: IMAGE EXTRACTION - 3-METHOD FALLBACK ─────────────────────────────

def extract_image_from_html(html_content):
    """Extract image from HTML using 3 methods."""
    # Method 1: og:image (most reliable)
    m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html_content, re.IGNORECASE)
    if m:
        return m.group(1)

    # Method 2: twitter:image
    m = re.search(r'<meta\s+(?:name|property)="twitter:image"\s+content="([^"]+)"', html_content, re.IGNORECASE)
    if m:
        return m.group(1)

    # Method 3: First article img
    m = re.search(r'<article[^>]*>.*?<img[^>]+src="([^"]+)"', html_content, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1)

    return None

def extract_image(url):
    """Extract article image with 3-method fallback chain.
    
    From Press Box: og:image → twitter:image → first article img
    """
    import subprocess
    try:
        r = subprocess.run(
            ["curl", "-sL", "--max-time", "8",
             "-A", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
             url],
            capture_output=True, text=True, timeout=12
        )
        return extract_image_from_html(r.stdout)
    except:
        return None

# ─── RSS SCRAPING ────────────────────────────────────────────────────────────

def scrape_rss(url, source_name):
    """Scrape RSS feed and return list of articles."""
    import subprocess
    articles = []
    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "15",
             "-A", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
             url],
            capture_output=True, text=True, timeout=20
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
                    "published": pub_date
                })

        log(f"Scraped {len(articles)} articles from {source_name}")
    except Exception as e:
        log(f"RSS error: {source_name} - {e}", "WARN")

    return articles

def scrape_all_sources():
    """Scrape all RSS sources in parallel (from Press Box)."""
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
    except:
        return True

def score_candidate(article, posted, feedback):
    """Score article with 7-layer scoring system.
    
    Adapted from Press Box v7 score_topic() but optimized for finance:
    - Layer 1: Impact keywords (crash/surge/negative) - market-moving news
    - Layer 2: Urgency signals (breaking > timely)
    - Layer 3: Indonesian relevance (local impact priority)
    - Layer 4: Penalties (boring/opinion noise)
    - Layer 5: Viral factors (engagement drivers)
    - Layer 6: Title quality (short = catchy)
    - Layer 7: Analytics feedback (adaptive learning)
    """
    title = article["title"].lower()
    desc = article["description"].lower()
    combined = f"{title} {desc}"

    # Skip if already posted
    if article["url"] in posted:
        return -1000

    # Skip if not fresh
    if not is_fresh(article.get("published", ""), hours=24):
        return -500

    score = 0

    # ── LAYER 1: IMPACT KEYWORDS (market-moving) ──────────────────────────
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

    # ── LAYER 2: URGENCY SIGNALS (breaking > timely) ──────────────────────
    for kw in URGENCY_HIGH:
        if kw in combined:
            score += 25
            break

    for kw in URGENCY_MEDIUM:
        if kw in combined:
            score += 15
            break

    # ── LAYER 3: INDONESIAN RELEVANCE (local priority) ────────────────────
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

    # ── LAYER 4: PENALTIES (boring/opinion noise) ─────────────────────────
    for kw in BORING_KEYWORDS:
        if kw in combined:
            score -= 15
            break

    for kw in OPINION_KEYWORDS:
        if kw in combined:
            score -= 20
            break

    # ── VIDEO EXCLUSION (skip video-based articles) ────────────────────────
    for kw in VIDEO_KEYWORDS:
        if kw in title:  # Check title only, not description
            score -= 100
            break

    # ── LAYER 5: VIRAL FACTORS (engagement drivers) ───────────────────────
    viral_count = 0
    for factor, keywords in VIRAL_FACTORS.items():
        for kw in keywords:
            if kw in combined:
                viral_count += 1
                score += 10
                break

    if viral_count >= 3:
        score += 50

    # ── LAYER 6: TITLE QUALITY (short = catchy) ───────────────────────────
    words = article["title"].split()
    if len(words) <= 8:
        score += 15
    elif len(words) > 15:
        score -= 10

    if re.search(r'\d+', article["title"]):
        score += 10

    # ── LAYER 7: ANALYTICS FEEDBACK (adaptive learning) ───────────────────
    score = apply_topic_boost(score, article["title"], feedback)
    score = apply_time_boost(score, feedback)

    return score

def select_best_candidate(articles, posted, feedback, posted_titles=None):
    """Select the best article with feedback boosts + title dedup."""
    scored = []
    skipped_similar = 0

    for article in articles:
        # FIX 1: Title similarity dedup
        if posted_titles and is_similar(article["title"], posted_titles):
            skipped_similar += 1
            continue

        score = score_candidate(article, posted, feedback)
        if score > 0:
            scored.append((score, article))

    if skipped_similar > 0:
        log(f"[DEDUP] Skipped {skipped_similar} similar titles")

    if not scored:
        return None

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_article = scored[0]
    log(f"Best candidate: {best_article['title']} (score: {best_score:.1f})")
    return best_article

# ─── FIX 3: CONTENT EXTRACTION - newspaper3k + fallback ──────────────────────

def extract_article_content(url):
    """Extract article content using 3-method chain.
    
    Method 1: newspaper3k (best quality, 3-5K chars)
    Method 2: curl + <article> tag (BBC, news sites)
    Method 3: curl + all <p> tags (fallback)
    """
    import subprocess

    # Method 1: newspaper3k (best quality)
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

    # Method 2: curl + <article> tag
    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "10",
             "-A", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
             url],
            capture_output=True, text=True, timeout=15
        )
        html_content = result.stdout

        # Try <article> tag first
        article_match = re.search(r'<article[^>]*>(.*?)</article>', html_content, re.DOTALL)
        if article_match:
            article_html = article_match.group(1)
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', article_html, re.DOTALL)
            text = ' '.join([re.sub(r'<[^>]+>', '', p).strip() for p in paragraphs if len(p) > 50])
            if len(text) > 500:
                log(f"[EXTRACT] curl article tag: {len(text)} chars")
                return text[:5000]

        # Method 3: All <p> tags
        paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', html_content, re.DOTALL)
        text = ' '.join([re.sub(r'<[^>]+>', '', p).strip() for p in paragraphs if len(p) > 50])
        if len(text) > 500:
            log(f"[EXTRACT] curl p tags: {len(text)} chars")
            return text[:5000]

        # Final fallback: strip all HTML
        text = re.sub(r'<[^>]+>', ' ', html_content)
        text = re.sub(r'\s+', ' ', text).strip()
        log(f"[EXTRACT] curl fallback: {len(text)} chars")
        return text[:5000]

    except Exception as e:
        log(f"[EXTRACT] curl failed: {e}", "ERROR")
        return ""

# ─── FIX 6: LLM MODEL FALLBACK (deepseek v4-flash → mimo v2.5) ──────────────

def call_llm(system_prompt, user_prompt, model):
    """Call LLM API with system + user prompt split."""
    load_env()
    api_key = os.environ.get("OPENCODE_GO_API_KEY", "")

    if not api_key:
        log("No API key found", "ERROR")
        return None

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": LLM_MAX_TOKENS,
        "temperature": 0.8,
        "reasoning_effort": "low",
        "stream": True
    }

    try:
        r = requests.post(LLM_API_URL, headers=headers, json=payload, timeout=LLM_TIMEOUT, stream=True)

        if r.status_code != 200:
            log(f"LLM API error ({model}): HTTP {r.status_code}", "ERROR")
            return None

        # Process SSE stream
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
                if not choices:  # Handle empty choices (usage chunk)
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

        # Use content first, fallback to reasoning
        final_content = content or reasoning

        if not final_content:
            log(f"Empty LLM response ({model})", "ERROR")
            return None

        log(f"[LLM] Response: content={len(content)}c, reasoning={len(reasoning)}c")
        return final_content

    except Exception as e:
        log(f"LLM error ({model}): {e}", "ERROR")
        return None

def extract_json_from_content(content):
    """Extract JSON from LLM content (handles multiple formats).
    
    Formats:
    1. {"slide_1": "text...", "slide_2": "text..."}  (string format)
    2. {"slide_1": {"hook": "..."}, "slide_2": {"content": "..."}}  (object format)
    3. Wrapped in ```json ... ``` blocks
    """
    # Strip markdown code blocks
    content = re.sub(r'```json\s*', '', content)
    content = re.sub(r'```\s*$', '', content)
    content = content.strip()

    # Find JSON
    json_match = re.search(r'\{[\s\S]*\}', content)
    if not json_match:
        return None

    try:
        data = json.loads(json_match.group())
    except json.JSONDecodeError:
        return None

    # Normalize format: convert strings to objects
    normalized = {}
    for i in range(1, 9):
        key = f"slide_{i}"
        if key in data:
            val = data[key]
            if isinstance(val, str):
                # String format: "slide_1": "text..."
                if i == 1:
                    normalized[key] = {"hook": val, "content": ""}
                else:
                    normalized[key] = {"hook": "", "content": val}
            elif isinstance(val, dict):
                # Object format: "slide_1": {"hook": "...", "content": "..."}
                normalized[key] = val
    
    if len(normalized) >= 6:
        return normalized
    return None

def generate_content(article, article_content):
    """Generate Threads content via LLM with model fallback.
    
    Primary: mimo-v2.5 (fast, good Indonesian)
    Fallback: minimax-m3 (slower but reliable)
    """
    system_prompt = """Output JSON only. Start with {. No preamble.

Extract 8 slides from the article below.

[SLIDES]
slide_1: HOOK — 150-300 chars. WAJIB: angka + konteks + drama.
  ✅ "4.000 pekerja Nike dirumahkan! Di tengah ekonomi goyah, ini baru permulaan."
  ❌ "Bos buruh buka suara soal PHK massal." (tanpa angka)

slide_2: APA YANG TERJADI — 300-500 chars
slide_3: KENAPA PENTING — 300-500 chars
slide_4: SIAPA YANG TERDAMPAK — 300-500 chars
slide_5: SUDUT PANDANG — 300-500 chars
slide_6: DAMPAK LEBIH LUAS — 300-500 chars
slide_7: YANG BELUM JELAS — 300-500 chars
slide_8: OPINI + FAKTA + CTA — 300-500 chars + URL
  WAJIB: akhiri dengan pertanyaan "Menurut lo, gimana...?"

[RULES]
- Bahasa Indonesia sehari-hari, bukan formal
- Newline antar kalimat = \\n\\n dalam JSON string
- Jangan pakai: em dash, hashtag, frasa AI generik
- Tiap slide = info BARU. Jangan ulang fakta dari slide sebelumnya
- Slide 3–7: empati ke orang kecil (pedagang, UMKM, pekerja)
- TOPIC LOCK: satu topik, satu sudut. Jangan campur cerita lain. Jangan tambah info yang tidak ada di artikel.

[JSON SCHEMA]
{"slide_1":"...","slide_2":"...","slide_3":"...","slide_4":"...","slide_5":"...","slide_6":"...","slide_7":"...","slide_8":"..."}"""

    user_prompt = f"""JUDUL: {article['title']}
SUMBER: {article['source']}
URL: {article['url']}

ARTIKEL:
{article_content[:1500]}"""


    # Try models in order (primary → fallback) with hook validation
    MAX_HOOK_RETRIES = 1  # Fail fast
    
    for model in LLM_MODELS:
        for attempt in range(MAX_HOOK_RETRIES):
            log(f"[LLM] Trying model: {model} (attempt {attempt + 1})")
            content = call_llm(system_prompt, user_prompt, model)

            if content:
                save_json(RAW_OUTPUT_FILE, {"raw": content, "model": model, "timestamp": datetime.now().isoformat()})

                slides_data = extract_json_from_content(content)
                if slides_data:
                    # Validate hook
                    hook = slides_data.get("slide_1", {}).get("hook", "")
                    is_valid, issues = validate_hook(hook)
                    
                    if is_valid:
                        log(f"[LLM] ✅ Success with {model} - Hook valid: {hook[:50]}...")
                        return slides_data
                    else:
                        log(f"[LLM] ⚠️ Hook invalid: {', '.join(issues)}", "WARN")
                        if attempt < MAX_HOOK_RETRIES - 1:
                            log(f"[LLM] Retrying with same model...")
                        continue
                else:
                    log(f"[LLM] ❌ JSON parse failed for {model}", "WARN")
                    break  # Move to next model
    
    log("[LLM] ❌ All models failed (hook validation failed or parse error)", "ERROR")
    return None

def add_smart_whitespace(content):
    """Add line breaks after sentences, but NOT after abbreviations like No., Mr., etc."""
    # Protect abbreviations temporarily
    abbreviations = ['No', 'Mr', 'Mrs', 'Dr', 'St', 'vs', 'etc', 'dll']
    protected = content
    for abbr in abbreviations:
        protected = protected.replace(f'{abbr}.', f'{abbr}[[DOT]]')
    
    # Split by: period/exclamation/question mark + space + capital letter
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', protected)
    
    # Restore abbreviations
    restored = [sent.replace('[[DOT]]', '.') for sent in sentences]
    
    # Join with double newlines
    return '\n\n'.join(restored)

def validate_hook(hook):
    """Validate that hook has all 3 required elements: ANGKA + KONTEKST + DRAMA.
    
    Returns: (is_valid, issues)
    """
    issues = []
    
    # Check 1: ANGKA (numbers, percentages, currency, years)
    # More flexible: any number at all
    has_angka = bool(re.search(r'\d+', hook))
    if not has_angka:
        issues.append("GAK ADA ANGKA SPESIFIK")
    
    # Check 2: KONTEKST (recognizable context words)
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
                     # Employment/Labor
                     'buruh', 'pekerja', 'karyawan', 'aryawan', 'tenaga kerja',
                     'lapangan kerja', 'phk', 'pemutusan', 'industri', 'pabrik',
                     'manufaktur', 'produksi', 'ekspor', 'impor']
    has_konteks = any(word.lower() in hook.lower() for word in konteks_words)
    if not has_konteks:
        issues.append("GAK ADA KONTEKST YANG JELAS")
    
    # Check 3: DRAMA (emotional/dramatic words)
    drama_words = ['naik', 'turun', 'anjlok', 'meledak', 'ambruk', 'jatuh', 'rally',
                   'kosong', 'langka', 'mahal', 'murah', 'sesak', 'miskin', 'kaya',
                   'PHK', 'tutup', 'bangkrut', 'gagal', 'guncang', 'terancam',
                   'tapi', 'malah', 'justru', 'tetep', 'terus', 'makin',
                   'hilang', 'ditarik', 'tarik', 'belum', 'gigit', 'was-wada',
                   'kaget', 'terkejut', 'miris', 'menyedihkan']
    has_drama = any(word.lower() in hook.lower() for word in drama_words)
    if not has_drama:
        issues.append("GAK ADA DRAMA/EMOSI")
    
    is_valid = len(issues) == 0
    return is_valid, issues
def format_slides(slides_data):
    """Format slides data into storytelling format with whitespace.
    
    Indonesian Threads style:
    - Slide 1: hook (1 emotional sentence with numbers)
    - Slides 2-8: content (short paragraphs with line breaks)
    - Whitespace after every sentence (but NOT after abbreviations like No., Mr., etc.)
    - No em dashes
    """
    slides = []
    for i in range(1, 9):
        key = f"slide_{i}"
        if key in slides_data:
            slide = slides_data[key]
            
            # Slide 1: hook field
            if i == 1:
                hook = slide.get("hook", "") or slide.get("title", "") or slide.get("content", "")
                # Remove em dashes
                hook = hook.replace('—', ', ').replace('–', ', ')
                slides.append({"hook": hook, "content": ""})
            # Slides 2-8: content field
            else:
                content = slide.get("content", "")
                # Remove em dashes
                content = content.replace('—', ', ').replace('–', ', ')
                # Add smart whitespace
                content = add_smart_whitespace(content)
                slides.append({"hook": "", "content": content})
    return slides

# ─── FIX 2: AUTO-POST TO THREADS ─────────────────────────────────────────────

def post_to_threads(staging_data):
    """Post slides to Threads using Press Box direct-post.py.
    
    Returns: (success, root_id, permalink)
    """
    import subprocess

    if not THREADS_SCRIPT.exists():
        log("[POST] Press Box direct-post.py not found - skipping auto-post", "WARN")
        return False, None, None

    # Format: storytelling style (Indonesian Threads)
    md_content = ""
    for i, slide in enumerate(staging_data['slides'], 1):
        hook = slide.get('hook', '')
        content = slide.get('content', '')
        
        # Slide 1: hook only (1 emotional sentence)
        if i == 1 and hook:
            md_content += f"{hook}\n\n---\n\n"
        # Slides 2-8: content with line breaks
        elif content:
            md_content += f"{content}\n\n---\n\n"

    # Write to temp file
    temp_file = DATA_DIR / "latest.md"
    temp_file.write_text(md_content)

    try:
        # Build command with optional image
        cmd = ["python3", str(THREADS_SCRIPT), "--file", str(temp_file)]
        
        # FIX: Pass image URL for Slide 1
        image_url = staging_data.get("image_url", "")
        if image_url:
            cmd.extend(["--image", image_url])
            log(f"[POST] 📷 Attaching image: {image_url[:60]}...")

        # Increased timeout to 120s (Threads API can be slow)
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120
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
            log(f"[POST] ❌ No root post ID found", "ERROR")
            log(f"[POST] Output: {output[:200]}")
            return False, None, None

    except subprocess.TimeoutExpired:
        log("[POST] ⚠️ Timeout (120s) - Threads API may be slow", "WARN")
        return False, None, None
    except Exception as e:
        log(f"[POST] ❌ Error: {e}", "ERROR")
        return False, None, None

# ─── FIX 5: ANALYTICS FEEDBACK LOOP ──────────────────────────────────────────

def update_analytics(staging_data, root_id=None, permalink=None):
    """Update analytics after posting - track engagement potential."""
    posted = load_json(POSTED_FILE, {})

    # Add to posted topics
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
            "views": 0
        }
    }

    posted[staging_data["url"]] = entry
    save_json(POSTED_FILE, posted)

    # Update title cache for dedup
    title_cache = load_json(TITLE_CACHE_FILE, {"titles": []})
    if staging_data["title"] not in title_cache["titles"]:
        title_cache["titles"].append(staging_data["title"])
        # Keep last 100 titles
        title_cache["titles"] = title_cache["titles"][-100:]
        save_json(TITLE_CACHE_FILE, title_cache)

    log(f"[ANALYTICS] Updated: {staging_data['title'][:50]}...")

# ─── MAIN PIPELINE ───────────────────────────────────────────────────────────

def run_pipeline():
    """Main pipeline execution with all 6 optimizations."""
    log("=== Market Monday Pipeline Started ===")

    # Create data dir
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Load state
    posted = load_json(POSTED_FILE, {})
    posted_urls = set(posted.keys())

    # Load title cache for dedup
    title_cache = load_json(TITLE_CACHE_FILE, {"titles": []})
    posted_titles = title_cache.get("titles", [])

    # Load analytics feedback
    feedback = load_feedback()

    # Scrape all sources (parallel)
    log("Scraping RSS sources...")
    articles = scrape_all_sources()

    if not articles:
        log("No articles found", "WARN")
        print("No articles found")
        sys.exit(2)

    log(f"Total articles scraped: {len(articles)}")

    # Select best candidate (with feedback boosts + title dedup)
    best = select_best_candidate(articles, posted_urls, feedback, posted_titles)

    if not best:
        log("No suitable candidate found", "WARN")
        print("No suitable candidate")
        sys.exit(2)

    # Extract article content (newspaper3k + fallback)
    log(f"Extracting content: {best['url'][:60]}...")
    article_content = extract_article_content(best["url"])

    if not article_content or len(article_content) < 100:
        log("Article content too short or empty", "WARN")
        print("Article content too short")
        sys.exit(2)

    # Generate content via LLM (with model fallback)
    log("Generating content via LLM...")
    slides_data = generate_content(best, article_content)

    if not slides_data:
        log("LLM generation failed", "ERROR")
        alert_telegram("LLM generation failed")
        print("LLM failed")
        sys.exit(1)

    # Format slides
    slides = format_slides(slides_data)

    if len(slides) < 8:
        log(f"Only {len(slides)} slides generated (need 8)", "WARN")

    # Extract image (3-method fallback)
    image_url = extract_image(best['url'])
    if image_url:
        log(f"📷 Image: {image_url[:60]}...")
    else:
        log("No image found")

    # Save to staging
    staging_data = {
        "title": best["title"],
        "url": best["url"],
        "source": best["source"],
        "score": score_candidate(best, posted_urls, feedback),
        "slides": slides,
        "image_url": image_url or "",
        "timestamp": datetime.now().isoformat()
    }

    save_json(STAGING_FILE, staging_data)

    # Save latest as markdown (storytelling format)
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

    # Auto-post to Threads
    if DRY_RUN:
        log("🏃 DRY RUN - skipping post to Threads")
        success = True
        root_id = "dry-run"
        permalink = "dry-run-mode"
    else:
        log("Auto-posting to Threads...")
        success, root_id, permalink = post_to_threads(staging_data)

    # Update analytics
    update_analytics(staging_data, root_id, permalink)

    if success:
        log(f"✅ Pipeline complete! Posted to Threads: {permalink}")
        print(f"✅ Pipeline complete: {best['title']} ({len(slides)} slides)")
        print(f"🔗 {permalink}")
    else:
        log(f"⚠️ Pipeline complete (not posted to Threads)")
        print(f"⚠️ Pipeline complete (staging only): {best['title']} ({len(slides)} slides)")

    return True

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Skip posting to Threads")
    args = parser.parse_args()
    
    DRY_RUN = args.dry_run
    if DRY_RUN:
        log("🏃 DRY RUN MODE - will NOT post to Threads")
    
    try:
        success = run_pipeline()
        sys.exit(0 if success else 1)
    except Exception as e:
        log(f"Pipeline error: {e}", "ERROR")
        alert_telegram(f"Pipeline error: {e}")
        sys.exit(1)
