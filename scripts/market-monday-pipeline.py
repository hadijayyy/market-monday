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
Updated: 18 Jun 2026 — Optimized HTTP actions, error handling & PEP 8 compliance
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
# Model routes — each model maps to its own API URL + key env var
# Primary: mistral-large-latest (Mistral direct, fast, 8.5/10 ID+EN)
# Fallback: MiniMax-M3 (tokenrouter, free, slow ~2 min — bumped LLM_TIMEOUT to 180s)
MODEL_ROUTES = {
    "mistral-large-latest": ("https://api.mistral.ai/v1/chat/completions", "MISTRAL_API_KEY"),
    "MiniMax-M3": ("https://api.tokenrouter.com/v1/chat/completions", "MINIMAX_API_KEY"),
}
# Primary → fallback chain (order matters — first success wins)
LLM_MODELS = ["mistral-large-latest", "MiniMax-M3"]
DRY_RUN = False
FORCE_MODEL = None
LLM_MAX_TOKENS = 10000  # bumped 6000→10000 — M3 verbose responses (~24k chars) were truncating mid-thought
LLM_TIMEOUT = 180  # bumped 90→180s to cover M3 fallback (~2 min/call per skill note)

# SIMILARITY
SIMILARITY_THRESHOLD = 0.35

# THREADS
THREADS_SCRIPT = SCRIPTS_DIR / "pressbox-direct-post.py"

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

# ─── SCORING KEYWORDS ────────────────────────────────────────────────────────
IMPACT_CRASH = {
    "crash", "crisis", "recession", "collapse", "plunge", "default",
    "bankruptcy", "layoff", "layoffs", "unemployment", "emergency",
    "panic", "meltdown", "turmoil", "slump", "downturn", "insolvency", "implosion",
    "anjlok", "ambruk", "jatuh", "gagal", "bangkrut", "phk", "resesi",
    "krisis", "darurat", "panik", "merosot", "menurun"
}

IMPACT_SURGE = {
    "surge", "rally", "soar", "boom", "breakthrough", "record",
    "historic", "milestone", "all-time high", "first time", "skyrocket",
    "outperform", "beat expectations", "strongest",
    "kenaikan", "meningkat", "melambung", "meroket", "menembus",
    "tembus", "rekor", "tertinggi", "terbesar", "pertama", "berhasil",
    "positif", "optimistis", "pulih", "rebound"
}

IMPACT_NEGATIVE = {
    "warning", "downgrade", "cut", "reduce", "slowdown", "weaken",
    "decline", "drop", "fall", "tumble", "sink", "miss", "miss expectations",
    "peringatan", "potong", "kurang", "perlambatan", "melemah",
    "penurunan", "merosot", "gagal", "meleset"
}

URGENCY_HIGH = {
    "breaking", "just in", "alert", "emergency", "urgent", "flash",
    "terbaru", "baru", "mendesak", "darurat", "segera", "breaking news"
}

URGENCY_MEDIUM = {
    "today", "this week", "imminent", "announce", "reveals", "expects",
    "hari ini", "minggu ini", "akan datang", "mengumumkan", "menguak",
    "memprediksi", "estimasi", "proyeksi"
}

INDO_HIGH = {
    "rupiah", "idr", "bi rate", "bank indonesia", "suku bunga",
    "ihsg", "idx", "ojk", "beban", "emiten", "saham indonesia",
    "jakarta", "indonesia", "pemerintah", "kementerian"
}

INDO_MEDIUM = {
    "komoditas", "batu bara", "nikel", "cpo", "kelapa sawit",
    "tembaga", "emas", "minyak mentah", "lng", "palm oil"
}

INDO_LOW = {
    "asia", "asean", "singapore", "malaysia", "thailand", "vietnam",
    "philippines", "china", "jepang", "korea"
}

BORING_KEYWORDS = {
    "quarterly report", "earnings preview", "market open", "market close",
    "trading update", "dividend announcement", "annual report",
    "regulatory filing", "proxy statement"
}

OPINION_KEYWORDS = {
    "opinion", "analysis", "column", "editorial", "commentary",
    "perspective", "viewpoint", "says analyst", "expert says"
}

VIDEO_KEYWORDS = {
    "video", "watch", "nonton", "tonton", "footage", "clip",
    "livestream", "live streaming", "replay"
}

PROMO_KEYWORDS = {
    "promo", "diskon", "cashback", "gratis", "free", "hadiah", "reward",
    "pameran", "expo", "fair", "festival", "event",
    "kunjungi", "datang ke", "hadir di", "acara",
    "berlangsung", "digelar", "diselenggarakan",
    "tiket", "registrasi", "daftar sekarang", "booking",
    "limited", "terbatas", "kuota", "slot",
    "voucher", "kupon", "bonus",
}

# Finance-Specific Sensitive Keywords (Threads/Meta Policy)
# Based on actual Threads community guidelines and Meta policies
# These keywords are associated with content that violates financial regulations
SENSITIVE_KEYWORDS = {
    # 1. FINANCIAL SCAMS & FRAUD (HIGH RISK)
    # These keywords are associated with content that can cause financial harm
    "jaminan untung", "pasti untung", "tanpa risiko",
    "kaya mendadak", "penghasilan pasif 100%", "gandakan uang",
    "untung 100% per hari", "tidak akan rugi", "rahasia sukses",
    "kesempatan eksklusif",
    
    "guaranteed returns", "risk-free investment", "get rich quick",
    "passive income 100%", "double your money", "100% profit daily",
    "no risk", "can't lose", "secret formula", "exclusive opportunity",
    
    # 2. UNREGULATED FINANCIAL SERVICES (MEDIUM RISK)
    # These keywords violate financial regulations
    "binary option", "sinyal forex", "pompa kripto",
    "tips orang dalam", "tips saham dijamin", "leverage 100x",
    "tanpa verifikasi", "perdagangan anonim", "investasi lepas pantai",
    "return bebas pajak",
    
    "binary options", "forex signals", "crypto pump",
    "insider tips", "stock tips guaranteed", "100x leverage",
    "no verification needed", "anonymous trading", "offshore investment",
    "tax-free returns",
    
    # 3. DEBT & LENDING SCAMS (MEDIUM RISK)
    # These keywords are associated with predatory lending
    "pinjaman instan", "tanpa cek kredit", "uang tunai darurat",
    "pinjaman gaji", "penarikan tunai", "konsolidasi hutang scam",
    "perbaikan kredit", "hindari kebangkrutan", "kebebasan keuangan cepat",
    "bebas hutang cepat",
    
    "instant loan", "no credit check", "emergency cash",
    "payday loan", "cash advance", "debt consolidation scam",
    "credit repair", "bankruptcy avoidance", "financial freedom fast",
    "debt-free quick",
    
    # 4. MARKET MANIPULATION (HIGH RISK)
    # These keywords are associated with market manipulation schemes
    "pump and dump", "pompa dan buang",
    "market manipulation", "manipulasi pasar",
    "insider trading", "dagang orang dalam",
    
    "pump and dump", "market manipulation", "insider trading",
    "wash trading", "front running", "spoofing",
    
    # 5. PREDATORY FINANCIAL SERVICES (MEDIUM RISK)
    # These keywords are associated with predatory lending
    "pinjaman ilegal", "rentenir", "lintah darat",
    "debt collector kasar", "penagih hutang kasar",
    "debt collector pukul", "debt collector ancam",
    "pinjaman online ilegal", "pinjol ilegal",
    "predatory lending", "pinjaman predatori",
    "skyrocketing interest", "bunga mencekik",
}

VIRAL_FACTORS = {
    "outrage_money": ["price", "cost", "debt", "money", "tax", "billion", "trillion", "rp", "harga", "biaya", "utang", "pajak"],
    "human_story": ["worker", "family", "household", "consumer", "employee", "pekerja", "keluarga", "rumah tangga", "konsumen"],
    "controversy": ["ban", "scandal", "fraud", "corruption", "protest", "korupsi", "skandal", "penipuan"],
    "record_milestone": ["record", "history", "milestone", "first ever", "highest", "terbesar", "tertinggi", "pertama"],
    "geopolitical": ["war", "conflict", "sanction", "tariff", "ban", "perang", "konflik", "sanksi"]
}

# Controversy / Drama / Clickbait keywords — boost score
CONTROVERSY_KEYWORDS = {
    "PHK", "PHK massal", "pemutusan hubungan kerja", "dirumahkan",
    "bangkrut", "gulung tikar", "kolaps", "default", "gagal bayar",
    "skandal", "korupsi", "suap", "gratifikasi", "nepotisme",
    "manipulasi", "kecurangan", "penipuan", "fraud",
    "protes", "demo", "unjuk rasa", "buruh mogok",
    "bocor", "bocoran", "kebocoran", "temuan",
    "kontroversi", "polemik", "heboh", "ramai",
    "viral", "terkenal", "famous",
    "sengketa", "gugatan", "class action",
    "merugikan", "kerugian", "rugi", "merugi",
}

DRAMA_KEYWORDS = {
    "tiba-tiba", "mendadak", "dikagetkan", "mengejutkan", "terkejut",
    "miris", "menyedihkan", "kasihan", "prihatin", "memprihatinkan",
    "drama", "kisah", "cerita", "pengakuan", "gestur",
    "gebrakan", "kejutan", "blunder", "skandal",
    "berani", "bantah", "tanggapi", "buka suara", "angkat bicara",
    "tuding", "tuduhan", "salahkan", "kritik", "cam",
    "makin", "semakin", "terus", "banjir", "membanjiri",
    "muncul", "terungkap", "terbongkar", "terkuak",
    "larang", "hentikan", "cabut", "batalkan",
    "dilarang", "ditangkap", "diamankan", "dibekukan",
    "anjlok", "merosot", "jatuh", "ambruk", "runtuh",
    "terpuruk", "terperosok", "gulung tikar", "bangkrut",
    "phk", "dirumahkan", "hentikan operasi",
    "gigit", "was-was", "cemas", "khawatir",
}

CLICKBAIT_KEYWORDS = {
    "bikin iri", "bikin penasaran", "ternyata", "rahasia",
    "mengungkap", "mengintip", "bongkar", "sorot",
    "viral", "heboh", "dibahas", "ramai diperbincangkan",
    "beredar", "beredar luas", "masif", "viral di media sosial",
    "terkenal", "famous", "populer",
    "tak terduga", "tak disangka", "di luar dugaan",
    "mengejutkan", "mencengangkan", "luar biasa",
    "parah", "mengerikan", "ngeri", "mencengangkan",
    "sensasional", "kontroversial", "penuh drama",
    "terungkap", "terbongkar", "terkuak",
    "muncul", "mencuat", "meledak",
    "langsung", "tiba-tiba", "mendadak", "dikagetkan",
    "rahasia", "tersembunyi", "tertutup",
    "paling", "ter", "sekali", "banget",
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
    """Check if article is within freshness window."""
    if not pub_date_str:
        return True
    try:
        from email.utils import parsedate_to_datetime
        pub_date = parsedate_to_datetime(pub_date_str)
        now = datetime.now(timezone.utc)
        age = now - pub_date
        return age.total_seconds() < hours * 3600
    except Exception as e:
        log(f"Date parse error: {e}", "WARN")
        return True

def score_candidate(article, posted, feedback):
    """Score article with 7-layer scoring system."""
    title = article["title"].lower()
    desc = article["description"].lower()
    combined = f"{title} {desc}"

    if article["url"] in posted:
        return -1000

    if not is_fresh(article.get("published", ""), hours=24):
        return -500

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
        "anomali", "manipulasi", "korupsi", "skandal", "penipuan", "gelap"
    ]
    
    has_economic_keyword = any(kw in combined for kw in ECONOMIC_KEYWORDS)
    if not has_economic_keyword:
        return -200

    # SARA & Sensitive content — HEAVY PENALTY (skip entirely)
    has_sensitive = any(kw.lower() in combined for kw in SENSITIVE_KEYWORDS)
    if has_sensitive:
        log(f"[SCORING] ❌ SENSITIVE content detected: {article['title'][:60]}...", "WARN")
        return -300

    score = 0
    matched_keywords = set()

    for kw in IMPACT_CRASH:
        if kw in combined and kw not in matched_keywords:
            score += 30
            matched_keywords.add(kw)
            break

    for kw in IMPACT_SURGE:
        if kw in combined and kw not in matched_keywords:
            score += 25
            matched_keywords.add(kw)
            break

    for kw in IMPACT_NEGATIVE:
        if kw in combined and kw not in matched_keywords:
            score += 20
            matched_keywords.add(kw)
            break

    for kw in URGENCY_HIGH:
        if kw in combined and kw not in matched_keywords:
            score += 25
            matched_keywords.add(kw)
            break

    for kw in URGENCY_MEDIUM:
        if kw in combined and kw not in matched_keywords:
            score += 15
            matched_keywords.add(kw)
            break

    for kw in INDO_HIGH:
        if kw in combined and kw not in matched_keywords:
            score += 40
            matched_keywords.add(kw)
            break

    for kw in INDO_MEDIUM:
        if kw in combined and kw not in matched_keywords:
            score += 25
            matched_keywords.add(kw)
            break

    for kw in INDO_LOW:
        if kw in combined and kw not in matched_keywords:
            score += 15
            matched_keywords.add(kw)
            break

    for kw in BORING_KEYWORDS:
        if kw in combined and kw not in matched_keywords:
            score -= 15
            matched_keywords.add(kw)
            break

    for kw in OPINION_KEYWORDS:
        if kw in combined and kw not in matched_keywords:
            score -= 20
            matched_keywords.add(kw)
            break

    for kw in VIDEO_KEYWORDS:
        if kw in title and kw not in matched_keywords:
            score -= 100
            matched_keywords.add(kw)
            break

    for kw in PROMO_KEYWORDS:
        if kw in combined and kw not in matched_keywords:
            score -= 50
            matched_keywords.add(kw)
            break

    # Controversy boost
    for kw in CONTROVERSY_KEYWORDS:
        if kw.lower() in combined and kw.lower() not in matched_keywords:
            score += 20
            matched_keywords.add(kw.lower())
            break

    # Drama boost
    for kw in DRAMA_KEYWORDS:
        if kw.lower() in combined and kw.lower() not in matched_keywords:
            score += 15
            matched_keywords.add(kw.lower())
            break

    # Clickbait boost
    for kw in CLICKBAIT_KEYWORDS:
        if kw.lower() in combined and kw.lower() not in matched_keywords:
            score += 10
            matched_keywords.add(kw.lower())
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
    """Select the best article with feedback boosts + title dedup."""
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
        return None, None

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_article = scored[0]
    log(f"Best candidate: {best_article['title']} (score: {best_score:.1f})")
    return best_article, best_score

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

    try:
        r = requests.post(api_url, headers=headers, json=payload, timeout=LLM_TIMEOUT, stream=True)
        
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
    for i in range(1, 8):
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
    system_prompt = """# ROLE
Content writer ekonomi pasar Indonesia. Nada: langsung, jujur, empati ke orang kecil.

# TASK
Ubah artikel jadi 7 slide Threads. JSON output.

# SLIDES (WAJIB saling terhubung, setiap slide build on previous)
1. Hook (2-3): STOP SCROLL. Gunakan salah satu teknik:
   - Provokasi: "Lo masih [X]? Mulai [Y], cuma bisa [Z]."
   - Kontradiksi: "[Fakta mengejutkan] — ternyata [alasan]"
   - Urgensi: "Mulai [tanggal], lo wajib [X]."
   JANGAN sekadar menyampaikan fakta. Bikin penasaran.

2. Apa yang Berubah (3-4): Fakta utama. APA yang berubah, SIAPA yang umumkan, KAPAN berlaku. Singkat, to the point.

3. Kenapa Ini Terjadi (3-4): Alasan di balik kebijakan. konteks sebelumnya + angka artikel. Jangan ulang slide 2.

4. Siapa yang Terdampak (3-4): SPESIFIK. "Lo yang [aktivitas spesifik], wajib [aksi]."

5. Fakta Mengejutkan (3-4): Angka/ data yang jarang disorot. "Jarang dibahas, tapi..."

6. Prediksi (3-4): "Kalau tren ini berlanjut..." — apa yang bakal terjadi. Inferensi logis.

7. Hot Take+CTA (2-3): Opini kontroversial/tajam. Tutup dengan "Menurut lo, [pertanyaan]?". Sertakan URL.

# RULES
- Slide 1-5: HANYA fakta artikel. WAJIB, BUKAN soft guideline.
  - JANGAN sebut angka, nama, tanggal, atau fakta yang TIDAK ADA di artikel.
  - Contoh SALAH: Artikel bilang "kerugian bank" → lo tulis "Rp 5 miliar" ← HALUSINASI.
  - Contoh BENAR: Artikel bilang "kerugian bank" → lo tulis "kerugian bank" atau "angka detail belum diumumkan".
  - Kalau info kurang lengkap, tulis "belum diumumkan" — JANGAN mengarang angka.
- Slide 6: inferensi logis, flag prediksi.
- Slide 7: opini + empati personal dibolehkan.
- Setiap slide HARUS connect ke slide sebelumnya. Ga boleh disjointed.
- Bahasa: Indonesia gaul kredibel. "Lo/gue" sparingly.
- Line break: gunakan \n\n antar kalimat.
- Dilarang: em dash (—), hashtag, frasa kosong, kalimat klise.
- Jangan sebut "slide" di konten.

# OUTPUT
Kembalikan HANYA JSON valid:
{"slide_1":"...","slide_2":"...","slide_3":"...","slide_4":"...","slide_5":"...","slide_6":"...","slide_7":"..."}
Tanpa teks sebelum/sesudah JSON."""

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
                    slides_data = extract_json_from_content(content)
                if not slides_data and reasoning:
                    log("[LLM] Content empty, extracting from reasoning...")
                    slides_data = extract_json_from_reasoning(reasoning, content)
                
                if slides_data:
                    hook = slides_data.get("slide_1", {}).get("hook", "") or slides_data.get("slide_1", {}).get("content", "")
                    is_valid, issues = validate_hook(hook)
                    
                    if is_valid:
                        # Normalize sentence counts (trim to max instead of rejecting)
                        slides_data, norm_changes = normalize_slide_sentences(slides_data)
                        if norm_changes:
                            log(f"[LLM] ✂️ Normalized: {'; '.join(norm_changes)}", "INFO")

                        # Add \\n\\n between every sentence (mobile readability on Threads)
                        ws_changes = 0
                        for i in range(1, 8):
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

    # Extract numbers (skip year-only and URL-like long sequences)
    numbers = set()
    for m in re.finditer(r'\d[\d.,]*\d|\d+', content):
        n = m.group()
        if n in {str(y) for y in range(2020, 2031)}:
            continue
        if len(n.replace('.', '').replace(',', '')) > 6:
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
    """Validate that hook has at least 2 of 3 elements: ANGKA + KONTEKS + DRAMA.
    
    ANGKA is optional — articles without numbers can still pass if KONTEKS + DRAMA are present.
    """
    issues = []
    
    has_angka = bool(re.search(r'\d+', hook))
    
    konteks_words = [
        'gaji', 'harga', 'sembako', 'BBM', 'rumah', 'IHSG', 'saham', 'investasi', 
        'properti', 'KPR', 'cicilan', 'pangan', 'beras', 'minyak', 'energi',
        'ekonomi', 'pasar', 'defisit', 'inflasi', 'suku bunga', 'BI rate',
        'ekspor', 'impor', 'neraca', 'komoditas', 'kripto', 'dollar', 'rupiah',
        'buruh', 'pekerja', 'karyawan', 'phk', 'industri', 'pabrik', 'umkm', 'usaha',
        'petani', 'pertanian', 'cabai', 'tanaman', 'panen',
        'asuransi', 'bank', 'pinjam', 'kredit', 'aset', 'dana', 'modal',
        'reksadana', 'obligasi', 'deposito', 'tabungan', 'kas',
        'pajak', 'regulasi', 'kebijakan', 'apbn', 'apbd',
        'IKN', 'ibu kota', 'nusantara', 'infrastruktur', 'pembangunan', 'proyek',
        'gedung', 'kota', 'pusat kota', 'cerdas', 'smart city', 'teknologi',
        'operasional', 'beroperasi', 'siap', 'dibangun', 'konstruksi',
        'utang', 'surat utang', 'bond', 'investasi asing',
        'RRC', 'China', 'asing', 'global', 'dunia', 'negara',
    ]
    has_konteks = any(word.lower() in hook.lower() for word in konteks_words)
    
    drama_words = [
        'naik', 'turun', 'anjlok', 'meledak', 'ambruk', 'jatuh', 'rally',
        'kosong', 'langka', 'mahal', 'murah', 'phk', 'bangkrut', 'gagal',
        'krisis', 'merugi', 'rugi', 'terpuruk', 'sengsara', 'kolaps', 'viral',
        'antre', 'antrean', 'berdesakan', 'desak', 'rebutan', 'berebut',
        'rela', 'berjuang', 'perjuangan', 'struggle',
        'miris', 'menyedihkan', 'kasihan', 'prihatin',
        'guncang', 'terancam', 'ancaman', 'bahaya', 'risiko',
        'panik', 'ketakutan', 'takut', 'khawatir', 'cemas',
        'heboh', 'ramai', 'polemik', 'kontroversi', 'sorot',
        'gebrakan', 'kejutan', 'terkejut', 'kaget',
        'darurat', 'emergency',
        'tutup', 'hentikan', 'berhenti', 'stop',
        'hilang', 'lenyap', 'tammat', 'berakhir',
        'miskin', 'kaya', 'semakin', 'makin',
        'gigit', 'was-was',
        'buka suara', 'angkat bicara', 'tanggapi', 'bantah',
    ]
    has_drama = any(word.lower() in hook.lower() for word in drama_words)
    
    # Count how many elements are present
    elements_present = sum([has_angka, has_konteks, has_drama])
    
    if elements_present < 1:
        if not has_angka:
            issues.append("GAK ADA ANGKA SPESIFIK")
        if not has_konteks:
            issues.append("GAK ADA KONTEKS YANG JELAS")
        if not has_drama:
            issues.append("GAK ADA DRAMA/EMOSI")
    
    return len(issues) == 0, issues

def count_sentences(text):
    """Count sentences in text (skips short fragments < 5 chars)."""
    if not text:
        return 0
    sentences = re.split(r'(?<=[.!?])\s+', text)
    return len([s for s in sentences if s.strip() and len(s.strip()) > 5])

def normalize_slide_sentences(slides_data, min_hook=2, max_hook=4, min_body=2, max_body=5):
    """Normalize slide sentence counts to fit within bounds (no reject — auto-fix).

    Per user spec (21 Jun 2026):
      - Slide 1 (hook): 2-4 sentences
      - Slides 2-7 (body): 2-5 sentences

    Behavior:
      - Over max → trim to first N sentences (keep first, drop rest)
      - Under min → pass through, log warning (padding risks fabrication)
    Returns: (normalized_slides_data, list_of_changes)
    """
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

    # Slide 1 (hook): max 4
    slide1 = slides_data.get('slide_1', {})
    if isinstance(slide1, dict):
        text1 = slide1.get('hook', '') or slide1.get('content', '')
        if text1:
            s1 = count_sentences(text1)
            if s1 > max_hook:
                trimmed = trim_text(text1, max_hook)
                if 'hook' in slide1 and slide1['hook']:
                    slide1['hook'] = trimmed
                else:
                    slide1['content'] = trimmed
            elif s1 < min_hook:
                changes.append(f"slide_1 under min ({s1}<{min_hook})")

    # Slides 2-7 (body): max 5
    for i in range(2, 8):
        slide = slides_data.get(f'slide_{i}', {})
        if not isinstance(slide, dict):
            continue
        text = slide.get('content', '') or slide.get('hook', '')
        if not text:
            continue
        s = count_sentences(text)
        if s > max_body:
            trimmed = trim_text(text, max_body)
            if 'content' in slide and slide['content']:
                slide['content'] = trimmed
            else:
                slide['hook'] = trimmed
        elif s < min_body:
            changes.append(f"slide_{i} under min ({s}<{min_body})")

    return slides_data, changes


def validate_slide_sentences(slides_data):
    """Validate sentence counts per slide (+1 tolerance)."""
    issues = []
    
    slide1 = slides_data.get('slide_1', {})
    if isinstance(slide1, dict):
        text1 = slide1.get('hook', '') or slide1.get('content', '')
    else:
        text1 = str(slide1)
    s1 = count_sentences(text1)
    if not (2 <= s1 <= 4):
        issues.append(f"slide_1: {s1} sentences (need 2-4)")
    
    for i in range(2, 7):
        slide = slides_data.get(f'slide_{i}', {})
        if isinstance(slide, dict):
            text = slide.get('content', '') or slide.get('hook', '')
        else:
            text = str(slide)
        s_count = count_sentences(text)
        if not (2 <= s_count <= 5):
            issues.append(f"slide_{i}: {s_count} sentences (need 2-5)")
    
    slide7 = slides_data.get('slide_7', {})
    if isinstance(slide7, dict):
        text7 = slide7.get('content', '') or slide7.get('hook', '')
    else:
        text7 = str(slide7)
    s7 = count_sentences(text7)
    if not (2 <= s7 <= 5):
        issues.append(f"slide_7: {s7} sentences (need 2-5)")
    
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
    
    for i in range(1, 8):
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
            if len(clean_num) > 6:
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
    """Format slides data into storytelling format with whitespace."""
    slides = []
    for i in range(1, 8):
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
    import subprocess

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

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        output = result.stdout
        root_id, permalink = None, None

        for line in output.split('\n'):
            if line.startswith('Root:'):
                root_id = line.split('Root:')[1].strip()
            elif line.startswith('Post:'):
                permalink = line.split('Post:')[1].strip()

        if root_id:
            log(f"[POST] ✅ Posted to Threads: {permalink}")
            return True, root_id, permalink
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
    """Update analytics data store after a post execution."""
    posted = load_json(POSTED_FILE, {})
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
    save_json(POSTED_FILE, posted)

    title_cache = load_json(TITLE_CACHE_FILE, {"titles": []})
    if staging_data["title"] not in title_cache["titles"]:
        title_cache["titles"].append(staging_data["title"])
        title_cache["titles"] = title_cache["titles"][-100:]
        save_json(TITLE_CACHE_FILE, title_cache)

    log(f"[ANALYTICS] Updated cache for: {staging_data['title'][:50]}...")

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
    best, best_score = select_best_candidate(articles, posted_urls, feedback, posted_titles)

    if not best:
        log("No eligible fresh content matches scoring thresholds.", "WARN")
        return False

    article_content = extract_article_content(best["url"])
    if len(article_content) < 100:
        log("Extraction results returned sub-par lengths.", "WARN")
        return False

    slides_data = generate_content(best, article_content)
    if not slides_data:
        alert_telegram("LLM core validation failures occurred.")
        return False

    slides = format_slides(slides_data)
    image_url = extract_image(best['url'])

    staging_data = {
        "title": best["title"],
        "url": best["url"],
        "source": best["source"],
        "score": best_score,
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
