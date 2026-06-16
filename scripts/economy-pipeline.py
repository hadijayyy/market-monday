#!/usr/bin/env python3
"""
ECONOMY PIPELINE — Threads Personal Branding Automation
Architecture reference: Press Box v4 (parkthebus)

Scrapes economics/market news from BBC, CNBC, Reuters, AP News
→ Scores candidates (controversy > news)
→ Generates content via LLM (mimo-v2.5)
→ Outputs to staging.json for posting

Author: Hadijayyy
Created: 17 Jun 2026
"""

import os
import sys
import json
import time
import hashlib
import subprocess
import html
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ─── CONFIG ──────────────────────────────────────────────────────────────────
DATA_DIR = Path.home() / ".hermes" / "economy"
SCRIPTS_DIR = Path.home() / ".hermes" / "scripts"
ENV_FILE = Path.home() / ".hermes" / ".env"
POST_SCRIPT = SCRIPTS_DIR / "economy-post.py"
DIRECT_POST_SCRIPT = SCRIPTS_DIR / "pressbox-direct-post.py"

STAGING_FILE = DATA_DIR / "staging.json"
POSTED_FILE = DATA_DIR / "posted_topics.json"
CACHE_FILE = DATA_DIR / "scrape_cache.json"
RAW_OUTPUT_FILE = DATA_DIR / "raw_llm_output.txt"
LATEST_FILE = DATA_DIR / "latest.md"

# RSS SOURCES — Economics/Market focus
RSS_SOURCES = [
    {
        "name": "BBC Business",
        "url": "https://feeds.bbci.co.uk/news/business/rss.xml",
        "type": "rss"
    },
    {
        "name": "BBC World",
        "url": "https://feeds.bbci.co.uk/news/world/rss.xml",
        "type": "rss"
    },
    {
        "name": "CNBC",
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
        "type": "rss"
    },
    {
        "name": "AP News",
        "url": "https://rsshub.app/apnews/topics/business",
        "type": "rss"
    },
]

# LLM CONFIG
LLM_API_URL = "https://opencode.ai/zen/go/v1/chat/completions"
LLM_MODEL = "mimo-v2.5"
LLM_MAX_TOKENS = 6000
LLM_TIMEOUT = 90

# SCORING — Economics/Market specific
CONTROVERSY_KEYWORDS = [
    "crisis", "crash", "recession", "inflation", "scandal", "corrupt",
    "fraud", "collapse", "plunge", "surge", "panic", "chaos", "warning",
    "emergency", "bankruptcy", "default", "layoff", "unemployment",
    "outage", "shutdown", "ban", "tariff", "sanction", "war", "conflict"
]

BOOMING_KEYWORDS = [
    "boom", "record", "surge", "rally", "soar", "breakthrough",
    "historic", "milestone", "first time", "never before", "all-time high"
]

BORING_KEYWORDS = [
    "quarterly report", "earnings preview", "market open", "market close",
    "trading update", "dividend announcement", "stock split"
]

VIRAL_FACTORS = {
    "outrage_money": ["price", "cost", "debt", "money", "pay", "tax", "billion", "million", "$", "trillion"],
    "human_story": ["worker", "family", "household", "consumer", "employee", "jobless"],
    "controversy": ["ban", "scandal", "fraud", "corruption", "protest", "boycott"],
    "record_milestone": ["record", "history", "milestone", "first ever", "highest", "lowest"]
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
    """Log to stderr (cron captures stdout only)."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", file=sys.stderr)

def alert_telegram(msg):
    """Send alert to Telegram."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("ALERT_CHAT", "")
    if token and chat_id:
        try:
            subprocess.run([
                "curl", "-s", "-X", "POST",
                f"https://api.telegram.org/bot{token}/sendMessage",
                "-d", f"chat_id={chat_id}",
                "-d", f"text=🤖 Economy Pipeline: {msg}",
                "-d", "parse_mode=HTML"
            ], timeout=10, capture_output=True)
        except:
            pass

# ─── RSS SCRAPING ────────────────────────────────────────────────────────────

def scrape_rss(url, source_name):
    """Scrape RSS feed and return list of articles."""
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
        # Simple XML parsing for RSS items
        items = re.findall(r'<item>(.*?)</item>', content, re.DOTALL)

        for item in items[:10]:  # Top 10 per source
            title_match = re.search(r'<title[^>]*>(.*?)</title>', item, re.DOTALL)
            link_match = re.search(r'<link[^>]*>(.*?)</link>', item, re.DOTALL)
            desc_match = re.search(r'<description[^>]*>(.*?)</description>', item, re.DOTALL)
            pub_match = re.search(r'<pubDate[^>]*>(.*?)</pubDate>', item, re.DOTALL)

            if title_match and link_match:
                title = html.unescape(title_match.group(1).strip())
                # Clean CDATA
                title = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', title)
                title = re.sub(r'<[^>]+>', '', title)  # Strip HTML tags

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
        log(f"RSS error: {source_name} — {e}", "WARN")

    return articles

def scrape_all_sources():
    """Scrape all RSS sources and return combined articles."""
    all_articles = []
    for source in RSS_SOURCES:
        articles = scrape_rss(source["url"], source["name"])
        all_articles.extend(articles)
        time.sleep(1)  # Rate limit
    return all_articles

# ─── SCORING ─────────────────────────────────────────────────────────────────

def is_fresh(pub_date_str, hours=24):
    """Check if article is within freshness window."""
    if not pub_date_str:
        return True  # Include if no date (don't exclude)
    try:
        # Try parsing common RSS date formats
        from email.utils import parsedate_to_datetime
        pub_date = parsedate_to_datetime(pub_date_str)
        now = datetime.now(timezone.utc)
        age = now - pub_date
        return age.total_seconds() < hours * 3600
    except:
        return True  # Include if can't parse

def is_already_posted(url, posted):
    """Check if URL was already posted."""
    return url in posted

def score_candidate(article, posted):
    """Score article for content potential. Higher = better."""
    title = article["title"].lower()
    desc = article["description"].lower()
    combined = f"{title} {desc}"

    # Skip if already posted
    if is_already_posted(article["url"], posted):
        return -1000

    # Skip if not fresh
    if not is_fresh(article.get("published", ""), hours=24):
        return -500

    score = 0

    # Controversy keywords (HIGH boost)
    for kw in CONTROVERSY_KEYWORDS:
        if kw in combined:
            score += 30

    # Boom keywords
    for kw in BOOMING_KEYWORDS:
        if kw in combined:
            score += 20

    # Boring keywords (PENALTY)
    for kw in BORING_KEYWORDS:
        if kw in combined:
            score -= 15

    # Viral factors
    viral_count = 0
    for factor, keywords in VIRAL_FACTORS.items():
        for kw in keywords:
            if kw in combined:
                viral_count += 1
                score += 10
                break

    # Viral bonus (3+ factors)
    if viral_count >= 3:
        score += 50

    # Title length (short = punchy)
    words = article["title"].split()
    if len(words) <= 8:
        score += 15
    elif len(words) > 15:
        score -= 10

    # Numbers in title (data-driven)
    if re.search(r'\d+', article["title"]):
        score += 10

    return score

def select_best_candidate(articles, posted):
    """Select the best article for content generation."""
    scored = []
    for article in articles:
        score = score_candidate(article, posted)
        if score > 0:
            scored.append((score, article))

    if not scored:
        return None

    # Sort by score (highest first)
    scored.sort(key=lambda x: x[0], reverse=True)

    # Return top candidate
    best_score, best_article = scored[0]
    log(f"Best candidate: {best_article['title']} (score: {best_score})")
    return best_article

# ─── CONTENT GENERATION ──────────────────────────────────────────────────────

def extract_article_content(url):
    """Extract article content via newspaper3k."""
    try:
        import newspaper
        article = newspaper.Article(url)
        article.download()
        article.parse()
        return article.text[:5000]  # Limit to 5000 chars
    except:
        # Fallback to curl
        try:
            result = subprocess.run(
                ["curl", "-sL", "--max-time", "10",
                 "-A", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                 url],
                capture_output=True, text=True, timeout=15
            )
            # Basic text extraction
            text = re.sub(r'<[^>]+>', ' ', result.stdout)
            text = re.sub(r'\s+', ' ', text).strip()
            return text[:5000]
        except:
            return ""

def generate_content(article, article_content):
    """Generate Threads content via LLM."""
    import requests
    load_env()
    api_key = os.environ.get("OPENCODE_GO_API_KEY", "")

    if not api_key:
        log("No API key found", "ERROR")
        return None

    prompt = f"""You are a professional Indonesian financial analyst writing for Threads (Instagram text-based social media).

Write a thread (8 slides) about this economics/market news. Target audience: Indonesian professionals aged 28-45 with middle-to-upper income.

ARTICLE TITLE: {article['title']}
SOURCE: {article['source']}
ARTICLE URL: {article['url']}

ARTICLE CONTENT:
{article_content[:3000]}

CRITICAL RULES:
1. Write in BAHASA INDONESIA (casual-professional tone)
2. Slide 1 (HOOK): Must be OUTRAGE or SHOCK. Use numbers. Max 8 words.
3. Slides 2-7: Story arc (Problem → Context → Comparison → Human → Big Picture → Stakes)
4. Slide 8: Provocative question with personal word ("kamu", "kita", "lo"). End with URL.
5. Every slide: 3-4 sentences, 50-70 words.
6. Connect to INDONESIAN CONTEXT (daya beli, BI, Rupiah, etc.)
7. NO HALLUCINATION — only use facts from the article.

HOOK FORMAT (Slide 1):
- EXACTLY TWO fragments separated by period
- Fragment 1: [NUMBER] + [CONTEXT] → "Rp 500 Triliun"
- Fragment 2: [NUMBER/YEAR] + [DRAMATIC WORD] → "Krisis 2026"
- NO full sentences. NO questions. MAX 8 words.

CTA FORMAT (Slide 8):
- Title: Provocative question with "kamu"/"kita"/"lo"
- Content: 3 sentences + newline + URL
- NO hashtags. NO emoji.

Output format: JSON with keys slide_1 through slide_8, each with "title" and "content"."""

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "model": LLM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": LLM_MAX_TOKENS,
        "temperature": 0.8
    }

    try:
        r = requests.post(LLM_API_URL, headers=headers, json=payload, timeout=LLM_TIMEOUT)

        if r.status_code != 200:
            log(f"LLM API error: HTTP {r.status_code} — {r.text[:200]}", "ERROR")
            return None

        response = r.json()

        # Handle reasoning models (content may be in different fields)
        choices = response.get("choices", [])
        if not choices:
            log("No choices in LLM response", "ERROR")
            return None

        msg = choices[0].get("message", {})
        content = msg.get("content") or msg.get("reasoning_content") or ""

        if not content:
            log("Empty LLM response", "ERROR")
            return None

        # Save raw output
        save_json(RAW_OUTPUT_FILE, {"raw": content, "timestamp": datetime.now().isoformat()})

        # Parse JSON from response
        # Try to extract JSON from the response
        json_match = re.search(r'\{[\s\S]*\}', content)
        if json_match:
            try:
                slides_data = json.loads(json_match.group())
                return slides_data
            except json.JSONDecodeError:
                # Try to fix common JSON issues
                fixed = json_match.group()
                fixed = fixed.replace('\n', '\\n')
                try:
                    slides_data = json.loads(fixed)
                    return slides_data
                except:
                    log("JSON parse failed", "ERROR")
                    return None

        log("No JSON found in LLM response", "ERROR")
        return None

    except Exception as e:
        log(f"LLM error: {e}", "ERROR")
        return None

def format_slides(slides_data):
    """Format slides data into posting format."""
    slides = []
    for i in range(1, 9):
        key = f"slide_{i}"
        if key in slides_data:
            slide = slides_data[key]
            title = slide.get("title", "")
            content = slide.get("content", "")
            slides.append({"title": title, "content": content})
    return slides

# ─── MAIN PIPELINE ───────────────────────────────────────────────────────────

def run_pipeline():
    """Main pipeline execution."""
    log("=== Economy Pipeline Started ===")

    # Load state
    posted = load_json(POSTED_FILE, {})
    posted_urls = set(posted.keys())

    # Scrape all sources
    log("Scraping RSS sources...")
    articles = scrape_all_sources()

    if not articles:
        log("No articles found", "WARN")
        print("No articles found")
        sys.exit(2)

    log(f"Total articles scraped: {len(articles)}")

    # Select best candidate
    best = select_best_candidate(articles, posted_urls)

    if not best:
        log("No suitable candidate found", "WARN")
        print("No suitable candidate")
        sys.exit(2)

    # Extract article content
    log(f"Extracting content: {best['url']}")
    article_content = extract_article_content(best["url"])

    if not article_content or len(article_content) < 100:
        log("Article content too short or empty", "WARN")
        # Try next candidate — for now just skip
        print("Article content too short")
        sys.exit(2)

    # Generate content via LLM
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
        # Still continue with what we have

    # Save to staging
    staging_data = {
        "title": best["title"],
        "url": best["url"],
        "source": best["source"],
        "slides": slides,
        "timestamp": datetime.now().isoformat()
    }

    save_json(STAGING_FILE, staging_data)

    # Also save latest as markdown
    md_content = f"# {best['title']}\n\n"
    md_content += f"Source: {best['source']}\n"
    md_content += f"URL: {best['url']}\n\n"
    md_content += "---\n\n"
    for i, slide in enumerate(slides, 1):
        md_content += f"## Slide {i}: {slide['title']}\n\n"
        md_content += f"{slide['content']}\n\n"
        md_content += "---\n\n"

    with open(LATEST_FILE, 'w') as f:
        f.write(md_content)

    log(f"Pipeline complete! Staging: {STAGING_FILE}")
    log(f"Slides generated: {len(slides)}")

    # Print success to stdout (cron captures this)
    print(f"Pipeline complete: {best['title']} ({len(slides)} slides)")

    return True

if __name__ == "__main__":
    try:
        success = run_pipeline()
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        log(f"Pipeline error: {e}", "ERROR")
        alert_telegram(f"Pipeline error: {e}")
        sys.exit(1)
