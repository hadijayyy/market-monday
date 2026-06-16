# Economy Threads Pipeline 🤖

Personal branding automation for Threads — posting economics & market insights for Indonesian professionals.

## Architecture

```
RSS Feed (BBC, CNBC)
    ↓
Scrape 30+ articles per cycle
    ↓
Score (controversy > news)
    ↓
Best candidate → newspaper3k extract
    ↓
LLM generate 8 slides (mimo-v2.5)
    ↓
staging.json
    ↓
Post to Threads → posted_topics.json
```

## Features

- **RSS Scraping** — BBC Business, BBC World, CNBC
- **Smart Scoring** — Controversy > News, viral factors, title optimization
- **LLM Generation** — 8-slide threads with hook, story arc, and CTA
- **Dedup** — URL-based tracking prevents reposts
- **Quality > Quantity** — Min 60min gap between posts

## Files

| File | Description |
|------|-------------|
| `scripts/economy-pipeline.py` | Main pipeline (scrape + score + generate) |
| `scripts/economy-post.py` | Post to Threads via API |

## Setup

### 1. Prerequisites

```bash
pip install newspaper3k lxml_html_clean requests
```

### 2. Environment Variables

Create `~/.hermes/.env`:

```bash
OPENCODE_GO_API_KEY=your_api_key
TELEGRAM_BOT_TOKEN=your_bot_token
ALERT_CHAT=your_chat_id
```

### 3. Threads API Token

Create `~/.hermes/threads_token.json`:

```json
{
  "user_id": your_user_id,
  "access_token": "your_access_token"
}
```

### 4. Cron Jobs

```bash
# Pipeline (generate content) — every hour at :15
15 * * * * python3 ~/.hermes/scripts/economy-pipeline.py

# Post (publish content) — every hour at :35
35 * * * * python3 ~/.hermes/scripts/economy-post.py
```

## Manual Run

```bash
# Generate content
python3 ~/.hermes/scripts/economy-pipeline.py

# Post to Threads
python3 ~/.hermes/scripts/economy-post.py
```

## Data Files

| File | Description |
|------|-------------|
| `data/staging.json` | Generated content waiting to post |
| `data/posted_topics.json` | Posted URLs (dedup tracking) |
| `data/scrape_cache.json` | Article cache (30min TTL) |
| `data/latest.md` | Latest generated content (readable) |

## Content Format

Each thread has 8 slides:

1. **Hook** — OUTRAGE/SHOCK with numbers (max 8 words)
2. **Problem** — What happened?
3. **Context** — Why it matters
4. **Comparison** — Connect to Indonesia
5. **Human Angle** — Quotes/emotion
6. **Big Picture** — Implications
7. **Stakes** — Why care now
8. **CTA** — Provocative question + URL

## Customization

### Add RSS Sources

Edit `RSS_SOURCES` in `economy-pipeline.py`:

```python
RSS_SOURCES = [
    {
        "name": "Reuters Business",
        "url": "https://rsshub.app/reuters/business",
        "type": "rss"
    },
]
```

### Adjust Scoring

Edit keywords in `economy-pipeline.py`:

```python
CONTROVERSY_KEYWORDS = ["crisis", "crash", "recession", ...]
VIRAL_FACTORS = {
    "outrage_money": ["price", "cost", "tax", ...],
    "human_story": ["worker", "family", ...],
}
```

## Based On

Architecture reference: [Press Box Pipeline](https://github.com/hadijayyy/pressbox-pipeline) — proven football content automation for Threads.

## License

MIT
