# Market Monday 📈

Personal branding automation for Threads — economics & market insights for Indonesian professionals.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  ⏰ SETIAP JAM :15                                         │
│  market-monday-pipeline.py                                 │
│  ├─ Scrape RSS (BBC, CNBC) → 30+ artikel                   │
│  ├─ Score (controversy > news + topic boosts)              │
│  ├─ Extract article (newspaper3k)                          │
│  └─ LLM generate 8 slides → staging.json                   │
├─────────────────────────────────────────────────────────────┤
│  ⏰ SETIAP JAM :35                                         │
│  economy-post.py                                           │
│  ├─ Read staging.json                                      │
│  ├─ Post ke Threads API                                    │
│  ├─ Update posted_topics.json (dedup)                      │
│  └─ Alert Telegram                                         │
├─────────────────────────────────────────────────────────────┤
│  ⏰ SETIAP MALAM 23:00                                     │
│  market-monday-analytics.py                                │
│  ├─ Fetch 20 post terakhir (Threads API)                   │
│  ├─ Hitung engagement (likes, replies, reposts)            │
│  ├─ Score per topic & time slot                            │
│  ├─ Generate market_feedback.json                          │
│  └─ Telegram report                                        │
│                                                             │
│  FEEDBACK LOOP:                                            │
│  market_feedback.json → pipeline boost topic yang perform  │
│  → Next post lebih baik → loop lagi                        │
└─────────────────────────────────────────────────────────────┘
```

## Features

- **RSS Scraping** — BBC Business, BBC World, CNBC
- **Smart Scoring** — Controversy > News, viral factors, title optimization
- **Analytics Feedback Loop** — Topic boosts from engagement data
- **LLM Generation** — 8-slide threads with hook, story arc, and CTA
- **Dedup** — URL-based tracking prevents reposts
- **Quality > Quantity** — Min 60min gap between posts

## Files

| File | Description |
|------|-------------|
| `scripts/market-monday-pipeline.py` | Main pipeline (scrape + score + generate) |
| `scripts/market-monday-analytics.py` | Analytics feedback loop |
| `scripts/economy-post.py` | Post to Threads via API |

## Setup

### 1. Prerequisites

```bash
pip install newspaper3k lxml_html_clean requests httpx
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
15 * * * * python3 ~/.hermes/scripts/market-monday-pipeline.py

# Post (publish content) — every hour at :35
35 * * * * python3 ~/.hermes/scripts/economy-post.py

# Analytics (feedback loop) — every night at 23:00
0 23 * * * python3 ~/.hermes/scripts/market-monday-analytics.py
```

## Manual Run

```bash
# Generate content
python3 ~/.hermes/scripts/market-monday-pipeline.py

# Post to Threads
python3 ~/.hermes/scripts/economy-post.py

# Run analytics feedback
python3 ~/.hermes/scripts/market-monday-analytics.py
```

## Data Files

| File | Description |
|------|-------------|
| `data/staging.json` | Generated content waiting to post |
| `data/posted_topics.json` | Posted URLs (dedup tracking) |
| `data/market_feedback.json` | Analytics feedback (topic boosts) |
| `data/market_analytics_report.md` | Latest analytics report |
| `data/latest.md` | Latest generated content (readable) |

## Analytics Feedback Loop

### How It Works

1. **Every night at 23:00** — `market-monday-analytics.py` fetches your last 20 posts
2. **Calculates engagement** — likes, replies, reposts, views, quotes
3. **Scores by topic** — inflasi, suku_bunga, global_market, etc.
4. **Scores by time** — pagi, siang, sore, malam
5. **Saves feedback** — `market_feedback.json` with boost percentages
6. **Pipeline reads feedback** — boosts topics/times that perform well

### Topic Tracking

| Topic | Keywords |
|-------|----------|
| inflasi | inflasi, inflation, harga, price, cpi |
| suku_bunga | suku bunga, interest rate, bi rate |
| global_market | wall street, saham, stock, ihsg |
| currency | rupiah, dollar, yen, forex |
| komoditas | minyak, oil, emas, gold |
| property | properti, property, rumah, kpr |
| tech_biz | ai, tech, startup, fintech |
| kebijakan | pajak, tax, regulasi, policy |
| karir | karir, career, gaji, phk, layoff |
| energi | energi, energy, listrik, bbm |
| global_event | perang, war, konflik, sanction |

### Boost Example

```json
{
  "topic_boosts": {
    "suku_bunga": {"avg_score": 85, "boost_pct": +42},
    "global_market": {"avg_score": 72, "boost_pct": +21},
    "property": {"avg_score": 45, "boost_pct": -15}
  }
}
```

Pipeline will prefer suku_bunga and global_market topics, avoid property.

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

Edit `RSS_SOURCES` in `market-monday-pipeline.py`:

```python
RSS_SOURCES = [
    {"name": "Reuters", "url": "https://rsshub.app/reuters/business", "type": "rss"},
]
```

### Adjust Scoring

Edit keywords in `market-monday-pipeline.py`:

```python
CONTROVERSY_KEYWORDS = ["crisis", "crash", "recession", ...]
VIRAL_FACTORS = {
    "outrage_money": ["price", "cost", "tax", ...],
    "human_story": ["worker", "family", ...],
}
```

### Add Topic Patterns

Edit `TOPIC_PATTERNS` in both pipeline and analytics:

```python
TOPIC_PATTERNS = {
    "crypto": ["bitcoin", "ethereum", "crypto", "blockchain"],
    "properti": ["properti", "property", "rumah", "kpr"],
}
```

## Based On

Architecture reference: [Press Box Pipeline](https://github.com/hadijayyy/pressbox-pipeline) — proven football content automation for Threads.

## License

MIT
