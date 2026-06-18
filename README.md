# Market Monday 📈

Personal branding automation for Threads — economics & market insights for Indonesian professionals.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  ⏰ SETIAP JAM :15                                         │
│  market-monday-pipeline.py                                 │
│  ├─ Scrape RSS (Detik, IDX, CNBC) → 30+ artikel           │
│  ├─ Score (controversy + topic boosts + feedback)          │
│  ├─ Extract article (curl + newspaper3k fallback)          │
│  ├─ Dedup (Jaccard similarity 0.5 threshold)              │
│  └─ LLM generate 8 slides → staging.json                   │
│      ├─ Primary: mimo-v2.5 (fast, Indonesian)              │
│      └─ Fallback: minimax-m3 (slower, reliable)            │
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

- **RSS Scraping** — Detik Finance, IDX Channel, CNBC Indonesia
- **Smart Scoring** — Controversy + News + Topic boosts + Analytics feedback
- **Analytics Feedback Loop** — Topic boosts from engagement data
- **LLM Generation** — 8-slide threads with hook, story arc, and URL
- **Dedup** — Jaccard similarity (0.5) + URL tracking prevents reposts
- **Quality > Quantity** — Min 60min gap between posts
- **Streaming** — SSE for faster LLM response
- **Reasoning Bypass** — Optimized prompts for speed

## Models

| Model | Role | Speed | Indonesian |
|-------|------|-------|------------|
| **mimo-v2.5** | Primary | ~40s | ✅ Excellent |
| **minimax-m3** | Fallback | ~60s | ✅ Good |

> deepseek-v4-flash fails with Indonesian prompts (returns 0 content).

## Hook Validation

Every hook must contain 3 elements:

| Element | Description | Example |
|---------|-------------|---------|
| **ANGKA** | Numbers, percentages, currency | Rp 301 T, 5%, 2025 |
| **KONTEKST** | Finance-related words | bank, harga, saham, utang |
| **DRAMA** | Emotional/dramatic words | naik, turun, tapi, mahal |

## Files

| File | Description |
|------|-------------|
| `scripts/market-monday-pipeline.py` | Main pipeline (scrape + score + generate) |
| `scripts/market-monday-analytics.py` | Analytics feedback loop |
| `scripts/economy-post.py` | Post to Threads via API |
| `scripts/economy-pipeline.py` | Legacy pipeline (deprecated) |

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
# Dry run (no posting)
python3 ~/.hermes/scripts/market-monday-pipeline.py --dry-run

# Generate content
python3 ~/.hermes/scripts/market-monday-pipeline.py

# Post to Threads
python3 ~/.hermes/scripts/economy-post.py

# Run analytics feedback
python3 ~/.hermes/scripts/market-monday-analytics.py
```

## CLI Flags

| Flag | Description |
|------|-------------|
| `--dry-run` | Generate content without posting to Threads |

## Data Files

| File | Description |
|------|-------------|
| `~/.hermes/market_monday/staging.json` | Generated content waiting to post |
| `~/.hermes/market_monday/posted_topics.json` | Posted URLs (dedup tracking) |
| `~/.hermes/market_monday/market_feedback.json` | Analytics feedback (topic boosts) |
| `~/.hermes/market_monday/latest.md` | Latest generated content (readable) |
| `~/.hermes/market_monday/raw_llm_output.txt` | Raw LLM output for debugging |

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
| inflasi | inflasi, harga, cpi, harga naik |
| suku_bunga | suku bunga, bi rate, interest rate |
| global_market | wall street, saham, ihsg, index |
| currency | rupiah, dollar, forex, kurs |
| komoditas | minyak, emas, gold, oil, gas |
| property | properti, rumah, kpr, cicilan |
| tech_biz | ai, tech, startup, fintech, digital |
| kebijakan | pajak, regulasi, subsidy, kebijakan |
| karir | karir, gaji, phk, layoffs, upah |
| energi | energi, listrik, bbm, pertamax |
| utang | utang, pinjam, kredit, bank, hutang |

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

1. **Hook** — OUTRAGE/SHOCK with numbers + context + drama (max 300 chars)
2. **Apa yang Terjadi** — What happened? (150-450 chars)
3. **Kenapa Penting** — Why it matters (150-450 chars)
4. **Siapa Terdampak** — Who's affected, empathy to small people (150-450 chars)
5. **Sudut Pandang** — Different perspective (150-450 chars)
6. **Dampak Lebih Luas** — Wider implications (150-450 chars)
7. **Yang Belum Jelas** — What's unclear (150-450 chars)
8. **Opini + Fakta** — Your opinion + facts from article + URL (150-450 chars)

### Hook Examples

✅ **Good:** "Menteri Keuangan Purbaya amankan Rp 301 T dari Bank Asia untuk proyek 2025-2029."
- ANGKA: Rp 301 T
- KONTEKST: Bank
- DRAMA: amankan

❌ **Bad:** "Pemerintah dapat pinjaman untuk pembangunan."
- No numbers
- No specific context
- No drama/emotion

## Scoring System

| Factor | Weight | Description |
|--------|--------|-------------|
| Controversy | +30 | Crisis, crash, scandal keywords |
| Viral Money | +25 | Price, cost, tax outrage |
| Viral Human | +20 | Worker, family, affected people |
| Freshness | +15 | Published < 2 hours ago |
| Topic Boost | +10 | From analytics feedback |
| Source Boost | +5 | Premium sources (CNBC, Detik) |

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

## Performance

| Metric | Before | After |
|--------|--------|-------|
| LLM Time | 46s | 40s |
| Total Time | 50s | 45s |
| Hook Validation | 3 attempts | 1-2 attempts |
| Streaming | ❌ | ✅ |
| Reasoning Tokens | 12K | 8K |

## Based On

Architecture reference: [Press Box Pipeline](https://github.com/hadijayyy/pressbox-pipeline) — proven football content automation for Threads.

## License

MIT
