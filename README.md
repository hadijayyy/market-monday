# Market Monday 📈

Personal branding automation for Threads — economics & market insights for Indonesian professionals.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  market-monday-pipeline.py (ALL-IN-ONE)                     │
│                                                             │
│  DEFAULT MODE:                                              │
│  ├─ Scrape RSS (CNBC Indonesia, Detik Finance, IDX Channel) │
│  ├─ Dedup (Jaccard similarity 0.35 threshold)              │
│  ├─ Score (7-layer: impact + urgency + relevance + viral)  │
│  ├─ Extract article (newspaper3k + native requests)         │
│  ├─ LLM generate 8 slides (deepseek → mimo fallback)       │
│  ├─ Validate (hook + sentence count + grounding)           │
│  └─ Post to Threads (via pressbox-direct-post.py)          │
│                                                             │
│  --benchmark   Test RSS source quality                      │
│  --analytics   Fetch engagement → update feedback JSON      │
│  --dry-run     Generate without posting                     │
│  --model X     Force specific model                        │
└─────────────────────────────────────────────────────────────┘
```

## Features

- **All-in-One** — 1 script menggantikan 3 file lama (pipeline + post + analytics)
- **RSS Scraping** — CNBC Indonesia, Detik Finance, IDX Channel (native requests, no curl)
- **7-Layer Scoring** — Impact + Urgency + Indo Relevance + Viral + Topic boost + Time boost
- **Analytics Feedback Loop** — Topic & time slot boosts dari engagement data
- **LLM Generation** — 8-slide threads dengan hook, story arc, dan URL
- **Dedup** — Jaccard similarity (0.35) + URL tracking prevents reposts
- **3 Validations** — Hook (ANGKA + KONTEKS + DRAMA) + Sentence count + Grounding check
- **Model Fallback** — deepseek-v4-flash → mimo-v2.5
- **Streaming** — SSE untuk faster LLM response

## CLI Usage

```bash
# Normal run — scrape, generate, post to Threads
python3 market-monday-pipeline.py

# Dry run — generate tanpa posting
python3 market-monday-pipeline.py --dry-run

# Benchmark RSS sources
python3 market-monday-pipeline.py --benchmark

# Run analytics — update market_feedback.json
python3 market-monday-pipeline.py --analytics

# Force specific model
python3 market-monday-pipeline.py --model mimo-v2.5
```

## Setup

### 1. Prerequisites

```bash
pip install requests newspaper3k lxml_html_clean httpx
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
  "user_id": 123456789,
  "access_token": "your_access_token"
}
```

### 4. Cron Job (Optional)

```bash
# Run every hour at :15
15 * * * * python3 ~/.hermes/scripts/market-monday-pipeline.py

# Analytics every night at 23:00
0 23 * * * python3 ~/.hermes/scripts/market-monday-pipeline.py --analytics
```

## Models

| Model | Role | Speed |
|-------|------|-------|
| **deepseek-v4-flash** | Primary | ~40-50s |
| **mimo-v2.5** | Fallback | ~25-50s |

## Validation System

### 1. Hook Validation (3 elements required)

| Element | Description | Example |
|---------|-------------|---------|
| **ANGKA** | Numbers, percentages, currency | Rp 15 juta, 5%, 1.000 orang |
| **KONTEKS** | Finance-related words | bank, harga, saham, pekerja |
| **DRAMA** | Emotional/dramatic words | antre, anjlok, bangkrut, viral |

### 2. Sentence Count (+1 tolerance)

| Slide | Required |
|-------|----------|
| Slide 1 (hook) | 2–3 sentences |
| Slides 2–7 | 3–4 sentences |
| Slide 8 (CTA) | 2–3 sentences |

### 3. Grounding Check (Lenient)

- Validates numbers in slides exist in article
- Excludes: years (2020–2030), single digits, long IDs, currency amounts

## Content Format

Each thread has 8 slides:

1. **Hook** — Angka + konteks + drama (2–3 kalimat)
2. **Apa yang Terjadi** — Fakta utama (3–4 kalimat)
3. **Kenapa Penting** — Konteks & angka (3–4 kalimat)
4. **Siapa Terdampak** — Fokus orang kecil (3–4 kalimat)
5. **Fakta Tersembunyi** — Yang jarang disorot media (3–4 kalimat)
6. **Analisis Dampak** — Inferensi logis, di-flag sebagai analisis (3–4 kalimat)
7. **Yang Belum Jelas** — Ketidakpastian dari artikel (3–4 kalimat)
8. **Opini + CTA** — Pendapat + "Menurut lo, ...?" + URL (2–3 kalimat)

### Hook Example

✅ **Good:** "Lebih dari 1.000 orang rela antre 2 km di bawah terik matahari demi gaji Rp 15 juta."
- ANGKA: 1.000 orang, 2 km, Rp 15 juta
- KONTEKS: gaji
- DRAMA: rela antre

❌ **Bad:** "Banyak orang melamar kerja di pabrik baru."
- No numbers
- No specific context
- No drama/emotion

## Scoring System (7 Layers)

| Layer | Factor | Score |
|-------|--------|-------|
| 1 | Impact CRASH (anjlok, bangkrut, krisis) | +30 |
| 1 | Impact SURGE (rekor, rally, tembus) | +25 |
| 1 | Impact NEGATIVE (turun, peringatan) | +20 |
| 2 | Urgency HIGH (breaking, terbaru) | +25 |
| 2 | Urgency MEDIUM (hari ini, mengumumkan) | +15 |
| 3 | Indo HIGH (rupiah, ihsg, bank indonesia) | +40 |
| 3 | Indo MEDIUM (batu bara, nikel, emas) | +25 |
| 3 | Indo LOW (asia, asean, china) | +15 |
| 4 | Boring keywords penalty | -15 |
| 4 | Opinion keywords penalty | -20 |
| 4 | Video keywords penalty | -100 |
| 5 | Viral factors (3+ categories) | +50 |
| 6 | Title quality (≤8 words) | +15 |
| 6 | Title has numbers | +10 |
| 7 | Analytics topic boost | up to +50 |
| 7 | Analytics time boost | up to +30 |

## Analytics Feedback Loop

```
Post to Threads
     ↓
--analytics (every night)
     ↓
Fetch 20 posts → calculate engagement
     ↓
Score by topic & time slot
     ↓
market_feedback.json
     ↓
Pipeline reads → boost topics that perform well
     ↓
Better posts → loop again
```

### Topic Tracking

| Topic | Keywords |
|-------|----------|
| inflasi | inflasi, harga, cpi, deflasi |
| suku_bunga | suku bunga, bi rate, rate hike |
| global_market | wall street, saham, ihsg, rally |
| currency | rupiah, dollar, forex, nilai tukar |
| komoditas | minyak, emas, batu bara, coal |
| property | properti, rumah, kpr, real estate |
| tech_biz | ai, tech, startup, fintech |
| kebijakan | pajak, regulasi, ojk, policy |
| karir | karir, gaji, phk, layoff |
| energi | energi, listrik, bbm, subsidi |
| global_event | perang, war, sanction, imf |

## Data Files

| File | Description |
|------|-------------|
| `~/.hermes/market_monday/staging.json` | Generated content waiting to post |
| `~/.hermes/market_monday/posted_topics.json` | Posted URLs (dedup tracking) |
| `~/.hermes/market_monday/market_feedback.json` | Analytics feedback (topic boosts) |
| `~/.hermes/market_monday/latest.md` | Latest generated content (readable) |
| `~/.hermes/market_monday/raw_llm_output.txt` | Raw LLM output for debugging |
| `~/.hermes/market_monday/benchmark_results.json` | RSS benchmark results |
| `~/.hermes/market_monday/market_analytics_report.md` | Analytics report |

## Performance

| Metric | Value |
|--------|-------|
| Scraping | <1s (parallel) |
| Content extraction | <1s |
| LLM generation | 40–90s per attempt |
| Total (success) | ~1–2 min |

## Based On

Architecture reference: [Press Box Pipeline](https://github.com/hadijayyy/pressbox-pipeline) — proven football content automation for Threads.

## License

MIT
