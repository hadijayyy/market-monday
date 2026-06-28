# Market Monday 📈

Automated finance content for [Threads](https://www.threads.net) — economics, market & crypto insights for Indonesian professionals, posted to **[@ryanhadiii](https://www.threads.net/@ryanhadiii)**.

**Status:** v18.0 — production.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│ scripts/market-monday-pipeline.py            (generation, ~2000 LOC) │
│                                                                      │
│  ├─ Scrape RSS     (Kontan, CNBC ID, Katadata, Bloomberg Technoz)    │
│  ├─ Filter          (keyword include + strict/ambiguous exclude)     │
│  ├─ Score           (v18: 7-component, threshold ≥50)                │
│  ├─ Pick            (title dedup, top_n=3 candidates)                │
│  ├─ Extract         (newspaper3k + native requests, article cache)   │
│  ├─ Generate        (Mistral primary → qwen/9router fallback)        │
│  ├─ Validate        (hook quality gate + grounding check + dedup)    │
│  └─ Stage           (6-slide JSON → ~/.hermes/market_monday/)        │
│                                                                      │
│  FLAGS:                                                              │
│  --benchmark     Test RSS source quality                             │
│  --analytics     Fetch engagement → update feedback                  │
│  --dry-run       Generate without posting                            │
│  --model X       Force specific model (mistral / qwen)               │
└──────────────────────────────────────────────────────────────────────┘
                               ↓ staging.json
┌──────────────────────────────────────────────────────────────────────┐
│ scripts/market-monday-post.py                (posting, 377 LOC)     │
│                                                                      │
│  Standalone — zero pressbox dependency (v17.2 purge).                │
│  Uses unified ThreadsAuth module for OAuth.                          │
│                                                                      │
│  --file PATH.md   Post 6 slides chained via reply_to_id              │
│  --verify         List recent posts + permalinks                     │
│  --delete POST_ID Remove a post                                      │
└──────────────────────────────────────────────────────────────────────┘
                               ↓
                         Threads API → @ryanhadiii
```

## Features

- **Model fallback** — `mistral-large-latest` (primary, ~17s) → `qwen/qwen3-32b` via 9router (fallback).
- **Article cache** — 30min TTL, 100-entry LRU. Same URL won't re-fetch.
- **Image validation** — HEAD accessibility check + finance-adapted quality scoring.
- **Dynamic prompt** — Analytics-fed preferred hooks, CTA patterns, tone adjustment.
- **6-slide structure** — HOOK → SETUP → COMPLICATION → INSIGHT → POV → CTA.
- **DEDUP rule** — Each entity appears in max 1 slide (prevents repetition).
- **Anti-hallucination** — FACT BANK extraction, grounding check, hook quality gate.
- **Sports/entertainment filter** — Blocks football, F1, NBA, drakor, musik, etc.
- **Self-healing** — `mm-preflight.py` (syntax + files) + `mm-autofix.py` (auto-repair).

## CLI Usage

```bash
# Normal run — scrape, generate, stage
python3 scripts/market-monday-pipeline.py

# Dry run — generate without posting
python3 scripts/market-monday-pipeline.py --dry-run

# Benchmark RSS sources
python3 scripts/market-monday-pipeline.py --benchmark

# Run analytics
python3 scripts/market-monday-pipeline.py --analytics

# Force specific model
python3 scripts/market-monday-pipeline.py --model mistral
python3 scripts/market-monday-pipeline.py --model qwen

# Manual post
python3 scripts/market-monday-post.py --file ~/.hermes/market_monday/latest.md

# Verify recent posts
python3 scripts/market-monday-post.py --verify

# Delete a post
python3 scripts/market-monday-post.py --delete POST_ID
```

## Setup

### 1. Prerequisites

```bash
pip install requests newspaper3k lxml_html_clean httpx
```

Python 3.11+. No GPU. No Docker.

### 2. Environment Variables

Add to `~/.hermes/.env`:

```bash
# LLM keys
MISTRAL_MM_KEY=your_mistral_api_key        # Primary
9ROUTER_KEY=sk_your_9router_key            # Fallback (optional)

# Telegram alerts (optional)
TELEGRAM_BOT_TOKEN=your_bot_token
ALERT_CHAT=your_chat_id
```

### 3. Threads Token

Create `~/.hermes/market_monday/threads_token.json`:

```json
{
  "user_id": 123456789,
  "access_token": "your_long_lived_token"
}
```

Account handle: `@ryanhadiii` (hardcoded in `pipeline.py` line 70).

### 4. Cron Jobs (Hermes)

| Job | Schedule | Script | Purpose |
|-----|----------|--------|---------|
| Market Monday — Pipeline | hourly :00 | `market-monday-pipeline.py` | Generate + stage content |
| Monday Market Post | hourly :30 | `market-monday-post.py` | Post staged content to Threads |
| Market Monday — Analytics | daily 23:00 | `market-monday-pipeline.py --analytics` | Fetch engagement → feedback |
| Market Monday — Pre-flight | hourly :55 | `mm-preflight.py` | Syntax + files check |

## Scoring System (v18)

Max 100, threshold ≥50.

| # | Component | Range | Logic |
|---|-----------|-------|-------|
| 1 | Keyword Match | 0-30 | `min(matched, 5) × 6`. Word-boundary for ≤4 char tokens. |
| 2 | Category | 0-20 | makro/saham/crypto = 20, cross = 10 |
| 3 | Recency | 0-15 | <6h = 15, 6-24h = 10, 24-48h = 5 |
| 4 | Data Specificity | 0-15 | Has %/Rp/bps/index = 15, any digit = 5 |
| 5 | Market Timing | 0-10 | 9-16 WIB = 10, 7-22 WIB = 5, night = 0 |
| 6 | Engagement | 0-10 | Boost if topic matches high-engagement past posts |
| 7 | Anti-clickbait | -10 | Penalty for listicle/generic titles ("5 cara...", "wajib tahu") |

**Hard rejects (→ -1):**
- Already posted URL
- Exclude keyword matched (noise, ads, sports/entertainment)
- Ambiguous exclude with no finance context nearby
- Future-dated article

## Filter Chain

```
RSS (4 sources × ~15 articles)
  → Exclude keywords (noise + non-editorial + sports/entertainment)
  → Include keyword match (4 categories, ~100+ terms)
  → Title dedup (fuzzy similarity)
  → Freshness (≤24h)
  → Finance niche LLM check
  → Score ≥50
  → Hook quality gate + grounding check
  → Stage
```

## Include Keywords (v17.7, ~100+ terms)

| Category | Examples |
|----------|----------|
| **makro** | rupiah, bank indonesia, inflasi, apbn, sri mulyani, pajak, kredit, investasi, bumn, phk, dolar, subsidi, bea cukai, defisit, fiskal |
| **saham** | ihsg, bei, ipo, dividen, buyback, foreign outflow, emiten, reksadana, obligasi, broker, sekuritas |
| **crypto** | bitcoin, ethereum, binance, indodax, defi, nft, blockchain, memecoin, bappebti |
| **cross** | the fed, fomc, harga emas, harga minyak, tarif, suku bunga, batu bara, cpo, komoditas |

## Exclude Keywords

| Category | Examples |
|----------|----------|
| **noise** | zodiak, gosip, artis, giveaway |
| **non_redaksional** | advertorial, press release, lowongan kerja |
| **olahraga_entertainment** | pildun, fifa, messi, ronaldo, motogp, nba, drakor, netflix, konser, anime |

Short tokens (≤4 char: nfl, nba, f1) use word-boundary regex.

## Models

| Model | Role | Speed | Endpoint |
|-------|------|-------|----------|
| **mistral-large-latest** | Primary | ~17s | `api.mistral.ai` (Mistral direct) |
| **qwen/qwen3-32b** | Fallback | ~10s | `172.17.0.1:20128` (9router) |

`LLM_TIMEOUT=60s`, `LLM_MAX_TOKENS=8000`, `temperature=0.5`.

## Content Format (6-Slide)

```
[SLIDE 1: HOOK]         → Emotional trigger, scroll-stopper. Proper noun + concrete detail.
[SLIDE 2: SETUP]        → What happened (who/what/when/where)
[SLIDE 3: COMPLICATION] → Stakes, risk, competing forces
[SLIDE 4: INSIGHT]      → Key data point, the "value" slide
[SLIDE 5: POV]          → "POV gue:" — opinion, must trace to article fact
[SLIDE 6: CTA]          → Rhetorical question + callback S1 + article URL
```

Each slide ≤500 chars. Image on root slide only. Chain posting (S2 replies to S1, etc.).

## Data Files

Under `~/.hermes/market_monday/`:

| File | Description |
|------|-------------|
| `staging.json` | Generated 6-slide content |
| `latest.md` | Readable format of staged content |
| `posted_topics.json` | Posted URLs (dedup) |
| `title_cache.json` | Title dedup cache |
| `article_cache.json` | Article content cache (30min TTL) |
| `market_feedback.json` | Analytics feedback |
| `raw_llm_output.txt` | Last raw LLM response (debug) |

## Self-Healing

| Script | Schedule | Function |
|--------|----------|----------|
| `mm-preflight.py` | :55 hourly | Python syntax + required files + data dir |
| `mm-autofix.py` | :10,:40 hourly | Auto-fix safe failures (timeout, stale staging, empty LLM) |

## Performance

| Metric | Value |
|--------|-------|
| RSS scraping | <1s (parallel, 4 workers) |
| Article extraction | <1s (cached) / ~2s (fresh) |
| LLM generation | ~17s (Mistral) / ~10s (qwen) |
| Total (success path) | ~20s |

## Changelog

| Version | Date | Key Changes |
|---------|------|-------------|
| **v18.0** | 28 Jun 2026 | 7-component scoring (keyword, category, recency, data, market timing, engagement, anti-clickbait). Threshold 60→50. 4 focused sources. |
| **v17.7** | 28 Jun 2026 | Expanded keywords (+60 terms), sports/entertainment exclude, word-boundary fix for short exclude tokens |
| **v17.6** | 28 Jun 2026 | Model fallback (mistral→qwen), article cache, image scoring, dynamic prompt, timeout 120→60s, DEDUP rule |
| v17.5 | 28 Jun 2026 | Model swap to 9router, chain posting fix, pressbox-style prompt |
| v17.4 | 22 Jun 2026 | 5 bug fixes in post.py |
| v17.3 | 22 Jun 2026 | Future-date reject, None safety |
| v17.2 | 21 Jun 2026 | Purge pressbox contamination from post.py |
| v17.1 | 21 Jun 2026 | Fork post.py for @ryanhadiii |
| v17 | 21 Jun 2026 | 5-component scoring, threshold ≥60 |

## Based On

Forked from [Press Box Pipeline](https://github.com/hadijayyy/pressbox-pipeline) — football content automation for Threads. Adapted for Indonesian finance niche. Post script standalone since v17.2.

## License

MIT
