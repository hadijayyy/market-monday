# Market Monday 📈

Personal branding automation for [Threads](https://www.threads.net) — economics & market insights for Indonesian professionals, posted to **[@ryanhadiii](https://www.threads.net/@ryanhadiii)**.

**Status:** v17.4 — production-tested, 29/29 tests pass.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│ scripts/market-monday-pipeline.py            (generation, 2018 LOC)  │
│                                                                      │
│  DEFAULT MODE:                                                       │
│  ├─ Scrape RSS  (CNBC Indo, Detik Finance, IDX Channel, parallel)    │
│  ├─ Filter     (keyword include + strict/ambiguous exclude)          │
│  ├─ Score      (v17: 5-component, threshold ≥60)                    │
│  ├─ Pick       (Jaccard title dedup @ 0.35, top_n=3 candidates)     │
│  ├─ Extract    (newspaper3k + native requests, full article body)   │
│  ├─ Generate   (LLM chain: mistral-large-latest → MiniMax-M3)       │
│  ├─ Validate   (hook 2-3 sent + per-slide bounds + grounding)       │
│  └─ Stage      (writes 6-slide JSON → ~/.hermes/market_monday/)     │
│                                                                      │
│  FLAGS:                                                              │
│  --benchmark   Test RSS source quality → writes benchmark_results   │
│  --analytics   Fetch engagement → update market_feedback.json       │
│  --dry-run     Generate without posting (saves staging only)         │
│  --model X     Force specific model (skip fallback chain)            │
└──────────────────────────────────────────────────────────────────────┘
                                  ↓ staging.json
┌──────────────────────────────────────────────────────────────────────┐
│ scripts/market-monday-post.py                (posting, 400 LOC)     │
│                                                                      │
│ Forked from pressbox-direct-post.py for @ryanhadiii (v17.1).         │
│ No pressbox dependency — fully standalone (v17.2 purge).             │
│                                                                      │
│  --file PATH.md   Post 6 slides chained via reply_to_id (5s delay)   │
│  --verify         List recent posts + permalinks                     │
│  --delete POST_ID Remove a post                                      │
└──────────────────────────────────────────────────────────────────────┘
                                  ↓
                            Threads API → @ryanhadiii
```

## Features

- **Two-script split** — pipeline generates, post.py ships. Same data dir, zero coupling.
- **RSS scraping** — 3 Indonesian finance sources (CNBC Indonesia, Detik Finance, IDX Channel) via `requests` + parallel `ThreadPoolExecutor`.
- **v17 5-component scoring** — Keyword (40) + Category (20) + Recency (15) + Specific data (15) + Source tier (10) = 100 max, **threshold ≥60**.
- **6-slide structure** — HOOK → SETUP → COMPLICATION → INSIGHT → POV → CTA (per `threads-finance-6slide` reference).
- **Anti-hallucination** — slides 1-4 verbatim/paraphrased from article only. Slides 5-6 allow POV, must be marked.
- **Dedup** — URL tracking (`posted_topics.json`) + Jaccard title similarity @ 0.35 threshold.
- **Validation** — hook length (2-3 sent, ≥4 words) + per-slide sentence bounds + grounding check (article numbers must appear in slides).
- **Model fallback chain** — `mistral-large-latest` (Mistral direct, ~17s) → `MiniMax-M3` (tokenrouter fallback, ~3-4 min).
- **Streaming** — Mistral SSE for faster first-token; MiniMax full response.
- **Analytics feedback** — topic/time boosts persisted to `market_feedback.json`, applied during scoring.

## CLI Usage

```bash
# Normal run — scrape, generate, post to Threads (via cron drain)
python3 scripts/market-monday-pipeline.py

# Dry run — generate staging JSON without posting
python3 scripts/market-monday-pipeline.py --dry-run

# Benchmark RSS sources (write benchmark_results.json)
python3 scripts/market-monday-pipeline.py --benchmark

# Run analytics — fetch engagement → update market_feedback.json
python3 scripts/market-monday-pipeline.py --analytics

# Force specific model (skip fallback chain)
python3 scripts/market-monday-pipeline.py --model mistral-large-latest

# Manual post from staging.md
python3 scripts/market-monday-post.py --file ~/.hermes/market_monday/latest.md

# List recent posts with permalinks
python3 scripts/market-monday-post.py --verify

# Delete a post
python3 scripts/market-monday-post.py --delete POST_ID
```

## Setup

### 1. Prerequisites

```bash
pip install requests newspaper3k lxml_html_clean httpx
```

Python 3.11+. No GPU needed. No Docker.

### 2. Environment Variables

Add to `~/.hermes/.env`:

```bash
# LLM keys (pipeline)
PIPELINE_MISTRAL_KEY=your_mistral_api_key     # Primary — Mistral direct
MINIMAX_API_KEY=your_tokenrouter_key   # Fallback — tokenrouter

# Telegram alerts (optional — for pipeline error notifications)
TELEGRAM_BOT_TOKEN=your_bot_token
ALERT_CHAT=your_chat_id
```

> **Note:** Models.dev's `env:` list for Mistral is `["MISTRAL_API_KEY"]`. After the rename to `PIPELINE_MISTRAL_KEY`, the picker no longer sees it as a known key — that's intentional, prevents leaks via models.dev.

### 3. Threads API Token

Create `~/.hermes/market_monday/threads_token.json` (one account = one file):

```json
{
  "user_id": 123456789,
  "access_token": "your_threads_access_token"
}
```

Account handle is hardcoded in `pipeline.py` (`THREADS_HANDLE = "@ryanhadiii"`) — edit if account changes.

### 4. Cron Job (Optional)

The post drain runs via the Hermes cron scheduler:

```cron
# Drain staging → @ryanhadiii every hour at :30
30 * * * * python3 ~/.hermes/scripts/market-monday-post.py
```

Pipeline generation is event-driven (triggered manually or by upstream feed). Analytics runs nightly at 23:00:

```cron
0 23 * * * python3 ~/.hermes/scripts/market-monday-pipeline.py --analytics
```

## Models

| Model | Role | Speed | Cost |
|-------|------|-------|------|
| **mistral-large-latest** | Primary | ~17s | Mistral direct API |
| **MiniMax-M3** | Fallback | ~3-4 min | Tokenrouter (free tier) |

`LLM_TIMEOUT=240s`, `LLM_MAX_TOKENS=16000`, `temperature=0.5`. Mistral uses streaming; MiniMax falls back to full response.

## Validation System

### 1. Hook Validation (substance check)

| Check | Rule |
|-------|------|
| Length | ≥10 chars stripped |
| Word count | ≥4 words |
| Sentence count | 2-3 sentences (v16.1, tightened from 1-2) |

> **Note:** v17 dropped the v15 ANGKA/KONTEKS/DRAMA requirement — substance is enforced via the LLM prompt's anti-hallucination rules, not post-hoc regex.

### 2. Per-Slide Sentence Bounds

| Slide | Role | Required |
|-------|------|----------|
| Slide 1 | HOOK | 2-3 sentences |
| Slide 2 | SETUP | 2-3 sentences |
| Slide 3 | COMPLICATION | 2-3 sentences |
| Slide 4 | INSIGHT | 2-3 sentences |
| Slide 5 | POV | 2-4 sentences |
| Slide 6 | CTA | 1-2 sentences |

### 3. Grounding Check (anti-hallucination)

- Validates that numbers in slides 1-4 exist in the source article
- Excludes: years (2020-2030), single digits, long IDs, common currency amounts
- If mismatch → slide is flagged and pipeline retries with stronger grounding prompt

## Content Format (6-Slide Structure)

```
[SLIDE 1: HOOK]         → Emotional trigger, NO facts yet (build curiosity)
[SLIDE 2: SETUP]        → What happened (context from article)
[SLIDE 3: COMPLICATION] → Why it matters / what's at stake
[SLIDE 4: INSIGHT]      → Key finding/data point from article
[SLIDE 5: POV]          → Your take — opinion allowed, mark "POV:"
[SLIDE 6: CTA]          → Question that loops back to slide 1 + URL
```

Each slide ≤500 chars (Threads API limit). Image attached to root slide only.

## Scoring System (v17, 5 Components)

Max 100 points, threshold ≥60 to enter pipeline.

| # | Component | Range | Logic |
|---|-----------|-------|-------|
| 1 | Keyword Match | 0-40 | `min(matched_count, 5) × 8`. Categories: makro, saham, crypto, cross. Short tokens (≤4 chars) use word-boundary regex to avoid substring false positives (e.g. `ara` in `Barat`). |
| 2 | Category Relevance | 0-20 | `makro`/`saham`/`crypto` = 20. `cross` = 10. Other = 0. |
| 3 | Recency | 0-15 | <6h = 15. 6-24h = 10. 24-48h = 5. >48h = 0. Future-dated = rejected (v17.3 fix). |
| 4 | Specific Data | 0-15 | `has_specific_data()` (regex for %) = 15. Has any digit = 7. No digit = 0. |
| 5 | Source Tier | 0-10 | Tier-1 (Kontan, Bisnis.com, CNBC Indo, Katadata, Investor Daily) = 10. Tier-2 (Detik, IDX, Kumparan, etc.) = 5. Other = 0. |

**Hard rejects (score → -1):**

- URL in `posted_topics.json` (already posted)
- Strict exclude keyword matched (`prediksi zodiak`, `lowongan kerja`, `advertorial`, etc.)
- Ambiguous exclude with no include keyword in ±100 char context window (e.g. `saham mata` vs `saham BCA`)
- Future-dated article (clock skew / TZ mismatch)
- `is_finance_niche()` LLM check fails (Mistral direct API, ~3-5s, cost ~$0.0016/call)

## RSS Sources

| Source | Type | Tier | Notes |
|--------|------|------|-------|
| CNBC Indonesia | rss | 1 | Primary Indonesia finance news |
| Detik Finance | rss | 2 | High volume, mixed quality |
| IDX Channel | rss | 2 | Official IDX news, slow updates |
| BBC Business | rss (benchmark only) | — | Not in default scrape, used for `--benchmark` cross-language quality check |

3 active sources by default. BBC Business is benchmark-only to test source quality without polluting staging.

## Topic Tracking (analytics)

Tracked in `market_feedback.json` for `--analytics` mode:

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

> **Note:** Topic/time boosts are persisted in feedback JSON but **not** applied in v17 scoring formula (removed per user spec 21 Jun 2026 — kept as stub for analytics backward compat).

## Data Files

All under `~/.hermes/market_monday/`:

| File | Description |
|------|-------------|
| `staging.json` | Generated 6-slide content waiting to post |
| `posted_topics.json` | Posted URLs (dedup tracking) |
| `market_feedback.json` | Analytics feedback (topic/time boosts — v17 stub) |
| `latest.md` | Latest generated content (readable) |
| `raw_llm_output.txt` | Raw LLM output for debugging |
| `title_cache.json` | Title dedup cache |
| `benchmark_results.json` | RSS source benchmark results |
| `market_analytics_report.md` | Analytics report (generated by `--analytics`) |

## Performance

| Metric | Value |
|--------|-------|
| RSS scraping | <1s (parallel, 4 workers) |
| Content extraction | <1s (newspaper3k) |
| LLM generation (Mistral) | ~17s |
| LLM generation (MiniMax fallback) | ~3-4 min |
| Total (success path, Mistral) | ~20-30s |
| Total (fallback path) | ~4-5 min |

## Testing

```bash
cd market-monday
pytest -q
```

**29/29 pass** across 3 test files:

| File | Tests | Focus |
|------|-------|-------|
| `tests/test_smoke.py` | 2 | Import + basic run |
| `tests/test_regression.py` | 18 | Scoring, dedup, validation, freshness, format_slides |
| `tests/test_post_regression.py` | 9 | market-monday-post.py posting edge cases (permalink, retry, lock race, char trim) |

## Changelog (highlights)

| Version | Date | Change |
|---------|------|--------|
| v17.4 | 22 Jun 2026 | 5 bugs in post.py (post_ids tuple race, root_pid None check, char trim, verify_posts JSON, token load) + 1 defensive (reasoning_content strip in pipeline) |
| v17.3 | 22 Jun 2026 | is_fresh future-date reject, format_slides None safety, select_best_candidate None handling, hard reject on negative score |
| v17.2 | 21 Jun 2026 | Purge ALL pressbox contamination from market-monday-post.py |
| v17.1 | 21 Jun 2026 | Fork post.py for @ryanhadiii (parameterized token path) |
| v17 | 21 Jun 2026 | Replace keyword/scoring system (5-component, threshold ≥60) per user spec |
| v16.1 | 21 Jun 2026 | HOOK tightened 1-2 → 2-3 sentences |
| v16 | 21 Jun 2026 | 6-slide structure (HOOK/SETUP/COMPLICATION/INSIGHT/POV/CTA) — threads-finance-6slide reference |
| v15 | 21 Jun 2026 | Narrative spine + Indonesian output + HOOK min 2 |
| Earlier | <21 Jun 2026 | Initial release — 8-slide, 7-layer scoring, deepseek-v4-flash/mimo-v2.5 chain |

## Based On

Architecture reference: [Press Box Pipeline](https://github.com/hadijayyy/pressbox-pipeline) — proven football content automation for Threads. The pipeline pattern was forked and adapted for Indonesian finance niche. The posting script was forked in v17.1, then made fully standalone in v17.2 (zero pressbox dependency).

## License

MIT
