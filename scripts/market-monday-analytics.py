#!/usr/bin/env python3
"""
Market Monday Analytics — Feedback Loop
Fetches last 20 posts, analyzes engagement, outputs:
1. market_feedback.json — consumed by pipeline for topic boosts
2. market_analytics_report.md — Telegram delivery

Niche: Economics & Market for Indonesian Professionals
Based on: pressbox-analytics-feedback.py

Usage:
    python3 ~/.hermes/scripts/market-monday-analytics.py
"""

import json
import os
import httpx
import re
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# ─── CONFIG ──────────────────────────────────────────────────────────────────
TOKEN_PATH = os.path.expanduser("~/.hermes/threads_token.json")
DATA_DIR = os.path.expanduser("~/.hermes/market_monday")
FEEDBACK_PATH = os.path.join(DATA_DIR, "market_feedback.json")
REPORT_PATH = os.path.join(DATA_DIR, "market_analytics_report.md")
WIB = timezone(timedelta(hours=7))

# ─── TOPIC DETECTION — Economics/Market ──────────────────────────────────────
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

def get_token():
    with open(TOKEN_PATH) as f:
        data = json.load(f)
    return data["access_token"], str(data["user_id"])

def fetch_recent_posts(tok, uid, limit=20):
    """Fetch recent posts from Threads API."""
    try:
        r = httpx.get(
            f"https://graph.threads.net/v1.0/{uid}/threads",
            params={"access_token": tok, "fields": "id,text,timestamp", "limit": limit},
            timeout=15
        )
        return r.json().get("data", [])
    except Exception as e:
        print(f"Error fetching posts: {e}")
        return []

def fetch_engagement(tok, post_id):
    """Fetch engagement metrics for a post."""
    try:
        r = httpx.get(
            f"https://graph.threads.net/v1.0/{post_id}/insights",
            params={
                "access_token": tok,
                "metric": "likes,replies,reposts,views,quotes",
                "period": "lifetime"
            },
            timeout=10
        )
        metrics = {"likes": 0, "replies": 0, "reposts": 0, "views": 0, "quotes": 0}
        for item in r.json().get("data", []):
            metrics[item["name"]] = item["values"][0]["value"]
        return metrics
    except:
        return {"likes": 0, "replies": 0, "reposts": 0, "views": 0, "quotes": 0}

def calc_score(m):
    """Calculate engagement score (weighted)."""
    return m["likes"] + m["replies"] * 3 + m["reposts"] * 2 + m["quotes"] * 2

def extract_topics(text):
    """Extract all matching topics from post text."""
    text_lower = text.lower()
    topics = []
    for topic, patterns in TOPIC_PATTERNS.items():
        for pattern in patterns:
            if pattern in text_lower:
                topics.append(topic)
                break
    return topics if topics else ["general"]

def to_wib_hour(ts):
    """Convert timestamp to WIB hour."""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(WIB).hour
    except:
        return 12  # default

def get_time_slot(hour):
    """Group hours into time slots for analysis."""
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

# ─── MAIN ANALYTICS ──────────────────────────────────────────────────────────

def main():
    print("📊 Market Monday Analytics — Starting...")

    # Create data dir
    os.makedirs(DATA_DIR, exist_ok=True)

    # Get token
    try:
        tok, uid = get_token()
    except Exception as e:
        print(f"❌ Token error: {e}")
        return 1

    # Fetch recent posts
    raw = fetch_recent_posts(tok, uid, limit=20)
    if not raw:
        print("⚠️ No posts found.")
        return 0

    print(f"📊 Analyzing {len(raw)} posts...")

    # Enrich with engagement metrics
    enriched = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_engagement, tok, p["id"]): p for p in raw}
        for future in as_completed(futures):
            post = futures[future]
            metrics = future.result()
            enriched.append({
                "text": post.get("text", ""),
                "ts": post["timestamp"],
                "post_id": post["id"],
                "metrics": metrics,
                "score": calc_score(metrics),
                "wib_hour": to_wib_hour(post["timestamp"]),
                "time_slot": get_time_slot(to_wib_hour(post["timestamp"])),
            })

    # Sort by score
    enriched.sort(key=lambda x: x["score"], reverse=True)

    # Calculate overall stats
    scores = [p["score"] for p in enriched]
    avg_score = sum(scores) / max(len(scores), 1)
    max_score = max(scores) if scores else 0
    min_score = min(scores) if scores else 0

    # ─── Topic Analysis ──────────────────────────────────────────────────────
    topic_stats = defaultdict(lambda: {"count": 0, "total_score": 0, "avg_score": 0})
    for post in enriched:
        topics = extract_topics(post["text"])
        for topic in topics:
            topic_stats[topic]["count"] += 1
            topic_stats[topic]["total_score"] += post["score"]

    # Calculate averages
    for topic in topic_stats:
        stats = topic_stats[topic]
        stats["avg_score"] = stats["total_score"] / max(stats["count"], 1)

    # Sort topics by avg score
    sorted_topics = sorted(topic_stats.items(), key=lambda x: x[1]["avg_score"], reverse=True)

    # ─── Time Slot Analysis ──────────────────────────────────────────────────
    time_stats = defaultdict(lambda: {"count": 0, "total_score": 0, "avg_score": 0})
    for post in enriched:
        slot = post["time_slot"]
        time_stats[slot]["count"] += 1
        time_stats[slot]["total_score"] += post["score"]

    for slot in time_stats:
        stats = time_stats[slot]
        stats["avg_score"] = stats["total_score"] / max(stats["count"], 1)

    # Sort time slots by avg score
    sorted_times = sorted(time_stats.items(), key=lambda x: x[1]["avg_score"], reverse=True)

    # ─── Top/Bottom Posts ────────────────────────────────────────────────────
    top_posts = enriched[:5]
    bottom_posts = enriched[-5:] if len(enriched) >= 5 else enriched

    # ─── Generate Feedback JSON ──────────────────────────────────────────────
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

    # Topic boosts (relative to average)
    for topic, stats in sorted_topics:
        boost = 0
        if avg_score > 0:
            boost = ((stats["avg_score"] - avg_score) / avg_score) * 100
        feedback["topic_boosts"][topic] = {
            "avg_score": round(stats["avg_score"], 1),
            "count": stats["count"],
            "boost_pct": round(boost, 1),
        }

    # Time boosts
    for slot, stats in sorted_times:
        boost = 0
        if avg_score > 0:
            boost = ((stats["avg_score"] - avg_score) / avg_score) * 100
        feedback["time_boosts"][slot] = {
            "avg_score": round(stats["avg_score"], 1),
            "count": stats["count"],
            "boost_pct": round(boost, 1),
        }

    # Best/worst
    feedback["best_topics"] = [t[0] for t in sorted_topics[:3]]
    feedback["worst_topics"] = [t[0] for t in sorted_topics[-3:]]
    feedback["best_times"] = [t[0] for t in sorted_times[:2]]
    feedback["worst_times"] = [t[0] for t in sorted_times[-2:]]

    # Save feedback
    with open(FEEDBACK_PATH, 'w') as f:
        json.dump(feedback, f, indent=2, ensure_ascii=False)
    print(f"✅ Feedback saved: {FEEDBACK_PATH}")

    # ─── Generate Report (Markdown) ──────────────────────────────────────────
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
        topics = extract_topics(post["text"])
        report += f"""**{i}. Score: {post['score']}** | Topics: {', '.join(topics)}
> {post['text'][:100]}...

"""

    report += f"""
---

## 📉 Bottom 5 Posts (Learning)

"""
    for i, post in enumerate(bottom_posts, 1):
        topics = extract_topics(post["text"])
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
    for time in feedback["best_times"]:
        report += f"- ⏰ **{time}** — higher engagement\n"

    # Save report
    with open(REPORT_PATH, 'w') as f:
        f.write(report)
    print(f"✅ Report saved: {REPORT_PATH}")

    # Print summary
    print(f"\n📊 Summary:")
    print(f"   Posts analyzed: {len(enriched)}")
    print(f"   Avg score: {avg_score:.1f}")
    print(f"   Best topics: {', '.join(feedback['best_topics'][:3])}")
    print(f"   Best times: {', '.join(feedback['best_times'])}")

    return 0

if __name__ == "__main__":
    exit(main())
