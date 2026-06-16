#!/usr/bin/env python3
"""
ECONOMY POST — Reads staging.json and posts to Threads
Architecture reference: Press Box v4 (pressbox-post.py)

Author: Hadijayyy
Created: 17 Jun 2026
"""

import os
import sys
import json
import time
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

# ─── CONFIG ──────────────────────────────────────────────────────────────────
DATA_DIR = Path.home() / ".hermes" / "economy"
SCRIPTS_DIR = Path.home() / ".hermes" / "scripts"
ENV_FILE = Path.home() / ".hermes" / ".env"
DIRECT_POST_SCRIPT = SCRIPTS_DIR / "pressbox-direct-post.py"

STAGING_FILE = DATA_DIR / "staging.json"
POSTED_FILE = DATA_DIR / "posted_topics.json"

# Minimum gap between posts (minutes)
MIN_POST_GAP = 60

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
            subprocess.run([
                "curl", "-s", "-X", "POST",
                f"https://api.telegram.org/bot{token}/sendMessage",
                "-d", f"chat_id={chat_id}",
                "-d", f"text=🤖 Economy Post: {msg}",
                "-d", "parse_mode=HTML"
            ], timeout=10, capture_output=True)
        except:
            pass

def is_posting_too_frequent():
    """Check if we posted too recently (Quality > Quantity)."""
    posted = load_json(POSTED_FILE, {})

    if not posted:
        return False

    # Check last 3 posts
    recent_posts = sorted(
        posted.values(),
        key=lambda x: x.get("timestamp", ""),
        reverse=True
    )[:3]

    now = datetime.now()
    for post in recent_posts:
        ts = post.get("timestamp", "")
        if ts:
            try:
                post_time = datetime.fromisoformat(ts)
                gap = (now - post_time).total_seconds() / 60
                if gap < MIN_POST_GAP:
                    log(f"Last post was {gap:.0f} min ago (min {MIN_POST_GAP})", "WARN")
                    return True
            except:
                pass

    return False

# ─── POSTING ─────────────────────────────────────────────────────────────────

def post_to_threads(slides, image_url=None):
    """Post slides to Threads via direct-post script."""
    if not DIRECT_POST_SCRIPT.exists():
        log(f"Direct post script not found: {DIRECT_POST_SCRIPT}", "ERROR")
        return False

    # Build command
    cmd = ["python3", str(DIRECT_POST_SCRIPT)]

    # Add image if available (only for first slide)
    if image_url:
        cmd.extend(["--image", image_url])

    # Write slides to temp file for the direct-post script
    slides_file = DATA_DIR / "temp_slides.txt"
    with open(slides_file, 'w') as f:
        for i, slide in enumerate(slides):
            if i > 0:
                f.write("\n---\n")
            f.write(f"{slide['title']}\n\n{slide['content']}")

    cmd.extend(["--file", str(slides_file)])

    log(f"Posting {len(slides)} slides to Threads...")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120  # 2 min timeout
        )

        if result.returncode == 0:
            log("Post successful!")
            # Try to extract post URL from output
            output = result.stdout
            return True
        else:
            log(f"Post failed: {result.stderr}", "ERROR")
            return False

    except subprocess.TimeoutExpired:
        log("Post timed out (120s)", "ERROR")
        return False
    except Exception as e:
        log(f"Post error: {e}", "ERROR")
        return False

# ─── TRACKING ────────────────────────────────────────────────────────────────

def update_tracking(staging_data):
    """Update posted_topics.json after successful post."""
    posted = load_json(POSTED_FILE, {})

    url = staging_data.get("url", "")
    if url:
        posted[url] = {
            "title": staging_data.get("title", ""),
            "timestamp": datetime.now().isoformat(),
            "source": staging_data.get("source", ""),
            "post_id": "POSTED"
        }
        save_json(POSTED_FILE, posted)
        log(f"Tracking updated: {url}")

def cleanup_staging():
    """Clear staging file after posting."""
    try:
        if STAGING_FILE.exists():
            STAGING_FILE.unlink()
        # Clean temp slides file
        temp_file = DATA_DIR / "temp_slides.txt"
        if temp_file.exists():
            temp_file.unlink()
        log("Staging cleaned up")
    except Exception as e:
        log(f"Cleanup error: {e}", "WARN")

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    """Main posting logic."""
    log("=== Economy Post Started ===")

    # Load environment
    load_env()

    # Check if staging exists
    if not STAGING_FILE.exists():
        log("No staging file found", "WARN")
        print("No staging file")
        sys.exit(2)

    # Load staging data
    staging_data = load_json(STAGING_FILE)
    if not staging_data:
        log("Staging file empty", "WARN")
        print("Staging empty")
        sys.exit(2)

    slides = staging_data.get("slides", [])
    if not slides:
        log("No slides in staging", "ERROR")
        print("No slides")
        sys.exit(1)

    # Check posting frequency
    if is_posting_too_frequent():
        log("Posting too frequent, skipping", "WARN")
        print("Too frequent")
        sys.exit(2)

    # Post to Threads
    success = post_to_threads(slides)

    if success:
        # Update tracking
        update_tracking(staging_data)

        # Cleanup
        cleanup_staging()

        # Print success
        title = staging_data.get("title", "Unknown")
        log(f"Posted: {title}")
        print(f"Posted: {title}")

        # Alert Telegram
        alert_telegram(f"Posted: {title}")

        sys.exit(0)
    else:
        log("Post failed", "ERROR")
        print("Post failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
