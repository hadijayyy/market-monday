#!/usr/bin/env python3
"""
Market Monday Post — Post 6-slide threads to Threads API for @ryanhadiii.

Standalone posting script (zero pressbox dependency, purged v17.2).
Each slide (separated by ===) becomes its own post chained via reply_to_id.
5 second delay between each slide ensures the parent is indexed.

Token: unified via threads_auth module (auto-refresh, multi-account).

Usage:
  python3 market-monday-post.py --file path.md
  python3 market-monday-post.py --verify
  python3 market-monday-post.py --delete POST_ID

Forked from pressbox-direct-post.py at v17.1 (8a22800), purged at v17.2 (f6a08b0).
"""

import json, sys, httpx, time, re
import atexit
from pathlib import Path

# Add threads-tools to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "threads-tools"))
from threads_auth import ThreadsAuth

THREADS_API = "https://graph.threads.net/v1.0"
_HTTP = httpx.Client(timeout=8)
atexit.register(_HTTP.close)

# Unified auth — account "ryanhadiii"
_AUTH = ThreadsAuth(account="ryanhadiii")

def load_token():
    """Load token via unified auth (auto-refreshes if needed)."""
    try:
        token, uid = _AUTH.get_token()
        return token, uid
    except Exception as e:
        print(f"❌ Auth failed: {e}", file=sys.stderr)
        print(f"   Run: python3 -c \"from threads_auth import ThreadsAuth; ThreadsAuth('ryanhadiii').login()\"", file=sys.stderr)
        raise SystemExit(1) from e

def create_container(uid, token, text, reply_to=None, image_url=None, max_retries=1):
    """Create a media container with retry on transient errors.
    If image_url provided (root slide only), tries IMAGE then falls back to TEXT."""
    if image_url and not reply_to:
        # Try IMAGE container first
        data = {"media_type": "IMAGE", "image_url": image_url, "text": text.strip(), "access_token": token}
        for attempt in range(max_retries + 1):
            try:
                r = _HTTP.post(f"{THREADS_API}/{uid}/threads", data=data)
                if r.status_code >= 500:
                    if attempt < max_retries:
                        wait_time = 2 + attempt
                        print(f"   ⚠️ Image HTTP {r.status_code} — retry {attempt+1}/{max_retries}", file=sys.stderr)
                        time.sleep(wait_time)
                        continue
                    print(f"   ⚠️ Image failed (HTTP {r.status_code}), fallback to TEXT", file=sys.stderr)
                    break
                result = r.json()
                if r.status_code == 200:
                    print(f"   📷 Image attached to root slide", file=sys.stderr)
                    return result["id"]
                if "transient" in str(result).lower() and attempt < max_retries:
                    wait_time = 2 + attempt
                    print(f"   ⚠️ Image transient — retry {attempt+1}/{max_retries}", file=sys.stderr)
                    time.sleep(wait_time)
                    continue
                print(f"   ⚠️ Image failed ({result.get('error',{}).get('message','?')}), fallback to TEXT", file=sys.stderr)
                break
            except httpx.TimeoutException:
                if attempt < max_retries:
                    print(f"   ⚠️ Image timeout — retry {attempt+1}/{max_retries}", file=sys.stderr)
                    time.sleep(2)
                    continue
                print(f"   ⚠️ Image timeout after retries, fallback to TEXT", file=sys.stderr)
                break
            except Exception as e:
                print(f"   ⚠️ Image error: {e}, fallback to TEXT", file=sys.stderr)
                break
        # Fall through to TEXT fallback
        print(f"   Using TEXT fallback for root slide", file=sys.stderr)

    # TEXT container (default or IMAGE fallback)
    data = {"media_type": "TEXT", "text": text.strip(), "access_token": token}
    if reply_to:
        data["reply_to_id"] = reply_to

    for attempt in range(max_retries + 1):
        try:
            r = _HTTP.post(f"{THREADS_API}/{uid}/threads", data=data)

            # Handle HTTP 500 Transient Server Errors safely
            if r.status_code >= 500:
                if attempt < max_retries:
                    wait_time = 2 + attempt
                    print(f"   ⚠️ Container HTTP {r.status_code} — retry {attempt+1}/{max_retries}", file=sys.stderr)
                    time.sleep(wait_time)
                    continue
                raise Exception(f"Container create failed with HTTP {r.status_code}: {r.text}")

            result = r.json()
            if r.status_code == 200:
                return result["id"]

            # Retry on explicit transient errors in JSON payload
            if "transient" in str(result).lower() and attempt < max_retries:
                wait_time = 2 + attempt
                print(f"   ⚠️ Container transient — retry {attempt+1}/{max_retries}", file=sys.stderr)
                time.sleep(wait_time)
                continue

            raise Exception(f"Container create failed: {result}")
        except httpx.TimeoutException:
            if attempt < max_retries:
                print(f"   ⚠️ Container timeout — retry {attempt+1}/{max_retries}", file=sys.stderr)
                time.sleep(2)
                continue
            raise
    raise Exception(f"Container create failed after {max_retries + 1} attempts")

def publish(uid, token, container_id, max_retries=1):
    """Publish a container. Returns published post ID."""
    for attempt in range(max_retries + 1):
        try:
            r = _HTTP.post(f"{THREADS_API}/{uid}/threads_publish",
                data={"creation_id": container_id, "access_token": token})

            if r.status_code >= 500:
                if attempt < max_retries:
                    wait_time = 2 + attempt
                    print(f"   ⚠️ Publish HTTP {r.status_code} — retry {attempt+1}/{max_retries}", file=sys.stderr)
                    time.sleep(wait_time)
                    continue
                raise Exception(f"Publish failed with HTTP {r.status_code}: {r.text}")

            result = r.json()
            if r.status_code == 200:
                return result.get("id")

            error_msg = result.get("error", {}).get("message", "")
            if "transient" in str(result).lower() and attempt < max_retries:
                wait_time = 2 + attempt
                print(f"   ⚠️ Publish transient: {error_msg[:60]} — retry {attempt+1}/{max_retries}", file=sys.stderr)
                time.sleep(wait_time)
                continue

            raise Exception(f"Publish failed: {result}")
        except httpx.TimeoutException:
            if attempt < max_retries:
                print(f"   ⚠️ Publish timeout — retry {attempt+1}/{max_retries}", file=sys.stderr)
                time.sleep(2)
                continue
            raise
    raise Exception(f"Publish failed after {max_retries + 1} attempts")

def get_latest_permalink(uid, token):
    """Get the actual post permalink (alphanumeric format) for the most recent post."""
    try:
        r = _HTTP.get(f"{THREADS_API}/{uid}/threads",
            params={"fields": "id,permalink,text", "limit": "3", "access_token": token},
            timeout=10)
        if r.status_code == 200:
            data = r.json().get("data", [])
            if data:
                return data[0].get("permalink", "")
    except Exception:
        pass
    return ""

def post_thread(uid, token, slides, image_url=None):
    """
    Post slides as a Threads CHAIN — each slide replies to the previous.
    S2 → S1, S3 → S2, S4 → S3, S5 → S4, S6 → S5.
    Pressbox method — ensures correct carousel order in Threads UI.

    image_url: attach to root slide (slide 1) if provided.
    """
    filtered = [s for s in slides if s.strip()]
    if not filtered:
        return []

    post_ids = []
    prev_post_id = None  # chain: each slide replies to previous

    for i, slide in enumerate(filtered):
        text = slide.strip()
        if not text:
            continue

        # Char-cap safety net: Threads API rejects > 500 chars
        if len(text) > 500:
            trimmed = text[:500]
            last_period = max(trimmed.rfind(". "), trimmed.rfind("! "), trimmed.rfind("? "))
            if last_period > 50:
                text = trimmed[:last_period + 1]
            else:
                text = trimmed[:-1].rstrip() + "…"
            print(f"   ✂️ Slide {i+1} char-trimmed to {len(text)} chars (final guard)", file=sys.stderr)

        try:
            if prev_post_id:
                print(f"   Slide {i+1}/{len(filtered)}: creating reply to slide {i} ({prev_post_id})...", file=sys.stderr)
            else:
                print(f"   Slide {i+1}/{len(filtered)}: creating root container...", file=sys.stderr)

            cid = create_container(uid, token, text, prev_post_id, image_url if i == 0 else None)
            print(f"   Slide {i+1}/{len(filtered)}: publishing...", file=sys.stderr)
            pid = publish(uid, token, cid)
            post_ids.append((i, pid))
            print(f"   Slide {i+1}/{len(filtered)}: → {pid}", file=sys.stderr)

            if i == 0:
                print(f"Root: {pid}")
                time.sleep(1)
                permalink = get_latest_permalink(uid, token)
                if permalink:
                    print(f"Post: {permalink}")
                else:
                    print(f"Post: https://www.threads.com/@ryanhadiii/post/{pid}")

            # Chain: next slide replies to THIS post
            prev_post_id = pid

        except Exception as e:
            print(f"   ⚠️ Slide {i+1}/{len(filtered)} failed: {e}", file=sys.stderr)
            # RETRY: wait 5s and try once more
            try:
                time.sleep(5)
                print(f"   🔄 Retrying slide {i+1}/{len(filtered)}...", file=sys.stderr)
                cid = create_container(uid, token, text, prev_post_id, image_url if i == 0 else None)
                pid = publish(uid, token, cid)
                post_ids.append((i, pid))
                print(f"   ✅ Slide {i+1}/{len(filtered)} retry succeeded: → {pid}", file=sys.stderr)
                if i == 0:
                    print(f"Root: {pid}")
                    time.sleep(1)
                    permalink = get_latest_permalink(uid, token)
                    if permalink:
                        print(f"Post: {permalink}")
                    else:
                        print(f"Post: https://www.threads.com/@ryanhadiii/post/{pid}")
                prev_post_id = pid
            except Exception as retry_err:
                print(f"   ❌ Slide {i+1}/{len(filtered)} retry also failed: {retry_err}", file=sys.stderr)
                if prev_post_id is None:
                    print(f"   🛑 Cannot continue — no root post.", file=sys.stderr)
                    return post_ids
                print(f"   Continuing with remaining slides...", file=sys.stderr)
            continue

        if i < len(filtered) - 1:
            # Rate limit pauses
            if i == 1:
                time.sleep(10)
            elif i == 3:
                time.sleep(10)
            else:
                time.sleep(3)

    return post_ids

def parse_slides(text):
    """Split text into slides by === separator."""
    slides = re.split(r'(?:\n|^)===\s*\n', text)
    return [s.strip() for s in slides if s.strip()]

def verify_posts(uid, token, limit=15):
    """Check recent posts."""
    try:
        r = _HTTP.get(f"{THREADS_API}/{uid}/threads",
            params={"access_token": token, "fields": "id,text,timestamp", "limit": limit})
        # Bug fix (v17.4): wrap r.json() in try/except — non-JSON responses (HTML error
        # page, 502/503) previously crashed the verify command.
        try:
            data = r.json().get("data", [])
        except (json.JSONDecodeError, ValueError) as e:
            print(f"❌ Non-JSON response from Threads API: {e}", file=sys.stderr)
            return []
    except httpx.HTTPError as e:
        print(f"❌ Network error during verify: {e}", file=sys.stderr)
        return []
    results = []
    for post in data:
        text = post.get("text", "")
        if text.strip():
            has_bare = bool(re.search(r'https?://[^\s\[\]]+', text))
            has_bracket = "[Source" in text or "[http" in text
            results.append((post["id"], has_bare, has_bracket, text[:80]))
    return results

def delete_post(uid, token, post_id):
    """Delete a post by ID."""
    r = _HTTP.delete(f"{THREADS_API}/{post_id}",
        params={"access_token": token})
    return r.status_code == 200

def main():
    token, uid = load_token()
    image_url = None

    if "--image" in sys.argv:
        idx = sys.argv.index("--image")
        if idx + 1 < len(sys.argv):
            image_url = sys.argv[idx + 1]
            print(f"📷 Image URL provided: {image_url[:60]}...", file=sys.stderr)

    if "--verify" in sys.argv:
        results = verify_posts(uid, token, 10)
        all_ok = True
        for pid, has_bare, has_bracket, preview in results:
            if has_bare:
                status = "✅ CLICKABLE"
            elif has_bracket:
                status = "⚠️ BRACKETED"
                all_ok = False
            else:
                status = "❌ NO URL"
                all_ok = False
            print(f"{status} | {pid} | {preview}")
        sys.exit(0 if all_ok else 1)

    if "--delete" in sys.argv:
        idx = sys.argv.index("--delete")
        if idx + 1 >= len(sys.argv):
            print("❌ Error: Missing POST_ID after --delete")
            sys.exit(1)
        pid = sys.argv[idx + 1]
        is_partial = "--partial" in sys.argv
        if delete_post(uid, token, pid):
            reason = " (partial cleanup)" if is_partial else ""
            print(f"✅ Deleted: {pid}{reason}")
        else:
            print(f"❌ Delete failed: {pid}")
        sys.exit(0)

    text = ""
    if "--file" in sys.argv:
        idx = sys.argv.index("--file")
        if idx + 1 >= len(sys.argv):
            print("❌ Error: Missing file path after --file")
            sys.exit(1)
        text = Path(sys.argv[idx + 1]).read_text().strip()
    elif not sys.stdin.isatty():
        text = sys.stdin.read().strip()
    elif len(sys.argv) > 1 and not sys.argv[1].startswith("--"):
        text = " ".join(sys.argv[1:])
    else:
        print("Usage: --file path.md, pipe stdin, or provide text")
        sys.exit(1)

    if not text:
        print("❌ Empty text")
        sys.exit(1)

    plain_text = re.sub(r'[*_~`#>\[\]|]', '', text).strip()
    if len(plain_text) < 50:
        print(f"❌ Text too short ({len(plain_text)} chars, min 50) — skipping to avoid empty post")
        sys.exit(1)

    slides = parse_slides(text)
    print(f"📝 {len(slides)} slides detected", file=sys.stderr)

    post_ids = post_thread(uid, token, slides, image_url)
    if not post_ids:
        print("❌ No slides posted")
        sys.exit(1)
    # post_ids is list of (slide_idx, pid) tuples; sort by slide_idx for stable output
    sorted_ids = sorted(post_ids, key=lambda x: x[0])
    pids_only = [pid for _, pid in sorted_ids]
    print(f"✅ Thread posted: {len(post_ids)} slides")
    print(f"   Root: {sorted_ids[0][1]}")
    for slide_idx, pid in sorted_ids:
        print(pid)

if __name__ == "__main__":
    main()