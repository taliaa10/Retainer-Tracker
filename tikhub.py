import json
import os
import time
import requests

TIKHUB_BASE = "https://api.tikhub.io"
TIKHUB_KEY = os.environ.get("TIKHUB_API_KEY")


def _headers():
    return {"Authorization": f"Bearer {TIKHUB_KEY}"}


def _get(path, params, retries=2):
    url = f"{TIKHUB_BASE}{path}"
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=_headers(), params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)


def _post(path, body, retries=2):
    url = f"{TIKHUB_BASE}{path}"
    for attempt in range(retries + 1):
        try:
            resp = requests.post(url, headers=_headers(), json=body, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)


def fetch_user_videos(username, count=30, cursor=0):
    """Fetch recent videos for a TikTok username."""
    return _get(
        "/api/v1/tiktok/app/v3/fetch_user_post_videos",
        {"unique_id": username, "count": count, "cursor": cursor}
    )


def fetch_video_detail(video_id):
    """Fetch single video detail (includes product stickers)."""
    return _get(
        "/api/v1/tiktok/app/v3/fetch_one_video",
        {"aweme_id": video_id}
    )


def lookup_product_name(product_id):
    """Try to resolve a product name from TikHub. Returns None if unavailable."""
    try:
        data = _get(
            "/api/v1/tiktok/app/v3/fetch_product_detail",
            {"product_id": product_id}
        )
        return (
            data.get("data", {}).get("product", {}).get("title")
            or data.get("data", {}).get("title")
        )
    except Exception:
        return None


def parse_videos(data):
    """
    Parse the raw TikHub fetch_user_post_videos response into a list of dicts.
    Returns: list of { video_id, description, cover_url, duration, posted_at,
                        views, likes, comments, tagged_product_id }
    """
    aweme_list = (
        data.get("data", {}).get("aweme_list")
        or data.get("data", {}).get("videos")
        or []
    )
    results = []
    for item in aweme_list:
        video_id = str(item.get("aweme_id", "") or item.get("id", ""))
        if not video_id:
            continue

        description = item.get("desc") or item.get("description") or ""

        # Cover URL — take first non-empty URL from the list
        cover_url = None
        cover = item.get("video", {}).get("cover", {}) or item.get("video", {}).get("origin_cover", {}) or {}
        url_list = cover.get("url_list") or []
        for u in url_list:
            if u and "http" in u:
                cover_url = u
                break

        # Duration in seconds
        duration = item.get("video", {}).get("duration")
        if duration and duration > 1000:
            duration = duration // 1000  # convert ms to seconds

        # Posted timestamp
        posted_at = item.get("create_time")

        # Stats
        stats = item.get("statistics") or item.get("stats") or {}
        views = stats.get("play_count") or stats.get("view_count") or 0
        likes = stats.get("digg_count") or stats.get("like_count") or 0
        comments = stats.get("comment_count") or 0

        tagged_product_id = _extract_product_id(item)

        results.append({
            "video_id": video_id,
            "description": description,
            "cover_url": cover_url,
            "duration": duration,
            "posted_at": posted_at,
            "views": views,
            "likes": likes,
            "comments": comments,
            "tagged_product_id": tagged_product_id,
        })

    return results


def _extract_product_id(item):
    """Extract the first tagged product ID from a video item dict.

    TikHub returns product stickers in anchors[].extra, which is a JSON string
    containing a list of objects. Product entries have type==33 and carry the
    product ID in the 'id' field.  We also check the older stickersOnItem
    structure as a fallback.
    """
    # Primary: anchors array (real API response format)
    for anchor in (item.get("anchors") or []):
        extra_raw = anchor.get("extra")
        if not extra_raw:
            continue
        try:
            entries = json.loads(extra_raw) if isinstance(extra_raw, str) else extra_raw
            for entry in (entries if isinstance(entries, list) else []):
                if entry.get("type") == 33 and entry.get("id"):
                    return str(entry["id"])
        except (ValueError, TypeError):
            pass

    # Fallback: stickersOnItem (older / alternate response shape)
    for sticker in (item.get("stickersOnItem") or item.get("stickers_on_item") or []):
        stype = sticker.get("stickerType") or sticker.get("sticker_type")
        if stype == 2:
            product_ids = sticker.get("productIds") or sticker.get("product_ids") or []
            if product_ids:
                return str(product_ids[0])

    # Fallback: anchor_info.icon_field_list
    for anchor in (item.get("anchor_info", {}).get("icon_field_list") or []):
        if anchor.get("type") == "product" and anchor.get("product_id"):
            return str(anchor["product_id"])

    return None


def parse_video_detail(data):
    """Parse fetch_one_video response for product tag."""
    detail = (
        data.get("data", {}).get("aweme_detail")
        or data.get("data", {})
    )
    return _extract_product_id(detail)


def fetch_video_product_stats(item_id, product_id, start_date):
    """Fetch per-video GMV/orders from Creator Center.

    start_date format: MM-DD-YYYY (e.g. "04-01-2025")
    Returns raw TikHub response.
    """
    cookie = os.environ.get("TIKTOK_COOKIE", "")
    return _post(
        "/api/v1/tiktok/creator/get_video_to_product_stats",
        {
            "cookie": cookie,
            "item_id": str(item_id),
            "product_id": str(product_id),
            "start_date": start_date,
        }
    )


def parse_video_product_stats(data):
    """Sum timed_stats segments → {gmv, orders, product_views, product_clicks}."""
    segments = data.get("data", {}).get("segments") or []
    gmv = 0.0
    orders = 0
    product_views = 0
    product_clicks = 0
    for seg in segments:
        for ts in (seg.get("timed_stats") or []):
            s = ts.get("stats") or {}
            gmv += float((s.get("product_revenue") or {}).get("amount") or 0)
            orders += int(s.get("order_cnt") or 0)
            product_views += int(s.get("product_view_cnt") or 0)
            product_clicks += int(s.get("product_click_cnt") or 0)
    return {
        "gmv": round(gmv, 2),
        "orders": orders,
        "product_views": product_views,
        "product_clicks": product_clicks,
    }
