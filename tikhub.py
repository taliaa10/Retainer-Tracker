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


def fetch_user_videos(username, count=30, max_cursor=None):
    """Fetch recent videos for a TikTok username."""
    params = {"unique_id": username, "count": count}
    if max_cursor:
        params["max_cursor"] = max_cursor
    return _get("/api/v1/tiktok/app/v3/fetch_user_post_videos", params)


def fetch_video_detail(video_id):
    """Fetch single video detail (includes product stickers)."""
    return _get(
        "/api/v1/tiktok/app/v3/fetch_one_video",
        {"aweme_id": video_id}
    )


def lookup_product_info(product_id):
    """Try to fetch product name + thumbnail from TikHub.
    Returns (name, thumbnail_url) — either/both may be None."""
    try:
        data = _get(
            "/api/v1/tiktok/app/v3/fetch_product_detail_v4",
            {"product_id": product_id}
        )
        product = data.get("data", {}).get("product", {}) or data.get("data", {})
        name = product.get("title") or product.get("name")
        # Try common image field shapes
        images = product.get("images") or product.get("image_list") or []
        thumbnail_url = None
        if isinstance(images, list) and images:
            img = images[0]
            if isinstance(img, dict):
                urls = img.get("url_list") or img.get("thumb_url_list") or []
                thumbnail_url = urls[0] if urls else img.get("url") or img.get("thumb_url")
            elif isinstance(img, str):
                thumbnail_url = img
        return name, thumbnail_url
    except Exception:
        return None, None


def lookup_product_name(product_id):
    name, _ = lookup_product_info(product_id)
    return name


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

        results.append({
            "video_id": video_id,
            "description": description,
            "cover_url": cover_url,
            "duration": duration,
            "posted_at": posted_at,
            "views": views,
            "likes": likes,
            "comments": comments,
            "all_product_ids": _extract_all_product_ids(item),
        })

    return results


def _extract_all_product_ids(item):
    """Return all tagged product IDs from a video item, in order found."""
    ids = []

    for anchor in (item.get("anchors") or []):
        extra_raw = anchor.get("extra")
        if not extra_raw:
            continue
        try:
            entries = json.loads(extra_raw) if isinstance(extra_raw, str) else extra_raw
            for entry in (entries if isinstance(entries, list) else []):
                if entry.get("type") == 33 and entry.get("id"):
                    pid = str(entry["id"])
                    if pid not in ids:
                        ids.append(pid)
        except (ValueError, TypeError):
            pass

    for sticker in (item.get("stickersOnItem") or item.get("stickers_on_item") or []):
        stype = sticker.get("stickerType") or sticker.get("sticker_type")
        if stype == 2:
            for pid in (sticker.get("productIds") or sticker.get("product_ids") or []):
                if str(pid) not in ids:
                    ids.append(str(pid))

    for anchor in (item.get("anchor_info", {}).get("icon_field_list") or []):
        if anchor.get("type") == "product" and anchor.get("product_id"):
            pid = str(anchor["product_id"])
            if pid not in ids:
                ids.append(pid)

    return ids


def _extract_product_id(item):
    ids = _extract_all_product_ids(item)
    return ids[0] if ids else None


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
    """Sum timed_stats from the first segment only.
    The API returns 2 segments: segment[0] is the all-time summary,
    segment[1] is the same period broken down daily. Using both double-counts."""
    segments = data.get("data", {}).get("data", {}).get("segments") or []
    if not segments:
        return {"gmv": 0.0, "orders": 0, "product_views": 0, "product_clicks": 0}
    gmv = 0.0
    orders = 0
    product_views = 0
    product_clicks = 0
    for ts in (segments[0].get("timed_stats") or []):
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
