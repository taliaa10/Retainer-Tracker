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

        # Product tag — look in stickersOnItem
        tagged_product_id = None
        stickers = item.get("stickersOnItem") or item.get("stickers_on_item") or []
        for sticker in stickers:
            stype = sticker.get("stickerType") or sticker.get("sticker_type")
            if stype == 2:  # product sticker type
                product_ids = sticker.get("productIds") or sticker.get("product_ids") or []
                if product_ids:
                    tagged_product_id = str(product_ids[0])
                    break

        # Fallback: check anchor_info for product links
        if not tagged_product_id:
            anchors = item.get("anchor_info", {}).get("icon_field_list") or []
            for anchor in anchors:
                if anchor.get("type") == "product":
                    tagged_product_id = str(anchor.get("product_id", ""))
                    break

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


def parse_video_detail(data):
    """Parse fetch_one_video response for product tag."""
    detail = (
        data.get("data", {}).get("aweme_detail")
        or data.get("data", {})
    )
    stickers = detail.get("stickersOnItem") or detail.get("stickers_on_item") or []
    for sticker in stickers:
        stype = sticker.get("stickerType") or sticker.get("sticker_type")
        if stype == 2:
            product_ids = sticker.get("productIds") or sticker.get("product_ids") or []
            if product_ids:
                return str(product_ids[0])
    return None
