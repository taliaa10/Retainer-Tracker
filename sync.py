import os
import time
import logging
from datetime import datetime, timezone

import db
import tikhub

logger = logging.getLogger(__name__)


def sync_creator():
    """Sync all videos from the creator's own TikTok page.
    Assigns each video to the client whose product it tags."""
    handle = db.get_setting('creator_handle')
    if not handle:
        logger.warning("No creator handle configured — set it in Settings")
        return 0

    products_map = db.get_products_map()  # {product_id: client_id}
    logger.info(f"Syncing @{handle} — {len(products_map)} known products")

    total_fetched = 0
    cursor = 0

    try:
        while True:
            raw = tikhub.fetch_user_videos(handle, count=30, cursor=cursor)
            videos = tikhub.parse_videos(raw)

            if not videos:
                break

            for v in videos:
                posted_at = None
                if v["posted_at"]:
                    try:
                        posted_at = datetime.fromtimestamp(
                            int(v["posted_at"]), tz=timezone.utc
                        )
                    except (ValueError, OSError):
                        pass

                tagged_product_id = v["tagged_product_id"]

                # For new videos with no product tag, try fetching video detail
                if not tagged_product_id and not db.video_exists(v["video_id"]):
                    try:
                        detail = tikhub.fetch_video_detail(v["video_id"])
                        tagged_product_id = tikhub.parse_video_detail(detail)
                        time.sleep(0.3)
                    except Exception as e:
                        logger.warning(f"Could not fetch detail for {v['video_id']}: {e}")

                # Assign to the client who owns this product
                client_id = products_map.get(tagged_product_id) if tagged_product_id else None

                db.upsert_video(
                    client_id=client_id,
                    video_id=v["video_id"],
                    description=v["description"],
                    cover_url=v["cover_url"],
                    duration=v["duration"],
                    posted_at=posted_at,
                )
                db.upsert_video_metrics(
                    video_id=v["video_id"],
                    views=v["views"],
                    likes=v["likes"],
                    comments=v["comments"],
                    tagged_product_id=tagged_product_id,
                )

            total_fetched += len(videos)

            has_more = (
                raw.get("data", {}).get("has_more")
                or raw.get("data", {}).get("hasMore")
            )
            next_cursor = (
                raw.get("data", {}).get("cursor")
                or raw.get("data", {}).get("nextCursor")
                or raw.get("data", {}).get("max_cursor")
            )

            if not has_more or not next_cursor or total_fetched >= 200:
                break

            cursor = next_cursor
            time.sleep(0.5)

        db.log_sync(None, "success", total_fetched)
        logger.info(f"Synced {total_fetched} videos from @{handle}")
        return total_fetched

    except Exception as e:
        db.log_sync(None, f"error: {e}", total_fetched)
        logger.error(f"Sync failed: {e}")
        raise


def sync_gmv():
    """Enrich tagged videos with real GMV/orders from Creator Center.
    Requires TIKTOK_COOKIE env var to be set."""
    if not os.environ.get("TIKTOK_COOKIE"):
        logger.warning("TIKTOK_COOKIE not set — skipping GMV sync")
        return 0

    tagged = db.get_tagged_videos_for_gmv()
    logger.info(f"GMV sync: enriching {len(tagged)} tagged videos")

    enriched = 0
    for v in tagged:
        posted_at = v["posted_at"]
        if posted_at and hasattr(posted_at, "strftime"):
            start_date = posted_at.strftime("%m-01-%Y")
        else:
            start_date = datetime.now().strftime("%m-01-%Y")

        try:
            raw = tikhub.fetch_video_product_stats(
                item_id=v["video_id"],
                product_id=v["tagged_product_id"],
                start_date=start_date,
            )
            stats = tikhub.parse_video_product_stats(raw)
            db.update_video_gmv(v["video_id"], stats["gmv"], stats["orders"])
            enriched += 1
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"GMV sync failed for video {v['video_id']}: {e}")

    logger.info(f"GMV sync complete: {enriched}/{len(tagged)} videos enriched")
    return enriched


def sync_all():
    """Called by APScheduler."""
    try:
        sync_creator()
    except Exception as e:
        logger.error(f"Scheduled sync failed: {e}")
    try:
        sync_gmv()
    except Exception as e:
        logger.error(f"Scheduled GMV sync failed: {e}")
