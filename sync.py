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
    max_cursor = None

    try:
        while True:
            raw = tikhub.fetch_user_videos(handle, count=30, max_cursor=max_cursor)
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

                all_pids = v["all_product_ids"]

                # For new videos with no product tags, try fetching video detail
                if not all_pids and not db.video_exists(v["video_id"]):
                    try:
                        detail = tikhub.fetch_video_detail(v["video_id"])
                        pid = tikhub.parse_video_detail(detail)
                        if pid:
                            all_pids = [pid]
                        time.sleep(0.3)
                    except Exception as e:
                        logger.warning(f"Could not fetch detail for {v['video_id']}: {e}")

                # Pick first product ID that matches a registered client
                tagged_product_id = None
                client_id = None
                for pid in all_pids:
                    if pid in products_map:
                        tagged_product_id = pid
                        client_id = products_map[pid]
                        break
                # Fall back to first product ID even if unregistered
                if not tagged_product_id and all_pids:
                    tagged_product_id = all_pids[0]

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
                    all_product_ids=all_pids or None,
                )

            total_fetched += len(videos)

            has_more = raw.get("data", {}).get("has_more")
            next_max_cursor = raw.get("data", {}).get("max_cursor")

            if not has_more or not next_max_cursor or total_fetched >= 200:
                break

            max_cursor = next_max_cursor
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
