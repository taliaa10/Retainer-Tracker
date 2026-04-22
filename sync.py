import time
import logging
from datetime import datetime, timezone

import db
import tikhub

logger = logging.getLogger(__name__)


def sync_client(client):
    """Sync all videos for a single client. Returns number of videos fetched."""
    client_id = client["id"]
    handle = client.get("tiktok_handle")

    if not handle:
        logger.info(f"Skipping client {client['brand_name']} — no TikTok handle set")
        return 0

    logger.info(f"Syncing {client['brand_name']} (@{handle})")
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

                db.upsert_video(
                    client_id=client_id,
                    video_id=v["video_id"],
                    description=v["description"],
                    cover_url=v["cover_url"],
                    duration=v["duration"],
                    posted_at=posted_at,
                )

                tagged_product_id = v["tagged_product_id"]

                # If no product found in list, try fetching video detail
                if not tagged_product_id and not db.video_exists(v["video_id"]):
                    try:
                        detail = tikhub.fetch_video_detail(v["video_id"])
                        tagged_product_id = tikhub.parse_video_detail(detail)
                        time.sleep(0.3)
                    except Exception as e:
                        logger.warning(f"Could not fetch detail for {v['video_id']}: {e}")

                db.upsert_video_metrics(
                    video_id=v["video_id"],
                    views=v["views"],
                    likes=v["likes"],
                    comments=v["comments"],
                    tagged_product_id=tagged_product_id,
                )

            total_fetched += len(videos)

            # Check if there are more pages
            has_more = (
                raw.get("data", {}).get("has_more")
                or raw.get("data", {}).get("hasMore")
            )
            next_cursor = (
                raw.get("data", {}).get("cursor")
                or raw.get("data", {}).get("nextCursor")
            )

            if not has_more or not next_cursor or total_fetched >= 200:
                break

            cursor = next_cursor
            time.sleep(0.5)  # be polite to TikHub

        db.log_sync(client_id, "success", total_fetched)
        logger.info(f"Synced {total_fetched} videos for {client['brand_name']}")
        return total_fetched

    except Exception as e:
        db.log_sync(client_id, f"error: {e}", total_fetched)
        logger.error(f"Sync failed for {client['brand_name']}: {e}")
        raise


def sync_all():
    """Sync all clients. Called by APScheduler."""
    clients = db.get_all_clients()
    logger.info(f"Starting sync for {len(clients)} clients")
    for client in clients:
        try:
            sync_client(client)
        except Exception as e:
            logger.error(f"Failed syncing {client['brand_name']}: {e}")
        time.sleep(1)
