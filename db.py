import os
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from datetime import date, timedelta
from contextlib import contextmanager

DATABASE_URL = os.environ.get('DATABASE_URL')
_pool = None


def init_pool():
    global _pool
    _pool = ThreadedConnectionPool(1, 5, DATABASE_URL, sslmode='require')


@contextmanager
def get_conn():
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def fetchall(sql, params=None):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def fetchone(sql, params=None):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None


def execute(sql, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            try:
                return cur.fetchone()[0]
            except Exception:
                return None


def init_db():
    init_pool()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    id SERIAL PRIMARY KEY,
                    tiktok_handle TEXT,
                    brand_name TEXT NOT NULL,
                    brand_color TEXT NOT NULL DEFAULT '#ffffff',
                    post_target INTEGER NOT NULL DEFAULT 30,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS retainer_periods (
                    id SERIAL PRIMARY KEY,
                    client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                    period_start DATE NOT NULL,
                    period_end DATE NOT NULL,
                    target_posts INTEGER NOT NULL DEFAULT 30,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                    product_id TEXT NOT NULL,
                    product_name TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    added_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(client_id, product_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    id SERIAL PRIMARY KEY,
                    client_id INTEGER REFERENCES clients(id) ON DELETE SET NULL,
                    video_id TEXT UNIQUE NOT NULL,
                    description TEXT,
                    cover_url TEXT,
                    duration INTEGER,
                    posted_at TIMESTAMPTZ,
                    synced_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS video_metrics (
                    id SERIAL PRIMARY KEY,
                    video_id TEXT NOT NULL REFERENCES videos(video_id) ON DELETE CASCADE,
                    views BIGINT DEFAULT 0,
                    likes INTEGER DEFAULT 0,
                    comments INTEGER DEFAULT 0,
                    gmv NUMERIC(12,2) DEFAULT 0,
                    orders INTEGER DEFAULT 0,
                    tagged_product_id TEXT,
                    recorded_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(video_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sync_log (
                    id SERIAL PRIMARY KEY,
                    client_id INTEGER REFERENCES clients(id) ON DELETE SET NULL,
                    synced_at TIMESTAMPTZ DEFAULT NOW(),
                    status TEXT,
                    videos_fetched INTEGER DEFAULT 0
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)

    # Drop NOT NULL on client_id for existing tables (idempotent migration)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE videos ALTER COLUMN client_id DROP NOT NULL")
    except Exception:
        pass


# ── SETTINGS ─────────────────────────────────────────────────────────────────

def get_setting(key, default=None):
    row = fetchone("SELECT value FROM settings WHERE key = %s", (key,))
    return row['value'] if row else default


def set_setting(key, value):
    execute("""
        INSERT INTO settings (key, value) VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """, (key, value))


def get_products_map():
    """Returns {product_id: client_id} for all active products."""
    rows = fetchall("SELECT product_id, client_id FROM products WHERE is_active = TRUE")
    return {r['product_id']: r['client_id'] for r in rows}


# ── CLIENTS ──────────────────────────────────────────────────────────────────

def get_all_clients():
    return fetchall("SELECT * FROM clients ORDER BY created_at")


def get_all_clients_with_period_stats():
    rows = fetchall("""
        SELECT
            c.*,
            rp.id             AS period_id,
            rp.period_start,
            rp.period_end,
            rp.target_posts,
            rp.status         AS period_status,
            COALESCE(
                (SELECT COUNT(*) FROM videos v
                 WHERE v.client_id = c.id
                   AND v.posted_at >= rp.period_start),
                0
            )                 AS posts_completed,
            (SELECT MAX(sl.synced_at) FROM sync_log sl
             WHERE sl.client_id = c.id AND sl.status = 'success')
                              AS last_synced
        FROM clients c
        LEFT JOIN retainer_periods rp
               ON rp.client_id = c.id AND rp.status = 'active'
        ORDER BY c.created_at
    """)
    today = date.today()
    for r in rows:
        target = r['target_posts'] or r['post_target'] or 30
        done = r['posts_completed'] or 0
        r['period_pct'] = round(min(done / target * 100, 100), 1)
        if r['period_end']:
            r['days_left'] = max((r['period_end'] - today).days, 0)
        else:
            r['days_left'] = None
    return rows


def get_client(client_id):
    return fetchone("SELECT * FROM clients WHERE id = %s", (client_id,))


def add_client(brand_name, tiktok_handle, brand_color, post_target):
    return execute(
        "INSERT INTO clients (brand_name, tiktok_handle, brand_color, post_target) "
        "VALUES (%s, %s, %s, %s) RETURNING id",
        (brand_name, tiktok_handle or None, brand_color, post_target)
    )


def update_client(client_id, brand_name, tiktok_handle, brand_color, post_target):
    execute(
        "UPDATE clients SET brand_name=%s, tiktok_handle=%s, brand_color=%s, post_target=%s WHERE id=%s",
        (brand_name, tiktok_handle or None, brand_color, post_target, client_id)
    )


def delete_client(client_id):
    execute("DELETE FROM clients WHERE id = %s", (client_id,))


# ── RETAINER PERIODS ──────────────────────────────────────────────────────────

def get_active_period(client_id):
    return fetchone(
        "SELECT * FROM retainer_periods WHERE client_id=%s AND status='active' ORDER BY created_at DESC LIMIT 1",
        (client_id,)
    )


def get_period_history(client_id):
    return fetchall(
        "SELECT * FROM retainer_periods WHERE client_id=%s ORDER BY period_start DESC",
        (client_id,)
    )


def start_period(client_id, period_start):
    start = period_start if isinstance(period_start, date) else date.fromisoformat(period_start)
    end = start + timedelta(days=30)
    execute(
        "UPDATE retainer_periods SET status='overdue' WHERE client_id=%s AND status='active'",
        (client_id,)
    )
    return execute(
        "INSERT INTO retainer_periods (client_id, period_start, period_end, target_posts) "
        "VALUES (%s, %s, %s, (SELECT post_target FROM clients WHERE id=%s)) RETURNING id",
        (client_id, start, end, client_id)
    )


def update_period(period_id, period_start, period_end):
    execute(
        "UPDATE retainer_periods SET period_start=%s, period_end=%s WHERE id=%s",
        (period_start, period_end, period_id)
    )


def complete_period(period_id):
    execute(
        "UPDATE retainer_periods SET status='completed' WHERE id=%s",
        (period_id,)
    )


# ── PRODUCTS ──────────────────────────────────────────────────────────────────

def get_client_products(client_id):
    return fetchall(
        "SELECT * FROM products WHERE client_id=%s AND is_active=TRUE ORDER BY added_at",
        (client_id,)
    )


def get_all_products_with_stats():
    return fetchall("""
        SELECT
            p.*,
            c.brand_name,
            c.brand_color,
            c.tiktok_handle,
            COALESCE(COUNT(DISTINCT vm.video_id), 0)   AS video_count,
            COALESCE(SUM(vm.views), 0)                 AS total_views,
            COALESCE(SUM(vm.gmv), 0)                   AS total_gmv,
            COALESCE(SUM(vm.orders), 0)                AS total_orders
        FROM products p
        JOIN clients c ON c.id = p.client_id
        LEFT JOIN video_metrics vm ON vm.tagged_product_id = p.product_id
        WHERE p.is_active = TRUE
        GROUP BY p.id, c.id
        ORDER BY total_gmv DESC
    """)


def get_product_summary_stats():
    return fetchone("""
        SELECT
            COUNT(DISTINCT p.id)                              AS total_products,
            COALESCE(SUM(vm.gmv), 0)                         AS total_gmv,
            COUNT(DISTINCT CASE WHEN vm.tagged_product_id IS NOT NULL THEN vm.video_id END) AS tagged_videos,
            COUNT(DISTINCT CASE WHEN sub.video_count = 0 THEN p.id END) AS dead_products
        FROM products p
        LEFT JOIN (
            SELECT tagged_product_id, COUNT(*) AS video_count
            FROM video_metrics WHERE tagged_product_id IS NOT NULL
            GROUP BY tagged_product_id
        ) sub ON sub.tagged_product_id = p.product_id
        LEFT JOIN video_metrics vm ON vm.tagged_product_id = p.product_id
        WHERE p.is_active = TRUE
    """)


def add_product(client_id, product_id, product_name):
    execute(
        "INSERT INTO products (client_id, product_id, product_name) VALUES (%s, %s, %s) "
        "ON CONFLICT (client_id, product_id) DO UPDATE SET is_active=TRUE, product_name=EXCLUDED.product_name",
        (client_id, product_id, product_name)
    )


def delete_product(product_id):
    execute("UPDATE products SET is_active=FALSE WHERE id=%s", (product_id,))


def get_product_videos(product_id_str, limit=6):
    return fetchall("""
        SELECT v.cover_url, vm.views
        FROM video_metrics vm
        JOIN videos v ON v.video_id = vm.video_id
        WHERE vm.tagged_product_id = %s
        ORDER BY vm.views DESC
        LIMIT %s
    """, (product_id_str, limit))


# ── VIDEOS ────────────────────────────────────────────────────────────────────

def get_client_videos(client_id, filter_type=None, limit=30):
    where = "WHERE v.client_id = %s"
    params = [client_id]
    if filter_type == 'tagged':
        where += " AND vm.tagged_product_id IS NOT NULL"
    elif filter_type == 'untagged':
        where += " AND vm.tagged_product_id IS NULL"
    return fetchall(f"""
        SELECT
            v.*,
            vm.views, vm.likes, vm.comments, vm.gmv, vm.orders,
            vm.tagged_product_id,
            p.product_name
        FROM videos v
        LEFT JOIN video_metrics vm ON vm.video_id = v.video_id
        LEFT JOIN products p ON p.product_id = vm.tagged_product_id AND p.client_id = v.client_id
        {where}
        ORDER BY v.posted_at DESC NULLS LAST
        LIMIT %s
    """, params + [limit])


def get_all_videos(client_id=None, filter_type=None, limit=100):
    where_clauses = []
    params = []
    if client_id:
        where_clauses.append("v.client_id = %s")
        params.append(client_id)
    if filter_type == 'tagged':
        where_clauses.append("vm.tagged_product_id IS NOT NULL")
    elif filter_type == 'untagged':
        where_clauses.append("vm.tagged_product_id IS NULL")
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    return fetchall(f"""
        SELECT
            v.*,
            c.brand_name, c.brand_color,
            vm.views, vm.likes, vm.comments, vm.gmv, vm.orders,
            vm.tagged_product_id,
            p.product_name
        FROM videos v
        JOIN clients c ON c.id = v.client_id
        LEFT JOIN video_metrics vm ON vm.video_id = v.video_id
        LEFT JOIN products p ON p.product_id = vm.tagged_product_id AND p.client_id = v.client_id
        {where}
        ORDER BY v.posted_at DESC NULLS LAST
        LIMIT %s
    """, params + [limit])


def get_client_stats(client_id):
    return fetchone("""
        SELECT
            COUNT(DISTINCT v.id)                                             AS video_count,
            COUNT(DISTINCT CASE WHEN vm.tagged_product_id IS NOT NULL
                                THEN v.id END)                               AS tagged_count,
            COALESCE(SUM(vm.views), 0)                                       AS total_views,
            COALESCE(SUM(vm.gmv), 0)                                         AS total_gmv,
            COUNT(DISTINCT p.id)                                             AS product_count
        FROM videos v
        LEFT JOIN video_metrics vm ON vm.video_id = v.video_id
        LEFT JOIN products p ON p.client_id = v.client_id AND p.is_active = TRUE
        WHERE v.client_id = %s
    """, (client_id,))


def get_top_products(client_id, limit=5):
    return fetchall("""
        SELECT
            p.product_id, p.product_name,
            COUNT(vm.video_id)   AS video_count,
            SUM(vm.orders)       AS total_orders,
            SUM(vm.gmv)          AS total_gmv
        FROM products p
        LEFT JOIN video_metrics vm ON vm.tagged_product_id = p.product_id
        WHERE p.client_id = %s AND p.is_active = TRUE
        GROUP BY p.id
        ORDER BY total_gmv DESC NULLS LAST
        LIMIT %s
    """, (client_id, limit))


def get_recent_activity(client_id, limit=8):
    return fetchall("""
        SELECT
            'video' AS event_type,
            v.video_id,
            v.description,
            v.posted_at         AS event_time,
            vm.views,
            vm.tagged_product_id,
            p.product_name
        FROM videos v
        LEFT JOIN video_metrics vm ON vm.video_id = v.video_id
        LEFT JOIN products p ON p.product_id = vm.tagged_product_id
        WHERE v.client_id = %s
        ORDER BY v.posted_at DESC NULLS LAST
        LIMIT %s
    """, (client_id, limit))


# ── SYNC ──────────────────────────────────────────────────────────────────────

def upsert_video(client_id, video_id, description, cover_url, duration, posted_at):
    execute("""
        INSERT INTO videos (client_id, video_id, description, cover_url, duration, posted_at, synced_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (video_id) DO UPDATE SET
            client_id  = COALESCE(EXCLUDED.client_id, videos.client_id),
            cover_url  = EXCLUDED.cover_url,
            synced_at  = NOW()
    """, (client_id, video_id, description, cover_url, duration, posted_at))


def upsert_video_metrics(video_id, views, likes, comments, tagged_product_id):
    execute("""
        INSERT INTO video_metrics (video_id, views, likes, comments, tagged_product_id, recorded_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (video_id) DO UPDATE SET
            views              = EXCLUDED.views,
            likes              = EXCLUDED.likes,
            comments           = EXCLUDED.comments,
            tagged_product_id  = EXCLUDED.tagged_product_id,
            recorded_at        = NOW()
    """, (video_id, views, likes, comments, tagged_product_id))


def log_sync(client_id, status, videos_fetched=0):
    execute(
        "INSERT INTO sync_log (client_id, status, videos_fetched) VALUES (%s, %s, %s)",
        (client_id, status, videos_fetched)
    )


def video_exists(video_id):
    row = fetchone("SELECT 1 FROM videos WHERE video_id=%s", (video_id,))
    return row is not None
