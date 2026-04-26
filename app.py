import os
import logging
from datetime import date, datetime, timezone

from flask import Flask, render_template, request, redirect, url_for, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

load_dotenv()

import db
import sync
import tikhub

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ── TEMPLATE FILTERS ──────────────────────────────────────────────────────────

@app.template_filter('num')
def fmt_num(n):
    if n is None:
        return '—'
    n = float(n)
    if n >= 1_000_000:
        v = n / 1_000_000
        return f'{v:.1f}M' if v % 1 != 0 else f'{int(v)}M'
    if n >= 1_000:
        v = n / 1_000
        return f'{v:.1f}K' if v % 1 != 0 else f'{int(v)}K'
    return str(int(n))


@app.template_filter('gmv')
def fmt_gmv(n):
    if not n:
        return '—'
    n = float(n)
    if n >= 1_000:
        return f'${n/1000:.1f}K'
    return f'${n:.0f}'


@app.template_filter('datefmt')
def fmt_date(d):
    if d is None:
        return '—'
    if isinstance(d, str):
        try:
            d = date.fromisoformat(d)
        except ValueError:
            return d
    return d.strftime('%b %-d')


@app.template_filter('timeago')
def fmt_timeago(dt):
    if dt is None:
        return 'never'
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return dt
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    diff = int((now - dt).total_seconds())
    if diff < 60:
        return 'just now'
    if diff < 3600:
        return f'{diff // 60}m ago'
    if diff < 86400:
        return f'{diff // 3600}h ago'
    return f'{diff // 86400}d ago'


@app.template_filter('duration')
def fmt_duration(secs):
    if secs is None:
        return ''
    secs = int(secs)
    m, s = divmod(secs, 60)
    return f'{m}:{s:02d}'


# ── ROUTES ────────────────────────────────────────────────────────────────────

@app.route('/')
def dashboard():
    clients = db.get_all_clients_with_period_stats()
    if not clients:
        return redirect(url_for('settings'))

    client_id = request.args.get('client', type=int)
    active = next((c for c in clients if c['id'] == client_id), clients[0])

    active_period = db.get_active_period(active['id'])
    stats = db.get_client_stats(
        active['id'],
        period_start=active_period.get('period_start') if active_period else None,
    )
    filter_type = request.args.get('filter')
    videos = db.get_client_videos(active['id'], filter_type=filter_type, limit=30)
    top_products = db.get_top_products(active['id'])
    recent = db.get_recent_activity(active['id'])
    today = date.today()

    return render_template(
        'dashboard.html',
        clients=clients,
        active=active,
        stats=stats,
        videos=videos,
        top_products=top_products,
        recent=recent,
        active_period=active_period,
        today=today,
        filter_type=filter_type,
    )


@app.route('/products')
def products():
    client_id = request.args.get('client', type=int)
    all_products = db.get_all_products_with_stats()
    summary = db.get_product_summary_stats()
    clients = db.get_all_clients()

    if client_id:
        filtered = [p for p in all_products if p['client_id'] == client_id]
    else:
        filtered = all_products

    # Attach video strips to each product
    for p in filtered:
        p['video_strip'] = db.get_product_videos(p['product_id'])

    return render_template(
        'products.html',
        products=filtered,
        all_products=all_products,
        summary=summary,
        clients=clients,
        active_client_id=client_id,
    )


@app.route('/videos')
def videos():
    client_id = request.args.get('client', type=int)
    filter_type = request.args.get('filter')
    clients = db.get_all_clients()
    all_videos = db.get_all_videos(client_id=client_id, filter_type=filter_type)
    creator_handle = db.get_setting('creator_handle', '')
    return render_template(
        'videos.html',
        videos=all_videos,
        clients=clients,
        active_client_id=client_id,
        filter_type=filter_type,
        creator_handle=creator_handle,
    )


@app.route('/settings')
def settings():
    clients = db.get_all_clients()
    client_data = []
    for c in clients:
        client_data.append({
            **c,
            'products': db.get_client_products(c['id']),
            'active_period': db.get_active_period(c['id']),
            'period_history': db.get_period_history(c['id']),
        })
    creator_handle = db.get_setting('creator_handle', '')
    return render_template('settings.html', client_data=client_data,
                           creator_handle=creator_handle, today=date.today())


# ── SETTINGS ACTIONS ──────────────────────────────────────────────────────────

@app.route('/settings/creator/update', methods=['POST'])
def update_creator():
    handle = request.form.get('creator_handle', '').strip().lstrip('@')
    db.set_setting('creator_handle', handle)
    return redirect(url_for('settings'))


@app.route('/settings/clients/add', methods=['POST'])
def add_client():
    brand_name = request.form['brand_name'].strip()
    tiktok_handle = request.form.get('tiktok_handle', '').strip().lstrip('@')
    brand_color = request.form.get('brand_color', '#ffffff')
    post_target = int(request.form.get('post_target', 30))
    db.add_client(brand_name, tiktok_handle, brand_color, post_target)
    return redirect(url_for('settings'))


@app.route('/settings/clients/<int:client_id>/update', methods=['POST'])
def update_client(client_id):
    brand_name = request.form['brand_name'].strip()
    tiktok_handle = request.form.get('tiktok_handle', '').strip().lstrip('@')
    brand_color = request.form.get('brand_color', '#ffffff')
    post_target = int(request.form.get('post_target', 30))
    db.update_client(client_id, brand_name, tiktok_handle, brand_color, post_target)
    return redirect(url_for('settings'))


@app.route('/settings/clients/<int:client_id>/delete', methods=['POST'])
def delete_client(client_id):
    db.delete_client(client_id)
    return redirect(url_for('settings'))


@app.route('/settings/products/add', methods=['POST'])
def add_product():
    client_id = int(request.form['client_id'])
    product_id = request.form['product_id'].strip()
    # Try to auto-fill product name from TikHub
    product_name = tikhub.lookup_product_name(product_id) or f"Product {product_id}"
    db.add_product(client_id, product_id, product_name)
    return redirect(url_for('settings'))


@app.route('/settings/products/<int:product_db_id>/delete', methods=['POST'])
def delete_product(product_db_id):
    db.delete_product(product_db_id)
    return redirect(url_for('settings'))


@app.route('/settings/periods/<int:client_id>/start', methods=['POST'])
def start_period(client_id):
    period_start = request.form.get('period_start') or str(date.today())
    db.start_period(client_id, period_start)
    return redirect(url_for('settings'))


@app.route('/settings/periods/<int:period_id>/update', methods=['POST'])
def update_period(period_id):
    period_start = request.form.get('period_start')
    period_end = request.form.get('period_end')
    db.update_period(period_id, period_start, period_end)
    return redirect(url_for('settings'))


@app.route('/settings/periods/<int:period_id>/complete', methods=['POST'])
def complete_period(period_id):
    db.complete_period(period_id)
    return redirect(url_for('settings'))


# ── SYNC API ──────────────────────────────────────────────────────────────────

@app.route('/api/sync', methods=['POST'])
def trigger_sync_all():
    try:
        count = sync.sync_creator()
        return jsonify({'status': 'ok', 'videos_fetched': count})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/sync/<int:client_id>', methods=['POST'])
def trigger_sync_client(client_id):
    try:
        count = sync.sync_creator()
        return jsonify({'status': 'ok', 'videos_fetched': count})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/sync/gmv', methods=['POST'])
def trigger_sync_gmv():
    try:
        count = sync.sync_gmv()
        return jsonify({'status': 'ok', 'videos_enriched': count})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/debug/fetch', methods=['GET'])
def debug_fetch():
    import requests as _requests
    handle = db.get_setting('creator_handle')
    if not handle:
        return jsonify({'error': 'no creator_handle set'}), 400

    def _fetch(params):
        url = f"{tikhub.TIKHUB_BASE}/api/v1/tiktok/app/v3/fetch_user_post_videos"
        resp = _requests.get(url, headers=tikhub._headers(), params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    try:
        # Page 1
        p1 = _fetch({'unique_id': handle, 'count': 10})
        d1 = p1.get('data', {})
        ids1 = [str(v.get('aweme_id') or v.get('id')) for v in (d1.get('aweme_list') or [])]
        max_cursor_ms = d1.get('max_cursor')

        results = {
            'page1': {
                'video_ids': ids1,
                'has_more': d1.get('has_more'),
                'max_cursor': max_cursor_ms,
                'min_cursor': d1.get('min_cursor'),
            }
        }

        if max_cursor_ms:
            max_cursor_s = max_cursor_ms // 1000

            for label, params in [
                ('param_cursor_ms',  {'unique_id': handle, 'count': 10, 'cursor': max_cursor_ms}),
                ('param_cursor_sec', {'unique_id': handle, 'count': 10, 'cursor': max_cursor_s}),
                ('param_max_cursor', {'unique_id': handle, 'count': 10, 'max_cursor': max_cursor_ms}),
                ('param_max_cursor_sec', {'unique_id': handle, 'count': 10, 'max_cursor': max_cursor_s}),
                ('param_min_cursor', {'unique_id': handle, 'count': 10, 'min_cursor': max_cursor_ms}),
            ]:
                pr = _fetch(params)
                dr = pr.get('data', {})
                idsr = [str(v.get('aweme_id') or v.get('id')) for v in (dr.get('aweme_list') or [])]
                results[label] = {
                    'same_as_page1': idsr == ids1,
                    'video_ids': idsr,
                    'max_cursor': dr.get('max_cursor'),
                    'has_more': dr.get('has_more'),
                }

        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/videos/<video_id>/assign', methods=['POST'])
def assign_video(video_id):
    data = request.get_json()
    client_id = data.get('client_id') if data else None
    if not client_id:
        return jsonify({'status': 'error', 'message': 'client_id required'}), 400
    db.assign_video_to_client(video_id, int(client_id))
    return jsonify({'status': 'ok'})


# ── STARTUP ───────────────────────────────────────────────────────────────────

def create_app():
    try:
        db.init_db()
    except Exception as e:
        logger.error(f"DB init failed at startup: {e}")
        logger.error("App will start but DB operations will fail — check DATABASE_URL")

    try:
        scheduler = BackgroundScheduler()
        scheduler.add_job(sync.sync_all, 'interval', hours=24, id='daily_sync')
        scheduler.start()
    except Exception as e:
        logger.error(f"Scheduler failed to start: {e}")

    return app


if __name__ == '__main__':
    create_app()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
else:
    create_app()
