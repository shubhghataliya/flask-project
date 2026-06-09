from flask import Flask, render_template, request, redirect, jsonify
import pandas as pd
import re
import os
import json
import time
import threading
import uuid
import requests as req
from io import StringIO
from datetime import datetime
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

MONTHS = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}

# ── Polymarket API config ─────────────────────────────────────────────────────
POLYMARKET_BASE = "https://gamma-api.polymarket.com"
REQUIRED_TITLE_SUBSTRING = "Up or Down"

TAG_CONFIG = {
    '5m':  {'tag_id': 102892, 'interval_minutes': 5},
    '15m': {'tag_id': 102467, 'interval_minutes': 15},
    '1h':  {'tag_id': 102175, 'interval_minutes': 60},
    '4h':  {'tag_id': 102531, 'interval_minutes': 240},
}

CRYPTO_CONFIG = {
    'btc':  {
        'name': 'Bitcoin',
        'keywords':     ['BTC', 'Bitcoin', 'bitcoin', 'BITCOIN'],
        'non_keywords': ['ETH', 'Ethereum', 'SOL', 'Solana', 'XRP', 'Ripple',
                         'DOGE', 'Dogecoin', 'HYPE', 'Hyperliquid', 'BNB', 'Binance'],
        'excluded_tags': ['39', '101312', '101267', '818', '100178', '102331', '102716'],
    },
    'eth':  {
        'name': 'Ethereum',
        'keywords':     ['ETH', 'Ethereum', 'ethereum', 'ETHEREUM'],
        'non_keywords': ['BTC', 'Bitcoin', 'SOL', 'Solana', 'XRP', 'Ripple',
                         'DOGE', 'Dogecoin', 'BNB', 'AVAX', 'MATIC', 'LINK'],
        'excluded_tags': ['235', '101312', '101267', '818', '100178', '102331', '102716'],
    },
    'sol':  {
        'name': 'Solana',
        'keywords':     ['SOL', 'Solana', 'solana', 'SOLANA'],
        'non_keywords': ['BTC', 'Bitcoin', 'ETH', 'Ethereum', 'XRP', 'DOGE'],
        'excluded_tags': ['39', '101312', '101267', '235', '100178', '102331', '102716'],
    },
    'xrp':  {
        'name': 'Ripple (XRP)',
        'keywords':     ['XRP', 'Ripple', 'ripple'],
        'non_keywords': ['BTC', 'Bitcoin', 'ETH', 'Ethereum', 'SOL', 'Solana',
                         'BNB', 'DOGE', 'HYPE'],
        'excluded_tags': ['39', '235', '818', '100178', '102331', '102716'],
    },
    'bnb':  {
        'name': 'BNB',
        'keywords':     ['BNB', 'Binance'],
        'non_keywords': ['BTC', 'Bitcoin', 'ETH', 'Ethereum', 'SOL', 'Solana',
                         'XRP', 'DOGE', 'HYPE'],
        'excluded_tags': ['39', '235', '818', '101267', '101312', '100178', '102331'],
    },
    'doge': {
        'name': 'Dogecoin',
        'keywords':     ['DOGE', 'Dogecoin', 'dogecoin'],
        'non_keywords': ['BTC', 'Bitcoin', 'ETH', 'Ethereum', 'SOL', 'XRP', 'BNB', 'HYPE'],
        'excluded_tags': ['39', '235', '818', '101267', '101312', '102331', '102716'],
    },
    'hype': {
        'name': 'Hyperliquid',
        'keywords':     ['HYPE', 'Hyperliquid', 'hyperliquid'],
        'non_keywords': ['BTC', 'Bitcoin', 'ETH', 'Ethereum', 'SOL', 'XRP', 'BNB', 'DOGE'],
        'excluded_tags': ['39', '235', '818', '101267', '101312', '100178', '102716'],
    },
}


def _get_resolution(market):
    """Return 'Up', 'Down', or None from raw API market dict."""
    if market.get('umaResolutionStatus') != 'resolved':
        return None
    prices = market.get('outcomePrices', [])
    outcomes = market.get('outcomes', [])
    if isinstance(prices, str):
        try: prices = json.loads(prices)
        except: return None
    if isinstance(outcomes, str):
        try: outcomes = json.loads(outcomes)
        except: return None
    for i, p in enumerate(prices):
        if str(p) == '1' or p == 1:
            return outcomes[i] if i < len(outcomes) else None
    return None


# ── Background job store ─────────────────────────────────────────────────────
_jobs = {}   # job_id -> dict with keys: status, fetched, total, result, error


def _et_to_utc(dt_str):
    """Parse a naive 'YYYY-MM-DDTHH:MM' string as US/Eastern (ET) and return a
    UTC-aware Timestamp, or None if empty/unparseable. DST is handled by the
    America/New_York zone."""
    if not dt_str:
        return None
    ts = pd.to_datetime(dt_str, errors='coerce')
    if pd.isna(ts):
        return None
    try:
        ts = ts.tz_localize('America/New_York', ambiguous=True, nonexistent='shift_forward')
    except Exception:
        ts = ts.tz_localize('America/New_York')
    return ts.tz_convert('UTC')


def fetch_markets_api(crypto, interval, count, job=None, start_dt=None, end_dt=None):
    """
    Fetch up to `count` resolved markets from Polymarket.
    If `start_dt` / `end_dt` (ISO datetime strings) are given, only markets whose
    close time (endDate, UTC) falls within [start_dt, end_dt] are returned.
    If `job` dict is provided, updates job['fetched'] live for progress polling.
    """
    cfg = CRYPTO_CONFIG[crypto]
    tag_id = TAG_CONFIG[interval]['tag_id']
    excluded = set(cfg['excluded_tags'])
    keywords = cfg['keywords']
    non_keywords = cfg['non_keywords']

    # Optional date-range bounds. Inputs are interpreted as US/Eastern (ET) and
    # converted to UTC to compare against each market's UTC close (endDate).
    start_ts = _et_to_utc(start_dt)
    end_ts   = _et_to_utc(end_dt)
    start_iso = start_ts.strftime('%Y-%m-%dT%H:%M:%SZ') if start_ts is not None else None
    end_iso   = end_ts.strftime('%Y-%m-%dT%H:%M:%SZ')   if end_ts   is not None else None

    all_markets = []
    seen_ids = set()
    cursor    = end_iso   # current upper bound string (end_date_max)
    cursor_ts = end_ts    # same bound as a Timestamp, for safe comparisons

    while len(all_markets) < count:
        params = {
            'closed': 'true',
            'limit': 100,
            'offset': 0,
            'order': 'endDate',
            'ascending': 'false',
            'tag_id': tag_id,
        }
        if excluded:
            params['exclude_tag_id'] = list(excluded)
        if cursor:
            params['end_date_max'] = cursor
        # Only add the lower bound when it is strictly below the upper bound;
        # the API rejects end_date_min >= end_date_max with a 422 "invalid time range".
        if start_iso:
            if cursor_ts is None or start_ts < cursor_ts:
                params['end_date_min'] = start_iso
            else:
                break   # range window collapsed — nothing left to fetch

        resp = req.get(f"{POLYMARKET_BASE}/events", params=params, timeout=30)
        resp.raise_for_status()
        events = resp.json()
        if not events:
            break

        batch_markets = []
        for event in events:
            title = event.get('title', '') or ''
            if REQUIRED_TITLE_SUBSTRING not in title:
                continue
            if not any(kw in title for kw in keywords):
                continue
            if any(kw in title for kw in non_keywords):
                continue
            event_tag_ids = {str(t.get('id', '')) for t in (event.get('tags') or [])}
            if event_tag_ids & excluded:
                continue
            for m in event.get('markets', []):
                mid = m.get('id')
                if not mid or mid in seen_ids:
                    continue
                # Keep only markets whose own close time is inside the range.
                if start_ts is not None or end_ts is not None:
                    edt = pd.to_datetime(m.get('endDate'), utc=True, errors='coerce')
                    if pd.isna(edt):
                        continue
                    if start_ts is not None and edt < start_ts:
                        continue
                    if end_ts is not None and edt > end_ts:
                        continue
                seen_ids.add(mid)
                m['_event_title'] = title
                batch_markets.append(m)

        if batch_markets:
            all_markets.extend(batch_markets)
            if job is not None:
                job['fetched'] = min(len(all_markets), count)

        oldest = min((e.get('endDate', '') for e in events if e.get('endDate')), default=None)
        if not oldest or oldest == cursor:
            break
        oldest_ts = pd.to_datetime(oldest, utc=True, errors='coerce')
        # Stop once we've reached (or passed) the start of the requested range.
        if start_ts is not None and pd.notna(oldest_ts) and oldest_ts <= start_ts:
            break
        cursor    = oldest
        cursor_ts = oldest_ts
        time.sleep(0.2)

    all_markets.sort(key=lambda m: m.get('endDate', ''), reverse=True)
    return all_markets[:count]


def _run_fetch_job(job_id, crypto, interval, count, start_dt=None, end_dt=None):
    job = _jobs[job_id]
    try:
        markets = fetch_markets_api(crypto, interval, count, job, start_dt, end_dt)
        if not markets:
            job['status'] = 'error'
            job['error'] = ('No markets found for the selected date range. '
                            'Try widening the dates, or a different coin/interval.') \
                           if (start_dt or end_dt) \
                           else 'No markets returned from API. Try a different coin or interval.'
            return
        rows, dates, dates_iso, timeframe = process_markets(markets, interval)
        job['result'] = (rows, dates, dates_iso, timeframe,
                         f"{crypto.upper()}_{interval}_live_{len(markets)}")
        job['fetched'] = len(markets)
        job['status'] = 'done'
    except Exception as e:
        job['status'] = 'error'
        job['error'] = str(e)


def process_markets(markets, interval):
    """Convert raw API markets into pivot table data (same output as process_csv)."""
    if interval in ('5m', '15m'):
        parse_fn = parse_question_15m
    elif interval == '4h':
        parse_fn = parse_question_4h
    else:
        parse_fn = parse_question_1h

    records = []
    for m in markets:
        question = m.get('question') or m.get('_event_title') or ''
        month, day, time_str = parse_fn(str(question))
        if not month:
            continue
        edt = pd.to_datetime(m.get('endDate'), utc=True, errors='coerce')
        if pd.isna(edt):
            continue
        year = edt.year
        if month == 12 and edt.month == 1:
            year -= 1
        elif month == 1 and edt.month == 12:
            year += 1
        try:
            d = datetime(year, month, day)
        except ValueError:
            continue
        result = _get_resolution(m)
        if not result:
            continue
        records.append({
            'date': d,
            'date_label': f"{d.strftime('%b')} {d.day}",
            'time': time_str,
            'time_minutes': time_to_minutes(time_str),
            'result': result,
        })

    return _build_pivot(records, interval)


def _build_pivot(records, timeframe):
    """Shared pivot builder used by both process_csv and process_markets."""
    if not records:
        raise ValueError("No valid records found.")

    rdf = pd.DataFrame(records)
    rdf = rdf.drop_duplicates(subset=['date_label', 'time'])

    date_order = sorted(rdf['date'].unique())
    date_labels = [f"{d.strftime('%b')} {d.day}" for d in date_order]
    seen = set()
    unique_dates = [x for x in date_labels if not (x in seen or seen.add(x))]
    dates_iso = [d.strftime('%Y-%m-%d') for d in date_order]

    time_df = rdf[['time', 'time_minutes']].drop_duplicates().sort_values('time_minutes')
    unique_times = list(time_df['time'])

    lookup = {(row['time'], row['date_label']): row['result'] for _, row in rdf.iterrows()}

    rows = []
    for ts in unique_times:
        cells = [lookup.get((ts, d)) for d in unique_dates]
        if any(c for c in cells):
            rows.append({'time': ts, 'cells': cells})

    return rows, unique_dates, dates_iso, timeframe

def parse_question_1h(question):
    m = re.search(r'-\s+(\w+)\s+(\d+),\s+(\d+(?:AM|PM))\s+ET', question, re.IGNORECASE)
    if not m:
        return None, None, None
    month = MONTHS.get(m.group(1).lower())
    return month, int(m.group(2)), m.group(3).upper()

def parse_question_15m(question):
    m = re.search(r'-\s+(\w+)\s+(\d+),\s+(\d+:\d+(?:AM|PM))-\d+:\d+(?:AM|PM)\s+ET', question, re.IGNORECASE)
    if not m:
        return None, None, None
    month = MONTHS.get(m.group(1).lower())
    return month, int(m.group(2)), m.group(3).upper()

def parse_question_4h(question):
    # Matches "- January 1, 8:00AM-12:00PM ET" or "- January 1, 12AM-4AM ET" style ranges
    m = re.search(r'-\s+(\w+)\s+(\d+),\s+(\d+(?::\d+)?(?:AM|PM))-\d+(?::\d+)?(?:AM|PM)\s+ET', question, re.IGNORECASE)
    if not m:
        # Fallback: plain hour like 1h format
        return parse_question_1h(question)
    month = MONTHS.get(m.group(1).lower())
    return month, int(m.group(2)), m.group(3).upper()

def time_to_minutes(t):
    m = re.match(r'(\d+)(?::(\d+))?(AM|PM)', t, re.IGNORECASE)
    if not m:
        return -1
    h, mins, p = int(m.group(1)), int(m.group(2) or 0), m.group(3).upper()
    if p == 'AM':
        h = 0 if h == 12 else h
    else:
        h = 12 if h == 12 else h + 12
    return h * 60 + mins

def detect_timeframe(df):
    slugs = df['Slug'].dropna().astype(str)
    if slugs.str.contains(r'-5m-', case=False, regex=True).any():
        return '5m'
    if slugs.str.contains(r'-15m-', case=False, regex=True).any():
        return '15m'
    if slugs.str.contains(r'-4h-', case=False, regex=True).any():
        return '4h'
    return '1h'

def process_csv(content):
    df = pd.read_csv(StringIO(content))
    df = df.drop_duplicates(subset=['Market ID'])
    df['_edt'] = pd.to_datetime(df['End Date'], utc=True, errors='coerce')
    df = df.dropna(subset=['_edt', 'Resolution Result'])

    timeframe = detect_timeframe(df)
    if timeframe in ('5m', '15m'):
        parse_question = parse_question_15m
    elif timeframe == '4h':
        parse_question = parse_question_4h
    else:
        parse_question = parse_question_1h

    records = []
    for _, row in df.iterrows():
        month, day, time_str = parse_question(str(row.get('Question', '')))
        if not month:
            continue
        edt = row['_edt']
        year = edt.year
        if month == 12 and edt.month == 1:
            year -= 1
        elif month == 1 and edt.month == 12:
            year += 1
        try:
            d = datetime(year, month, day)
        except ValueError:
            continue
        records.append({
            'date': d,
            'date_label': f"{d.strftime('%b')} {d.day}",
            'time': time_str,
            'time_minutes': time_to_minutes(time_str),
            'result': str(row['Resolution Result']).strip()
        })

    return _build_pivot(records, timeframe)

def get_saved_files():
    return sorted(f for f in os.listdir(UPLOAD_FOLDER) if f.lower().endswith('.csv'))

def render_result(rows, dates, dates_iso, timeframe, filename):
    up = sum(1 for r in rows for c in r['cells'] if c == 'Up')
    down = sum(1 for r in rows for c in r['cells'] if c == 'Down')
    total = up + down
    return render_template('result.html',
                           dates=dates,
                           dates_iso=dates_iso,
                           rows=rows,
                           up_count=up,
                           down_count=down,
                           total=total,
                           timeframe=timeframe,
                           filename=filename,
                           saved_files=get_saved_files(),
                           up_pct=round(up / total * 100, 1) if total else 0)

@app.route('/')
def index():
    return render_template('result.html',
                           dates=[], dates_iso=[], rows=[],
                           up_count=0, down_count=0, total=0,
                           timeframe='15m', filename='',
                           saved_files=get_saved_files(), up_pct=0)

@app.route('/load/<filename>')
def load_file(filename):
    safe_name = secure_filename(filename)
    path = os.path.join(UPLOAD_FOLDER, safe_name)
    if not os.path.exists(path):
        return redirect('/')
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        rows, dates, dates_iso, timeframe = process_csv(content)
        return render_result(rows, dates, dates_iso, timeframe, safe_name)
    except Exception:
        return redirect('/')

@app.route('/fetch', methods=['POST'])
def fetch_start():
    crypto   = request.form.get('crypto', 'btc').lower()
    interval = request.form.get('interval', '15m').lower()
    start_dt = request.form.get('start') or None
    end_dt   = request.form.get('end') or None
    try:
        count = max(1, int(request.form.get('count', 2000)))
    except ValueError:
        count = 2000

    if crypto not in CRYPTO_CONFIG:
        return jsonify({'error': f'Unknown coin: {crypto}'}), 400
    if interval not in TAG_CONFIG:
        return jsonify({'error': f'Unknown interval: {interval}'}), 400

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {'status': 'running', 'fetched': 0, 'total': count, 'result': None, 'error': None}

    t = threading.Thread(target=_run_fetch_job,
                         args=(job_id, crypto, interval, count, start_dt, end_dt), daemon=True)
    t.start()

    return jsonify({'job_id': job_id})


@app.route('/fetch-status/<job_id>')
def fetch_status(job_id):
    job = _jobs.get(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify({
        'status':  job['status'],
        'fetched': job['fetched'],
        'total':   job['total'],
        'error':   job['error'],
    })


@app.route('/fetch-result/<job_id>')
def fetch_result(job_id):
    job = _jobs.pop(job_id, None)
    if not job or job['status'] != 'done' or not job['result']:
        return redirect('/')
    rows, dates, dates_iso, timeframe, filename = job['result']
    return render_result(rows, dates, dates_iso, timeframe, filename)


@app.route('/delete/<filename>', methods=['POST'])
def delete_file(filename):
    safe_name = secure_filename(filename)
    path = os.path.join(UPLOAD_FOLDER, safe_name)
    if os.path.exists(path):
        os.remove(path)
    return redirect('/')

if __name__ == '__main__':
    app.run(debug=True)
