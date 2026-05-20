from flask import Flask, render_template, request, redirect
import pandas as pd
import re
import os
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
    if timeframe == '15m':
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

    if not records:
        raise ValueError("No valid records found in the CSV file.")

    rdf = pd.DataFrame(records)
    rdf = rdf.drop_duplicates(subset=['date_label', 'time'])

    date_order = sorted(rdf['date'].unique())
    date_labels = [f"{d.strftime('%b')} {d.day}" for d in date_order]
    seen = set()
    unique_dates = [x for x in date_labels if not (x in seen or seen.add(x))]

    time_df = rdf[['time', 'time_minutes']].drop_duplicates().sort_values('time_minutes')
    unique_times = list(time_df['time'])

    lookup = {(row['time'], row['date_label']): row['result'] for _, row in rdf.iterrows()}

    rows = []
    for ts in unique_times:
        cells = [lookup.get((ts, d)) for d in unique_dates]
        if any(c for c in cells):
            rows.append({'time': ts, 'cells': cells})

    return rows, unique_dates, timeframe

def get_saved_files():
    return sorted(f for f in os.listdir(UPLOAD_FOLDER) if f.lower().endswith('.csv'))

def render_result(rows, dates, timeframe, filename):
    up = sum(1 for r in rows for c in r['cells'] if c == 'Up')
    down = sum(1 for r in rows for c in r['cells'] if c == 'Down')
    total = up + down
    return render_template('result.html',
                           dates=dates,
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
    return render_template('index.html', saved_files=get_saved_files())

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files.get('file')
    if not file or not file.filename:
        return render_template('index.html', error="Please select a file.", saved_files=get_saved_files())
    if not file.filename.lower().endswith('.csv'):
        return render_template('index.html', error="Please upload a CSV file.", saved_files=get_saved_files())
    try:
        content = file.read().decode('utf-8-sig')
        safe_name = secure_filename(file.filename)
        with open(os.path.join(UPLOAD_FOLDER, safe_name), 'w', encoding='utf-8') as f:
            f.write(content)
        rows, dates, timeframe = process_csv(content)
        return render_result(rows, dates, timeframe, safe_name)
    except Exception as e:
        return render_template('index.html', error=str(e), saved_files=get_saved_files())

@app.route('/load/<filename>')
def load_file(filename):
    safe_name = secure_filename(filename)
    path = os.path.join(UPLOAD_FOLDER, safe_name)
    if not os.path.exists(path):
        return render_template('index.html', error="File not found.", saved_files=get_saved_files())
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        rows, dates, timeframe = process_csv(content)
        return render_result(rows, dates, timeframe, safe_name)
    except Exception as e:
        return render_template('index.html', error=str(e), saved_files=get_saved_files())

@app.route('/delete/<filename>', methods=['POST'])
def delete_file(filename):
    safe_name = secure_filename(filename)
    path = os.path.join(UPLOAD_FOLDER, safe_name)
    if os.path.exists(path):
        os.remove(path)
    return redirect('/')

if __name__ == '__main__':
    app.run(debug=True)
