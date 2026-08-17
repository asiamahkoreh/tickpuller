"""
Tick Data Puller
-----------------
Built by Asiamah Koreh for Birim Capital.

A simple local web app for pulling historical FX tick data (via the `duka`
library / Dukascopy feed) for backtesting. Runs entirely on your machine —
Flask just serves a page in your browser so you don't have to touch the
command line for day-to-day pulls.

SETUP (one-time):
    pip install flask duka

RUN:
    python app.py

Then open your browser to:
    http://127.0.0.1:5000

HOW IT WORKS:
    - You pick a symbol, a start date, and an end date (can be years apart).
    - The app splits the range into monthly chunks and downloads each chunk
      in the background using duka's `app()` function, one at a time, so it
      doesn't hammer the data provider or lock up.
    - Progress and any errors show live on the page (auto-refreshes).
    - Finished files land in the `tickdata/<SYMBOL>/` folder, one CSV per
      month, plus a combined CSV once all chunks finish.
"""

import os
import csv
import glob
import threading
import traceback
from datetime import datetime, timedelta
from calendar import monthrange

from flask import Flask, request, jsonify, render_template_string

from duka.app.app import app as duka_app
from duka.core.utils import TimeFrame

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "tickdata")
os.makedirs(DATA_DIR, exist_ok=True)

app = Flask(__name__)

# ---------------------------------------------------------------------------
# In-memory job state (single job at a time, kept simple on purpose)
# ---------------------------------------------------------------------------
job_lock = threading.Lock()
job_state = {
    "running": False,
    "symbol": None,
    "total_chunks": 0,
    "done_chunks": 0,
    "current_label": "",
    "log": [],
    "error": None,
    "finished": False,
    "output_file": None,
}


def log(msg):
    job_state["log"].append(msg)
    # keep the log from growing forever
    if len(job_state["log"]) > 500:
        job_state["log"] = job_state["log"][-500:]


def month_chunks(start: datetime, end: datetime):
    """Split a date range into (chunk_start, chunk_end) month-sized pieces."""
    chunks = []
    cur = datetime(start.year, start.month, 1)
    while cur <= end:
        last_day = monthrange(cur.year, cur.month)[1]
        chunk_start = max(cur, start)
        chunk_end = min(datetime(cur.year, cur.month, last_day), end)
        chunks.append((chunk_start, chunk_end))
        # advance to first of next month
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1)
        else:
            cur = datetime(cur.year, cur.month + 1, 1)
    return chunks


def combine_csvs(symbol_dir, symbol, start, end):
    """Merge every CSV in symbol_dir into one sorted, deduped output file."""
    all_files = sorted(glob.glob(os.path.join(symbol_dir, "*.csv")))
    if not all_files:
        return None

    combined_path = os.path.join(
        symbol_dir,
        f"{symbol}_{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}_COMBINED.csv",
    )

    header = None
    rows = []
    for f in all_files:
        if f == combined_path:
            continue
        with open(f, newline="") as fh:
            reader = csv.reader(fh)
            file_header = next(reader, None)
            if file_header is None:
                continue
            if header is None:
                header = file_header
            for row in reader:
                if row:
                    rows.append(row)

    if header is None:
        return None

    # sort by first column (timestamp) and drop exact duplicate rows
    rows = sorted(set(tuple(r) for r in rows), key=lambda r: r[0])

    with open(combined_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)

    return combined_path


def run_job(symbol, start, end, threads):
    try:
        symbol_dir = os.path.join(DATA_DIR, symbol)
        os.makedirs(symbol_dir, exist_ok=True)

        chunks = month_chunks(start, end)
        job_state["total_chunks"] = len(chunks)
        job_state["done_chunks"] = 0
        log(f"Starting pull for {symbol}: {start.date()} to {end.date()} "
            f"({len(chunks)} monthly chunk(s))")

        for chunk_start, chunk_end in chunks:
            label = f"{chunk_start.strftime('%Y-%m')}"
            job_state["current_label"] = label
            log(f"Downloading {label} ...")
            try:
                duka_app(
                    symbols=[symbol],
                    start=chunk_start,
                    end=chunk_end,
                    threads=threads,
                    timeframe=TimeFrame.TICK,
                    folder=symbol_dir,
                    header=True,
                )
                log(f"  done: {label}")
            except Exception as chunk_err:
                # keep going on other months even if one fails
                log(f"  FAILED: {label} -> {chunk_err}")

            job_state["done_chunks"] += 1

        log("All chunks attempted. Combining into one file...")
        combined = combine_csvs(symbol_dir, symbol, start, end)
        if combined:
            job_state["output_file"] = combined
            log(f"Combined file ready: {combined}")
        else:
            log("No data was downloaded — check the log above for errors.")

        job_state["finished"] = True

    except Exception:
        job_state["error"] = traceback.format_exc()
        log("ERROR:\n" + job_state["error"])
    finally:
        job_state["running"] = False


@app.route("/")
def index():
    return render_template_string(PAGE)


@app.route("/start", methods=["POST"])
def start():
    with job_lock:
        if job_state["running"]:
            return jsonify({"ok": False, "message": "A job is already running."})

        data = request.get_json()
        symbol = data.get("symbol", "").strip().upper()
        start_str = data.get("start")
        end_str = data.get("end")
        threads = int(data.get("threads", 4))

        if not symbol or not start_str or not end_str:
            return jsonify({"ok": False, "message": "Symbol, start, and end are required."})

        try:
            start = datetime.strptime(start_str, "%Y-%m-%d")
            end = datetime.strptime(end_str, "%Y-%m-%d")
        except ValueError:
            return jsonify({"ok": False, "message": "Dates must be YYYY-MM-DD."})

        if end < start:
            return jsonify({"ok": False, "message": "End date must be after start date."})

        # reset state
        job_state.update({
            "running": True,
            "symbol": symbol,
            "total_chunks": 0,
            "done_chunks": 0,
            "current_label": "",
            "log": [],
            "error": None,
            "finished": False,
            "output_file": None,
        })

        t = threading.Thread(target=run_job, args=(symbol, start, end, threads), daemon=True)
        t.start()

    return jsonify({"ok": True})


@app.route("/status")
def status():
    return jsonify(job_state)


PAGE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Tick Data Puller — Birim Capital</title>
<style>
  :root {
    --bg: #0f1115; --panel: #171a21; --border: #262b36;
    --text: #e6e8ec; --muted: #8b92a3; --accent: #4f8cff; --good: #35c47a; --bad: #ff5d5d;
  }
  * { box-sizing: border-box; }
  body {
    background: var(--bg); color: var(--text);
    font-family: -apple-system, Segoe UI, Roboto, sans-serif;
    max-width: 720px; margin: 40px auto; padding: 0 20px;
  }
  h1 { font-size: 22px; margin-bottom: 4px; }
  p.sub { color: var(--muted); margin-top: 0; margin-bottom: 4px; }
  p.credit { color: var(--muted); font-size: 12.5px; margin-top: 0; margin-bottom: 28px; }
  .panel {
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 10px; padding: 20px; margin-bottom: 20px;
  }
  label { display: block; font-size: 13px; color: var(--muted); margin: 14px 0 6px; }
  input, select {
    width: 100%; padding: 10px 12px; border-radius: 6px;
    border: 1px solid var(--border); background: #10131a; color: var(--text);
    font-size: 14px;
  }
  .row { display: flex; gap: 12px; }
  .row > div { flex: 1; }
  button {
    margin-top: 20px; width: 100%; padding: 12px; border: none;
    border-radius: 6px; background: var(--accent); color: white;
    font-size: 15px; font-weight: 600; cursor: pointer;
  }
  button:disabled { background: #3a3f4b; cursor: not-allowed; }
  .bar-track {
    height: 8px; background: #10131a; border-radius: 4px; overflow: hidden; margin-top: 16px;
  }
  .bar-fill { height: 100%; background: var(--accent); width: 0%; transition: width .3s; }
  .status-line { margin-top: 10px; font-size: 13px; color: var(--muted); }
  .log {
    margin-top: 16px; background: #0b0d12; border: 1px solid var(--border);
    border-radius: 6px; padding: 12px; height: 220px; overflow-y: auto;
    font-family: Consolas, monospace; font-size: 12.5px; white-space: pre-wrap;
  }
  .done { color: var(--good); font-weight: 600; }
  .fail { color: var(--bad); font-weight: 600; }
</style>
</head>
<body>
  <h1>Tick Data Puller</h1>
  <p class="sub">Pull years of historical FX tick data for backtesting. Runs locally, saves to your tickdata folder.</p>
  <p class="credit">Built by Asiamah Koreh for Birim Capital</p>

  <div class="panel">
    <label>Symbol</label>
    <select id="symbol">
      <option>EURUSD</option>
      <option>GBPUSD</option>
      <option>USDJPY</option>
      <option>AUDUSD</option>
      <option>USDCHF</option>
      <option>USDCAD</option>
      <option>NZDUSD</option>
    </select>

    <div class="row">
      <div>
        <label>Start date</label>
        <input type="date" id="start" value="2024-01-01">
      </div>
      <div>
        <label>End date</label>
        <input type="date" id="end" value="2024-12-31">
      </div>
    </div>

    <label>Parallel threads (lower = safer, higher = faster but more likely to error)</label>
    <select id="threads">
      <option value="1">1 (safest)</option>
      <option value="2" selected>2</option>
      <option value="4">4</option>
    </select>

    <button id="startBtn" onclick="startJob()">Download</button>

    <div class="bar-track"><div class="bar-fill" id="barFill"></div></div>
    <div class="status-line" id="statusLine">Idle.</div>
    <div class="log" id="log"></div>
  </div>

<script>
async function startJob() {
  const symbol = document.getElementById('symbol').value;
  const start = document.getElementById('start').value;
  const end = document.getElementById('end').value;
  const threads = document.getElementById('threads').value;

  const res = await fetch('/start', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({symbol, start, end, threads})
  });
  const data = await res.json();
  if (!data.ok) {
    alert(data.message);
    return;
  }
  document.getElementById('startBtn').disabled = true;
  poll();
}

async function poll() {
  const res = await fetch('/status');
  const s = await res.json();

  const pct = s.total_chunks ? Math.round((s.done_chunks / s.total_chunks) * 100) : 0;
  document.getElementById('barFill').style.width = pct + '%';

  let statusText = s.running
    ? `Working on ${s.current_label || '...'}  (${s.done_chunks}/${s.total_chunks} chunks)`
    : (s.finished ? 'Finished.' : 'Idle.');
  document.getElementById('statusLine').innerHTML = s.finished
    ? '<span class="done">Finished — see log below.</span>'
    : (s.error ? '<span class="fail">Error — see log below.</span>' : statusText);

  document.getElementById('log').textContent = s.log.join('\\n');
  document.getElementById('log').scrollTop = document.getElementById('log').scrollHeight;

  if (s.running) {
    setTimeout(poll, 1000);
  } else {
    document.getElementById('startBtn').disabled = false;
  }
}

// resume polling on page load in case a job is already running
poll();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("Tick Data Puller running at http://127.0.0.1:5000")
    app.run(debug=False, port=5000)
