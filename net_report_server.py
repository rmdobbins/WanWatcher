#!/usr/bin/env python3
"""Lightweight web server for WanWatcher reports."""

from __future__ import annotations

import argparse
import html
import os
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from urllib.parse import parse_qs, urlencode, urlparse

os.environ.setdefault("MPLBACKEND", "Agg")
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import net_quality_report
import net_trace_view
import net_view

DEFAULTS = {
    "db": "netstats.db",
    "iface": "TOTAL",
    "host": "",
    "minutes": 240,
    "trace_hours": 24.0,
    "trace_runs": 50,
    "trace_target": "",
    "quality_hours": 24.0,
    "issues_days": 7,
    "latency_ms": 120.0,
    "jitter_ms": 25.0,
    "loss_pct": 1.0,
    "refresh": 60,
}

CACHE_TTL_S = 20
_CACHE: dict[str, tuple[float, bytes, str]] = {}


def _get_param(qs, key, default, cast=str, allow_empty=False):
    if key not in qs:
        return default
    value = qs[key][0]
    if value == "" and not allow_empty:
        return default
    try:
        return cast(value)
    except (ValueError, TypeError):
        return default


def _get_optional_float(qs, key, default):
    if key not in qs:
        return default
    value = qs[key][0].strip().lower()
    if value in ("", "0", "none", "all"):
        return None
    try:
        return float(value)
    except ValueError:
        return default


def _get_optional_int(qs, key, default):
    if key not in qs:
        return default
    value = qs[key][0].strip().lower()
    if value in ("", "0", "none", "all"):
        return None
    try:
        return int(value)
    except ValueError:
        return default


def _parse_params(qs):
    return {
        "db": _get_param(qs, "db", DEFAULTS["db"]),
        "iface": _get_param(qs, "iface", DEFAULTS["iface"]),
        "host": _get_param(qs, "host", DEFAULTS["host"], allow_empty=True),
        "minutes": _get_param(qs, "minutes", DEFAULTS["minutes"], int),
        "trace_hours": _get_optional_float(qs, "trace_hours", DEFAULTS["trace_hours"]),
        "trace_runs": _get_optional_int(qs, "trace_runs", DEFAULTS["trace_runs"]),
        "trace_target": _get_param(qs, "trace_target", DEFAULTS["trace_target"], allow_empty=True),
        "quality_hours": _get_optional_float(qs, "quality_hours", DEFAULTS["quality_hours"]),
        "issues_days": _get_param(qs, "issues_days", DEFAULTS["issues_days"], int),
        "latency_ms": _get_param(qs, "latency_ms", DEFAULTS["latency_ms"], float),
        "jitter_ms": _get_param(qs, "jitter_ms", DEFAULTS["jitter_ms"], float),
        "loss_pct": _get_param(qs, "loss_pct", DEFAULTS["loss_pct"], float),
        "refresh": _get_param(qs, "refresh", DEFAULTS["refresh"], int),
    }


def _cache_get(key):
    entry = _CACHE.get(key)
    if not entry:
        return None
    ts, payload, content_type = entry
    if time.time() - ts > CACHE_TTL_S:
        _CACHE.pop(key, None)
        return None
    return payload, content_type


def _cache_set(key, payload, content_type):
    _CACHE[key] = (time.time(), payload, content_type)


def _placeholder_png(text, width=9, height=3):
    fig, ax = plt.subplots(figsize=(width, height))
    ax.axis("off")
    ax.text(0.5, 0.5, text, ha="center", va="center", fontsize=12)
    fig.tight_layout()
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=130)
    plt.close(fig)
    return buf.getvalue()


def _db_exists(db_path):
    return os.path.isfile(db_path)


def _format_ts(ts):
    if ts is None:
        return "n/a"
    if hasattr(ts, "to_pydatetime"):
        ts = ts.to_pydatetime()
    if isinstance(ts, datetime):
        ts = ts.astimezone(timezone.utc)
        return ts.isoformat().replace("+00:00", "Z")
    return str(ts)


def _format_stat(stats, key):
    if not stats or stats.get(key) is None:
        return "n/a"
    return f"{stats[key]:.1f}"


def _build_quality_summary(params):
    if not _db_exists(params["db"]):
        return {"empty": True, "message": "DB not found."}
    df = net_quality_report.load_quality_data(
        params["db"],
        params["iface"],
        params["host"],
        params["quality_hours"],
    )
    if df.empty:
        return {"empty": True, "message": "No quality data."}

    df["high_latency"] = df["latency_ms"] >= params["latency_ms"]
    df["high_jitter"] = df["jitter_ms"] >= params["jitter_ms"]
    df["high_loss"] = df["loss_pct"] >= params["loss_pct"]
    df["any_issue"] = df[["high_latency", "high_jitter", "high_loss"]].any(axis=1)

    stats_latency = net_quality_report.describe_series(df["latency_ms"])
    stats_jitter = net_quality_report.describe_series(df["jitter_ms"])
    stats_loss = net_quality_report.describe_series(df["loss_pct"])
    total = len(df)

    def pct(series):
        return 0.0 if series.empty else float(series.mean() * 100.0)

    return {
        "empty": False,
        "rows": total,
        "start": df["ts"].iloc[0],
        "end": df["ts"].iloc[-1],
        "median_gap_s": float(df["gap_s"].median()) if "gap_s" in df else None,
        "stats_latency": stats_latency,
        "stats_jitter": stats_jitter,
        "stats_loss": stats_loss,
        "high_latency": (int(df["high_latency"].sum()), pct(df["high_latency"])),
        "high_jitter": (int(df["high_jitter"].sum()), pct(df["high_jitter"])),
        "high_loss": (int(df["high_loss"].sum()), pct(df["high_loss"])),
        "any_issue": (int(df["any_issue"].sum()), pct(df["any_issue"])),
    }


def _build_trace_summary(params):
    if not _db_exists(params["db"]):
        return {"empty": True, "message": "DB not found."}
    target = params["trace_target"]
    if not target:
        import sqlite3

        con = sqlite3.connect(params["db"])
        try:
            target = net_trace_view.default_target(con, params["host"] or None)
        finally:
            con.close()
    if not target:
        return {"empty": True, "message": "No traceroute targets found."}

    runs_df, df = net_trace_view.load_trace_data(
        params["db"],
        target,
        params["host"] or None,
        params["trace_hours"],
        params["trace_runs"],
    )
    if runs_df.empty or df.empty:
        return {"empty": True, "message": "No traceroute data."}

    route_keys = net_trace_view.build_route_keys(df)
    runs_sorted, route_id_map = net_trace_view.annotate_routes(runs_df, route_keys)
    last = runs_df.iloc[0]

    return {
        "empty": False,
        "target": target,
        "runs": len(runs_df),
        "start": df["ts"].min(),
        "end": df["ts"].max(),
        "route_count": len(route_id_map),
        "last_ts": last["ts_utc"],
        "last_exit": last["exit_code"],
        "last_hops": last["hop_count"],
        "route_change_count": int((runs_sorted["route_id"] != runs_sorted["route_id"].shift()).sum()) - 1,
    }


def _render_latency_png(params):
    if not _db_exists(params["db"]):
        return _placeholder_png("DB not found."), "image/png"
    df = net_view.load_data(params["db"], params["iface"], params["minutes"], host=params["host"] or None)
    if df.empty:
        return _placeholder_png("No latency data."), "image/png"
    buf = BytesIO()
    net_view.plot_latency(df, params["iface"], params["host"] or "any", params["minutes"], buf)
    return buf.getvalue(), "image/png"


def _render_throughput_png(params):
    if not _db_exists(params["db"]):
        return _placeholder_png("DB not found."), "image/png"
    df = net_view.load_data(params["db"], params["iface"], params["minutes"], host=params["host"] or None)
    if df.empty:
        return _placeholder_png("No throughput data."), "image/png"
    has_thr = "thr_down_mbps" in df.columns or "thr_up_mbps" in df.columns
    if not has_thr:
        return _placeholder_png("Throughput not recorded."), "image/png"
    buf = BytesIO()
    net_view.plot_throughput(df, params["iface"], params["host"] or "any", params["minutes"], buf)
    return buf.getvalue(), "image/png"


def _render_trace_stats_png(params):
    if not _db_exists(params["db"]):
        return _placeholder_png("DB not found."), "image/png"
    target = params["trace_target"]
    if not target:
        import sqlite3

        con = sqlite3.connect(params["db"])
        try:
            target = net_trace_view.default_target(con, params["host"] or None)
        finally:
            con.close()
    if not target:
        return _placeholder_png("No traceroute target."), "image/png"

    runs_df, df = net_trace_view.load_trace_data(
        params["db"],
        target,
        params["host"] or None,
        params["trace_hours"],
        params["trace_runs"],
    )
    if runs_df.empty or df.empty:
        return _placeholder_png("No traceroute data."), "image/png"

    title = f"Traceroute hop stats | target={target} host={params['host'] or 'any'}"
    buf = BytesIO()
    made = net_trace_view.plot_hop_stats(df, title, buf)
    plt.close("all")
    if not made:
        return _placeholder_png("Traceroute stats not available."), "image/png"
    return buf.getvalue(), "image/png"


def _render_issues_png(params):
    if not _db_exists(params["db"]):
        return _placeholder_png("DB not found."), "image/png"
    df = net_quality_report.load_quality_data(
        params["db"],
        params["iface"],
        params["host"],
        params["quality_hours"],
    )
    if df.empty:
        return _placeholder_png("No quality data."), "image/png"

    df["high_latency"] = df["latency_ms"] >= params["latency_ms"]
    df["high_jitter"] = df["jitter_ms"] >= params["jitter_ms"]
    df["high_loss"] = df["loss_pct"] >= params["loss_pct"]
    df["any_issue"] = df[["high_latency", "high_jitter", "high_loss"]].any(axis=1)

    buf = BytesIO()
    net_quality_report.plot_daily_issue_lines(df, params["issues_days"], buf)
    if buf.getbuffer().nbytes == 0:
        return _placeholder_png("Issues plot not available."), "image/png"
    return buf.getvalue(), "image/png"


def _render_index(params):
    quality = _build_quality_summary(params)
    trace = _build_trace_summary(params)
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    query = urlencode({k: v for k, v in params.items() if v is not None})

    refresh_meta = ""
    if params["refresh"] and params["refresh"] > 0:
        refresh_meta = f'<meta http-equiv="refresh" content="{params["refresh"]}">'

    def row(label, value):
        return f"<tr><th>{html.escape(label)}</th><td>{value}</td></tr>"

    quality_rows = []
    if quality.get("empty"):
        quality_rows.append(row("Status", html.escape(quality.get("message", "No data"))))
    else:
        quality_rows.extend(
            [
                row("Window", f"{_format_ts(quality['start'])} -> {_format_ts(quality['end'])}"),
                row("Samples", str(quality["rows"])),
                row("Median gap", f"{quality['median_gap_s']:.1f}s" if quality.get("median_gap_s") else "n/a"),
                row(
                    "Latency (ms)",
                    f"median { _format_stat(quality['stats_latency'], 'median') } | "
                    f"p95 { _format_stat(quality['stats_latency'], 'p95') } | "
                    f"p99 { _format_stat(quality['stats_latency'], 'p99') } | "
                    f"max { _format_stat(quality['stats_latency'], 'max') }",
                ),
                row(
                    "Jitter (ms)",
                    f"median { _format_stat(quality['stats_jitter'], 'median') } | "
                    f"p95 { _format_stat(quality['stats_jitter'], 'p95') } | "
                    f"p99 { _format_stat(quality['stats_jitter'], 'p99') } | "
                    f"max { _format_stat(quality['stats_jitter'], 'max') }",
                ),
                row(
                    "Loss (%)",
                    f"median { _format_stat(quality['stats_loss'], 'median') } | "
                    f"p95 { _format_stat(quality['stats_loss'], 'p95') } | "
                    f"p99 { _format_stat(quality['stats_loss'], 'p99') } | "
                    f"max { _format_stat(quality['stats_loss'], 'max') }",
                ),
                row(
                    "High latency",
                    f"{quality['high_latency'][0]} "
                    f"({quality['high_latency'][1]:.2f}%)",
                ),
                row(
                    "High jitter",
                    f"{quality['high_jitter'][0]} "
                    f"({quality['high_jitter'][1]:.2f}%)",
                ),
                row(
                    "Packet loss",
                    f"{quality['high_loss'][0]} "
                    f"({quality['high_loss'][1]:.2f}%)",
                ),
                row(
                    "Any issue",
                    f"{quality['any_issue'][0]} "
                    f"({quality['any_issue'][1]:.2f}%)",
                ),
            ]
        )

    trace_rows = []
    if trace.get("empty"):
        trace_rows.append(row("Status", html.escape(trace.get("message", "No data"))))
    else:
        trace_rows.extend(
            [
                row("Target", html.escape(str(trace["target"]))),
                row("Runs", str(trace["runs"])),
                row("Window", f"{_format_ts(trace['start'])} -> {_format_ts(trace['end'])}"),
                row("Routes", f"{trace['route_count']}"),
                row("Route changes", str(trace["route_change_count"])),
                row("Last run", f"{_format_ts(trace['last_ts'])}"),
                row("Last exit", str(trace["last_exit"])),
                row("Last hops", str(trace["last_hops"])),
            ]
        )

    def value(name):
        v = params.get(name)
        return html.escape("" if v is None else str(v))

    form_html = f"""
      <form class="filters" method="get">
        <label>DB <input name="db" value="{value('db')}"></label>
        <label>Interface <input name="iface" value="{value('iface')}"></label>
        <label>Host label <input name="host" value="{value('host')}"></label>
        <label>Minutes <input name="minutes" type="number" min="1" value="{value('minutes')}"></label>
        <label>Trace hours <input name="trace_hours" value="{value('trace_hours')}"></label>
        <label>Trace runs <input name="trace_runs" value="{value('trace_runs')}"></label>
        <label>Trace target <input name="trace_target" value="{value('trace_target')}"></label>
        <label>Quality hours <input name="quality_hours" value="{value('quality_hours')}"></label>
        <label>Issues days <input name="issues_days" value="{value('issues_days')}"></label>
        <label>Latency ms <input name="latency_ms" value="{value('latency_ms')}"></label>
        <label>Jitter ms <input name="jitter_ms" value="{value('jitter_ms')}"></label>
        <label>Loss % <input name="loss_pct" value="{value('loss_pct')}"></label>
        <label>Refresh s <input name="refresh" value="{value('refresh')}"></label>
        <button type="submit">Apply</button>
      </form>
    """

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  {refresh_meta}
  <title>WanWatcher Report</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;600&family=Space+Grotesk:wght@500;700&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg: #f3efe8;
      --panel: #fffdf9;
      --ink: #1f2a33;
      --muted: #5c6770;
      --accent: #c46a3c;
      --accent-2: #2f6f6e;
      --border: #e2d6c7;
      --shadow: rgba(41, 30, 16, 0.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background: radial-gradient(circle at top left, #f6f1e8 0%, #efe6da 45%, #e7dacb 100%);
      min-height: 100vh;
      padding: 32px;
    }}
    header {{
      display: flex;
      flex-wrap: wrap;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 24px;
    }}
    header h1 {{
      font-family: "Space Grotesk", sans-serif;
      font-size: 32px;
      margin: 0;
      letter-spacing: -0.5px;
    }}
    header .meta {{
      color: var(--muted);
      font-size: 14px;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 18px;
      margin-bottom: 28px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 16px 18px;
      box-shadow: 0 10px 30px var(--shadow);
      animation: rise 420ms ease-out both;
    }}
    .card h2 {{
      font-family: "Space Grotesk", sans-serif;
      font-size: 18px;
      margin: 0 0 12px 0;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      text-align: left;
      padding: 6px 4px;
      border-bottom: 1px solid rgba(0,0,0,0.05);
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      width: 45%;
    }}
    .charts {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 18px;
    }}
    .chart {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 10px 30px var(--shadow);
      animation: fade 520ms ease-out both;
    }}
    .chart h3 {{
      font-family: "Space Grotesk", sans-serif;
      font-size: 16px;
      margin: 0 0 12px 0;
      color: var(--accent);
    }}
    img {{
      width: 100%;
      height: auto;
      border-radius: 10px;
      border: 1px solid rgba(0, 0, 0, 0.08);
      background: #fff;
    }}
    details {{
      margin-top: 24px;
      background: rgba(255, 255, 255, 0.55);
      border-radius: 14px;
      padding: 12px 16px;
      border: 1px dashed var(--border);
    }}
    summary {{
      font-family: "Space Grotesk", sans-serif;
      cursor: pointer;
      color: var(--accent-2);
      font-weight: 600;
    }}
    .filters {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px 16px;
      margin-top: 12px;
    }}
    .filters label {{
      display: flex;
      flex-direction: column;
      gap: 6px;
      font-size: 13px;
      color: var(--muted);
    }}
    .filters input {{
      padding: 8px 10px;
      border-radius: 10px;
      border: 1px solid var(--border);
      font-size: 14px;
    }}
    .filters button {{
      align-self: end;
      padding: 10px 14px;
      border: none;
      border-radius: 12px;
      background: var(--accent);
      color: #fff;
      font-weight: 600;
      cursor: pointer;
      box-shadow: 0 8px 16px rgba(196, 106, 60, 0.25);
    }}
    .filters button:hover {{
      filter: brightness(1.05);
    }}
    @keyframes rise {{
      from {{ opacity: 0; transform: translateY(12px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    @keyframes fade {{
      from {{ opacity: 0; transform: translateY(16px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    @media (max-width: 700px) {{
      body {{ padding: 20px; }}
      header h1 {{ font-size: 26px; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>WanWatcher Report</h1>
    <div class="meta">DB: {html.escape(params["db"])} | Generated: {now}</div>
  </header>

  <section class="cards">
    <article class="card">
      <h2>Quality Summary</h2>
      <table>
        {''.join(quality_rows)}
      </table>
    </article>
    <article class="card">
      <h2>Traceroute Summary</h2>
      <table>
        {''.join(trace_rows)}
      </table>
    </article>
  </section>

  <section class="charts">
    <article class="chart">
      <h3>Latency / Availability (last {params["minutes"]} min)</h3>
      <img src="/img/latency.png?{query}" alt="Latency chart">
    </article>
    <article class="chart">
      <h3>Throughput (last {params["minutes"]} min)</h3>
      <img src="/img/throughput.png?{query}" alt="Throughput chart">
    </article>
    <article class="chart">
      <h3>Traceroute Hop Stats</h3>
      <img src="/img/trace_stats.png?{query}" alt="Traceroute hop stats">
    </article>
    <article class="chart">
      <h3>Issues by Hour (last {params["issues_days"]} days)</h3>
      <img src="/img/issues.png?{query}" alt="Issues by hour chart">
    </article>
  </section>

  <details>
    <summary>Filters and thresholds</summary>
    {form_html}
  </details>
</body>
</html>
"""


class ReportHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("", "/"):
            qs = parse_qs(parsed.query)
            params = _parse_params(qs)
            html_body = _render_index(params)
            payload = html_body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path.startswith("/img/"):
            qs = parse_qs(parsed.query)
            params = _parse_params(qs)
            name = parsed.path.replace("/img/", "").lower()
            cache_key = f"{name}?{urlencode(params)}"
            cached = _cache_get(cache_key)
            if cached:
                payload, content_type = cached
            else:
                if name == "latency.png":
                    payload, content_type = _render_latency_png(params)
                elif name == "throughput.png":
                    payload, content_type = _render_throughput_png(params)
                elif name == "trace_stats.png":
                    payload, content_type = _render_trace_stats_png(params)
                elif name == "issues.png":
                    payload, content_type = _render_issues_png(params)
                else:
                    self.send_error(404, "Unknown image")
                    return
                _cache_set(cache_key, payload, content_type)

            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        self.send_error(404, "Not found")

    def log_message(self, format, *args):
        return


def parse_args():
    ap = argparse.ArgumentParser(description="Serve WanWatcher reports over HTTP.")
    ap.add_argument("--bind", default="127.0.0.1", help="Bind address (use 0.0.0.0 for LAN access)")
    ap.add_argument("--port", type=int, default=8000, help="Port to serve on")
    ap.add_argument("--db", default=DEFAULTS["db"], help="SQLite DB path")
    ap.add_argument("--iface", default=DEFAULTS["iface"], help="Interface label (e.g., TOTAL)")
    ap.add_argument("--host-label", default=DEFAULTS["host"], help="Host label filter")
    ap.add_argument("--minutes", type=int, default=DEFAULTS["minutes"], help="Minutes back for charts")
    ap.add_argument("--trace-hours", type=float, default=DEFAULTS["trace_hours"], help="Traceroute lookback window")
    ap.add_argument("--trace-runs", type=int, default=DEFAULTS["trace_runs"], help="Max traceroute runs")
    ap.add_argument("--trace-target", default=DEFAULTS["trace_target"], help="Traceroute target (default: latest)")
    ap.add_argument("--quality-hours", type=float, default=DEFAULTS["quality_hours"], help="Quality summary lookback")
    ap.add_argument("--issues-days", type=int, default=DEFAULTS["issues_days"], help="Days for issues plot")
    ap.add_argument("--latency-ms", type=float, default=DEFAULTS["latency_ms"], help="Latency threshold (ms)")
    ap.add_argument("--jitter-ms", type=float, default=DEFAULTS["jitter_ms"], help="Jitter threshold (ms)")
    ap.add_argument("--loss-pct", type=float, default=DEFAULTS["loss_pct"], help="Loss threshold (%)")
    ap.add_argument("--refresh", type=int, default=DEFAULTS["refresh"], help="Auto-refresh seconds (0 disables)")
    return ap.parse_args()


def main():
    args = parse_args()
    DEFAULTS.update(
        {
            "db": args.db,
            "iface": args.iface,
            "host": args.host_label,
            "minutes": args.minutes,
            "trace_hours": args.trace_hours,
            "trace_runs": args.trace_runs,
            "trace_target": args.trace_target,
            "quality_hours": args.quality_hours,
            "issues_days": args.issues_days,
            "latency_ms": args.latency_ms,
            "jitter_ms": args.jitter_ms,
            "loss_pct": args.loss_pct,
            "refresh": args.refresh,
        }
    )

    server = ThreadingHTTPServer((args.bind, args.port), ReportHandler)
    print(f"WanWatcher report server: http://{args.bind}:{args.port}/")
    server.serve_forever()


if __name__ == "__main__":
    main()
