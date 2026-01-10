#!/usr/bin/env python3
"""Summarize packet loss, latency, and jitter patterns from the net_logger SQLite DB."""

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import pandas as pd
import matplotlib.pyplot as plt


def parse_args():
    ap = argparse.ArgumentParser(description="Analyze network quality patterns.")
    ap.add_argument("--db", default="netstats.db", help="SQLite DB path")
    ap.add_argument("--iface", default="TOTAL", help="Interface to analyze (e.g., TOTAL, Ethernet, Wi-Fi)")
    ap.add_argument("--host", default="", help="Optional host label filter")
    ap.add_argument("--since-hours", type=float, default=None, help="Limit analysis to the last N hours (default: entire DB)")
    ap.add_argument("--compare-days", type=int, default=7, help="Days back to include in day-to-day issue plot")
    ap.add_argument("--issues-plot", default="", help="Optional PNG path for the day-to-day issues line chart")
    ap.add_argument("--latency-ms", type=float, default=120.0, help="High-latency threshold (ping avg/ms)")
    ap.add_argument("--jitter-ms", type=float, default=25.0, help="High-jitter threshold (ping jitter/ms)")
    ap.add_argument("--loss-pct", type=float, default=1.0, help="High packet-loss threshold (%)")
    ap.add_argument("--top-hours", type=int, default=5, help="How many hours-of-day to surface for patterns")
    ap.add_argument("--span-limit", type=int, default=5, help="How many of the worst spans to print per metric")
    ap.add_argument("--max-gap-mult", type=float, default=3.0,
                    help="Join consecutive high samples if gap <= median_sample_gap * this factor")
    return ap.parse_args()


def load_quality_data(db_path: str, iface: str, host: str, since_hours: float | None):
    clauses = ["iface = ?"]
    params: List[Any] = [iface]
    if host:
        clauses.append("host = ?")
        params.append(host)
    if since_hours:
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        clauses.append("ts_utc >= ?")
        params.append(since.isoformat())

    where = " AND ".join(clauses)
    q = f"""
      SELECT ts_utc, host, iface,
             ping_avg_ms, ping_ms, ping_jitter_ms, ping_loss_pct,
             dns_ms, avail_ok
      FROM net_metrics
      WHERE {where}
      ORDER BY ts_utc ASC
    """
    con = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(q, con, params=params)
    finally:
        con.close()

    if df.empty:
        return df

    df["ts"] = pd.to_datetime(df["ts_utc"], utc=True)
    df["latency_ms"] = df["ping_avg_ms"].combine_first(df.get("ping_ms"))
    df["jitter_ms"] = df.get("ping_jitter_ms")
    df["loss_pct"] = df.get("ping_loss_pct")
    df["hour"] = df["ts"].dt.hour
    df["gap_s"] = df["ts"].diff().dt.total_seconds()
    return df


def percentile(series: pd.Series, pct: float):
    if series is None or series.dropna().empty:
        return None
    return float(series.dropna().quantile(pct / 100.0))


def describe_series(series: pd.Series) -> Dict[str, float]:
    if series is None or series.dropna().empty:
        return {}
    clean = series.dropna()
    return {
        "median": float(clean.median()),
        "p95": percentile(clean, 95),
        "p99": percentile(clean, 99),
        "max": float(clean.max()),
    }


def format_stats(name: str, stats: Dict[str, float]) -> str:
    if not stats:
        return f"{name}: no data"
    return (f"{name}: median={stats['median']:.1f} "
            f"p95={stats['p95']:.1f} p99={stats['p99']:.1f} max={stats['max']:.1f}")


def print_issue_histogram(df: pd.DataFrame, width: int = 40):
    """Text histogram for any-issue counts by hour-of-day (UTC)."""
    grouped = df.groupby("hour").agg(
        samples=("any_issue", "count"),
        issues=("any_issue", "sum"),
    )
    grouped = grouped.reindex(range(24), fill_value=0)
    max_issues = grouped["issues"].max()
    if max_issues <= 0:
        print("Issues/hour histogram: no issues to plot")
        return

    print("Issues/hour histogram (UTC):")
    for hour in range(24):
        row = grouped.loc[hour]
        issues = int(row["issues"])
        samples = int(row["samples"])
        rate = (issues / samples * 100.0) if samples > 0 else 0.0
        bar_len = int(round((issues / max_issues) * width)) if max_issues else 0
        bar = "#" * bar_len
        print(f"  {hour:02d} | {bar:<{width}} | issues={issues:4d} rate={rate:5.1f}% samples={samples:5d}")


def plot_daily_issue_lines(df: pd.DataFrame, days: int, out_path: str):
    """Plot hourly issue rates for each of the last N days, colored per day."""
    if not out_path:
        return
    if df.empty:
        print("Issues line plot skipped: no data")
        return
    end_ts = df["ts"].max()
    start_ts = end_ts - timedelta(days=days)
    recent = df[df["ts"] >= start_ts].copy()
    if recent.empty:
        print("Issues line plot skipped: no data in requested window")
        return

    recent["date"] = recent["ts"].dt.date
    fig, ax = plt.subplots(figsize=(10, 5))
    hours = list(range(24))

    for date, sub in recent.groupby("date"):
        rates = sub.groupby("hour")["any_issue"].mean().reindex(hours, fill_value=0.0) * 100.0
        ax.plot(hours, rates, marker="o", label=str(date))

    ax.set_title(f"Issues per Hour (last {days} days) | iface={recent['iface'].iat[0]}")
    ax.set_xlabel("Hour of day (UTC)")
    ax.set_ylabel("Issue rate (%)")
    ax.set_xticks(hours)
    ax.grid(True, alpha=0.3)
    ax.legend(title="Date", fontsize="small")
    plt.tight_layout()
    plt.savefig(out_path, dpi=130)
    plt.close(fig)
    print(f"Wrote issues line chart: {out_path}")


def find_spans(df: pd.DataFrame, flag_col: str, value_col: str, median_gap: float, max_gap_mult: float):
    if df.empty or flag_col not in df:
        return []
    max_gap = (median_gap or 0) * max_gap_mult
    spans = []
    current = None

    for row in df.itertuples():
        ts = row.ts
        flagged = getattr(row, flag_col)
        val = getattr(row, value_col) if value_col in df.columns else None
        if not flagged:
            if current:
                spans.append(current)
                current = None
            continue

        if current is None:
            current = {"start": ts, "end": ts, "max": val, "samples": 1}
            continue

        gap = (ts - current["end"]).total_seconds()
        if max_gap and gap > max_gap:
            spans.append(current)
            current = {"start": ts, "end": ts, "max": val, "samples": 1}
        else:
            current["end"] = ts
            current["samples"] += 1
            if val is not None:
                current["max"] = max(current["max"], val) if current["max"] is not None else val

    if current:
        spans.append(current)

    for span in spans:
        dur = (span["end"] - span["start"]).total_seconds()
        span["duration_s"] = dur if dur > 0 else (median_gap or 0)
    spans.sort(key=lambda s: (s["max"] if s["max"] is not None else 0, s["duration_s"]), reverse=True)
    return spans


def print_hourly_patterns(df: pd.DataFrame, value_col: str, flag_col: str, top_hours: int, label: str):
    if value_col not in df or df[value_col].dropna().empty:
        print(f"{label} patterns: no data")
        return
    grouped = df.groupby("hour").agg(
        samples=(value_col, "count"),
        high_count=(flag_col, "sum"),
        high_rate=(flag_col, "mean"),
        median_val=(value_col, "median"),
        p95_val=(value_col, lambda s: percentile(s, 95)),
    )
    grouped = grouped[grouped["samples"] > 0].sort_values("high_rate", ascending=False)
    print(f"{label} top hours (UTC):")
    for hour, row in grouped.head(top_hours).iterrows():
        rate_pct = (row["high_rate"] or 0) * 100.0
        print(f"  {hour:02d}h  samples={int(row['samples'])}  high={int(row['high_count'])} "
              f"rate={rate_pct:4.1f}%  median={row['median_val']:.1f}  p95={row['p95_val']:.1f}")


def main():
    args = parse_args()
    df = load_quality_data(args.db, args.iface, args.host, args.since_hours)
    if df.empty:
        print("No rows found for the given filters.")
        return

    median_gap = df["gap_s"].median()
    start, end = df["ts"].iloc[0], df["ts"].iloc[-1]
    print(f"[net_quality_report] iface={args.iface} host={args.host or 'any'} rows={len(df)} "
          f"window={start} -> {end} UTC")
    print(f"Median sample gap: {median_gap:.1f}s | thresholds: latency>={args.latency_ms}ms "
          f"jitter>={args.jitter_ms}ms loss>={args.loss_pct}%")

    stats_latency = describe_series(df["latency_ms"])
    stats_jitter = describe_series(df["jitter_ms"])
    stats_loss = describe_series(df["loss_pct"])
    print(format_stats("Latency (ms)", stats_latency))
    print(format_stats("Jitter (ms)", stats_jitter))
    print(format_stats("Loss (%)", stats_loss))

    df["high_latency"] = df["latency_ms"] >= args.latency_ms
    df["high_jitter"] = df["jitter_ms"] >= args.jitter_ms
    df["high_loss"] = df["loss_pct"] >= args.loss_pct
    df["any_issue"] = df[["high_latency", "high_jitter", "high_loss"]].any(axis=1)

    def pct(series):
        return 0.0 if series.empty else float(series.mean() * 100.0)

    print()
    print(f"High latency samples: {df['high_latency'].sum()} ({pct(df['high_latency']):.2f}% of rows)")
    print(f"High jitter samples : {df['high_jitter'].sum()} ({pct(df['high_jitter']):.2f}% of rows)")
    print(f"Packet loss samples : {df['high_loss'].sum()} ({pct(df['high_loss']):.2f}% of rows)")
    print(f"Any-issue samples   : {df['any_issue'].sum()} ({pct(df['any_issue']):.2f}% of rows)")

    print()
    print_hourly_patterns(df, "latency_ms", "high_latency", args.top_hours, "Latency")
    print_hourly_patterns(df, "jitter_ms", "high_jitter", args.top_hours, "Jitter")
    print_hourly_patterns(df, "loss_pct", "high_loss", args.top_hours, "Loss")
    print_hourly_patterns(df, "latency_ms", "any_issue", args.top_hours, "Any issue")

    print()
    print_issue_histogram(df)
    plot_daily_issue_lines(df, args.compare_days, args.issues_plot)

    spans = {
        "High latency": find_spans(df, "high_latency", "latency_ms", median_gap, args.max_gap_mult),
        "High jitter": find_spans(df, "high_jitter", "jitter_ms", median_gap, args.max_gap_mult),
        "Packet loss": find_spans(df, "high_loss", "loss_pct", median_gap, args.max_gap_mult),
    }
    for label, span_list in spans.items():
        print()
        if not span_list:
            print(f"{label}: no spans found")
            continue
        print(f"{label} spans (worst {min(len(span_list), args.span_limit)}):")
        for span in span_list[: args.span_limit]:
            dur_min = (span["duration_s"] or 0) / 60.0
            max_val = span["max"]
            max_txt = f"max={max_val:.1f}" if max_val is not None else "max=NA"
            print(f"  {span['start']} -> {span['end']}  "
                  f"dur={dur_min:5.2f} min  samples={span['samples']}  {max_txt}")


if __name__ == "__main__":
    main()
