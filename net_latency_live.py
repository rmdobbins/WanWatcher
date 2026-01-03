#!/usr/bin/env python3
"""Live latency viewer that updates from the net_logger SQLite database."""

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pandas as pd


def load_latency_window(db_path, iface, minutes, host=None):
    """Load a recent latency window for one interface (and optional host)."""
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    con = sqlite3.connect(db_path)
    q = """
      SELECT ts_utc, host, iface,
             ping_ms, ping_min_ms, ping_avg_ms, ping_max_ms, ping_jitter_ms, ping_loss_pct,
             dns_ms, avail_ok
      FROM net_metrics
      WHERE ts_utc >= ? AND iface = ?
    """
    params = [since.isoformat(), iface]
    if host:
        q += " AND host = ?"
        params.append(host)
    q += " ORDER BY ts_utc ASC"

    try:
        df = pd.read_sql_query(q, con, params=params)
    finally:
        con.close()

    if df.empty:
        return df

    df["ts"] = pd.to_datetime(df["ts_utc"], utc=True).dt.tz_convert("UTC")
    return df


def set_line(line, x, series):
    """Update a matplotlib line with new data or hide it when no data."""
    if series is not None and not series.empty and pd.notna(series).any():
        line.set_data(x, series)
        line.set_visible(True)
    else:
        line.set_data([], [])
        line.set_visible(False)


def main():
    ap = argparse.ArgumentParser(description="Display latency metrics in real time.")
    ap.add_argument("--db", default="netstats.db", help="SQLite DB path")
    ap.add_argument("--iface", default="TOTAL", help="Interface to display (e.g., TOTAL, Ethernet, Wi-Fi)")
    ap.add_argument("--host", default="", help="Optional host label filter")
    ap.add_argument("--minutes", type=int, default=120, help="How many minutes back to keep on screen")
    ap.add_argument("--refresh", type=float, default=5.0, help="Refresh interval seconds")
    args = ap.parse_args()

    fig, ax = plt.subplots(figsize=(10, 5))
    ax2 = ax.twinx()

    ping_line, = ax.plot([], [], label="Ping avg (ms)", color="tab:blue")
    jitter_line, = ax.plot([], [], label="Ping jitter (ms)", color="tab:green", linestyle="--")
    dns_line, = ax.plot([], [], label="DNS lookup (ms)", color="tab:orange", linestyle=":")
    loss_line, = ax2.plot([], [], label="Ping loss (%)", color="tab:red", alpha=0.6)

    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Milliseconds")
    ax2.set_ylabel("Loss %")
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.autofmt_xdate()

    title_base = f"Live Latency | iface={args.iface} host={args.host or 'any'} | window={args.minutes} min"
    avail_fill = None

    def refresh(_frame):
        nonlocal avail_fill
        df = load_latency_window(args.db, args.iface, args.minutes, args.host or None)

        if avail_fill:
            avail_fill.remove()
            avail_fill = None

        if df.empty:
            set_line(ping_line, [], None)
            set_line(jitter_line, [], None)
            set_line(dns_line, [], None)
            set_line(loss_line, [], None)
            now = datetime.now(timezone.utc)
            ax.set_xlim(now - timedelta(minutes=args.minutes), now)
            ax.set_title(title_base + " (waiting for data)")
            fig.canvas.draw_idle()
            return ping_line, jitter_line, dns_line, loss_line

        times = df["ts"]
        ping_series = df["ping_avg_ms"] if "ping_avg_ms" in df else df.get("ping_ms")
        set_line(ping_line, times, ping_series)
        set_line(jitter_line, times, df.get("ping_jitter_ms"))
        set_line(dns_line, times, df.get("dns_ms"))
        set_line(loss_line, times, df.get("ping_loss_pct"))

        end_ts = times.iloc[-1]
        start_ts = end_ts - pd.Timedelta(minutes=args.minutes)
        ax.set_xlim(start_ts, end_ts)

        ax.relim()
        ax.autoscale_view()

        if loss_line.get_visible():
            ax2.relim()
            ax2.autoscale_view()
        else:
            ax2.set_ylim(0, 1)
            ax2.set_yticks([])

        if "avail_ok" in df.columns and df["avail_ok"].notna().any():
            ymin, ymax = ax.get_ylim()
            avail_fill = ax.fill_between(
                times,
                ymin,
                ymax,
                where=df["avail_ok"].fillna(1) == 0,
                step="post",
                color="red",
                alpha=0.08,
                zorder=0,
            )

        latest_ping = None
        if ping_series is not None and pd.notna(ping_series).any():
            latest_ping = ping_series[pd.notna(ping_series)].iloc[-1]
        title = title_base if latest_ping is None else f"{title_base} | latest={latest_ping:.1f} ms"
        ax.set_title(title)

        handles, labels = [], []
        for line in (ping_line, jitter_line, dns_line, loss_line):
            if line.get_visible():
                handles.append(line)
                labels.append(line.get_label())
        if handles:
            ax.legend(handles, labels, loc="upper right")
        else:
            leg = ax.get_legend()
            if leg:
                leg.remove()

        return ping_line, jitter_line, dns_line, loss_line

    print(f"[net_latency_live] Watching {args.db} every {args.refresh}s. Close the window or Ctrl+C to exit.")
    anim = FuncAnimation(fig, refresh, interval=args.refresh * 1000)
    plt.show()
    return anim


if __name__ == "__main__":
    main()
