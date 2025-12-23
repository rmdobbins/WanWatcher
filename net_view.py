#!/usr/bin/env python3
import argparse
import sqlite3
from datetime import datetime, timedelta, timezone

import matplotlib.pyplot as plt
import pandas as pd


def load_data(db_path, iface, minutes, host=None):
    """Load a window of samples for one interface (and optional host)."""
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    con = sqlite3.connect(db_path)
    q = """
      SELECT ts_utc, host, iface,
             bytes_sent_rate, bytes_recv_rate,
             packets_sent_rate, packets_recv_rate,
             ping_ms, ping_min_ms, ping_avg_ms, ping_max_ms, ping_jitter_ms, ping_loss_pct,
             dns_ms, avail_ok,
             thr_down_mbps, thr_up_mbps, thr_jitter_ms, thr_loss_pct, thr_method,
             errin_delta, errout_delta, dropin_delta, dropout_delta,
             errin, errout, dropin, dropout
      FROM net_metrics
      WHERE ts_utc >= ? AND iface = ?
    """
    params = [since.isoformat(), iface]
    if host:
        q += " AND host = ?"
        params.append(host)
    q += " ORDER BY ts_utc ASC"
    df = pd.read_sql_query(q, con, params=params)
    con.close()
    if df.empty:
        return df

    df["ts"] = pd.to_datetime(df["ts_utc"], utc=True).dt.tz_convert("UTC")
    df["dt_s"] = df["ts"].diff().dt.total_seconds()
    df["dt_s"] = df["dt_s"].where(df["dt_s"] > 0, other=pd.NA)

    # Compute error/drop rates per second using deltas (preferred) or diffs as fallback.
    for base in ["errin", "errout", "dropin", "dropout"]:
        delta_col = f"{base}_delta"
        rate_col = f"{base}_rate"
        if delta_col in df.columns and df[delta_col].notna().any():
            df[rate_col] = df[delta_col] / df["dt_s"]
        elif base in df.columns:
            df[rate_col] = df[base].diff() / df["dt_s"]

    return df


def maybe_save_or_show(path):
    plt.tight_layout()
    if path:
        plt.savefig(path, dpi=130)
        plt.close()
    else:
        plt.show()


def _availability_shading(ax, df):
    if "avail_ok" not in df.columns:
        return
    avail = df["avail_ok"]
    if avail.isna().all():
        return
    ymin, ymax = ax.get_ylim()
    ax.fill_between(
        df["ts"],
        ymin,
        ymax,
        where=avail == 0,
        step="post",
        color="red",
        alpha=0.08,
        label="Availability fail",
    )
    ax.set_ylim(ymin, ymax)


def _apply_scale(ax, series_list, scale_type, clip_pct):
    combined = pd.concat(series_list).dropna()
    if combined.empty:
        return
    if scale_type == "log":
        combined = combined[combined > 0]
    if combined.empty:
        return
    upper = combined.quantile(clip_pct / 100.0) if clip_pct and clip_pct < 100 else combined.max()
    if not pd.notna(upper) or upper <= 0:
        return
    if scale_type == "log":
        lower = max(combined.min() * 0.8, 0.1)
        ax.set_yscale("log")
        ax.set_ylim(lower, upper * 1.1)
    else:
        ax.set_ylim(0, upper * 1.1)


def plot_byte_rates(df, iface, host, minutes, out_path, scale_type, clip_pct):
    plt.figure(figsize=(10, 4.5))
    plt.plot(df["ts"], df["bytes_sent_rate"], label="Up (bytes/s)")
    plt.plot(df["ts"], df["bytes_recv_rate"], label="Down (bytes/s)")
    _apply_scale(plt.gca(), [df["bytes_sent_rate"], df["bytes_recv_rate"]], scale_type, clip_pct)
    plt.title(f"Byte Rates | iface={iface} host={host or 'any'} | last {minutes} min")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Bytes per second")
    plt.legend()
    plt.grid(True)
    maybe_save_or_show(out_path)


def plot_packet_rates(df, iface, host, minutes, out_path, scale_type, clip_pct):
    plt.figure(figsize=(10, 4.5))
    plt.plot(df["ts"], df["packets_sent_rate"], label="Packets up (pkts/s)")
    plt.plot(df["ts"], df["packets_recv_rate"], label="Packets down (pkts/s)")
    _apply_scale(plt.gca(), [df["packets_sent_rate"], df["packets_recv_rate"]], scale_type, clip_pct)
    plt.title(f"Packet Rates | iface={iface} host={host or 'any'} | last {minutes} min")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Packets per second")
    plt.legend()
    plt.grid(True)
    maybe_save_or_show(out_path)


def plot_latency(df, iface, host, minutes, out_path):
    plt.figure(figsize=(10, 5))
    ax = plt.gca()
    plotted = False

    if "ping_avg_ms" in df.columns:
        ax.plot(df["ts"], df["ping_avg_ms"], label="Ping avg (ms)", color="tab:blue")
        plotted = True
        if "ping_min_ms" in df.columns and "ping_max_ms" in df.columns:
            ax.fill_between(df["ts"], df["ping_min_ms"], df["ping_max_ms"],
                            color="tab:blue", alpha=0.1, label="Ping min/max")
    elif "ping_ms" in df.columns:
        ax.plot(df["ts"], df["ping_ms"], label="Ping (ms)", color="tab:blue")
        plotted = True

    if "ping_jitter_ms" in df.columns:
        ax.plot(df["ts"], df["ping_jitter_ms"], label="Ping jitter (ms)", color="tab:green", linestyle="--")

    if "dns_ms" in df.columns:
        ax.plot(df["ts"], df["dns_ms"], label="DNS lookup (ms)", color="tab:orange", linestyle=":")
        plotted = True

    # Show availability failures as shaded regions
    _availability_shading(ax, df)

    # Loss on secondary axis
    if "ping_loss_pct" in df.columns and df["ping_loss_pct"].notna().any():
        ax2 = ax.twinx()
        ax2.plot(df["ts"], df["ping_loss_pct"], color="tab:red", alpha=0.5, label="Ping loss (%)")
        ax2.set_ylabel("Loss %")
        handles1, labels1 = ax.get_legend_handles_labels()
        handles2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(handles1 + handles2, labels1 + labels2, loc="upper right")
    elif plotted:
        ax.legend(loc="upper right")

    ax.set_title(f"Latency / Availability | iface={iface} host={host or 'any'} | last {minutes} min")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Milliseconds")
    ax.grid(True)
    maybe_save_or_show(out_path)


def plot_throughput(df, iface, host, minutes, out_path):
    if "thr_down_mbps" not in df.columns and "thr_up_mbps" not in df.columns:
        return

    plt.figure(figsize=(10, 5))
    ax = plt.gca()
    if "thr_down_mbps" in df.columns:
        ax.plot(df["ts"], df["thr_down_mbps"], marker="o", label="Down (Mb/s)", linestyle="-")
    if "thr_up_mbps" in df.columns:
        ax.plot(df["ts"], df["thr_up_mbps"], marker="o", label="Up (Mb/s)", linestyle="-")

    # Throughput quality on secondary axis
    ax2 = ax.twinx()
    has_quality = False
    if "thr_jitter_ms" in df.columns and df["thr_jitter_ms"].notna().any():
        ax2.plot(df["ts"], df["thr_jitter_ms"], color="tab:purple", linestyle="--", label="Jitter (ms)")
        has_quality = True
    if "thr_loss_pct" in df.columns and df["thr_loss_pct"].notna().any():
        ax2.plot(df["ts"], df["thr_loss_pct"], color="tab:red", linestyle=":", label="Loss (%)")
        ax2.set_ylabel("Jitter/Loss")
        has_quality = True
    else:
        ax2.set_yticks([])

    # Method markers
    if "thr_method" in df.columns and df["thr_method"].notna().any():
        method_colors = {
            "iperf3": "tab:blue",
            "iperf3-rev": "tab:orange",
            "iperf3-bidir": "tab:green",
            "http": "tab:purple",
        }
        for method, group in df.groupby(df["thr_method"].fillna("unknown")):
            ax.scatter(group["ts"], group.get("thr_down_mbps"), label=f"{method} down", alpha=0.7,
                       color=method_colors.get(method, "gray"), marker="o")
            ax.scatter(group["ts"], group.get("thr_up_mbps"), label=f"{method} up", alpha=0.7,
                       color=method_colors.get(method, "gray"), marker="^")

    handles1, labels1 = ax.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(handles1 + handles2, labels1 + labels2, loc="upper right")

    ax.set_title(f"Throughput | iface={iface} host={host or 'any'} | last {minutes} min")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Mb/s")
    ax.grid(True)
    maybe_save_or_show(out_path)


def plot_errors(df, iface, host, minutes, out_path):
    rate_cols = ["errin_rate", "errout_rate", "dropin_rate", "dropout_rate"]
    if not any(c in df.columns for c in rate_cols):
        return
    plt.figure(figsize=(10, 4.5))
    if "errin_rate" in df.columns:
        plt.plot(df["ts"], df["errin_rate"], label="errin (/s)")
    if "errout_rate" in df.columns:
        plt.plot(df["ts"], df["errout_rate"], label="errout (/s)")
    if "dropin_rate" in df.columns:
        plt.plot(df["ts"], df["dropin_rate"], label="dropin (/s)")
    if "dropout_rate" in df.columns:
        plt.plot(df["ts"], df["dropout_rate"], label="dropout (/s)")
    _apply_scale(plt.gca(), [df[c] for c in rate_cols if c in df.columns], "linear", 99.5)
    plt.title(f"Error/Drop Rates | iface={iface} host={host or 'any'} | last {minutes} min")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Events per second")
    plt.legend()
    plt.grid(True)
    maybe_save_or_show(out_path)


def main():
    ap = argparse.ArgumentParser(description="Visualize network metrics from net_logger SQLite DB.")
    ap.add_argument("--db", required=True, help="SQLite DB path (e.g., netstats.db)")
    ap.add_argument("--iface", default="TOTAL", help="Interface to display (e.g., TOTAL, Ethernet, Wi-Fi)")
    ap.add_argument("--host", default="", help="Optional host label filter")
    ap.add_argument("--minutes", type=int, default=240, help="How many minutes back to show")
    ap.add_argument("--out", default="", help="Optional base filename to save figures (adds suffixes)")
    ap.add_argument("--export-csv", default="", help="Optional CSV path to export the windowed data")
    ap.add_argument("--rate-scale", choices=["linear", "log"], default="linear", help="Scale for byte/packet rate charts")
    ap.add_argument("--rate-clip", type=float, default=99.5, help="Percentile to cap rate Y-axis (e.g., 99, 99.5, 100 for none)")
    args = ap.parse_args()

    df = load_data(args.db, args.iface, args.minutes, host=args.host or None)
    if df.empty:
        print("No rows found for the given window/interface.")
        return

    if args.export_csv:
        df.to_csv(args.export_csv, index=False)
        print(f"Exported {len(df)} rows to {args.export_csv}")

    base = args.out
    def out_path(kind):
        if not base:
            return ""
        if base.endswith(".png"):
            return base.replace(".png", f"_{kind}.png")
        return f"{base}_{kind}.png"

    plot_byte_rates(df, args.iface, args.host or "any", args.minutes, out_path("rates"), args.rate_scale, args.rate_clip)
    plot_packet_rates(df, args.iface, args.host or "any", args.minutes, out_path("packets"), args.rate_scale, args.rate_clip)
    plot_latency(df, args.iface, args.host or "any", args.minutes, out_path("latency"))
    plot_throughput(df, args.iface, args.host or "any", args.minutes, out_path("throughput"))
    plot_errors(df, args.iface, args.host or "any", args.minutes, out_path("errors"))


if __name__ == "__main__":
    main()
