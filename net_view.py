#!/usr/bin/env python3
import argparse, sqlite3
from datetime import datetime, timedelta, timezone
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np  # NEW

def load_data(db_path, iface, minutes):
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    con = sqlite3.connect(db_path)
    q = """
      SELECT ts_utc, host, iface,
             bytes_sent_rate, bytes_recv_rate,
             packets_sent_rate, packets_recv_rate,
             ping_ms, dns_ms,
             thr_down_mbps, thr_up_mbps, thr_jitter_ms, thr_loss_pct, thr_method,
             bytes_sent, bytes_recv, packets_sent, packets_recv,
             errin, errout, dropin, dropout
      FROM net_metrics
      WHERE ts_utc >= ? AND iface = ?
      ORDER BY ts_utc ASC
    """
    df = pd.read_sql_query(q, con, params=(since.isoformat(), iface))
    con.close()
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts_utc"], utc=True).dt.tz_convert("UTC")
    df["dt_s"] = df["ts"].diff().dt.total_seconds()
    for col in ["errin","errout","dropin","dropout","packets_sent","packets_recv","bytes_sent","bytes_recv"]:
        if col in df.columns:
            df[col+"_diff"] = df[col].diff()
    df["dt_s"] = df["dt_s"].where(df["dt_s"] > 0, other=pd.NA)
    for base in ["errin","errout","dropin","dropout"]:
        if base+"_diff" in df.columns:
            df[base+"_rate"] = (df[base+"_diff"] / df["dt_s"]).astype("float")
    return df

def _polyfit_series(ts, y, degree):
    """Return (ts, y_fit) evaluated on the original ts, or None if not enough points."""
    # convert datetimes to seconds since first sample for numerical stability
    mask = pd.notna(y)
    if mask.sum() < degree + 1:
        return None
    t0 = ts.iloc[0]
    x = (ts[mask] - t0).dt.total_seconds().to_numpy()
    yy = y[mask].to_numpy(dtype=float)
    try:
        coeff = np.polyfit(x, yy, deg=degree)
        poly = np.poly1d(coeff)
        x_full = (ts - t0).dt.total_seconds().to_numpy()
        y_fit = poly(x_full)
        return ts, y_fit
    except Exception:
        return None

def maybe_save_or_show(fig_base_out):
    if fig_base_out:
        plt.tight_layout()
        plt.savefig(fig_base_out, dpi=120)
        plt.close()
    else:
        plt.tight_layout()
        plt.show()

def main():
    ap = argparse.ArgumentParser(description="Plot network metrics (incl. throughput, error/drop rates) with Ping/DNS best-fit curves.")
    ap.add_argument("--db", required=True, help="SQLite DB path (e.g., netstats.db)")
    ap.add_argument("--iface", default="TOTAL", help="Interface to display (e.g., TOTAL, eth0, Wi-Fi)")
    ap.add_argument("--minutes", type=int, default=120, help="How many minutes back to show")
    ap.add_argument("--out", default="", help="Optional base filename to save figures (creates *_rates.png, etc.)")
    ap.add_argument("--export-csv", default="", help="Optional CSV path to export the windowed data")
    ap.add_argument("--fit-degree", type=int, default=2, help="Polynomial degree for Ping/DNS best-fit (e.g., 1, 2, 3)")
    args = ap.parse_args()

    df = load_data(args.db, args.iface, args.minutes)
    if df.empty:
        print("No rows found for the given window/interface.")
        return

    if args.export_csv:
        df.to_csv(args.export_csv, index=False)
        print(f"Exported {len(df)} rows to {args.export_csv}")

    base = args.out.rsplit(".", 1)
    out_base = args.out if args.out else ""
    out_rates   = (args.out if len(base)==2 else (args.out + ".png")).replace(".png", "_rates.png")     if out_base else ""
    out_pkts    = (args.out if len(base)==2 else (args.out + ".png")).replace(".png", "_pkts.png")      if out_base else ""
    out_lat     = (args.out if len(base)==2 else (args.out + ".png")).replace(".png", "_latency.png")   if out_base else ""
    out_thr     = (args.out if len(base)==2 else (args.out + ".png")).replace(".png", "_throughput.png")if out_base else ""
    out_err     = (args.out if len(base)==2 else (args.out + ".png")).replace(".png", "_errors.png")    if out_base else ""

    # 1) Byte rates
    plt.figure(figsize=(10, 4.5))
    plt.plot(df["ts"], df["bytes_sent_rate"], label="Up (bytes/s)")
    plt.plot(df["ts"], df["bytes_recv_rate"], label="Down (bytes/s)")
    plt.title(f"Byte Rates — iface={args.iface} (last {args.minutes} min)")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Bytes per second")
    plt.legend(); plt.grid(True)
    maybe_save_or_show(out_rates)

    # 2) Packet rates
    plt.figure(figsize=(10, 4.5))
    plt.plot(df["ts"], df["packets_sent_rate"], label="Packets Up (pkts/s)")
    plt.plot(df["ts"], df["packets_recv_rate"], label="Packets Down (pkts/s)")
    plt.title(f"Packet Rates — iface={args.iface} (last {args.minutes} min)")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Packets per second")
    plt.legend(); plt.grid(True)
    maybe_save_or_show(out_pkts)

    # 3) Ping & DNS with best-fit curves
    plt.figure(figsize=(10, 4.5))
    have_any = False
    if "ping_ms" in df.columns:
        have_any = True
        plt.plot(df["ts"], df["ping_ms"], label="Ping (ms)")
        fit = _polyfit_series(df["ts"], df["ping_ms"], degree=args.fit_degree)
        if fit:
            ts_fit, y_fit = fit
            plt.plot(ts_fit, y_fit, linestyle="--", label=f"Ping fit (deg {args.fit_degree})")
    if "dns_ms" in df.columns:
        have_any = True
        plt.plot(df["ts"], df["dns_ms"], label="DNS lookup (ms)")
        fit = _polyfit_series(df["ts"], df["dns_ms"], degree=args.fit_degree)
        if fit:
            ts_fit, y_fit = fit
            plt.plot(ts_fit, y_fit, linestyle="--", label=f"DNS fit (deg {args.fit_degree})")
    if not have_any:
        plt.text(0.5, 0.5, "No Ping/DNS columns found", ha="center", va="center", transform=plt.gca().transAxes)

    plt.title(f"Ping / DNS — iface={args.iface} (last {args.minutes} min)")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Milliseconds")
    plt.legend(); plt.grid(True)
    maybe_save_or_show(out_lat)

    # 4) Throughput (Mb/s)
    if "thr_down_mbps" in df.columns or "thr_up_mbps" in df.columns:
        plt.figure(figsize=(10, 4.5))
        if "thr_down_mbps" in df.columns:
            plt.plot(df["ts"], df["thr_down_mbps"], label="Throughput Down (Mb/s)")
        if "thr_up_mbps" in df.columns:
            plt.plot(df["ts"], df["thr_up_mbps"], label="Throughput Up (Mb/s)")
        plt.title(f"Throughput — iface={args.iface} (last {args.minutes} min)")
        plt.xlabel("Time (UTC)")
        plt.ylabel("Mb/s")
        plt.legend(); plt.grid(True)
        maybe_save_or_show(out_thr)

    # 5) Error/Drop rates (per second)
    if {"errin_rate","errout_rate","dropin_rate","dropout_rate"}.intersection(df.columns):
        plt.figure(figsize=(10, 4.5))
        if "errin_rate" in df.columns:
            plt.plot(df["ts"], df["errin_rate"], label="errin (/s)")
        if "errout_rate" in df.columns:
            plt.plot(df["ts"], df["errout_rate"], label="errout (/s)")
        if "dropin_rate" in df.columns:
            plt.plot(df["ts"], df["dropin_rate"], label="dropin (/s)")
        if "dropout_rate" in df.columns:
            plt.plot(df["ts"], df["dropout_rate"], label="dropout (/s)")
        plt.title(f"Error/Drop Rates — iface={args.iface} (last {args.minutes} min)")
        plt.xlabel("Time (UTC)")
        plt.ylabel("Events per second")
        plt.legend(); plt.grid(True)
        maybe_save_or_show(out_err)

    # 6) Ping jitter (ms): instantaneous |Δping| and rolling std
    plt.figure(figsize=(10, 4.5))
    jitter_abs = df["ping_ms"].diff().abs()
    plt.plot(df["ts"], jitter_abs, label="Ping jitter |Δms|")
    plt.plot(df["ts"], df["ping_ms"].rolling(window=12, min_periods=4).std(), linestyle="--",
             label="Ping jitter (rolling σ)")
    plt.title(f"Ping Jitter — iface={args.iface} (last {args.minutes} min)")
    plt.xlabel("Time (UTC)");
    plt.ylabel("Milliseconds");
    plt.legend();
    plt.grid(True)
    maybe_save_or_show((args.out if args.out else "").replace(".png", "_jitter.png") if args.out else "")


if __name__ == "__main__":
    main()
