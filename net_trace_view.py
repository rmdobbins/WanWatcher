#!/usr/bin/env python3
"""View and analyze traceroute history from the SQLite DB."""

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FixedLocator


def default_target(con, host):
    q = "SELECT target FROM trace_runs"
    params = []
    if host:
        q += " WHERE host = ?"
        params.append(host)
    q += " ORDER BY ts_utc DESC LIMIT 1"
    row = con.execute(q, params).fetchone()
    return row[0] if row else None


def load_trace_data(db_path, target, host, since_hours, runs):
    con = sqlite3.connect(db_path)
    try:
        where = []
        params = []
        if target:
            where.append("target = ?")
            params.append(target)
        if host:
            where.append("host = ?")
            params.append(host)
        if since_hours:
            since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
            where.append("ts_utc >= ?")
            params.append(since.isoformat())

        q = "SELECT id, ts_utc, host, target, exit_code, hop_count FROM trace_runs"
        if where:
            q += " WHERE " + " AND ".join(where)
        q += " ORDER BY ts_utc DESC"
        if runs:
            q += " LIMIT ?"
            params = params + [runs]

        runs_df = pd.read_sql_query(q, con, params=params)
        if runs_df.empty:
            return runs_df, pd.DataFrame()

        run_ids = runs_df["id"].tolist()
        placeholders = ",".join(["?"] * len(run_ids))
        q2 = f"""
          SELECT r.id AS run_id, r.ts_utc, r.host, r.target, r.exit_code, r.hop_count,
                 h.hop, h.hop_host, h.hop_ip, h.rtt_ms1, h.rtt_ms2, h.rtt_ms3, h.rtt_avg_ms, h.status
          FROM trace_runs r
          JOIN trace_hops h ON h.run_id = r.id
          WHERE r.id IN ({placeholders})
          ORDER BY r.ts_utc ASC, h.hop ASC
        """
        df = pd.read_sql_query(q2, con, params=run_ids)
    finally:
        con.close()

    if df.empty:
        return runs_df, df

    df["ts"] = pd.to_datetime(df["ts_utc"], utc=True).dt.tz_convert("UTC")
    if "rtt_avg_ms" not in df.columns or df["rtt_avg_ms"].isna().all():
        df["rtt_avg_ms"] = df[["rtt_ms1", "rtt_ms2", "rtt_ms3"]].mean(axis=1)
    else:
        df["rtt_avg_ms"] = df["rtt_avg_ms"].fillna(
            df[["rtt_ms1", "rtt_ms2", "rtt_ms3"]].mean(axis=1)
        )
    return runs_df, df


def maybe_save(path):
    plt.tight_layout()
    if path:
        plt.savefig(path, dpi=130)


def _hop_identity(row):
    ip = row.get("hop_ip")
    if pd.notna(ip):
        return str(ip)
    if row.get("status") == "timeout":
        return "*"
    host = row.get("hop_host")
    if pd.notna(host):
        return str(host)
    return "?"


def build_route_keys(df):
    ordered = df.sort_values(["run_id", "hop"]).copy()
    ordered["hop_id"] = ordered.apply(_hop_identity, axis=1)
    return ordered.groupby("run_id")["hop_id"].apply(tuple)


def format_route_key(route_key, max_hops, max_len=120):
    if not route_key:
        return "[]"
    hops = list(route_key)
    if max_hops:
        hops = hops[:max_hops]
    text = " > ".join(hops)
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def annotate_routes(runs_df, route_keys):
    runs_sorted = runs_df.copy()
    runs_sorted["route_key"] = runs_sorted["id"].map(route_keys)
    runs_sorted = runs_sorted.sort_values("ts_utc")

    route_id_map = {}
    route_ids = []
    for key in runs_sorted["route_key"]:
        if key not in route_id_map:
            route_id_map[key] = len(route_id_map) + 1
        route_ids.append(route_id_map[key])

    runs_sorted["route_id"] = route_ids
    return runs_sorted, route_id_map


def print_last_run(runs_df, df, max_hops):
    if runs_df.empty or df.empty:
        print("No traceroute runs found.")
        return

    last = runs_df.iloc[0]
    run_id = last["id"]
    sub = df[df["run_id"] == run_id].copy().sort_values("hop")
    if max_hops:
        sub = sub[sub["hop"] <= max_hops]

    ts = pd.to_datetime(last["ts_utc"], utc=True)
    print(f"Last run: {ts} UTC | target={last['target']} host={last['host']} exit={last['exit_code']} hops={last['hop_count']}")
    for row in sub.itertuples():
        label = row.hop_host or row.hop_ip or "unknown"
        rtt = row.rtt_avg_ms
        rtt_txt = f"{rtt:.1f} ms" if pd.notna(rtt) else "*"
        status = row.status or "unknown"
        print(f"  {int(row.hop):2d}  {label:30s}  {rtt_txt:>8s}  {status}")


def print_hop_summary(df, max_hops):
    if df.empty:
        return

    grouped = df.groupby("hop")
    print()
    print("Hop IP key (all IPs per hop with RTT stats):")
    for hop, g in grouped:
        if max_hops and hop > max_hops:
            continue
        ip_groups = g.groupby("hop_ip", dropna=True)
        print(f"  Hop {int(hop):2d}:")
        if ip_groups.ngroups == 0:
            print("    NA (no IPs recorded)")
            continue
        stats = []
        for ip, ig in ip_groups:
            rtts = ig["rtt_avg_ms"].dropna()
            median = rtts.median() if not rtts.empty else None
            p95 = rtts.quantile(0.95) if not rtts.empty else None
            p99 = rtts.quantile(0.99) if not rtts.empty else None
            timeout_rate = (ig["status"] == "timeout").mean() * 100.0
            stats.append((len(ig), ip, median, p95, p99, timeout_rate))

        for count, ip, median, p95, p99, timeout_rate in sorted(stats, key=lambda s: s[0], reverse=True):
            ip_label = str(ip)
            med_txt = f"{median:.1f}" if median is not None else "NA"
            p95_txt = f"{p95:.1f}" if p95 is not None else "NA"
            p99_txt = f"{p99:.1f}" if p99 is not None else "NA"
            print(f"    {ip_label:15s} n={count:3d}  median={med_txt:>6s} ms  "
                  f"p95={p95_txt:>6s} ms  p99={p99_txt:>6s} ms  timeout={timeout_rate:5.1f}%")

    print()
    print("Hop summary (median/p95/p99 RTT, timeout rate, unique IPs):")
    for hop, g in grouped:
        if max_hops and hop > max_hops:
            continue
        rtts = g["rtt_avg_ms"].dropna()
        median = rtts.median() if not rtts.empty else None
        p95 = rtts.quantile(0.95) if not rtts.empty else None
        p99 = rtts.quantile(0.99) if not rtts.empty else None
        timeout_rate = (g["status"] == "timeout").mean() * 100.0
        uniq = g["hop_ip"].nunique(dropna=True)
        top_ip = g["hop_ip"].dropna().value_counts().head(1)
        top_ip = top_ip.index[0] if not top_ip.empty else ""
        med_txt = f"{median:.1f}" if median is not None else "NA"
        p95_txt = f"{p95:.1f}" if p95 is not None else "NA"
        p99_txt = f"{p99:.1f}" if p99 is not None else "NA"
        print(f"  {int(hop):2d}  median={med_txt:>6s} ms  p95={p95_txt:>6s} ms  p99={p99_txt:>6s} ms  timeout={timeout_rate:5.1f}%  uniq_ip={uniq:2d}  top_ip={top_ip}")


def plot_last_run(runs_df, df, out_path):
    if runs_df.empty or df.empty:
        return False
    last = runs_df.iloc[0]
    sub = df[df["run_id"] == last["id"]].copy().sort_values("hop")
    if sub.empty:
        return False

    plt.figure(figsize=(10, 4.5))
    ax = plt.gca()
    ax.plot(sub["hop"], sub["rtt_avg_ms"], marker="o", label="RTT avg (ms)")
    timeouts = sub[sub["status"] == "timeout"]
    if not timeouts.empty:
        ax.scatter(timeouts["hop"], [0] * len(timeouts), color="tab:red", marker="x", label="timeout")
        ax.legend()
    max_hop = int(sub["hop"].max())
    ax.set_xlim(0.5, max_hop + 0.5)
    ax.xaxis.set_major_locator(FixedLocator(list(range(1, max_hop + 1))))
    ax.set_title(f"Traceroute RTTs (last run) | target={last['target']} host={last['host']}")
    ax.set_xlabel("Hop")
    ax.set_ylabel("RTT avg (ms)")
    ax.grid(True)
    maybe_save(out_path)
    return True


def plot_hop_stats(df, title, out_path):
    if df.empty:
        return False

    grouped = df.groupby("hop")
    stats = grouped["rtt_avg_ms"].agg(
        median="median",
        p95=lambda s: s.dropna().quantile(0.95) if not s.dropna().empty else None,
        p99=lambda s: s.dropna().quantile(0.99) if not s.dropna().empty else None,
    )
    timeout_rate = grouped["status"].apply(lambda s: (s == "timeout").mean() * 100.0)
    stats = stats.join(timeout_rate.rename("timeout_rate"))
    stats = stats.dropna(subset=["median"], how="all")
    if stats.empty:
        return False

    plt.figure(figsize=(10, 4.5))
    ax = plt.gca()

    max_hop = int(df["hop"].max())

    hop_values = []
    hop_positions = []
    for hop, g in grouped:
        rtts = g["rtt_avg_ms"].dropna().values
        if rtts.size == 0:
            continue
        hop_values.append(rtts)
        hop_positions.append(hop)
    if hop_values:
        ax.boxplot(
            hop_values,
            positions=hop_positions,
            widths=0.6,
            patch_artist=True,
            boxprops={"facecolor": "#d9e7f2", "alpha": 0.6},
            medianprops={"color": "#1f77b4"},
            whiskerprops={"color": "#7a7a7a"},
            capprops={"color": "#7a7a7a"},
            showfliers=False,
        )

    ax.plot(stats.index, stats["median"], marker="o", label="Median RTT (ms)")
    if stats["p95"].notna().any():
        ax.plot(stats.index, stats["p95"], marker=".", linestyle="--", label="p95 RTT (ms)")
    if stats["p99"].notna().any():
        ax.plot(stats.index, stats["p99"], marker=".", linestyle=":", label="p99 RTT (ms)")
    ax.set_xlim(0.5, max_hop + 0.5)
    ax.xaxis.set_major_locator(FixedLocator(list(range(1, max_hop + 1))))
    ax.set_xlabel("Hop")
    ax.set_ylabel("RTT (ms)")
    ax.grid(True)

    if stats["timeout_rate"].notna().any() and stats["timeout_rate"].max() > 0:
        ax2 = ax.twinx()
        ax2.plot(stats.index, stats["timeout_rate"], color="tab:red", alpha=0.5, label="Timeout %")
        ax2.set_ylabel("Timeout %")
        handles1, labels1 = ax.get_legend_handles_labels()
        handles2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(handles1 + handles2, labels1 + labels2, loc="upper right")
    else:
        ax.legend(loc="upper right")

    plt.title(title)
    maybe_save(out_path)
    return True


def print_route_changes(runs_sorted, route_id_map, max_hops):
    if runs_sorted.empty:
        return
    print()
    print("Routes (unique hop sequences):")
    route_counts = runs_sorted.groupby("route_id")["id"].count()
    for key, rid in sorted(route_id_map.items(), key=lambda kv: kv[1]):
        count = int(route_counts.get(rid, 0))
        hops = len(key) if key else 0
        print(f"  route {rid:2d}  runs={count:3d}  hops={hops:2d}  {format_route_key(key, max_hops)}")

    print()
    if len(route_id_map) <= 1:
        print("Route changes: none (single route in window)")
        return

    print("Route changes:")
    prev_id = None
    for row in runs_sorted.itertuples():
        if row.route_id != prev_id:
            label = format_route_key(row.route_key, max_hops)
            if prev_id is None:
                print(f"  {row.ts_utc} route_id={row.route_id} (initial) {label}")
            else:
                print(f"  {row.ts_utc} route_id={prev_id} -> {row.route_id} {label}")
            prev_id = row.route_id


def main():
    ap = argparse.ArgumentParser(description="Analyze traceroute history from the SQLite DB.")
    ap.add_argument("--db", default="netstats.db", help="SQLite DB path")
    ap.add_argument("--target", default="", help="Target to analyze (default: latest)")
    ap.add_argument("--host", default="", help="Optional host label filter")
    ap.add_argument("--since-hours", type=float, default=24.0, help="How many hours back to include")
    ap.add_argument("--runs", type=int, default=50, help="Max number of runs to load")
    ap.add_argument("--max-hops", type=int, default=30, help="Max hops to print in summaries")
    ap.add_argument("--out", default="", help="Optional base filename to save figures (adds suffixes)")
    ap.add_argument("--export-csv", default="", help="Optional CSV path to export joined hop data")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    try:
        target = args.target or default_target(con, args.host or None)
    finally:
        con.close()

    if not target:
        print("No traceroute targets found.")
        return

    runs_df, df = load_trace_data(args.db, target, args.host or None, args.since_hours, args.runs)
    if runs_df.empty or df.empty:
        print("No traceroute runs found for the given filters.")
        return

    if args.export_csv:
        df.to_csv(args.export_csv, index=False)
        print(f"Exported {len(df)} rows to {args.export_csv}")

    start = df["ts"].min()
    end = df["ts"].max()
    print(f"[net_trace_view] target={target} host={args.host or 'any'} runs={len(runs_df)} window={start} -> {end} UTC")
    print_last_run(runs_df, df, args.max_hops)
    print_hop_summary(df, args.max_hops)

    route_keys = build_route_keys(df)
    runs_sorted, route_id_map = annotate_routes(runs_df, route_keys)
    print_route_changes(runs_sorted, route_id_map, args.max_hops)

    base = args.out
    def out_path(kind):
        if not base:
            return ""
        if base.endswith(".png"):
            return base.replace(".png", f"_{kind}.png")
        return f"{base}_{kind}.png"

    plots_made = 0
    #plots_made += int(plot_last_run(runs_df, df, out_path("last")))
    title = f"Traceroute hop stats | target={target} host={args.host or 'any'}"
    plots_made += int(plot_hop_stats(df, title, out_path("stats")))

    for key, rid in sorted(route_id_map.items(), key=lambda kv: kv[1]):
        run_ids = runs_sorted.loc[runs_sorted["route_id"] == rid, "id"].tolist()
        route_df = df[df["run_id"].isin(run_ids)]
        if route_df.empty:
            continue
        title = f"Route {rid} hop stats | target={target} host={args.host or 'any'} runs={len(run_ids)}"
        plots_made += int(plot_hop_stats(route_df, title, out_path(f"route{rid}")))

    if plots_made:
        plt.show()


if __name__ == "__main__":
    main()
