#!/usr/bin/env python3
"""
Network metrics logger with optional throughput tests.

Usage examples:
  # Basic logging (TOTAL only)
  python net_logger.py --db netstats.db --interval 10

  # iPerf3: upload (client->server, default)
  python net_logger.py --iperf iperf.he.net --iperf-duration 8

  # iPerf3: download (reverse, server->client)
  python net_logger.py --iperf iperf.he.net --iperf-reverse --iperf-duration 8

  # iPerf3: both directions in one run
  python net_logger.py --iperf iperf.he.net --iperf-bidir --iperf-duration 8

  # HTTP fallback (download only)
  python net_logger.py --http-url https://speed.hetzner.de/100MB.bin --http-seconds 8
"""
import argparse, sqlite3, time, socket, subprocess, sys, platform, shutil, json
from datetime import datetime, timezone

# ---- deps ----
try:
    import psutil
except ImportError:
    print("psutil is required. Install with: python -m pip install psutil", file=sys.stderr)
    sys.exit(1)

try:
    import requests
except Exception:
    requests = None


# ---- helpers ----
def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()


def _table_has_column(conn, table, col):
    cur = conn.execute(f"PRAGMA table_info({table});")
    return any(r[1] == col for r in cur.fetchall())


def ensure_schema(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS net_metrics (
        ts_utc TEXT NOT NULL,
        host TEXT NOT NULL,
        iface TEXT NOT NULL,
        bytes_sent INTEGER,
        bytes_recv INTEGER,
        packets_sent INTEGER,
        packets_recv INTEGER,
        errin INTEGER,
        errout INTEGER,
        dropin INTEGER,
        dropout INTEGER,
        bytes_sent_rate REAL,
        bytes_recv_rate REAL,
        packets_sent_rate REAL,
        packets_recv_rate REAL,
        ping_ms REAL,
        dns_ms REAL
    );
    """)
    # Add throughput columns if missing
    for col, typ in [
        ("thr_down_mbps", "REAL"),
        ("thr_up_mbps", "REAL"),
        ("thr_jitter_ms", "REAL"),
        ("thr_loss_pct", "REAL"),
        ("thr_method", "TEXT"),
    ]:
        if not _table_has_column(conn, "net_metrics", col):
            conn.execute(f"ALTER TABLE net_metrics ADD COLUMN {col} {typ};")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_net_metrics_ts ON net_metrics(ts_utc);")
    conn.commit()


def ping_ms(target, timeout=2):
    system = platform.system()
    cmd = ["ping", "-n", "1", "-w", str(int(timeout * 1000)), target] if system == "Windows" \
          else ["ping", "-c", "1", "-W", str(int(timeout)), target]
    try:
        start = time.time()
        out = subprocess.run(cmd, capture_output=True, text=True)
        elapsed_ms = (time.time() - start) * 1000.0
        if out.returncode == 0:
            text = out.stdout
            for tok in text.replace("=", " ").replace("<", " ").split():
                if tok.lower().endswith("ms"):
                    num = tok[:-2]
                    if num.replace(".", "", 1).isdigit():
                        try:
                            return float(num)
                        except ValueError:
                            pass
            return elapsed_ms
        return None
    except Exception:
        return None


def dns_lookup_ms(hostname):
    start = time.time()
    try:
        socket.gethostbyname(hostname)
        return (time.time() - start) * 1000.0
    except Exception:
        return None


def aggregate_counters(pernic):
    nic = psutil.net_io_counters(pernic=pernic)
    rows = {}
    if pernic:
        for name, stats in nic.items():
            rows[name] = stats
        # derive TOTAL
        total = psutil._common.snetio(
            bytes_sent=sum(s.bytes_sent for s in nic.values()),
            bytes_recv=sum(s.bytes_recv for s in nic.values()),
            packets_sent=sum(s.packets_sent for s in nic.values()),
            packets_recv=sum(s.packets_recv for s in nic.values()),
            errin=sum(s.errin for s in nic.values()),
            errout=sum(s.errout for s in nic.values()),
            dropin=sum(getattr(s, "dropin", 0) for s in nic.values()),
            dropout=sum(getattr(s, "dropout", 0) for s in nic.values()),
        )
        rows["TOTAL"] = total
    else:
        rows["TOTAL"] = nic  # single snetio
    return rows


def rate(prev, curr, dt):
    if dt <= 0:
        return {k: 0.0 for k in ("bytes_sent_rate", "bytes_recv_rate", "packets_sent_rate", "packets_recv_rate")}
    return {
        "bytes_sent_rate": (curr.bytes_sent - prev.bytes_sent) / dt,
        "bytes_recv_rate": (curr.bytes_recv - prev.bytes_recv) / dt,
        "packets_sent_rate": (curr.packets_sent - prev.packets_sent) / dt,
        "packets_recv_rate": (curr.packets_recv - prev.packets_recv) / dt,
    }


# ---- throughput tests ----
def iperf3_throughput(server, duration=5, port=None, reverse=False, bidir=False):
    """Return dict with down/up Mbps using iperf3 JSON output."""
    if not shutil.which("iperf3"):
        return None
    base = ["iperf3", "-c", server, "-J", "-t", str(int(duration))]
    if port:
        base += ["-p", str(int(port))]
    if reverse:
        base += ["-R"]
    if bidir:
        base += ["--bidir"]
    try:
        r = subprocess.run(base, capture_output=True, text=True, timeout=duration + 12)
        if r.returncode != 0:
            return None
        js = json.loads(r.stdout)
        end = js.get("end", {})
        sum_sent = end.get("sum_sent", {})        # client -> server (upload)
        sum_received = end.get("sum_received", {})  # server -> client (download when -R or bidir)
        def to_mbps(val): return (val or 0) / 1e6

        down_mbps = up_mbps = None
        if bidir:
            # In bidir, iperf3 still reports client sums for both directions
            down_mbps = to_mbps(sum_received.get("bits_per_second"))
            up_mbps   = to_mbps(sum_sent.get("bits_per_second"))
            method = "iperf3-bidir"
        elif reverse:
            down_mbps = to_mbps(sum_received.get("bits_per_second"))  # server -> client
            up_mbps   = None
            method = "iperf3-rev"
        else:
            up_mbps   = to_mbps(sum_sent.get("bits_per_second"))      # client -> server
            down_mbps = None
            method = "iperf3"

        return {
            "thr_down_mbps": down_mbps,
            "thr_up_mbps": up_mbps,
            "thr_jitter_ms": None,
            "thr_loss_pct": None,
            "thr_method": method,
        }
    except Exception:
        return None


def http_download_throughput(url, seconds=5):
    """Best-effort HTTP download throughput (downlink only)."""
    if requests is None:
        return None
    start = time.time()
    total = 0
    try:
        with requests.get(url, stream=True, timeout=seconds + 8) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=128 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if time.time() - start >= seconds:
                    break
    except Exception:
        return None
    elapsed = time.time() - start
    if elapsed <= 0:
        return None
    mbps = (total * 8) / 1e6 / elapsed
    return {
        "thr_down_mbps": mbps,
        "thr_up_mbps": None,
        "thr_jitter_ms": None,
        "thr_loss_pct": None,
        "thr_method": "http",
    }


# ---- DB insert ----
def insert_samples(conn, host, ts, rows, prev_rows, dt, ping_val, dns_val, thr):
    with conn:
        for iface, curr in rows.items():
            prev = prev_rows.get(iface, curr)
            rates = rate(prev, curr, dt)

            # Only write throughput to TOTAL row
            t_down = thr["thr_down_mbps"] if (thr and iface == "TOTAL") else None
            t_up   = thr["thr_up_mbps"]   if (thr and iface == "TOTAL") else None
            t_jit  = thr["thr_jitter_ms"] if (thr and iface == "TOTAL") else None
            t_loss = thr["thr_loss_pct"]  if (thr and iface == "TOTAL") else None
            t_meth = thr["thr_method"]    if (thr and iface == "TOTAL") else None

            conn.execute("""
            INSERT INTO net_metrics(
                ts_utc, host, iface,
                bytes_sent, bytes_recv, packets_sent, packets_recv,
                errin, errout, dropin, dropout,
                bytes_sent_rate, bytes_recv_rate, packets_sent_rate, packets_recv_rate,
                ping_ms, dns_ms,
                thr_down_mbps, thr_up_mbps, thr_jitter_ms, thr_loss_pct, thr_method
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, host, iface,
                int(curr.bytes_sent), int(curr.bytes_recv),
                int(curr.packets_sent), int(curr.packets_recv),
                int(curr.errin), int(curr.errout),
                int(getattr(curr, "dropin", 0)), int(getattr(curr, "dropout", 0)),
                float(rates["bytes_sent_rate"]), float(rates["bytes_recv_rate"]),
                float(rates["packets_sent_rate"]), float(rates["packets_recv_rate"]),
                None if iface != "TOTAL" else (None if ping_val is None else float(ping_val)),
                None if iface != "TOTAL" else (None if dns_val is None else float(dns_val)),
                t_down, t_up, t_jit, t_loss, t_meth
            ))


# ---- main ----
def main():
    ap = argparse.ArgumentParser(description="Log networking metrics (with optional throughput tests) to SQLite.")
    ap.add_argument("--db", default="netstats.db", help="SQLite DB path")
    ap.add_argument("--interval", type=float, default=10.0, help="Sample interval seconds")
    ap.add_argument("--ping", default="8.8.8.8", help="Ping target (IP/hostname)")
    ap.add_argument("--dns", default="google.com", help="Hostname to resolve for DNS timing")
    ap.add_argument("--pernic", action="store_true", help="Log per-interface metrics as well as total")
    ap.add_argument("--host-label", default=socket.gethostname(), help="Override host label")

    # throughput options
    ap.add_argument("--iperf", help="iperf3 server to test against (TCP). Example: iperf.he.net")
    ap.add_argument("--iperf-port", type=int, help="iperf3 server port")
    ap.add_argument("--iperf-duration", type=int, default=5, help="iperf3 test duration seconds")
    ap.add_argument("--iperf-reverse", action="store_true", help="Reverse (server->client) = download test")
    ap.add_argument("--iperf-bidir", action="store_true", help="Bidirectional up & down in one run")
    ap.add_argument("--http-url", help="HTTP URL to download for a downlink-only throughput test")
    ap.add_argument("--http-seconds", type=int, default=5, help="HTTP download test duration seconds")

    args = ap.parse_args()

    conn = sqlite3.connect(args.db, timeout=10)
    ensure_schema(conn)

    prev_rows = aggregate_counters(pernic=args.pernic)
    prev_t = time.time()

    print(f"[net_logger] DB={args.db} Interval={args.interval}s perNIC={args.pernic}. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(args.interval)
            ts = now_utc_iso()
            t = time.time()
            dt = t - prev_t
            rows = aggregate_counters(pernic=args.pernic)

            p_ms = ping_ms(args.ping)
            d_ms = dns_lookup_ms(args.dns)

            # optional throughput (prefer iperf if provided; else HTTP if provided)
            thr = None
            if args.iperf:
                thr = iperf3_throughput(
                    args.iperf,
                    duration=args.iperf_duration,
                    port=args.iperf_port,
                    reverse=args.iperf_reverse,
                    bidir=args.iperf_bidir,
                ) or thr
            if thr is None and args.http_url:
                thr = http_download_throughput(args.http_url, seconds=args.http_seconds)

            insert_samples(conn, args.host_label, ts, rows, prev_rows, dt, p_ms, d_ms, thr)

            # console heartbeat (TOTAL)
            total = rows["TOTAL"]
            up_rate = (total.bytes_sent - prev_rows["TOTAL"].bytes_sent) / max(dt, 1)
            thr_str = "None"
            if thr:
                parts = []
                if thr.get("thr_down_mbps") is not None:
                    parts.append(f"{round(thr['thr_down_mbps'],1)}↓")
                if thr.get("thr_up_mbps") is not None:
                    parts.append(f"{round(thr['thr_up_mbps'],1)}↑")
                thr_str = "/".join(parts) if parts else "None"
                method = thr.get("thr_method")
            else:
                method = None

            print(f"{ts} up={int(up_rate)} B/s ping={None if p_ms is None else round(p_ms,1)} ms "
                  f"dns={None if d_ms is None else round(d_ms,1)} ms thr={thr_str} Mb/s ({method})")

            prev_rows, prev_t = rows, t
    except KeyboardInterrupt:
        print("\n[net_logger] Stopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
