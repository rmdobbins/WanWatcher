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

  # Lighter throughput cadence + retention
  python net_logger.py --iperf iperf.he.net --throughput-every 6 --keep-days 7
"""
import argparse, sqlite3, time, socket, subprocess, sys, platform, shutil, json
from datetime import datetime, timezone, timedelta

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
    # Add additional columns if missing
    for col, typ in [
        ("thr_down_mbps", "REAL"),
        ("thr_up_mbps", "REAL"),
        ("thr_jitter_ms", "REAL"),
        ("thr_loss_pct", "REAL"),
        ("thr_method", "TEXT"),
        ("ping_min_ms", "REAL"),
        ("ping_avg_ms", "REAL"),
        ("ping_max_ms", "REAL"),
        ("ping_jitter_ms", "REAL"),
        ("ping_loss_pct", "REAL"),
        ("avail_ok", "INTEGER"),
        ("errin_delta", "INTEGER"),
        ("errout_delta", "INTEGER"),
        ("dropin_delta", "INTEGER"),
        ("dropout_delta", "INTEGER"),
        ("isup", "INTEGER"),
        ("speed_mbps", "INTEGER"),
        ("duplex", "TEXT"),
        ("mtu", "INTEGER"),
        ("ip4", "TEXT"),
        ("mac", "TEXT"),
    ]:
        if not _table_has_column(conn, "net_metrics", col):
            conn.execute(f"ALTER TABLE net_metrics ADD COLUMN {col} {typ};")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_net_metrics_ts ON net_metrics(ts_utc);")
    conn.commit()


def ping_stats(target, timeout=2, count=3):
    """Send multiple probes; return min/avg/max/jitter and loss%."""
    system = platform.system()
    base_cmd = ["ping", "-n", "1", "-w", str(int(timeout * 1000)), target] if system == "Windows" \
               else ["ping", "-c", "1", "-W", str(int(timeout)), target]
    samples = []

    for _ in range(max(1, count)):
        try:
            start = time.perf_counter()
            out = subprocess.run(base_cmd, capture_output=True, text=True, timeout=timeout + 1)
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            if out.returncode == 0:
                text = out.stdout
                found = None
                for tok in text.replace("=", " ").replace("<", " ").split():
                    if tok.lower().endswith("ms"):
                        num = tok[:-2]
                        if num.replace(".", "", 1).isdigit():
                            try:
                                found = float(num)
                                break
                            except ValueError:
                                pass
                samples.append(found if found is not None else elapsed_ms)
        except Exception:
            pass
        time.sleep(0.05)

    sent = max(1, count)
    received = len(samples)
    if received == 0:
        return None

    loss_pct = (1 - (received / float(sent))) * 100.0
    min_ms = min(samples)
    max_ms = max(samples)
    avg_ms = sum(samples) / received
    jitter_ms = max_ms - min_ms if received > 1 else 0.0

    return {
        "min_ms": min_ms,
        "avg_ms": avg_ms,
        "max_ms": max_ms,
        "jitter_ms": jitter_ms,
        "loss_pct": loss_pct,
    }


def dns_lookup_ms(hostname):
    start = time.perf_counter()
    try:
        socket.gethostbyname(hostname)
        return (time.perf_counter() - start) * 1000.0
    except Exception:
        return None


def _duplex_name(val):
    mapping = {
        getattr(psutil, "NIC_DUPLEX_FULL", None): "full",
        getattr(psutil, "NIC_DUPLEX_HALF", None): "half",
        getattr(psutil, "NIC_DUPLEX_UNKNOWN", None): None,
    }
    return mapping.get(val, None)


def _addr_info(addrs):
    ip4 = mac = None
    for a in addrs or []:
        fam = getattr(a, "family", None)
        if fam == socket.AF_INET:
            ip4 = a.address
        elif fam in (getattr(psutil, "AF_LINK", None), getattr(socket, "AF_LINK", None), getattr(socket, "AF_PACKET", None)):
            mac = a.address
    return ip4, mac


def aggregate_counters(pernic):
    iface_stats = psutil.net_if_stats()
    iface_addrs = psutil.net_if_addrs()
    nic = psutil.net_io_counters(pernic=pernic)
    rows, meta = {}, {}
    if pernic:
        for name, stats_row in nic.items():
            rows[name] = stats_row
            st = iface_stats.get(name)
            ip4, mac = _addr_info(iface_addrs.get(name, []))
            meta[name] = {
                "isup": None if st is None else int(bool(st.isup)),
                "speed_mbps": None if st is None else st.speed,
                "duplex": None if st is None else _duplex_name(st.duplex),
                "mtu": None if st is None else st.mtu,
                "ip4": ip4,
                "mac": mac,
            }
        # derive TOTAL
        if nic:
            sample = next(iter(nic.values()))
            fields = sample._fields
            totals = [sum(getattr(s, f, 0) for s in nic.values()) for f in fields]
            total = sample.__class__(*totals)
        else:
            base = psutil.net_io_counters(pernic=False)
            total = base.__class__(*([0] * len(base))) if base else None
        rows["TOTAL"] = total
        meta["TOTAL"] = {
            "isup": None,
            "speed_mbps": None,
            "duplex": None,
            "mtu": None,
            "ip4": None,
            "mac": None,
        }
    else:
        rows["TOTAL"] = nic  # single snetio
        meta["TOTAL"] = {
            "isup": None,
            "speed_mbps": None,
            "duplex": None,
            "mtu": None,
            "ip4": None,
            "mac": None,
        }
    return rows, meta


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

        def quality(sum_obj):
            jitter = sum_obj.get("jitter_ms") if isinstance(sum_obj, dict) else None
            loss_pct = sum_obj.get("lost_percent") if isinstance(sum_obj, dict) else None
            if loss_pct is None and isinstance(sum_obj, dict):
                lost = sum_obj.get("lost_packets")
                total = sum_obj.get("packets")
                if lost is not None and total:
                    try:
                        loss_pct = (float(lost) / float(total)) * 100.0
                    except Exception:
                        loss_pct = None
            return jitter, loss_pct

        jitter_down, loss_down = quality(sum_received)
        jitter_up, loss_up = quality(sum_sent)

        down_mbps = up_mbps = None
        jitter_ms = loss_pct = None
        if bidir:
            # In bidir, iperf3 still reports client sums for both directions
            down_mbps = to_mbps(sum_received.get("bits_per_second"))
            up_mbps   = to_mbps(sum_sent.get("bits_per_second"))
            jitter_ms = jitter_down or jitter_up
            loss_pct = loss_down if loss_down is not None else loss_up
            method = "iperf3-bidir"
        elif reverse:
            down_mbps = to_mbps(sum_received.get("bits_per_second"))  # server -> client
            up_mbps   = None
            jitter_ms = jitter_down
            loss_pct = loss_down
            method = "iperf3-rev"
        else:
            up_mbps   = to_mbps(sum_sent.get("bits_per_second"))      # client -> server
            down_mbps = None
            jitter_ms = jitter_up
            loss_pct = loss_up
            method = "iperf3"

        return {
            "thr_down_mbps": down_mbps,
            "thr_up_mbps": up_mbps,
            "thr_jitter_ms": jitter_ms,
            "thr_loss_pct": loss_pct,
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
def insert_samples(conn, host, ts, rows, meta, prev_rows, dt, ping_stats_val, dns_val, thr, avail_ok):
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
            info = meta.get(iface, {})

            errin_delta = max(int(curr.errin - prev.errin), 0)
            errout_delta = max(int(curr.errout - prev.errout), 0)
            dropin_delta = max(int(getattr(curr, "dropin", 0) - getattr(prev, "dropin", 0)), 0)
            dropout_delta = max(int(getattr(curr, "dropout", 0) - getattr(prev, "dropout", 0)), 0)

            ping_avg = ping_stats_val["avg_ms"] if (iface == "TOTAL" and ping_stats_val) else None
            ping_min = ping_stats_val["min_ms"] if (iface == "TOTAL" and ping_stats_val) else None
            ping_max = ping_stats_val["max_ms"] if (iface == "TOTAL" and ping_stats_val) else None
            ping_jit = ping_stats_val["jitter_ms"] if (iface == "TOTAL" and ping_stats_val) else None
            ping_loss = ping_stats_val["loss_pct"] if (iface == "TOTAL" and ping_stats_val) else None
            avail_val = int(bool(avail_ok)) if (iface == "TOTAL" and avail_ok is not None) else None

            placeholders = ",".join(["?"] * 38)
            conn.execute(f"""
            INSERT INTO net_metrics(
                ts_utc, host, iface,
                bytes_sent, bytes_recv, packets_sent, packets_recv,
                errin, errout, dropin, dropout,
                bytes_sent_rate, bytes_recv_rate, packets_sent_rate, packets_recv_rate,
                ping_ms, dns_ms,
                thr_down_mbps, thr_up_mbps, thr_jitter_ms, thr_loss_pct, thr_method,
                ping_min_ms, ping_avg_ms, ping_max_ms, ping_jitter_ms, ping_loss_pct,
                avail_ok, errin_delta, errout_delta, dropin_delta, dropout_delta,
                isup, speed_mbps, duplex, mtu, ip4, mac
            ) VALUES ({placeholders})
            """, (
                ts, host, iface,
                int(curr.bytes_sent), int(curr.bytes_recv),
                int(curr.packets_sent), int(curr.packets_recv),
                int(curr.errin), int(curr.errout),
                int(getattr(curr, "dropin", 0)), int(getattr(curr, "dropout", 0)),
                float(rates["bytes_sent_rate"]), float(rates["bytes_recv_rate"]),
                float(rates["packets_sent_rate"]), float(rates["packets_recv_rate"]),
                None if iface != "TOTAL" else (None if ping_avg is None else float(ping_avg)),
                None if iface != "TOTAL" else (None if dns_val is None else float(dns_val)),
                t_down, t_up, t_jit, t_loss, t_meth,
                ping_min, ping_avg, ping_max, ping_jit, ping_loss,
                avail_val, errin_delta, errout_delta, dropin_delta, dropout_delta,
                info.get("isup"), info.get("speed_mbps"), info.get("duplex"), info.get("mtu"),
                info.get("ip4"), info.get("mac")
            ))


# ---- main ----
def main():
    ap = argparse.ArgumentParser(description="Log networking metrics (with optional throughput tests) to SQLite.")
    ap.add_argument("--db", default="netstats.db", help="SQLite DB path")
    ap.add_argument("--interval", type=float, default=10.0, help="Sample interval seconds")
    ap.add_argument("--ping", default="8.8.8.8", help="Ping target (IP/hostname)")
    ap.add_argument("--ping-count", type=int, default=3, help="Number of ping probes per interval (>=1)")
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
    ap.add_argument("--throughput-every", type=int, default=1, help="Run throughput test every N samples (0 disables throughput tests)")
    ap.add_argument("--keep-days", type=float, help="Prune DB rows older than this many days")

    args = ap.parse_args()

    conn = sqlite3.connect(args.db, timeout=10)
    ensure_schema(conn)

    prev_rows, _ = aggregate_counters(pernic=args.pernic)
    prev_t = time.perf_counter()
    loop_count = 0

    print(f"[net_logger] DB={args.db} Interval={args.interval}s perNIC={args.pernic}. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(args.interval)
            loop_count += 1
            ts = now_utc_iso()
            t = time.perf_counter()
            dt = t - prev_t
            rows, meta = aggregate_counters(pernic=args.pernic)

            p_stats = ping_stats(args.ping, count=max(1, args.ping_count))
            d_ms = dns_lookup_ms(args.dns)
            avail_ok = (p_stats is not None) and (d_ms is not None)

            # optional throughput (prefer iperf if provided; else HTTP if provided)
            thr = None
            run_thr = args.throughput_every is not None and args.throughput_every != 0 \
                      and (loop_count % max(1, abs(args.throughput_every)) == 0)
            if run_thr and args.iperf:
                thr = iperf3_throughput(
                    args.iperf,
                    duration=args.iperf_duration,
                    port=args.iperf_port,
                    reverse=args.iperf_reverse,
                    bidir=args.iperf_bidir,
                ) or thr
            if run_thr and thr is None and args.http_url:
                thr = http_download_throughput(args.http_url, seconds=args.http_seconds)

            insert_samples(conn, args.host_label, ts, rows, meta, prev_rows, dt, p_stats, d_ms, thr, avail_ok)

            if args.keep_days:
                cutoff = datetime.now(timezone.utc) - timedelta(days=args.keep_days)
                with conn:
                    conn.execute("DELETE FROM net_metrics WHERE ts_utc < ?", (cutoff.isoformat(),))

            # console heartbeat (TOTAL)
            total = rows["TOTAL"]
            up_rate = (total.bytes_sent - prev_rows["TOTAL"].bytes_sent) / max(dt, 1e-9)
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

            ping_avg = None if p_stats is None else round(p_stats["avg_ms"], 1)
            ping_jit = None if p_stats is None else round(p_stats["jitter_ms"], 1)
            ping_loss = None if p_stats is None else round(p_stats["loss_pct"], 1)
            availability = "ok" if avail_ok else "fail"

            print(f"{ts} up={int(up_rate)} B/s ping={ping_avg} ms jitter={ping_jit} ms loss={ping_loss}% "
                  f"dns={None if d_ms is None else round(d_ms,1)} ms thr={thr_str} Mb/s ({method}) avail={availability}")

            prev_rows, prev_t = rows, t
    except KeyboardInterrupt:
        print("\n[net_logger] Stopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
