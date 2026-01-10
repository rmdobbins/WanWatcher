#!/usr/bin/env python3
"""
Traceroute logger that runs in a loop and records results to SQLite.

Usage example:
  python net_traceroute_logger.py --db netstats.db --target 8.8.8.8 --interval 300
"""
import argparse
import ipaddress
import platform
import shutil
import socket
import sqlite3
import subprocess
import time
import re
from datetime import datetime, timezone, timedelta


def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()


def _table_has_column(conn, table, col):
    cur = conn.execute(f"PRAGMA table_info({table});")
    return any(r[1] == col for r in cur.fetchall())


def ensure_schema(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS trace_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_utc TEXT NOT NULL,
        host TEXT NOT NULL,
        target TEXT NOT NULL,
        tool TEXT,
        max_hops INTEGER,
        timeout_ms INTEGER,
        query_count INTEGER,
        exit_code INTEGER,
        hop_count INTEGER,
        raw_output TEXT
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS trace_hops (
        run_id INTEGER NOT NULL,
        hop INTEGER NOT NULL,
        hop_host TEXT,
        hop_ip TEXT,
        rtt_ms1 REAL,
        rtt_ms2 REAL,
        rtt_ms3 REAL,
        rtt_avg_ms REAL,
        status TEXT
    );
    """)

    for col, typ in [
        ("tool", "TEXT"),
        ("max_hops", "INTEGER"),
        ("timeout_ms", "INTEGER"),
        ("query_count", "INTEGER"),
        ("exit_code", "INTEGER"),
        ("hop_count", "INTEGER"),
        ("raw_output", "TEXT"),
    ]:
        if not _table_has_column(conn, "trace_runs", col):
            conn.execute(f"ALTER TABLE trace_runs ADD COLUMN {col} {typ};")

    for col, typ in [
        ("hop_host", "TEXT"),
        ("hop_ip", "TEXT"),
        ("rtt_ms1", "REAL"),
        ("rtt_ms2", "REAL"),
        ("rtt_ms3", "REAL"),
        ("rtt_avg_ms", "REAL"),
        ("status", "TEXT"),
    ]:
        if not _table_has_column(conn, "trace_hops", col):
            conn.execute(f"ALTER TABLE trace_hops ADD COLUMN {col} {typ};")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_trace_runs_ts ON trace_runs(ts_utc);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trace_runs_target ON trace_runs(target);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trace_hops_run ON trace_hops(run_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trace_hops_ip ON trace_hops(hop_ip);")
    conn.commit()


def _build_command(target, max_hops, timeout_ms, query_count, no_dns, tool_override):
    system = platform.system()
    tool = tool_override
    if not tool:
        tool = "tracert" if system == "Windows" else "traceroute"

    if system == "Windows":
        cmd = [tool, "-h", str(int(max_hops)), "-w", str(int(timeout_ms))]
        if no_dns:
            cmd.append("-d")
        cmd.append(target)
        timeout_s = max(5, (max_hops * max(1, query_count) * (timeout_ms / 1000.0)) + 5)
        return cmd, timeout_s, None

    if not shutil.which(tool):
        return None, None, f"{tool} not found on PATH"

    timeout_s = max(1, int(round(timeout_ms / 1000.0)))
    cmd = [tool, "-m", str(int(max_hops)), "-w", str(timeout_s), "-q", str(int(query_count))]
    if no_dns:
        cmd.append("-n")
    cmd.append(target)
    timeout_s = max(5, (max_hops * max(1, query_count) * timeout_s) + 5)
    return cmd, timeout_s, None


def run_traceroute(target, max_hops, timeout_ms, query_count, no_dns, tool_override):
    cmd, timeout_s, err = _build_command(target, max_hops, timeout_ms, query_count, no_dns, tool_override)
    if err:
        return None, None, err, None
    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            errors="replace",
        )
        output = (r.stdout or "") + ("\n" + r.stderr if r.stderr else "")
        return output, r.returncode, None, cmd
    except subprocess.TimeoutExpired as e:
        output = (e.stdout or "") + ("\n" + e.stderr if e.stderr else "")
        return output, -1, "timeout", cmd
    except Exception as e:
        return None, -1, str(e), cmd


_RTT_RE = re.compile(r"(<)?(\d+(?:\.\d+)?)\s*ms", re.IGNORECASE)


def _parse_rtts(line):
    rtts = []
    for m in _RTT_RE.finditer(line):
        val = float(m.group(2))
        if m.group(1):
            val = max(val * 0.5, 0.1)
        rtts.append(val)
    return rtts


def _strip_hop_prefix(prefix):
    parts = prefix.split()
    if len(parts) <= 1:
        return None
    host = " ".join(parts[1:])
    return host if host else None


def _host_from_tail(prefix):
    idx = prefix.rfind(" ms")
    if idx != -1:
        host = prefix[idx + 3:].strip()
        return host if host else None
    return _strip_hop_prefix(prefix)


def _parse_host_ip(line):
    m = re.search(r"\[(?P<ip>[^\]]+)\]\s*$", line)
    if m:
        ip = m.group("ip").strip()
        host = _host_from_tail(line[:m.start()].strip())
        return host, ip

    m = re.search(r"\((?P<ip>[^)]+)\)", line)
    if m:
        ip = m.group("ip").strip()
        host = _strip_hop_prefix(line[:m.start()].strip())
        return host, ip

    for tok in re.split(r"\s+", line.strip()):
        try:
            ipaddress.ip_address(tok)
            return None, tok
        except ValueError:
            continue
    return None, None


def _hop_status(line, rtts):
    text = line.lower()
    if "timed out" in text or "timeout" in text:
        return "timeout"
    if "*" in line and not rtts:
        return "timeout"
    if rtts:
        return "ok"
    return "unknown"


def parse_hops(output):
    hops = []
    for line in (output or "").splitlines():
        m = re.match(r"^\s*(\d+)\s+", line)
        if not m:
            continue
        hop = int(m.group(1))
        rtts = _parse_rtts(line)
        host, ip = _parse_host_ip(line)
        rtt1 = rtts[0] if len(rtts) > 0 else None
        rtt2 = rtts[1] if len(rtts) > 1 else None
        rtt3 = rtts[2] if len(rtts) > 2 else None
        rtt_avg = (sum(rtts) / len(rtts)) if rtts else None
        hops.append({
            "hop": hop,
            "hop_host": host,
            "hop_ip": ip,
            "rtt_ms1": rtt1,
            "rtt_ms2": rtt2,
            "rtt_ms3": rtt3,
            "rtt_avg_ms": rtt_avg,
            "status": _hop_status(line, rtts),
        })
    return hops


def insert_run(conn, host, ts, target, tool, max_hops, timeout_ms, query_count, exit_code, raw_output, hops):
    hop_count = len(hops)
    with conn:
        cur = conn.execute("""
        INSERT INTO trace_runs(
            ts_utc, host, target, tool, max_hops, timeout_ms, query_count, exit_code, hop_count, raw_output
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ts, host, target, tool, max_hops, timeout_ms, query_count, exit_code, hop_count, raw_output
        ))
        run_id = cur.lastrowid
        for hop in hops:
            conn.execute("""
            INSERT INTO trace_hops(
                run_id, hop, hop_host, hop_ip, rtt_ms1, rtt_ms2, rtt_ms3, rtt_avg_ms, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                hop["hop"],
                hop["hop_host"],
                hop["hop_ip"],
                hop["rtt_ms1"],
                hop["rtt_ms2"],
                hop["rtt_ms3"],
                hop["rtt_avg_ms"],
                hop["status"],
            ))
    return run_id


def prune_old(conn, keep_days):
    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)
    cutoff_iso = cutoff.isoformat()
    with conn:
        conn.execute("DELETE FROM trace_hops WHERE run_id IN (SELECT id FROM trace_runs WHERE ts_utc < ?)", (cutoff_iso,))
        conn.execute("DELETE FROM trace_runs WHERE ts_utc < ?", (cutoff_iso,))


def main():
    ap = argparse.ArgumentParser(description="Log traceroute results to SQLite in a loop.")
    ap.add_argument("--db", default="netstats.db", help="SQLite DB path")
    ap.add_argument("--target", default="8.8.8.8", help="Traceroute target host/IP")
    ap.add_argument("--interval", type=float, default=300.0, help="Seconds between traceroute runs")
    ap.add_argument("--max-hops", type=int, default=30, help="Max hop count")
    ap.add_argument("--timeout-ms", type=int, default=2000, help="Per-probe timeout in ms")
    ap.add_argument("--queries", type=int, default=3, help="Probes per hop (Unix only)")
    ap.add_argument("--no-dns", action="store_true", help="Disable DNS lookups (use -d/-n)")
    ap.add_argument("--host-label", default=socket.gethostname(), help="Override host label")
    ap.add_argument("--tool", default="", help="Override traceroute tool name/path")
    ap.add_argument("--keep-days", type=float, help="Prune DB rows older than this many days")
    ap.add_argument("--once", action="store_true", help="Run a single traceroute and exit")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db, timeout=10)
    ensure_schema(conn)

    print(f"[net_traceroute_logger] DB={args.db} Target={args.target} Interval={args.interval}s. Ctrl+C to stop.")
    try:
        while True:
            ts = now_utc_iso()
            output, exit_code, err, cmd = run_traceroute(
                args.target,
                args.max_hops,
                args.timeout_ms,
                args.queries,
                args.no_dns,
                args.tool or None,
            )
            tool = cmd[0] if cmd else (args.tool or ("tracert" if platform.system() == "Windows" else "traceroute"))
            if output is None and err:
                print(f"{ts} error={err}")
                if args.once:
                    break
                time.sleep(args.interval)
                continue

            hops = parse_hops(output or "")
            insert_run(
                conn,
                args.host_label,
                ts,
                args.target,
                tool,
                args.max_hops,
                args.timeout_ms,
                args.queries,
                exit_code,
                output or "",
                hops,
            )

            if args.keep_days:
                prune_old(conn, args.keep_days)

            hop_count = len(hops)
            status = "ok" if exit_code == 0 else "err"
            print(f"{ts} target={args.target} hops={hop_count} status={status}")

            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[net_traceroute_logger] Stopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
