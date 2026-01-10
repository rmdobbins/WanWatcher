# WanWatcher

Simple network health logger and viewer built around SQLite.

## Requirements
- Python 3.12+ (with `venv`)
- iperf3 binary on PATH if you want throughput tests
- Python deps: `pip install -r requirements.txt`

## Setup
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Logging metrics
Run `net_logger.py` to collect counters, latency, availability, and optional throughput:
```powershell
python net_logger.py --db netstats.db --interval 10 --pernic --ping-count 5 --throughput-every 6 --keep-days 7 --iperf iperf.he.net --iperf-duration 8
```
Key options:
- `--interval`: sample period seconds.
- `--pernic`: log per-interface plus TOTAL.
- `--ping-count`: number of probes per interval (min/avg/max, jitter, loss are stored).
- `--dns`: hostname for DNS timing; `avail_ok` is set when ping + DNS succeed.
- Throughput: `--iperf ...` (TCP, up/down/bidir), or `--http-url ...` for download-only. Control cadence with `--throughput-every N` (0 disables throughput).
- `--keep-days`: prune rows older than N days to limit DB size.

## Traceroute logging
Run `net_traceroute_logger.py` to record traceroute hops to the same SQLite DB:
```powershell
python net_traceroute_logger.py --db netstats.db --target 8.8.8.8 --interval 300 --max-hops 30 --timeout-ms 2000
```
Key options:
- `--interval`: seconds between traceroute runs.
- `--target`: host/IP to trace.
- `--no-dns`: skip DNS lookups for faster, numeric-only hops.
- `--keep-days`: prune traceroute rows older than N days.

## Traceroute viewing
Analyze traceroute history and plot hop RTTs:
```powershell
python net_trace_view.py --db netstats.db --target 8.8.8.8 --since-hours 24 --runs 50
```
Notes:
- Saves two charts when `--out` is provided (last run + hop stats) and always shows the graphs.
- Use `--export-csv` to dump joined run/hop rows.

## Viewing metrics (matplotlib)
Render charts or export CSV with `net_view.py` (matplotlib UI supports pan/zoom):
```powershell
python net_view.py --db netstats.db --iface TOTAL --minutes 1440 --rate-clip 99.5 --rate-scale linear --out net_view.png --export-csv net_view.csv
```
Highlights:
- Byte/packet rates.
- Adjustable rate scaling: `--rate-clip` percentile caps Y-axis for spikes; `--rate-scale` can be `linear` or `log`.
- Latency band (ping min/max + avg), jitter, DNS, and shaded regions where `avail_ok` is false.
- Throughput up/down with jitter/loss overlays and method markers (iperf/http).
- Error/drop rates using stored deltas.
- Filter by host with `--host <label>`.

## Notes
- Schema evolves automatically; new columns are added on startup.
- iperf3 tests consume bandwidth—adjust `--throughput-every` to reduce load.
- The DB is plain SQLite; you can query it directly for custom dashboards.
