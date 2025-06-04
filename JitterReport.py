import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime
import re
from collections import defaultdict

# Regexes to match each line pattern
idle_line_pattern = re.compile(
    r"Idle Latency:\s*([\d\.]+)\s*ms\s*\(jitter:\s*([\d\.]+)ms,\s*low:\s*([\d\.]+)ms,\s*high:\s*([\d\.]+)ms\)"
)
download_header_pattern = re.compile(r"Download:\s*", re.IGNORECASE)
upload_header_pattern = re.compile(r"Upload:\s*", re.IGNORECASE)
latency_line_pattern = re.compile(
    r"^\s*([\d\.]+)\s*ms\s*\(jitter:\s*([\d\.]+)ms,\s*low:\s*([\d\.]+)ms,\s*high:\s*([\d\.]+)ms\)"
)

def parse_speed_test_by_lines(full_output):
    """
    Parse the full speed test output line by line to extract:
      - Idle Latency
      - Download Latency (line after "Download:")
      - Upload Latency   (line after "Upload:")

    Returns a dict like:
    {
      "idle":     {"latency": float, "jitter": float, "low": float, "high": float},
      "download": {...},
      "upload":   {...}
    }
    or {} if something is missing.
    """
    result = {}
    lines = full_output.splitlines()

    expect_download_latency = False
    expect_upload_latency = False

    for line in lines:
        line_stripped = line.strip()

        # 1) Idle Latency line
        m_idle = idle_line_pattern.search(line_stripped)
        if m_idle:
            result["idle"] = {
                "latency": float(m_idle.group(1)),
                "jitter":  float(m_idle.group(2)),
                "low":     float(m_idle.group(3)),
                "high":    float(m_idle.group(4))
            }
            continue

        # 2) Check for "Download:" header
        if download_header_pattern.search(line_stripped):
            expect_download_latency = True
            expect_upload_latency = False
            continue

        # 3) Check for "Upload:" header
        if upload_header_pattern.search(line_stripped):
            expect_download_latency = False
            expect_upload_latency = True
            continue

        # 4) Latency line for Download/Upload
        m_latency = latency_line_pattern.search(line)
        if m_latency:
            parsed = {
                "latency": float(m_latency.group(1)),
                "jitter":  float(m_latency.group(2)),
                "low":     float(m_latency.group(3)),
                "high":    float(m_latency.group(4))
            }
            if expect_download_latency:
                result["download"] = parsed
                expect_download_latency = False
            elif expect_upload_latency:
                result["upload"] = parsed
                expect_upload_latency = False

    # Only return if we found all three
    if "idle" in result and "download" in result and "upload" in result:
        return result
    return {}

def main():
    db_file = "ping_results.db"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Retrieve rows with a valid speed_test output.
    cursor.execute("SELECT timestamp, speed_test FROM ping_results WHERE speed_test IS NOT NULL")
    rows = cursor.fetchall()
    conn.close()

    # Bin the latencies by hour.
    idle_bins = defaultdict(list)
    download_bins = defaultdict(list)
    upload_bins = defaultdict(list)

    for timestamp_str, speed_test_output in rows:
        try:
            ts = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        parsed = parse_speed_test_by_lines(speed_test_output)
        if not parsed:
            continue  # Skip if any latency is missing

        # Bin by the start of the hour.
        hour_bin = ts.replace(minute=0, second=0, microsecond=0)

        idle_bins[hour_bin].append(parsed["idle"]["latency"])
        download_bins[hour_bin].append(parsed["download"]["latency"])
        upload_bins[hour_bin].append(parsed["upload"]["latency"])

    # If no valid data was found, exit.
    if not idle_bins and not download_bins and not upload_bins:
        print("No valid speed test data found.")
        return

    # Prepare sorted bins and corresponding data for each metric.
    sorted_bins = sorted(idle_bins.keys())
    idle_data = [idle_bins[b] for b in sorted_bins]
    download_data = [download_bins[b] for b in sorted_bins]
    upload_data = [upload_bins[b] for b in sorted_bins]
    labels = [b.strftime("%Y-%m-%d %H:00") for b in sorted_bins]

    # Create a single figure with 3 subplots.
    fig, axs = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

    axs[0].boxplot(idle_data, labels=labels, showfliers=True)
    axs[0].set_title("Idle Latency by Hour")
    axs[0].set_ylabel("Latency (ms)")
    axs[0].grid(True)

    axs[1].boxplot(download_data, labels=labels, showfliers=True)
    axs[1].set_title("Download Latency by Hour")
    axs[1].set_ylabel("Latency (ms)")
    axs[1].grid(True)

    axs[2].boxplot(upload_data, labels=labels, showfliers=True)
    axs[2].set_title("Upload Latency by Hour")
    axs[2].set_ylabel("Latency (ms)")
    axs[2].set_xlabel("Hour")
    axs[2].grid(True)
    plt.setp(axs[2].xaxis.get_majorticklabels(), rotation=45, ha='right')

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
