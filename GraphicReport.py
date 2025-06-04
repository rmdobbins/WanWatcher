import sqlite3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import re

def parse_speed_test(speed_str):
    """
    Parses the speed test output to extract download and upload speeds in Mbps.
    Expected format: "Download: <value> Mbps" and "Upload: <value> Mbps".
    Returns a tuple (download, upload) as floats if found, otherwise (None, None).
    """
    download_match = re.search(r"Download:\s*([\d\.]+)\s*Mbps", speed_str)
    upload_match = re.search(r"Upload:\s*([\d\.]+)\s*Mbps", speed_str)
    download = float(download_match.group(1)) if download_match else None
    upload = float(upload_match.group(1)) if upload_match else None
    return download, upload

def main():
    db_file = "ping_results.db"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Retrieve timestamp, ping value, and speed test output from the database.
    cursor.execute("SELECT timestamp, ping_value, speed_test FROM ping_results")
    rows = cursor.fetchall()
    conn.close()

    # Lists to hold our data for ping and speed test.
    times_ping, ping_values = [], []
    times_speed, download_values, upload_values = [], [], []

    for timestamp_str, ping_val, speed_test in rows:
        try:
            ts = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        # Record ping data if available.
        if ping_val is not None:
            times_ping.append(ts)
            ping_values.append(ping_val)

        # Parse and record speed test data if available.
        if speed_test:
            d, u = parse_speed_test(speed_test)
            if d is not None and u is not None:
                times_speed.append(ts)
                download_values.append(d)
                upload_values.append(u)

    # Calculate jitter as the absolute difference between consecutive ping values.
    times_jitter, jitter_values = [], []
    for i in range(1, len(ping_values)):
        jitter = abs(ping_values[i] - ping_values[i - 1])
        jitter_values.append(jitter)
        times_jitter.append(times_ping[i])

    # Create a chart with three subplots.
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True, figsize=(12, 12))

    # Plot ping data.
    ax1.plot(times_ping, ping_values, marker='o', linestyle='-', color='blue', label="Ping (ms)")
    ax1.set_title("Ping Over Time")
    ax1.set_ylabel("Ping (ms)")
    ax1.grid(True)
    ax1.legend()

    # Plot speed test data.
    ax2.plot(times_speed, download_values, marker='x', linestyle='-', color='green', label="Download (Mbps)")
    ax2.plot(times_speed, upload_values, marker='x', linestyle='-', color='red', label="Upload (Mbps)")
    # Add horizontal lines for stated speeds.
    ax2.axhline(y=1000, color='purple', linestyle='--', label="Stated Download (1Gbps)")
    ax2.axhline(y=50, color='orange', linestyle='--', label="Stated Upload (50Mbps)")
    ax2.set_title("Speed Test Over Time")
    ax2.set_ylabel("Speed (Mbps)")
    ax2.grid(True)
    ax2.legend()

    # Plot jitter data.
    ax3.plot(times_jitter, jitter_values, marker='s', linestyle='-', color='magenta', label="Jitter (ms)")
    ax3.set_title("Jitter Over Time")
    ax3.set_ylabel("Jitter (ms)")
    ax3.set_xlabel("Time")
    ax3.grid(True)
    ax3.legend()

    # Format x-axis to show dates nicely.
    ax3.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    plt.gcf().autofmt_xdate()

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
