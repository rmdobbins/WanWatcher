import sqlite3
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

    # Ping summary
    cursor.execute("SELECT COUNT(*) FROM ping_results")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM ping_results WHERE success = 1")
    successes = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ping_results WHERE success = 0")
    failures = cursor.fetchone()[0]
    uptime = (successes / total * 100) if total > 0 else 0

    cursor.execute("SELECT AVG(ping_value), MIN(ping_value), MAX(ping_value) FROM ping_results WHERE success = 1")
    ping_stats = cursor.fetchone()
    avg_ping, min_ping, max_ping = ping_stats if ping_stats else (None, None, None)

    print("ISP Reporting Summary")
    print("---------------------")
    print("Ping Results:")
    print(f"Total attempts: {total}")
    print(f"Successful pings: {successes}")
    print(f"Failed pings: {failures}")
    print(f"Uptime: {uptime:.2f}%")
    if avg_ping is not None:
        print(f"Average ping: {avg_ping:.2f} ms, Min: {min_ping} ms, Max: {max_ping} ms")
    else:
        print("No successful ping data available.")

    # Speed test summary
    cursor.execute("SELECT speed_test FROM ping_results WHERE speed_test IS NOT NULL")
    rows = cursor.fetchall()
    download_speeds = []
    upload_speeds = []
    for (speed_str,) in rows:
        if speed_str:
            d, u = parse_speed_test(speed_str)
            if d is not None and u is not None:
                download_speeds.append(d)
                upload_speeds.append(u)

    if download_speeds and upload_speeds:
        avg_download = sum(download_speeds) / len(download_speeds)
        min_download = min(download_speeds)
        max_download = max(download_speeds)
        avg_upload = sum(upload_speeds) / len(upload_speeds)
        min_upload = min(upload_speeds)
        max_upload = max(upload_speeds)
        print("\nSpeed Test Results:")
        print(f"Total tests: {len(download_speeds)}")
        print(f"Download speeds (Mbps): Avg: {avg_download:.2f}, Min: {min_download:.2f}, Max: {max_download:.2f}")
        print(f"Upload speeds (Mbps):   Avg: {avg_upload:.2f}, Min: {min_upload:.2f}, Max: {max_upload:.2f}")
    else:
        print("\nNo valid speed test data available.")

    conn.close()

if __name__ == "__main__":
    main()
