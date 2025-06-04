import os
import time
import subprocess
import re
import sqlite3
from datetime import datetime

def init_db(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ping_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            host TEXT,
            success INTEGER,
            ping_value INTEGER,
            traceroute TEXT,
            speed_test TEXT
        )
    ''')
    conn.commit()
    return conn

def parse_ping_output(output):
    """Parses ping output and returns the average ping time as an integer."""
    output = output.decode("utf-8", errors="ignore")
    match = re.search(r'Average = (\d+)', output)
    return int(match.group(1)) if match else None

def ping(host, latency):
    """Pings a host and returns a tuple (success, ping_value)."""
    try:
        result = subprocess.run(["ping", host], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        ping_value = None
        if result.returncode == 0:
            ping_value = parse_ping_output(result.stdout)
            if ping_value is None or ping_value > latency:
                return False, ping_value
            return True, ping_value
        else:
            print(result.stdout)
            return False, ping_value
    except Exception as e:
        print(e)
        return False, None

def trace_route(host):
    """Runs traceroute to the given host and returns the output."""
    try:
        result = subprocess.run(["pathping", "-q", "10", host], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return result.stdout
    except Exception as e:
        return f"Traceroute failed: {str(e)}"

def run_speed_test():
    """Runs a network speed test and returns the download and upload speeds."""
    try:
        result = subprocess.run([r".\SpeedTest\speedtest.exe"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return result.stdout
    except Exception as e:
        return f"Speed test failed: {str(e)}"

def update_log(success_count, failure_count, log_file):
    """Updates the log file to keep a summary at the top."""
    with open(log_file, "r") as file:
        lines = file.readlines()
    lines[0] = f"Success: {success_count}, Failures: {failure_count}\n"
    with open(log_file, "w") as file:
        file.writelines(lines)

def main():
    host = "8.8.8.8"
    minimum_latency = 100
    log_file = "ping_log.txt"
    db_file = "ping_results.db"
    success_count = 0
    failure_count = 0

    # Initialize log file
    if not os.path.exists(log_file):
        with open(log_file, "w") as file:
            file.write("Success: 0, Failures: 0\n")

    # Initialize database
    conn = init_db(db_file)
    cursor = conn.cursor()

    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        speed_test_output = None
        traceroute_output = None

        success, ping_value = ping(host, minimum_latency)
        if success:
            success_count += 1
            log_entry = f"[{timestamp}] Ping to {host} successful. Ping time: {ping_value}\n"
            # Run speed test every 600 successful pings
            if success_count % 120 == 0:
                log_entry += f"[{timestamp}] Running SpeedTest...\n"
                speed_test_output = run_speed_test()
                log_entry += speed_test_output + "\n"
        else:
            failure_count += 1
            log_entry = f"[{timestamp}] Ping to {host} FAILED. Ping time: {ping_value}\n"
            log_entry += f"[{timestamp}] Running traceroute...\n"
            traceroute_output = trace_route(host)
            log_entry += traceroute_output + "\n"
            log_entry += f"[{timestamp}] Running SpeedTest...\n"
            speed_test_output = run_speed_test()
            log_entry += speed_test_output + "\n"

        with open(log_file, "a") as file:
            file.write(log_entry)
        update_log(success_count, failure_count, log_file)

        # Insert result into database
        cursor.execute(
            "INSERT INTO ping_results (timestamp, host, success, ping_value, traceroute, speed_test) VALUES (?, ?, ?, ?, ?, ?)",
            (timestamp, host, 1 if success else 0, ping_value, traceroute_output, speed_test_output)
        )
        conn.commit()

        time.sleep(5)

if __name__ == "__main__":
    main()
