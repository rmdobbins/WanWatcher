import os
import time
import subprocess
import re
from datetime import datetime

def parse_ping_output(output):
    """Parses ping output and returns an array of four integer time values."""
    output = output.decode("utf-8", errors="ignore")
    match = re.search(r'Average = (\d+)', output)
    average_ping = int(match.group(1)) if match else None
    return int(average_ping)

def ping(host, latency):
    """Pings a host and returns True if successful, False otherwise."""
    try:
        result = subprocess.run(["ping", host], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            ping_out = parse_ping_output(result.stdout)
            if ping_out > latency:
                return False
            return True
        else:
            print(result.stdout)
            return False

    except Exception as e:
        print(e)
        return False


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

    # Update summary at the top
    lines[0] = f"Success: {success_count}, Failures: {failure_count}\n"

    with open(log_file, "w") as file:
        file.writelines(lines)


def main():
    host = "8.8.8.8"
    minimum_latency = 100
    log_file = "ping_log.txt"
    success_count = 0
    failure_count = 0

    # Initialize log file
    if not os.path.exists(log_file):
        with open(log_file, "w") as file:
            file.write("Success: 0, Failures: 0\n")

    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if ping(host, minimum_latency):
            success_count += 1
            log_entry = f"[{timestamp}] Ping to {host} successful.\n"
            if success_count % 600 == 0:
                log_entry += f"[{timestamp}] Running SpeedTest...\n"
                speed_test_output = run_speed_test()
                log_entry += speed_test_output + "\n"

        else:
            failure_count += 1
            log_entry = f"[{timestamp}] Ping to {host} FAILED. Running traceroute...\n"
            traceroute_output = trace_route(host)
            log_entry += traceroute_output + "\n"
            log_entry += f"[{timestamp}] Running SpeedTest...\n"
            speed_test_output = run_speed_test()
            log_entry += speed_test_output + "\n"

        with open(log_file, "a") as file:
            file.write(log_entry)

        update_log(success_count, failure_count, log_file)
        time.sleep(5)


if __name__ == "__main__":
    main()
