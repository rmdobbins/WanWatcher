import os
import time
import subprocess
import re
from datetime import datetime

def run_speed_test():
    """Runs a network speed test and returns the download and upload speeds."""
    try:
        result = subprocess.run([r".\SpeedTest\speedtest.exe"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return result.stdout
    except Exception as e:
        return f"Speed test failed: {str(e)}"

if __name__ == "__main__":
    print(run_speed_test())