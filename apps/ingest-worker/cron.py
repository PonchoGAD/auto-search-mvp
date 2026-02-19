import time
import subprocess

INTERVAL = 900  # 15 minutes


def run():
    while True:
        print("[CRON] starting ingest cycle")
        subprocess.run(["python", "main.py"])
        print(f"[CRON] sleeping {INTERVAL}s")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    run()
