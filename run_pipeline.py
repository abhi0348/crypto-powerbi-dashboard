import schedule
import time
import subprocess
import logging
import os
from datetime import datetime

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s — %(message)s"
)

def run_pipeline():
    print(f"\n{'='*50}")
    print(f"Pipeline started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    logging.info("Pipeline started")

    steps = [
        ("Extract",   "python extract/coingecko_extractor.py"),
        ("Transform", "python transform/cleaner.py"),
        ("Load",      "python load/db_loader.py"),
    ]

    for step_name, command in steps:
        print(f"\n  [{step_name}] Running...")
        start = datetime.now()

        result = subprocess.run(
            command, shell=True,
            capture_output=True, text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )

        duration = (datetime.now() - start).seconds

        if result.returncode == 0:
            print(f"  [{step_name}] SUCCESS ({duration}s)")
            logging.info(f"{step_name} — SUCCESS ({duration}s)")
        else:
            print(f"  [{step_name}] FAILED")
            print(f"  Error: {result.stderr}")
            logging.error(f"{step_name} — FAILED: {result.stderr}")
            print("\nPipeline stopped due to error.")
            return False

    print(f"\nPipeline complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("Pipeline complete")
    return True

def scheduled_job():
    print(f"\nScheduled run triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    run_pipeline()

# Run once immediately when you start the script
print("Running pipeline now...")
run_pipeline()

# Schedule to run every day at 08:00 AM
schedule.every().day.at("08:00").do(scheduled_job)

# Also allow running every hour for testing
# schedule.every().hour.do(scheduled_job)

print("\nScheduler is running.")
print("Next run: every day at 08:00 AM")
print("Press Ctrl+C to stop.\n")

while True:
    schedule.run_pending()
    time.sleep(60)