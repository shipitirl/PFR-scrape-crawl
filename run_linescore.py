# run_linescore.py
import os
from pathlib import Path
from src.linescore import build_linescores_for_index

def main():
    # Polite defaults (adjust if needed)
    os.environ.setdefault("PFR_MIN_INTERVAL", "2.0")
    os.environ.setdefault("PFR_MAX_ATTEMPTS", "5")
    os.environ.setdefault("PFR_RETRY_AFTER_CAP", "45.0")
    os.environ.setdefault("PFR_CONNECT_TIMEOUT", "6.0")
    os.environ.setdefault("PFR_READ_TIMEOUT", "15.0")
    os.environ.setdefault("PFR_CACHE_SECS", "86400")

    index_csv = Path("data/boxscore_index.csv")
    outdir = Path("data/boxscores_linescore")

    processed, skipped = build_linescores_for_index(index_csv, outdir, limit=None)
    print(f"Processed: {processed} games; Skipped (already had): {len(skipped)}")

if __name__ == "__main__":
    main()
