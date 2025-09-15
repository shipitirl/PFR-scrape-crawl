from pathlib import Path
from src.boxtotals import build_totals_for_index

def main():
    index_csv = Path("data/boxscore_index.csv")
    outdir = Path("data/boxscores_totals")
    # set a limit for testing; change to None to run full
    processed, skipped = build_totals_for_index(index_csv, outdir, limit=100)
    print(f"Processed: {processed} games")
    print(f"Skipped (already done): {len(skipped)}")

if __name__ == "__main__":
    main()
