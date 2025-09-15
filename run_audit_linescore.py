from pathlib import Path
import pandas as pd

def main():
    index_csv = Path("data/boxscore_index.csv")
    outdir = Path("data/boxscores_linescore")
    idx = pd.read_csv(index_csv)
    idx["game_id"] = idx["game_id"].astype(str).str.lower()

    have = {p.stem.lower() for p in outdir.glob("*.parquet")} | {p.stem.lower() for p in outdir.glob("*.csv")}
    idx["have"] = idx["game_id"].isin(have)
    missing = idx[~idx["have"]].copy()

    print(f"Total in index: {len(idx)}")
    print(f"Have linescore: {len(idx) - len(missing)}")
    print(f"Missing linescore: {len(missing)}")

    if not missing.empty:
        missing_out = Path("data/missing_linescore.csv")
        missing[["game_id","date","home","away"]].to_csv(missing_out, index=False)
        print(f"Wrote missing list → {missing_out}")

if __name__ == "__main__":
    main()
