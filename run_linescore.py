from pathlib import Path
import os
import pandas as pd
from src.linescore import fetch_linescore
from src.fetch import RateLimited, FetchError

def main():
    # no throttle; skip on 429
    os.environ.setdefault("PFR_MIN_INTERVAL", "0.0")
    os.environ.setdefault("PFR_POLICY", "skip")

    outdir = Path("data/boxscores_linescore")
    outdir.mkdir(parents=True, exist_ok=True)

    idx = pd.read_csv("data/boxscore_index.csv")
    idx["game_id"] = idx["game_id"].astype(str).str.lower()

    have = {p.stem.lower() for p in outdir.glob("*.parquet")} | {p.stem.lower() for p in outdir.glob("*.csv")}
    missing = idx[~idx["game_id"].isin(have)].sample(frac=1.0, random_state=42)  # shuffle

    processed = 0
    limit = 150  # adjust as you like

    for _, row in missing.iterrows():
        gid = row["game_id"]
        try:
            df = fetch_linescore(gid)
            if df is not None and not df.empty:
                df.to_parquet(outdir / f"{gid}.parquet", index=False)
                df.to_csv(outdir / f"{gid}.csv", index=False)
                processed += 1
                if processed % 10 == 0:
                    print(f"[info] processed: {processed}")
        except RateLimited:
            print(f"[429] skip {gid}")
        except FetchError as e:
            print(f"[fail] {gid}: {e}")
        except Exception as e:
            print(f"[fail] {gid}: {e}")

        if processed >= limit:
            break

    print(f"Processed in this pass: {processed}")

if __name__ == "__main__":
    main()
