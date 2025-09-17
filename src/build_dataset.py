# src/build_dataset.py
from pathlib import Path
import pandas as pd

def load_csv_or_parquet(base: Path) -> pd.DataFrame:
    if base.with_suffix(".parquet").exists():
        return pd.read_parquet(base.with_suffix(".parquet"))
    return pd.read_csv(base.with_suffix(".csv"))

def build_game_features(
    boxscore_index_csv: Path,
    linescore_wide_base: Path,
    totals_wide_base: Path,
    out_base: Path
):
    idx = pd.read_csv(boxscore_index_csv)
    idx["game_id"] = idx["game_id"].astype(str).str.lower()

    ls = load_csv_or_parquet(linescore_wide_base)
    ls["game_id"] = ls["game_id"].astype(str).str.lower()

    tot = load_csv_or_parquet(totals_wide_base)
    tot["game_id"] = tot["game_id"].astype(str).str.lower()

    # Minimal metadata from index
    meta_cols = [c for c in ["game_id","season","date","week","game_type","home","away"] if c in idx.columns]
    meta = idx[meta_cols].drop_duplicates("game_id")

    # Merge: meta + linescore + totals
    df = meta.merge(ls, on="game_id", how="left").merge(tot, on="game_id", how="left")

    # Derive outcomes and checks
    if {"away_total","home_total"}.issubset(df.columns):
        df["point_diff"] = df["home_total"] - df["away_total"]
        df["winner"] = df.apply(
            lambda r: r["home"] if pd.notna(r.get("home_total")) and pd.notna(r.get("away_total")) and r["home_total"]>r["away_total"]
            else (r["away"] if pd.notna(r.get("home_total")) and pd.notna(r.get("away_total")) and r["away_total"]>r["home_total"]
            else None),
            axis=1
        )
        df["loser"] = df.apply(
            lambda r: r["away"] if r.get("winner")==r.get("home") else (r["home"] if r.get("winner")==r.get("away") else None),
            axis=1
        )

    out_base.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_base.with_suffix(".parquet"), index=False)
    df.to_csv(out_base.with_suffix(".csv"), index=False)
    print(f"Saved modeling table: {len(df):,} rows → {out_base.name}.(parquet|csv)")

def main():
    boxscore_index_csv = Path("data/boxscore_index.csv")
    linescore_wide_base = Path("data/boxscores_linescore_all_wide")
    totals_wide_base = Path("data/boxscores_totals_all_wide")  # you created this earlier
    out_base = Path("data/model/game_features")
    build_game_features(boxscore_index_csv, linescore_wide_base, totals_wide_base, out_base)

if __name__ == "__main__":
    main()
