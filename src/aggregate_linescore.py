# src/aggregate_linescore.py
from pathlib import Path
import pandas as pd

def load_all_linescores(indir: Path) -> pd.DataFrame:
    files = sorted(indir.glob("*.parquet"))
    frames = []
    for p in files:
        try:
            frames.append(pd.read_parquet(p))
        except Exception:
            try:
                frames.append(pd.read_csv(p.with_suffix(".csv")))
            except Exception:
                pass
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    # normalize types
    for c in ["q1","q2","q3","q4","ot","total"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["side"] = df["side"].astype("category")
    return df

def tidy_linescore(indir: Path, out_base: Path) -> pd.DataFrame:
    df = load_all_linescores(indir)
    if df.empty:
        print("No linescores found.")
        return df
    df = df.drop_duplicates(subset=["game_id","side"])
    df.to_parquet(out_base.with_suffix(".parquet"), index=False)
    df.to_csv(out_base.with_suffix(".csv"), index=False)
    print(f"Saved tidy linescores: {len(df):,} rows → {out_base.name}.(parquet|csv)")
    return df

def wide_linescore(tidy: pd.DataFrame, out_base: Path) -> pd.DataFrame:
    if tidy.empty:
        return pd.DataFrame()
    # pivot to one row per game
    cols = ["q1","q2","q3","q4","ot","total","team"]
    keep = ["game_id","side"] + [c for c in cols if c in tidy.columns]
    tmp = tidy[keep].copy()
    wide_parts = []
    for side in ("away","home"):
        part = tmp[tmp["side"]==side].drop(columns=["side"]).copy()
        part = part.set_index("game_id")
        part.columns = [f"{side}_{c}" if c!="game_id" else c for c in part.columns]
        wide_parts.append(part)

    wide = wide_parts[0].join(wide_parts[1], how="outer")
    wide = wide.reset_index()
    wide.to_parquet(out_base.with_suffix(".parquet"), index=False)
    wide.to_csv(out_base.with_suffix(".csv"), index=False)
    print(f"Saved wide linescores: {len(wide):,} games → {out_base.name}.(parquet|csv)")
    return wide

def main():
    indir = Path("data/boxscores_linescore")
    out_tidy = Path("data/boxscores_linescore_all")
    out_wide = Path("data/boxscores_linescore_all_wide")
    tidy = tidy_linescore(indir, out_tidy)
    wide_linescore(tidy, out_wide)

if __name__ == "__main__":
    main()
