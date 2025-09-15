from pathlib import Path
from typing import List, Dict, Tuple
import pandas as pd
import re
from functools import reduce

# -----------------------------
# Normalization dictionaries
# -----------------------------
STAT_ALIASES = {
    "first downs": "first_downs",
    "firstdowns": "first_downs",

    "total yards": "total_yards",
    "total yds": "total_yards",
    "tot yards": "total_yards",
    "tot yds": "total_yards",

    "rushing yards": "rush_yards",
    "rush yards": "rush_yards",
    "rush-yds": "rush_yards",

    "passing yards": "pass_yards",
    "pass yards": "pass_yards",
    "pass-yds": "pass_yards",

    "turnovers": "turnovers",
    "to": "turnovers",

    "fumbles lost": "fumbles_lost",
    "fum lost": "fumbles_lost",

    "interceptions thrown": "ints_thrown",
    "int thrown": "ints_thrown",
    "interceptions": "ints_thrown",

    "penalties": "penalties",
    "penalties-yards": "penalties_yards",
    "pen-yds": "penalties_yards",
    "penalties/yds": "penalties_yards",

    "third down conv.": "third_down_conv",
    "third-down conv.": "third_down_conv",
    "3rd down conv.": "third_down_conv",
    "third down conv": "third_down_conv",

    "fourth down conv.": "fourth_down_conv",
    "4th down conv.": "fourth_down_conv",
    "fourth down conv": "fourth_down_conv",

    "time of possession": "time_of_possession",
    "possession time": "time_of_possession",
    "time of poss": "time_of_possession",
}

INT_LIKE = {
    "first_downs",
    "total_yards",
    "rush_yards",
    "pass_yards",
    "turnovers",
    "fumbles_lost",
    "ints_thrown",
    "penalties",
    "penalties_yards",
}

PCT_LIKE = {
    "third_down_conv",
    "fourth_down_conv",
}

TIME_LIKE = {"time_of_possession"}


# -----------------------------
# Helpers
# -----------------------------
def _normalize_stat_name(s: str) -> str:
    key = s.strip().lower()
    key = key.replace("–", "-").replace("—", "-")
    key = re.sub(r"\s+", " ", key)
    return STAT_ALIASES.get(key, re.sub(r"[^a-z0-9_]+", "_", key).strip("_"))

def _to_int_safe(val: str):
    try:
        return int(str(val).strip())
    except Exception:
        return None

def _to_pct_tuple(text: str) -> Tuple[int, int]:
    """
    Convert strings like '5-12' or '5/12' to (made=5, att=12).
    """
    m = re.match(r"^\s*(\d+)\s*[-/]\s*(\d+)\s*$", str(text))
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))

def _to_seconds_mmss(text: str):
    m = re.match(r"^\s*(\d+):(\d{2})\s*$", str(text))
    if not m:
        return None
    return int(m.group(1)) * 60 + int(m.group(2))

# -----------------------------
# Load & tidy long format
# -----------------------------
def load_all_totals(indir: Path) -> pd.DataFrame:
    files = sorted(indir.glob("*.parquet"))
    frames: List[pd.DataFrame] = []
    for p in files:
        try:
            df = pd.read_parquet(p)
            frames.append(df)
        except Exception:
            # CSV fallback
            try:
                df = pd.read_csv(p.with_suffix(".csv"))
                frames.append(df)
            except Exception:
                continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)

def tidy_totals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Normalize stat names
    df["stat_norm"] = df["stat"].astype(str).map(_normalize_stat_name)

    # Keep raw string copies
    df["away_val_raw"] = df["away_value"].astype(str)
    df["home_val_raw"] = df["home_value"].astype(str)

    # Typed fields we will fill
    for side in ("away", "home"):
        df[f"{side}_val_int"] = None
        df[f"{side}_made"] = None
        df[f"{side}_att"] = None
        df[f"{side}_seconds"] = None

    # Row-wise targeted parsing
    for idx, row in df.iterrows():
        s = row["stat_norm"]

        if s in INT_LIKE:
            for side in ("away", "home"):
                df.at[idx, f"{side}_val_int"] = _to_int_safe(row[f"{side}_val_raw"])

        if s in PCT_LIKE:
            for side in ("away", "home"):
                made, att = _to_pct_tuple(row[f"{side}_val_raw"])
                df.at[idx, f"{side}_made"] = made
                df.at[idx, f"{side}_att"] = att

        if s in TIME_LIKE:
            for side in ("away", "home"):
                df.at[idx, f"{side}_seconds"] = _to_seconds_mmss(row[f"{side}_val_raw"])

    return df

# -----------------------------
# Pivot wide
# -----------------------------
def pivot_totals_wide(df_tidy: pd.DataFrame) -> pd.DataFrame:
    """
    One row per game with numeric features. Ensures all pieces keep keys:
    ['game_id','away_team','home_team'] before merging.
    """
    if df_tidy.empty:
        return pd.DataFrame()

    key_cols = ["game_id", "away_team", "home_team"]
    # Build a base index of games
    base = df_tidy[key_cols].drop_duplicates().reset_index(drop=True)

    pieces: List[pd.DataFrame] = []

    # ---- Integer-like stats (first downs, yards, etc.)
    ints = df_tidy[df_tidy["stat_norm"].isin(INT_LIKE)][
        key_cols + ["stat_norm", "away_val_int", "home_val_int"]
    ].drop_duplicates()

    if not ints.empty:
        ints_w = (
            ints
            .pivot_table(index=key_cols, columns="stat_norm",
                         values=["away_val_int", "home_val_int"], aggfunc="first")
        )
        # Flatten columns: ('away_val_int','first_downs') -> 'away_first_downs'
        ints_w.columns = [f"{'away' if a=='away_val_int' else 'home'}_{b}" for (a, b) in ints_w.columns.to_flat_index()]
        ints_w = ints_w.reset_index()
        pieces.append(ints_w)

    # ---- Conversions like third/fourth down "made-att"
    conv = df_tidy[df_tidy["stat_norm"].isin(PCT_LIKE)][
        key_cols + ["stat_norm", "away_made", "away_att", "home_made", "home_att"]
    ].drop_duplicates()

    if not conv.empty:
        # Reshape to clear column names: away_third_down_conv_made, away_third_down_conv_att, etc.
        conv_away = conv[key_cols + ["stat_norm", "away_made", "away_att"]].copy()
        for col in ["away_made", "away_att"]:
            conv_away[col] = pd.to_numeric(conv_away[col], errors="coerce")
        conv_away = conv_away.pivot_table(index=key_cols, columns="stat_norm",
                                          values=["away_made", "away_att"], aggfunc="first")
        conv_away.columns = [f"away_{b}_{a.split('_')[1]}" for (a, b) in conv_away.columns.to_flat_index()]
        conv_away = conv_away.reset_index()

        conv_home = conv[key_cols + ["stat_norm", "home_made", "home_att"]].copy()
        for col in ["home_made", "home_att"]:
            conv_home[col] = pd.to_numeric(conv_home[col], errors="coerce")
        conv_home = conv_home.pivot_table(index=key_cols, columns="stat_norm",
                                          values=["home_made", "home_att"], aggfunc="first")
        conv_home.columns = [f"home_{b}_{a.split('_')[1]}" for (a, b) in conv_home.columns.to_flat_index()]
        conv_home = conv_home.reset_index()

        conv_w = pd.merge(conv_away, conv_home, on=key_cols, how="outer")
        pieces.append(conv_w)

    # ---- Time-like stats (time of possession in seconds)
    tpos = df_tidy[df_tidy["stat_norm"].isin(TIME_LIKE)][
        key_cols + ["stat_norm", "away_seconds", "home_seconds"]
    ].drop_duplicates()

    if not tpos.empty:
        tpos_w = (
            tpos
            .pivot_table(index=key_cols, columns="stat_norm",
                         values=["away_seconds", "home_seconds"], aggfunc="first")
        )
        tpos_w.columns = [f"{'away' if a=='away_seconds' else 'home'}_{b}_seconds" for (a, b) in tpos_w.columns.to_flat_index()]
        tpos_w = tpos_w.reset_index()
        pieces.append(tpos_w)

    # If we have no pieces, return empty
    if not pieces:
        return pd.DataFrame()

    # Merge all pieces on the keys; start from base so keys are preserved
    wide = base.copy()
    for part in pieces:
        if not set(key_cols).issubset(part.columns):
            # Skip malformed piece
            continue
        wide = pd.merge(wide, part, on=key_cols, how="left")

    return wide

# -----------------------------
# Orchestration
# -----------------------------
def aggregate_totals(indir: Path, out_base: Path):
    df_all = load_all_totals(indir)
    if df_all.empty:
        print("No per-game totals found.")
        return

    tidy = tidy_totals(df_all)
    out_long_parquet = out_base.with_suffix(".parquet")
    out_long_csv = out_base.with_suffix(".csv")
    tidy.to_parquet(out_long_parquet, index=False)
    tidy.to_csv(out_long_csv, index=False)
    print(f"Saved tidy totals: {len(tidy):,} rows → {out_base.name}.(parquet|csv)")

    wide = pivot_totals_wide(tidy)
    if not wide.empty:
        out_wide = out_base.parent / (out_base.stem + "_wide")
        wide.to_parquet(out_wide.with_suffix(".parquet"), index=False)
        wide.to_csv(out_wide.with_suffix(".csv"), index=False)
        print(f"Saved wide totals: {len(wide):,} games → {out_wide.name}.(parquet|csv)")
    else:
        print("No wide totals produced (no numeric stats detected).")

def main():
    indir = Path("data/boxscores_totals")
    out_base = Path("data/boxscores_totals_all")
    aggregate_totals(indir, out_base)

if __name__ == "__main__":
    main()
