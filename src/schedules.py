from pathlib import Path
import pandas as pd
from .fetch import get
from .parse import read_single_table_by_id, clean_columns

BASE = "https://www.pro-football-reference.com"

def scrape_schedule_year(year: int) -> pd.DataFrame:
    url = f"{BASE}/years/{year}/games.htm"
    html = get(url)
    df = read_single_table_by_id(html, "games")
    if df.empty:
        return df
    df = clean_columns(df)

    # Drop any header repeat rows
    if "week" in df.columns:
        df = df[df["week"] != "Week"]

    # Standardize some columns if present
    rename_map = {
        "winner/tie": "winner",
        "loser/tie": "loser",
        "ptsw": "pts_w",
        "ptsl": "pts_l",
        "game_site": "site",
    }
    rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    df["season"] = year
    df.columns = [str(c).strip() for c in df.columns]

    for col in ["week", "pts_w", "pts_l"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.reset_index(drop=True)

def scrape_schedules(start_year=2002, end_year=None) -> pd.DataFrame:
    import datetime as dt
    if end_year is None:
        end_year = dt.date.today().year
    frames = []
    for y in range(start_year, end_year + 1):
        dfy = scrape_schedule_year(y)
        if not dfy.empty:
            frames.append(dfy)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "date" in df.columns:
        df = df[df["date"].notna()]
    return df

def main():
    outdir = Path("data")
    outdir.mkdir(parents=True, exist_ok=True)
    df = scrape_schedules(2002)
    if df.empty:
        print("No data scraped. Check parser/table id.")
        return
    df.to_parquet(outdir / "schedules.parquet", index=False)
    df.to_csv(outdir / "schedules.csv", index=False)
    print(f"Saved {len(df):,} rows to data/schedules.parquet and data/schedules.csv")

if __name__ == "__main__":
    main()
