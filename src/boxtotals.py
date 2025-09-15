from pathlib import Path
from typing import Optional, Tuple, List
from io import StringIO
import time
import pandas as pd
from bs4 import BeautifulSoup
from .fetch import get
from .parse import uncomment_html

BASE = "https://www.pro-football-reference.com"

def _read_html_tables(markup: str) -> List[pd.DataFrame]:
    try:
        return pd.read_html(StringIO(markup))
    except ValueError:
        return []

def _find_team_totals_table(soup: BeautifulSoup) -> Optional[pd.DataFrame]:
    # 1) Common id
    table = soup.find("table", id="team_stats")
    if table:
        dfs = _read_html_tables(str(table))
        if dfs:
            return dfs[0]

    # 2) Heuristic fallback
    for t in soup.find_all("table"):
        df_list = _read_html_tables(str(t))
        if not df_list:
            continue
        df = df_list[0]
        if df.empty or df.shape[1] < 3:
            continue
        first_col = str(df.columns[0]).strip().lower()
        col0_vals = df.iloc[:, 0].astype(str).str.lower()
        signals = ("total yards", "turnovers", "first downs", "time of possession", "penalties")
        if first_col in ("team stats", "stat", "team_stats", "statistics") or col0_vals.str.contains("|".join(signals)).any():
            return df
    return None

def _normalize_team_totals_df(df: pd.DataFrame, away_team: str, home_team: str) -> pd.DataFrame:
    if df.shape[1] >= 3:
        df = df.iloc[:, :3]
    else:
        return pd.DataFrame()

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    col1_name, col2_name = str(df.columns[1]).strip(), str(df.columns[2]).strip()
    df.rename(columns={df.columns[0]: "stat", df.columns[1]: "col_awaymaybe", df.columns[2]: "col_homemaybe"}, inplace=True)

    df["stat"] = df["stat"].astype(str).str.strip()
    df = df[df["stat"].str.len() > 0]
    df = df[df["stat"].str.lower() != "stat"].reset_index(drop=True)

    df["away_value"] = df["col_awaymaybe"].astype(str).str.strip()
    df["home_value"] = df["col_homemaybe"].astype(str).str.strip()

    nonempty_mask = (df["away_value"].map(lambda s: s != "") | df["home_value"].map(lambda s: s != ""))
    df = df.loc[nonempty_mask].copy()

    df["away_team_header"] = col1_name
    df["home_team_header"] = col2_name
    df["away_team"] = away_team
    df["home_team"] = home_team

    return df[["stat","away_value","home_value","away_team","home_team","away_team_header","home_team_header"]]

def fetch_boxscore_totals(game_id: str, away_team: str, home_team: str) -> Optional[pd.DataFrame]:
    url = f"{BASE}/boxscores/{game_id}.htm"
    html = get(url)
    soup = BeautifulSoup(uncomment_html(html), "lxml")
    df_raw = _find_team_totals_table(soup)
    if df_raw is None or df_raw.empty:
        return None
    df_tidy = _normalize_team_totals_df(df_raw, away_team, home_team)
    if df_tidy.empty:
        return None
    df_tidy.insert(0, "game_id", game_id)
    return df_tidy

def save_game_totals(df: pd.DataFrame, outdir: Path) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    gid = df["game_id"].iloc[0]
    p_parquet, p_csv = outdir / f"{gid}.parquet", outdir / f"{gid}.csv"
    df.to_parquet(p_parquet, index=False)
    df.to_csv(p_csv, index=False)
    return p_parquet

def build_totals_for_index(index_csv: Path, outdir: Path, limit: Optional[int]=None) -> Tuple[int, List[str]]:
    idx = pd.read_csv(index_csv)
    need = {"game_id","home","away"}
    if not need.issubset(idx.columns):
        raise ValueError(f"Index missing columns: {need - set(idx.columns)}")

    outdir.mkdir(parents=True, exist_ok=True)
    existing = {p.stem for p in outdir.glob("*.parquet")}
    processed, skipped = 0, []
    failures: List[str] = []

    for _, row in idx.iterrows():
        gid, away, home = str(row["game_id"]), str(row.get("away", "")), str(row.get("home", ""))

        if gid in existing:
            skipped.append(gid)
            continue

        try:
            df = fetch_boxscore_totals(gid, away, home)
        except Exception as e:
            # Log + continue; small nap to be polite after an exception (e.g., repeated 429s)
            print(f"[warn] {gid}: {e}")
            failures.append(gid)
            time.sleep(3.0)
            continue

        if df is not None and not df.empty:
            save_game_totals(df, outdir)
            processed += 1

        # Optional tiny pause between games (in addition to fetch throttle)
        time.sleep(0.3)

        if limit and processed >= limit:
            break

    if failures:
        print(f"[info] Failed/Skipped due to errors: {len(failures)} (e.g., rate limits). You can re-run to resume.")
    return processed, skipped
