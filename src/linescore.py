from pathlib import Path
from typing import Optional, List, Dict, Tuple
import pandas as pd
from bs4 import BeautifulSoup
from .fetch import get, FetchError, RateLimited
from .parse import uncomment_html

BASE = "https://www.pro-football-reference.com"

Q_KEYS: Dict[str, List[str]] = {
    "q1": ["q1", "1", "pts_q1", "pts1"],
    "q2": ["q2", "2", "pts_q2", "pts2"],
    "q3": ["q3", "3", "pts_q3", "pts3"],
    "q4": ["q4", "4", "pts_q4", "pts4"],
    "ot": ["ot", "pts_ot"],
    "total": ["total", "t", "pts", "pts_total"],
}

def _get_int(cell) -> Optional[int]:
    if cell is None:
        return None
    txt = cell.get_text(strip=True)
    if txt == "":
        return None
    try:
        return int(txt)
    except ValueError:
        return None

def _select_first(tr: BeautifulSoup, keys: List[str]):
    for k in keys:
        el = tr.find(["td", "th"], attrs={"data-stat": k})
        if el:
            return el
    return None

def _parse_linescore_from_dom(soup: BeautifulSoup) -> Optional[pd.DataFrame]:
    table = soup.find("table", id="linescore")
    if not table:
        return None
    body = table.find("tbody")
    if not body:
        return None

    rows = body.find_all("tr", recursive=False)
    if len(rows) < 2:
        return None

    out_rows = []
    for idx, side in enumerate(["away", "home"]):
        tr = rows[idx]
        team_cell = _select_first(tr, ["team", "tm"])
        team_name = ""
        if team_cell:
            a = team_cell.find("a")
            team_name = a.get_text(strip=True) if a else team_cell.get_text(" ", strip=True)
            for junk in ("via Sports Logos.net", "About logos"):
                if junk in team_name:
                    team_name = team_name.replace(junk, "").strip()

        vals = {}
        for key, candidates in Q_KEYS.items():
            cell = _select_first(tr, candidates)
            vals[key] = _get_int(cell)

        out_rows.append({
            "side": side,
            "team": team_name,
            "q1": vals.get("q1"),
            "q2": vals.get("q2"),
            "q3": vals.get("q3"),
            "q4": vals.get("q4"),
            "ot": vals.get("ot"),
            "total": vals.get("total"),
        })

    return pd.DataFrame(out_rows)

def fetch_linescore(game_id: str) -> Optional[pd.DataFrame]:
    url = f"{BASE}/boxscores/{game_id}.htm"
    html = get(url)
    soup = BeautifulSoup(uncomment_html(html), "lxml")
    df_ls = _parse_linescore_from_dom(soup)
    if df_ls is None or df_ls.empty:
        return None
    df_ls.insert(0, "game_id", game_id)
    return df_ls

def save_linescore(df: pd.DataFrame, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    gid = df["game_id"].iloc[0]
    df.to_parquet(outdir / f"{gid}.parquet", index=False)
    df.to_csv(outdir / f"{gid}.csv", index=False)

def build_linescores_for_index(index_csv: Path, outdir: Path, limit: Optional[int] = None) -> Tuple[int, List[str]]:
    idx = pd.read_csv(index_csv)
    outdir.mkdir(parents=True, exist_ok=True)
    existing = {p.stem for p in outdir.glob("*.parquet")}
    processed, skipped, ratelimited, failures = 0, [], [], []

    for _, row in idx.iterrows():
        gid = str(row["game_id"])
        if gid in existing:
            skipped.append(gid)
            continue

        try:
            df = fetch_linescore(gid)
            if df is not None and not df.empty:
                save_linescore(df, outdir)
                processed += 1
            else:
                failures.append(gid)
        except RateLimited:
            print(f"[429] skip {gid}")
            ratelimited.append(gid)
        except FetchError as e:
            print(f"[fail] {gid}: {e}")
            failures.append(gid)
        except Exception as e:
            print(f"[fail] {gid}: {e}")
            failures.append(gid)

        if limit and processed >= limit:
            break

    if ratelimited:
        print(f"[info] skipped {len(ratelimited)} due to 429 — rerun will fill from cache")
    if failures:
        print(f"[info] failed to parse {len(failures)} games")

    return processed, skipped
