# src/linescore.py
from pathlib import Path
from typing import Optional, List, Dict, Tuple
import pandas as pd
from bs4 import BeautifulSoup
from .fetch import get, FetchError
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

JUNK_STRINGS = ("via Sports Logos.net", "About logos")

def _clean_team_name(name: str) -> str:
    team_name = (name or "").strip()
    for junk in JUNK_STRINGS:
        if junk in team_name:
            team_name = team_name.replace(junk, "")
    return team_name.strip()

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

def _parse_row_no_datastat(tr: BeautifulSoup) -> Optional[Dict[str, Optional[int]]]:
    cells = tr.find_all(["td", "th"], recursive=False)
    if not cells:
        return None

    team_idx = None
    team_name = ""
    for idx, cell in enumerate(cells):
        a = cell.find("a", href=True)
        if a and "/teams/" in a.get("href", ""):
            team_idx = idx
            team_name = a.get_text(strip=True)
            break
    if team_idx is None:
        # fall back to the first non-empty text cell
        for idx, cell in enumerate(cells):
            text = cell.get_text(" ", strip=True)
            if text:
                team_idx = idx
                team_name = text
                break

    team_name = _clean_team_name(team_name)

    numeric_vals: List[Optional[int]] = []
    if team_idx is not None:
        for cell in cells[team_idx + 1:]:
            val = _get_int(cell)
            if val is not None:
                numeric_vals.append(val)

    if not numeric_vals:
        return {"team": team_name}

    q1 = numeric_vals[0] if len(numeric_vals) > 0 else None
    q2 = numeric_vals[1] if len(numeric_vals) > 1 else None
    q3 = numeric_vals[2] if len(numeric_vals) > 2 else None
    q4 = numeric_vals[3] if len(numeric_vals) > 3 else None
    total = numeric_vals[-1] if numeric_vals else None
    ot = None
    if len(numeric_vals) > 5:
        ot_vals = numeric_vals[4:-1]
        ot = sum(v for v in ot_vals if v is not None)
    elif len(numeric_vals) == 5:
        ot = None
    elif len(numeric_vals) == 4:
        total = None

    return {
        "team": team_name,
        "q1": q1,
        "q2": q2,
        "q3": q3,
        "q4": q4,
        "ot": ot,
        "total": total,
    }

def _find_linescore_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    table = soup.find("table", id="linescore")
    if table:
        return table
    return soup.find("table", class_=lambda c: c and "linescore" in c)

def _parse_linescore_from_dom(soup: BeautifulSoup) -> Optional[pd.DataFrame]:
    table = _find_linescore_table(soup)
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

        fallback = _parse_row_no_datastat(tr)

        team_cell = _select_first(tr, ["team", "tm"])
        team_name = ""
        if team_cell:
            a = team_cell.find("a")
            if a and a.get_text(strip=True):
                team_name = a.get_text(strip=True)
            else:
                team_name = team_cell.get_text(" ", strip=True)
        elif fallback:
            team_name = fallback.get("team", "") or ""

        team_name = _clean_team_name(team_name)

        vals: Dict[str, Optional[int]] = {}
        for key, candidates in Q_KEYS.items():
            cell = _select_first(tr, candidates)
            vals[key] = _get_int(cell)

        if fallback:
            for key in ("q1", "q2", "q3", "q4", "ot", "total"):
                if vals.get(key) is None:
                    vals[key] = fallback.get(key)
            if not team_name:
                team_name = fallback.get("team", "") or ""

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
    gid = str(game_id).lower()
    url = f"{BASE}/boxscores/{gid}.htm"
    html = get(url)
    soup = BeautifulSoup(uncomment_html(html), "lxml")
    df_ls = _parse_linescore_from_dom(soup)
    if df_ls is None or df_ls.empty:
        return None
    df_ls.insert(0, "game_id", gid)
    return df_ls

def save_linescore(df: pd.DataFrame, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    gid = df["game_id"].iloc[0]
    df.to_parquet(outdir / f"{gid}.parquet", index=False)
    df.to_csv(outdir / f"{gid}.csv", index=False)

def build_linescores_for_index(index_csv: Path, outdir: Path, limit: Optional[int] = None) -> Tuple[int, List[str]]:
    idx = pd.read_csv(index_csv)
    if "game_id" not in idx.columns:
        raise ValueError("Index CSV missing 'game_id'.")
    idx["game_id"] = idx["game_id"].astype(str).str.lower()

    outdir.mkdir(parents=True, exist_ok=True)
    existing = {p.stem.lower() for p in outdir.glob("*.parquet")} | {p.stem.lower() for p in outdir.glob("*.csv")}
    processed, skipped, failures = 0, [], []

    for _, row in idx.iterrows():
        gid = str(row["game_id"]).lower()
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
        except FetchError as e:
            print(f"[warn] {gid}: {e}")
            failures.append(gid)

        if limit and processed >= limit:
            break
        if processed and processed % 25 == 0:
            print(f"[info] processed so far: {processed}")

    if failures:
        print(f"[info] failed/empty: {len(failures)}")

    return processed, skipped
