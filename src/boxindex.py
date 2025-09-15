from pathlib import Path
import re
import pandas as pd
from bs4 import BeautifulSoup
from .fetch import get
from .parse import uncomment_html

BASE = "https://www.pro-football-reference.com"
GAMES_TABLE_ID = "games"
HREF_RE = re.compile(r"^/boxscores/\d{8}0[a-z]{3}\.htm$")

def _parse_schedule_for_boxlinks(html: str, season: int) -> pd.DataFrame:
    """
    Extract box score links + basic game meta from a season schedule page.

    Returns columns:
      season, date, week, game_type, home, away, game_id, url,
      winner, loser, pts_w, pts_l, ot
    """
    soup = BeautifulSoup(uncomment_html(html), "lxml")
    table = soup.find("table", id=GAMES_TABLE_ID)
    if table is None:
        return pd.DataFrame()

    def get_text(tr, keys):
        """Return stripped text from the first matching data-stat cell (or '')."""
        for k in keys:
            td = tr.find("td", {"data-stat": k})
            if td:
                return td.get_text(strip=True)
        return ""

    def get_team(tr, keys):
        """Return team name from <a> text if present, else plain cell text."""
        for k in keys:
            td = tr.find("td", {"data-stat": k})
            if td:
                a = td.find("a")
                return a.get_text(strip=True) if a else td.get_text(strip=True)
        return ""

    def get_int(tr, keys):
        """Parse integer from the first matching cell, else None."""
        s = get_text(tr, keys)
        try:
            return int(s)
        except (TypeError, ValueError):
            return None

    rows = []
    for tr in table.select("tbody tr"):
        classes = tr.get("class", [])
        if any(c in ("thead", "spacer") for c in classes):
            continue

        # boxscore link
        box_td = tr.find("td", {"data-stat": "boxscore_word"})
        if not box_td:
            continue
        a = box_td.find("a", href=True)
        if not a or not HREF_RE.search(a["href"]):
            continue

        href = a["href"]
        url = f"{BASE}{href}"
        game_id = href.split("/")[-1].replace(".htm", "")

        # basic fields
        date  = get_text(tr, ["game_date", "date_game", "date"])
        week  = get_text(tr, ["week_num", "week"])
        gtype = get_text(tr, ["game_type"])
        loc   = get_text(tr, ["game_location"])  # "@" for away winner, "" for home, sometimes "N" for neutral

        # winner/loser (older schedules expose these explicitly)
        winner = get_team(tr, ["winner", "winner_team"])
        loser  = get_team(tr, ["loser", "loser_team"])

        # visitor/home (newer schedules)
        visitor = get_team(tr, ["visitor", "visitor_team"])
        home_tm = get_team(tr, ["home_team", "home"])

        # derive home/away reliably
        if winner and loser:
            if loc == "@":   # winner listed as away team
                away = winner
                home = loser
            else:            # blank or neutral -> treat winner as home for indexing
                home = winner
                away = loser
        else:
            home = home_tm
            away = visitor

        # scores and OT
        pts_w = get_int(tr, ["pts_win", "ptsw"])
        pts_l = get_int(tr, ["pts_lose", "ptsl"])
        # Some seasons mark OT with "OT" text; any non-empty value means True
        ot = bool(get_text(tr, ["overtime", "ot"]))

        rows.append({
            "season": season,
            "date": date,
            "week": week,
            "game_type": gtype,
            "home": home,
            "away": away,
            "game_id": game_id,
            "url": url,
            "winner": winner or "",
            "loser": loser or "",
            "pts_w": pts_w,
            "pts_l": pts_l,
            "ot": ot,
        })

    return pd.DataFrame(rows)

def build_boxscore_index(start_year=2002, end_year=None) -> pd.DataFrame:
    import datetime as dt
    if end_year is None:
        end_year = dt.date.today().year
    frames = []
    for y in range(start_year, end_year + 1):
        html = get(f"{BASE}/years/{y}/games.htm")
        dfi = _parse_schedule_for_boxlinks(html, y)
        if not dfi.empty:
            frames.append(dfi)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    if "week" in df.columns:
        df["week"] = pd.to_numeric(df["week"], errors="coerce")
    df.replace({"": pd.NA}, inplace=True)  # pretty CSV roundtrip
    df = df.drop_duplicates(subset=["game_id"]).reset_index(drop=True)
    return df

def main():
    out = Path("data")
    out.mkdir(exist_ok=True, parents=True)
    df = build_boxscore_index(2002)
    if df.empty:
        print("No boxscore links found.")
        return
    df.to_parquet(out / "boxscore_index.parquet", index=False)
    df.to_csv(out / "boxscore_index.csv", index=False)
    print(f"Saved {len(df):,} games to data/boxscore_index.(csv|parquet)")

if __name__ == "__main__":
    main()
