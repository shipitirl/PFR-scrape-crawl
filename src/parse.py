import re
import pandas as pd
from bs4 import BeautifulSoup

COMMENT_RE = re.compile(r"<!--(.*?)-->", re.DOTALL)

def uncomment_html(html: str) -> str:
    # Many PFR tables are comment-wrapped; expose them before parsing
    return COMMENT_RE.sub(lambda m: m.group(1), html)

def read_single_table_by_id(html: str, table_id: str) -> pd.DataFrame:
    """Return a DataFrame for a specific table id; returns empty df if missing."""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id=table_id)
    if table is None:
        html2 = uncomment_html(html)
        soup2 = BeautifulSoup(html2, "lxml")
        table = soup2.find("table", id=table_id)
    if table is None:
        return pd.DataFrame()
    df_list = pd.read_html(str(table))
    return df_list[0] if df_list else pd.DataFrame()

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df
