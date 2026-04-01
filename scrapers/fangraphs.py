"""
Scraper module for FanGraphs (fangraphs.com).
Uses pybaseball as primary source (handles Cloudflare), with direct URL fallback.
Returns pandas DataFrames.
"""

import time
import unicodedata
import warnings
import io
import re
import requests
import pandas as pd
import numpy as np
from pybaseball import batting_stats, pitching_stats

RATE_LIMIT = 1.5 # seconds between calls

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

FG_PARK_FACTORS_API = (
    "https://www.fangraphs.com/api/guts/park-factors?"
    "lg=all&season={season}"
)

FG_PARK_FACTORS_HTML = (
    "https://www.fangraphs.com/guts.aspx?type=pf&"
    "teamid=0&season={season}"
)

def _request(url: str, timeout: int = 60) -> requests.Response:
    """Rate-limited GET with browser headers."""
    time.sleep(RATE_LIMIT)
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp

def _safe_col(df: pd.DataFrame, src: str, dest: str) -> pd.Series:
    """Return column or NaN + warning if missing."""
    if src in df.columns:
        return df[src]
    warnings.warn(f"Column '{src}' not found in FanGraphs response (filling with NAN)")
    return pd.Series([np.nan] * len(df), name=dest)

def _find_col(df: pd.DataFrame, candidates: list[str], fallback: str) -> pd.Series:
    """Find first matching column from candidates list."""
    for c in candidates:
        if c in df.columns:
            return df[c]
    return _safe_col(df, candidates[0], fallback)

def _pct_to_float(series: pd.Series) -> pd.Series:
    """Convert percentage strings like '25.3 %' to floats."""
    if series.dtype == object:
        return (
            series.astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(" ", "", regex=False)
            .apply(pd.to_numeric, errors="coerce")
        )
    return series

# Hitting leaderboard

def get_fg_hitting(season: int = 2025) -> pd.DataFrame:
    """Pull FanGraphs hitter leaderboard via pybaseball. Returns cleaned DataFrame."""
    raw = batting_stats(season, qual=50)

    out = pd.DataFrame()
    out["fg_id"] = _find_col(raw, ["IDfg", "playerid", "PlayerId", "xMLBAMID"], "fg_id")
    out["name"] = _find_col(raw, ["Name", "PlayerName", "player_name"], "name")
    out["team"] = _find_col(raw, ["Team", "team"], "team")

    stat_map = {
        "wRC+": "wRC_plus",
        "WAR": "WAR",
        "wOBA": "wOBA",
        "BABIP": "BABIP",
        "GB%": "GB_pct",
        "FB%": "FB_pct",
        "LD%": "LD_pct",
    }
    alt_names = {
        "wRC+": ["wRC+", "wRC_plus", "wRCplus"],
        "GB%": ["GB%", "GB_pct", "GB"],
        "FB%": ["FB%", "FB_pct", "FB"],
        "LD%": ["LD%", "LD_pct", "LD"],
    }

    for src, dest in stat_map.items():
        if src in raw.columns:
            out[dest] = raw[src]
        else:
            found = False
            for alt in alt_names.get(src, [dest]):
                if alt in raw.columns:
                    out[dest] = raw[alt]
                    found = True
                    break
            if not found:
                out[dest] = _safe_col(raw, src, dest)

    for col in ["GB_pct", "FB_pct", "LD_pct"]:
        if col in out.columns:
            out[col] = _pct_to_float(out[col])

    return out.reset_index(drop=True)

#Pitching leaderboard
def get_fg_pitching(season: int = 2025) -> pd.DataFrame:
    """Pull FanGraphs pitcher leaderboard via pybaseball. Returns cleaned DataFrame."""
    raw = pitching_stats(season, qual=10)

    out = pd.DataFrame()
    out["fg_id"] = _find_col(raw, ["IDfg", "playerid", "PlayerId", "xMLBAMID"], "fg_id")
    out["name"] = _find_col(raw, ["Name", "PlayerName", "player_name"], "name")
    out["team"] = _find_col(raw, ["Team", "team"], "team")

    stat_map = {
        "FIP": "FIP",
        "xFIP": "xFIP",
        "SIERA": "SIERA",
        "WAR": "WAR",
        "GB%": "GB_pct",
        "K%": "K_pct",
        "BB%": "BB_pct",
    }
    alt_names = {
        "GB%": ["GB%", "GB_pct", "GB"],
        "K%": ["K%", "K_pct", "K"],
        "BB%": ["BB%", "BB_pct", "BB"],
    }

    for src, dest in stat_map.items():
        if src in raw.columns:
            out[dest] = raw[src]
        else:
            found = False
            for alt in alt_names.get(src, [dest]):
                if alt in raw.columns:
                    out[dest] = raw[alt]
                    found = True
                    break
            if not found:
                out[dest] = _safe_col(raw, src, dest)

    for col in ["GB_pct", "K_pct", "BB_pct"]:
        if col in out.columns:
            out[col] = _pct_to_float(out[col])

    return out.reset_index(drop=True)

#Park factors
def get_park_factors(season: int = 2025) -> pd.DataFrame:
    """Fetch FanGraphs park factors. Returns DataFrame with team, basic, and 5yr factors."""
    # Try pybaseball first
    try:
        from pybaseball import park_factors as pb_park_factors
        raw = pb_park_factors(season)
        if raw is not None and len(raw) > 0:
            return _parse_park_factors(raw)
    except Exception:
        pass

    # HTML scrape
    raw = _scrape_park_factors_html(season)
    return _parse_park_factors(raw)

def _parse_park_factors(raw: pd.DataFrame) -> pd.DataFrame:
    """Extract team, basic, and 5yr park factor columns from raw data."""
    team_candidates = ["Team", "team", "TeamName", "teamName", "Name"]
    basic_candidates = ["1yr", "1 yr", "Basic", "basic", "PF", "pf", "Park Factor", "Factor"]
    five_yr_candidates = ["Basic (5yr)", "5yr", "5 yr", "FiveYear", "multi", "3yr", "3 yr"]

    def _find(candidates):
        for c in candidates:
            for col in raw.columns:
                if c.lower() == col.lower().strip():
                    return col
        return None

    team_col = _find(team_candidates)
    basic_col = _find(basic_candidates)
    five_yr_col = _find(five_yr_candidates)

    out = pd.DataFrame()
    out["team"] = raw[team_col].astype(str).str.strip() if team_col else _safe_col(raw, "Team", "team")
    out["park_factor_basic"] = raw[basic_col] if basic_col else _safe_col(raw, "Basic", "park_factor_basic")

    if five_yr_col:
        out["park_factor_5yr"] = raw[five_yr_col]
    else:
        warnings.warn("No 5-year park factor column found — using basic as fallback")
        out["park_factor_5yr"] = out["park_factor_basic"]

    return out.reset_index(drop=True)

def _scrape_park_factors_html(season: int) -> pd.DataFrame:
    """Fallback: scrape park factors from the HTML guts page."""
    resp = _request(FG_PARK_FACTORS_HTML.format(season=season))
    tables = pd.read_html(io.StringIO(resp.text))
    if not tables:
        raise ValueError("No tables found on FanGraphs park factors page")
    return max(tables, key=len)

#Crosswalk: FanGrapgs ID -> MLBAM ID
def build_fg_mlbam_crosswalk(fg_df: pd.DataFrame, mlbam_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join FanGraphs data to MLBAM IDs using name + team.
    Returns a crosswalk DataFrame with fg_id, mlbam_id, name, team, match_status.
    """

    def _normalize_name(name: str) -> str:
        if pd.isna(name):
            return ""
        name = str(name).strip()
        # Handle "Last, First" format
        if "," in name:
            parts = name.split(",", 1)
            name = f"{parts[1].strip()} {parts[0].strip()}"
        # Strip accents: é→e, ñ→n, etc.
        name = unicodedata.normalize("NFD", name)
        name = "".join(c for c in name if unicodedata.category(c) != "Mn")
        name = name.lower()
        name = re.sub(r"\bjr\.?\b", "", name)
        name = re.sub(r"\bsr\.?\b", "", name)
        name = re.sub(r"\bii+\b", "", name)
        name = re.sub(r"[.\-']", "", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name

    def _normalize_team(team: str) -> str:
        if pd.isna(team):
            return ""
        return str(team).strip().lower()

    # Build normalized keys
    fg = fg_df[["fg_id", "name", "team"]].copy()
    fg["name_norm"] = fg["name"].apply(_normalize_name)
    fg["team_norm"] = fg["team"].apply(_normalize_team)

    mlbam = mlbam_df[["mlbam_id", "name", "team"]].copy()
    mlbam["name_norm"] = mlbam["name"].apply(_normalize_name)
    mlbam["team_norm"] = mlbam["team"].apply(_normalize_team)

    # Pass 1: exact normalized name match
    merged = fg.merge(
        mlbam[["mlbam_id", "name_norm"]],
        on="name_norm",
        how="left",
    )
    merged = merged.drop_duplicates(subset=["fg_id"], keep="first")

    matched = merged["mlbam_id"].notna()

    # Pass 2: for unmatched, try last-name + team
    if not matched.all():
        unmatched_fg = merged.loc[~matched].copy()
        unmatched_fg["last_name"] = unmatched_fg["name_norm"].str.split().str[-1]

        mlbam_pass2 = mlbam.copy()
        mlbam_pass2["last_name"] = mlbam_pass2["name_norm"].str.split().str[-1]

        pass2 = unmatched_fg.merge(
            mlbam_pass2[["mlbam_id", "last_name", "team_norm"]].rename(
                columns={"mlbam_id": "mlbam_id_p2"}
            ),
            on=["last_name", "team_norm"],
            how="left",
        ).drop_duplicates(subset=["fg_id"], keep="first")

        p2_map = pass2.set_index("fg_id")["mlbam_id_p2"].dropna()
        merged.loc[merged["fg_id"].isin(p2_map.index), "mlbam_id"] = (
            merged.loc[merged["fg_id"].isin(p2_map.index), "fg_id"].map(p2_map)
        )

    result = merged[["fg_id", "mlbam_id", "name", "team"]].copy()
    result["match_status"] = np.where(result["mlbam_id"].notna(), "matched", "unmatched")
    result["mlbam_id"] = result["mlbam_id"].astype("Int64")

    return result.reset_index(drop=True)

#Test
if __name__ == "__main__":
    import os

    print("FG Hitting (first 10):")
    fgh = get_fg_hitting(2025)
    print(f"Shape: {fgh.shape}")
    print(fgh.head(10).to_string(index=False))

    print("\nFG Pitching (first 10):")
    fgp = get_fg_pitching(2025)
    print(f"Shape: {fgp.shape}")
    print(fgp.head(10).to_string(index=False))

    print("\nPark Factors (first 10):")
    pf = get_park_factors(2025)
    print(f"Shape: {pf.shape}")
    print(pf.head(10).to_string(index=False))

    print("\nCrosswalk (hitting):")
    mlbam_players = pd.read_parquet(
        os.path.join(os.path.dirname(__file__), "..", "data", "raw_mlb_api_players.parquet")
    )
    xwalk = build_fg_mlbam_crosswalk(fgh, mlbam_players)
    total = len(xwalk)
    n_matched = (xwalk["match_status"] == "matched").sum()
    print(f"Total: {total}, Matched: {n_matched}, Unmatched: {total - n_matched}")
    print(f"Match rate: {n_matched / total * 100:.1f}%")
    unmatched = xwalk[xwalk["match_status"] == "unmatched"]
    if len(unmatched) > 0:
        print("\nUnmatched rows:")
        print(unmatched.to_string(index=False))