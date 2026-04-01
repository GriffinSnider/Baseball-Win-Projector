"""
Scraper module for Baseball Savant (baseballsavant.mlb.com)
Downloads Statcasts leaderboard CSVs. Returns pandas DataFrame keyed on mlbam_id
"""

import time
import warnings
import io
import requests
import pandas as pd

RATE_LIMIT = 1.0 # seconds between calls

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

HITTING_URL = (
    "https://baseballsavant.mlb.com/leaderboard/custom?"
    "year={season}&type=batter&filter=&sort=5&sortDir=desc&min=50&"
    "selections=xba,xslg,xwoba,xobp,xiso,avg_swing_speed,fast_swing_rate,"
    "blasts_contact,blasts_swing,squared_up_contact,squared_up_swing,"
    "avg_swing_length,swords,attack_angle,attack_direction,ideal_angle_rate,"
    "vertical_swing_path,exit_velocity_avg,launch_angle_avg,"
    "sweet_spot_percent,barrel_batted_rate&csv=true"
)

PITCHING_URL = (
    "https://baseballsavant.mlb.com/leaderboard/custom?"
    "year={season}&type=pitcher&filter=&sort=5&sortDir=desc&"
    "min=10&selections=xera,xba,xslg,xwoba,xobp,xiso,"
    "k_percent,bb_percent,whiff_percent,oz_swing_percent,"
    "exit_velocity_avg,sweet_spot_percent,"
    "barrel_batted_rate,hard_hit_percent,groundballs_percent,"
    "woba&csv=true"
)


def _download_csv(url: str) -> pd.DataFrame:
    """Download a CSV from Baseball Savant with rate limiting."""
    time.sleep(RATE_LIMIT)
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))


def _safe_col(df: pd.DataFrame, src: str, dest: str) -> pd.Series:
    """Return a column renamed, or NaN + warning if missing."""
    if src in df.columns:
        return df[src]
    warnings.warn(f"Column '{src}' not found in Savant response — filling with NaN")
    return pd.Series([float("nan")] * len(df), name=dest)

# Statcast hitting

def get_statcast_hitting(season: int = 2025) -> pd.DataFrame:
    """Download Statcast hitting leaderboard and return cleaned DataFrame."""
    url = HITTING_URL.format(season=season)
    raw = _download_csv(url)

    col_map = {
        "player_id": "mlbam_id",
        "last_name, first_name": "name",
        "xba": "xBA",       #expected batting average
        "xslg": "xSLG",     #expected slugging percentage
        "xwoba": "xwOBA",   #expected weighted on-base average
        "xobp": "xOBP",     #expected on-base percentage
        "xiso": "xISO",     #expected isolated power

        #bat tracking
        "avg_swing_speed": "avg_swing_speed",           #average bat speed
        "fast_swing_rate": "fast_swing_rate",           # % of swings above speed threshold
        "blasts_contact": "blasts_contact",             # % of contact classified as "blast"
        "blasts_swing": "blasts_swing",                 # % of swings producing blast contact
        "squared_up_contact": "squared_up_contact",     # % of well-centered contact
        "squared_up_swing": "squared_up_swing",         # % of swings resulting in squared-up contact
        "avg_swing_length": "avg_swing_length",         # average swing path length
        "swords": "swords",                             # swings with extreme miss / no contact
        "attack_angle": "attack_angle",                 # vertical bat angle at contact
        "attack_direction": "attack_direction",         # horizontal swing direction
        "ideal_angle_rate": "ideal_angle_rate",         # % of swings in optimal launch window
        "vertical_swing_path": "vertical_swing_path",   # steepness of swing plane

        #batted ball outcomes
        "exit_velocity_avg": "exit_velocity_avg",       # average exit velocity
        "launch_angle_avg": "launch_angle_avg",         # average launch angle
        "sweet_spot_percent": "sweet_spot_percent",     # % of batted balls in ideal angle range
        "barrel_batted_rate": "barrel_batted_rate",     # % of batted balls classified as barrels
    }

    alt_names = {
        "player_name": ["last_name, first_name", "player_name", "name"],
        "player_id": ["player_id"],
    }

    out = pd.DataFrame()
    for src, dest in col_map.items():
        if src in raw.columns:
            out[dest] = raw[src]
        else:
            # Check alternates for key columns
            found = False
            for alt in alt_names.get(src, []):
                if alt in raw.columns:
                    out[dest] = raw[alt]
                    found = True
                    break
            if not found:
                out[dest] = _safe_col(raw, src, dest)

    out["mlbam_id"] = out["mlbam_id"].astype(int)
    return out.reset_index(drop=True)

# Statcast pitching

def get_statcast_pitching(season: int = 2025) -> pd.DataFrame:
    """Download Statcast pitching leaderboard and return cleaned DataFrame."""
    url = PITCHING_URL.format(season=season)
    raw = _download_csv(url)

    col_map = {
        "player_id": "mlbam_id",
        "last_name, first_name": "name",
        "xera": "xERA",     #expected ERA base on quality of contact
        "xba": "xBA",       #expected batting average allowed
        "xslg": "xSLG",     #expected slugging percentage allowed
        "xwoba": "xwOBA",   #expected weighted on-base average allowed
        "xobp": "xOBP",     #expected on-base percentage allowed
        "xiso": "xISO",     #expected isolated power allowed
        "woba": "wOBA",     #actual weighted on-base average allowed

        #plate disciple
        "k_percent": "k_percent",               #strikeout rate
        "bb_percent": "bb_percent",             #walk rate
        "whiff_percent": "whiff_percent",       #swing and miss / total swings
        "oz_swing_percent": "chase_percent",    #swing rate on pitches outside the zone

        #batted ball outcomes allowed
        "exit_velocity_avg": "exit_velocity_avg",       #average exit velocity allowed
        "sweet_spot_percent": "sweet_spot_percent",     # % of BBE at 8-32 deg launch angle
        "barrel_batted_rate": "barrel_batted_rate",     # % of BBE classified as barrels
        "hard_hit_percent": "hard_hit_percent",         # $ of BBE >= 95 mph exit velocity
        "groundballs_percent": "groundballs_percent",   # ground ball rate allowed
    }

    alt_names = {
        "player_name": ["last_name, first_name", "player_name", "name"],
        "player_id": ["player_id"],
    }

    out = pd.DataFrame()
    for src, dest in col_map.items():
        if src in raw.columns:
            out[dest] = raw[src]
        else:
            found = False
            for alt in alt_names.get(src, []):
                if alt in raw.columns:
                    out[dest] = raw[alt]
                    found = True
                    break
            if not found:
                out[dest] = _safe_col(raw, src, dest)

    out["mlbam_id"] = out["mlbam_id"].astype(int)
    return out.reset_index(drop=True)

#Test

if __name__ == "__main__":
    print("Statcast Hitting (first 10):")
    hit = get_statcast_hitting(2025)
    print(f"Shape: {hit.shape}")
    print(hit.head(10).to_string(index=False))

    print("\nStatcast Pitching (first 10):")
    pit = get_statcast_pitching(2025)
    print(f"Shape: {pit.shape}")
    print(pit.head(10).to_string(index=False))

