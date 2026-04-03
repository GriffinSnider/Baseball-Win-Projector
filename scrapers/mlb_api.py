"""
Scraper module for the MLB stats API (statsapi.mlb.com)
All functions return pandas Dataframe keyed on mlbam_id
"""

import time
import requests
import pandas as pd

BASE_URL = "https://statsapi.mlb.com/api/v1"
RATE_LIMIT = 0.5 # seconds between calls

def _get(endpoint: str, params: dict | None = None) -> dict:
    """Make a rate-limited GET request to the MLB Stats API"""
    time.sleep(RATE_LIMIT)
    url = f"{BASE_URL}{endpoint}"
    resp = requests.get(url, params=params, timeout = 30)
    resp.raise_for_status()
    return resp.json()

# Player Roster
def get_all_players(season: int = 2025) -> pd.DataFrame:
    """Return DataFrame of all MLB players for a season.

    Team assignment uses currentTeam as a default; this gets overridden
    in get_hitting_stats / get_pitching_stats with the team where the
    player accumulated the most PA/IP (handles mid-season trades).
    """
    data = _get(f"/sports/1/players", params={"season": season})
    rows = []
    for p in data.get("people", []):
        rows.append({
            "mlbam_id": p["id"],
            "name": p.get("fullName"),
            "position": p.get("primaryPosition", {}).get("abbreviation"),
            "team": p.get("currentTeam", {}).get("name"),
        })
    return pd.DataFrame(rows)

# Hitting Stats
def get_hitting_stats(season: int = 2025) -> pd.DataFrame:
    """Return season hitting stats for all players with plate appearances."""
    players = get_all_players(season)
    hitter_ids = players["mlbam_id"].tolist()

    # Fetch in batches of 100 to avoid enormous URLs
    all_rows = []
    for i in range(0, len(hitter_ids), 100):
        batch = hitter_ids[i : i + 100]
        ids_str = ",".join(str(pid) for pid in batch)
        data = _get(
            "/people",
            params={
                "personIds": ids_str,
                "hydrate": f"stats(group=hitting,type=season,season={season})",
            },
        )
        for p in data.get("people", []):
            stats_list = p.get("stats", [])
            if not stats_list:
                continue
            splits = stats_list[0].get("splits", [])
            # Find the team where this player had the most PA
            best_team = None
            best_pa = 0
            combined_split = None
            for split in splits:
                team_name = split.get("team", {}).get("name")
                s = split.get("stat", {})
                pa = int(s.get("plateAppearances", 0))
                if team_name and pa > best_pa:
                    best_team = team_name
                    best_pa = pa
                if not team_name and pa > 0:
                    combined_split = split  # combined total row
            # Prefer combined row (traded players); fall back to best per-team row
            use_split = combined_split
            if use_split is None:
                # Non-traded player: only per-team rows exist
                for split in splits:
                    if split.get("team", {}).get("name") == best_team:
                        use_split = split
                        break
            if use_split:
                s = use_split.get("stat", {})
                if s.get("plateAppearances"):
                    row = _parse_hitting(p["id"], s)
                    row["primary_team"] = best_team
                    all_rows.append(row)

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    # deduplicate: keep the row with most PA per player
    df = df.sort_values("PA", ascending = False).drop_duplicates(subset = "mlbam_id")
    return df.reset_index(drop = True)

def _parse_hitting(mlbam_id: int, s: dict) -> dict:
    pa = int(s.get("plateAppearances", 0))
    bb = int(s.get("baseOnBalls", 0))
    so = int(s.get("strikeOuts", 0))
    return {
        "mlbam_id": mlbam_id,
        "PA": pa,
        "AVG": s.get("avg"),
        "OBP": s.get("obp"),
        "SLG": s.get("slg"),
        "OPS": s.get("ops"),
        "HR": int(s.get("homeRuns", 0)),
        "RBI": int(s.get("rbi", 0)),
        "BB": bb,
        "SO": so,
        "BB_pct": round(bb / pa * 100, 1) if pa else 0.0,
        "K_pct": round(so / pa * 100, 1) if pa else 0.0,
    }

#Pitching Stats
def get_pitching_stats(season: int = 2025) -> pd.DataFrame:
    """Return season pitching stats for all players with innings pitched"""
    players = get_all_players(season)
    pitcher_ids = players[players["position"] == "P"]["mlbam_id"].tolist()

    all_rows = []
    for i in range(0, len(pitcher_ids), 100):
        batch = pitcher_ids[i: i + 100]
        ids_str = ",".join(str(pid) for pid in batch)
        data = _get(
            "/people",
            params={
                "personIds": ids_str,
                "hydrate": f"stats(group=pitching,type=season,season={season})",
            },
        )
        for p in data.get("people", []):
            stats_list = p.get("stats", [])
            if not stats_list:
                continue
            splits = stats_list[0].get("splits", [])
            # Find the team where this player had the most IP
            best_team = None
            best_ip = 0.0
            combined_split = None
            for split in splits:
                team_name = split.get("team", {}).get("name")
                s = split.get("stat", {})
                ip = float(s.get("inningsPitched", 0))
                if team_name and ip > best_ip:
                    best_team = team_name
                    best_ip = ip
                if not team_name and ip > 0:
                    combined_split = split
            # Prefer combined row (traded players); fall back to best per-team row
            use_split = combined_split
            if use_split is None:
                for split in splits:
                    if split.get("team", {}).get("name") == best_team:
                        use_split = split
                        break
            if use_split:
                s = use_split.get("stat", {})
                if s.get("inningsPitched"):
                    row = _parse_pitching(p["id"], s)
                    row["primary_team"] = best_team
                    all_rows.append(row)

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df = df.sort_values("IP", ascending = False).drop_duplicates(subset = "mlbam_id")
    return df.reset_index(drop = True)

def _parse_pitching(mlbam_id: int, s: dict) -> dict:
    ip = float(s.get("inningsPitched", 0))
    so = int(s.get("strikeOuts", 0))
    bb = int(s.get("baseOnBalls", 0))
    hr = int(s.get("homeRuns", 0))
    return {
        "mlbam_id": mlbam_id,
        "GS": int(s.get("gamesStarted", 0)),
        "G": int(s.get("gamesPitched", 0)),
        "IP": ip,
        "ERA": s.get("era"),
        "WHIP": s.get("whip"),
        "SO": so,
        "BB": bb,
        "HR": hr,
        "K_per9": round(so / ip * 9, 2) if ip else 0.0,
        "BB_per9": round(bb / ip * 9, 2) if ip else 0.0,
        "HR_per9": round(hr / ip * 9, 2) if ip else 0.0,
    }

#Player Search
def search_player(name: str, limit: int = 5) -> pd.DataFrame:
    """Fuzzy search for a player by name. Returns top match"""
    data = _get("/people/search", params={"names": name})
    rows = []
    for p in data.get("people", [])[:limit]:
        rows.append({
            "mlbam_id": p["id"],
            "name": p.get("fullName"),
            "position": p.get("primaryPosition", {}).get("abbreviation"),
            "team": p.get("currentTeam", {}).get("name"),
            "active": p.get("active"),
        })
    return pd.DataFrame(rows)

#Test
if __name__ == "__main__":
    print("Player Search: Ohtani")
    print(search_player("Ohtani").to_string(index=False))

    print("\nAll Players (first 10):")
    players = get_all_players(2025)
    print(players.head(10).to_string(index=False))

    print("\nHitting Stats (first 10):")
    hitting = get_hitting_stats(2025)
    print(hitting.head(10).to_string(index=False))

    print("\nPitching Stats (first 10):")
    pitching = get_pitching_stats(2025)
    print(pitching.head(10).to_string(index=False))





