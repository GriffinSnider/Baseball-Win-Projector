import requests
import json
import pandas as pd

BASE_URL = "https://statsapi.mlb.com/api/v1"

def get_player_stats(player_id=660271, season=2025, group="hitting"):
    url = f"{BASE_URL}/people"
    params = {
        "personIds": player_id,
        "hydrate": f"stats(group={group},type=season,season={season})"
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    try:
        stats = data["people"][0]["stats"][0]["splits"][0]["stat"]
        return stats
    except (KeyError, IndexError):
        print("No stats found.")
        return None


if __name__ == "__main__":
    stats = get_player_stats()

    if stats:
        print("\n=== ALL AVAILABLE FIELDS ===\n")
        for key in stats.keys():
            print(key)

        print("\n=== FULL JSON ===\n")
        print(json.dumps(stats, indent=2))

        # Optional: convert to DataFrame
        df = pd.DataFrame([stats])
        print("\n=== DATAFRAME PREVIEW ===\n")
        print(df.head())