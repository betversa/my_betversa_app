import os
import requests
import json
import time

# Retrieve the API key from an environment variable,
# falling back to a hardcoded API key if not found.
API_KEY = os.getenv("API_KEY") or "99a50c93601ebfe1fa0970f01838ae3d"
if not API_KEY:
    raise ValueError("API_KEY is not set! Please set the API_KEY environment variable or update the fallback.")

# ===== Sports Configuration =====
SPORTS_CONFIG = {
    "NBA": {
        "sport_key": "basketball_nba",
        "player_prop_markets": {"player_points", "player_assists", "player_rebounds", "player_threes", "player_points_rebounds_assists"}
    },
    "MLB": {
        "sport_key": "baseball_mlb",
        "player_prop_markets": {"batter_hits", "batter_total_bases", "pitcher_strikeouts", "batter_home_runs", "batter_hits_runs_rbis", "pitcher_hits_allowed", "pitcher_earned_runs", "pitcher_outs"}
    },
    "NHL": {
        "sport_key": "icehockey_nhl",
        "player_prop_markets": {"player_points", "player_shots_on_goal", "player_assists", "player_goals"}
    },
}

STANDARD_MARKETS = {"h2h", "spreads", "totals", "alternate_spreads", "alternate_totals",
                    "h2h_h1", "h2h_q1", "h2h_p1", "h2h_1st_5_innings", "team_totals", "totals_1st_1_innings"}
ODDS_FORMAT = "american"  # Options: "american" or "decimal"
DATE_FORMAT = "iso"
BOOKMAKERS = "pinnacle,fanduel,draftkings,betmgm,espnbet,williamhill_us,betonlinag,lowvig,betrivers,hardrockbet"
EVENT_ODDS_FLAG = "true"

def pull_events(sport_key, accepted_markets):
    """Fetch events for a given sport."""
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events"
    params = {
        "api_key": API_KEY,
        "regions": "us,eu",
        "markets": ",".join(accepted_markets),
        "oddsFormat": ODDS_FORMAT,
        "dateFormat": DATE_FORMAT,
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Failed to retrieve events for {sport_key}: {response.status_code} {response.text}")
        return []
    return response.json()

def pull_event_odds(sport_key, event_id, accepted_markets):
    """Fetch odds for a specific event."""
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
    params = {
        "api_key": API_KEY,
        "regions": "us,eu",
        "markets": ",".join(accepted_markets),
        "oddsFormat": ODDS_FORMAT,
        "dateFormat": DATE_FORMAT,
        "bookmakers": BOOKMAKERS,
        "eventOdds": EVENT_ODDS_FLAG
    }
    response = requests.get(url, params=params)
    if response.status_code == 422:
        params["markets"] = "h2h,spreads,totals"
        response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Failed to retrieve odds for event {event_id}: {response.status_code} {response.text}")
        return None
    odds_data = response.json()
    if isinstance(odds_data, list) and odds_data:
        return odds_data[0]  # Assume the first item holds the odds data.
    elif isinstance(odds_data, dict):
        return odds_data
    else:
        return None

def main():
    all_events = []
    # Loop over each sport in the configuration and fetch events.
    for sport_label, config in SPORTS_CONFIG.items():
        sport_key = config["sport_key"]
        player_prop_markets = config.get("player_prop_markets", set())
        accepted_markets = STANDARD_MARKETS.union(player_prop_markets)
        print(f"Fetching events for {sport_label} ({sport_key}) with markets: {accepted_markets}")
        events = pull_events(sport_key, accepted_markets)
        if events:
            print(f"Retrieved {len(events)} events for {sport_label}")
            for event in events:
                # Add the sport label for later context.
                event["sport_label"] = sport_label
                # Fetch and attach full odds data for this event.
                odds = pull_event_odds(sport_key, event.get("id"), accepted_markets)
                event["odds"] = odds
                all_events.append(event)
                # Sleep briefly to avoid API rate limits.
                time.sleep(1)
        else:
            print(f"No events returned for {sport_label}")
    # Save all events (with full odds data) to a single JSON file.
    with open("data/all_odds.json", "w") as f:
        json.dump(all_events, f, indent=4)
    print("All odds (with events and bookmakers data) saved to all_odds.json")

if __name__ == "__main__":
    main()
