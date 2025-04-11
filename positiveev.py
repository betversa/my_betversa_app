import json

INPUT_FILE = "data/all_odds.json"
OUTPUT_FILE = "data/positive_ev_plays.json"

# Convert American odds to implied probability
def american_to_implied_prob(odds):
    odds = float(odds)
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)

# Convert American odds to profit on a $100 stake
def american_to_profit(odds):
    odds = float(odds)
    if odds > 0:
        return odds
    else:
        return 10000 / abs(odds)

# Convert decimal probability to no-vig American odds
def no_vig_american_odds(prob):
    if prob == 0:
        return None
    dec = 1 / prob
    if dec >= 2:
        return int((dec - 1) * 100)
    else:
        return int(-100 / (dec - 1))

# Calculate fair no-vig probabilities from Pinnacle odds
def calculate_no_vig_probabilities(outcomes):
    if len(outcomes) != 2:
        return None, None, None
    try:
        odds_1 = outcomes[0].get("price")
        odds_2 = outcomes[1].get("price")
        prob_1 = american_to_implied_prob(odds_1)
        prob_2 = american_to_implied_prob(odds_2)
        total = prob_1 + prob_2
        no_vig_1 = prob_1 / total
        no_vig_2 = prob_2 / total
        return no_vig_1, no_vig_2, outcomes
    except Exception:
        return None, None, None

# Calculate market width (difference between the two Pinnacle odds)
def calculate_market_width(pinnacle_outcomes):
    if len(pinnacle_outcomes) != 2:
        return None
    try:
        odds_1 = abs(float(pinnacle_outcomes[0].get("price")))
        odds_2 = abs(float(pinnacle_outcomes[1].get("price")))
        return round(abs(odds_1 - odds_2), 2)
    except Exception:
        return None

# EV calculation using no-vig probability from Pinnacle against the sportsbook odds
def calculate_ev(fair_prob, sportsbook_odds):
    profit_if_win = american_to_profit(sportsbook_odds)
    return round((fair_prob * profit_if_win) - (1 - fair_prob) * 100, 4)

# Aggregate odds from all sportsbooks for a given play within an event.
def aggregate_odds_for_play(event, market_key, team, point, description):
    aggregated = []
    # Loop over all bookmakers for the event
    for bookmaker in event.get("odds", {}).get("bookmakers", []):
        bk = bookmaker.get("key")
        for market in bookmaker.get("markets", []):
            if market.get("key") != market_key:
                continue
            for outcome in market.get("outcomes", []):
                # Basic matching on team name
                if outcome.get("name") != team:
                    continue
                # For spreads or totals, check that the point matches
                if market_key in {"spreads", "alternate_spreads", "totals", "alternate_totals"}:
                    if outcome.get("point") != point:
                        continue
                # For player-specific markets, also verify the description and point
                if market_key.startswith(("player", "batter", "pitcher")):
                    if outcome.get("description") != description or outcome.get("point") != point:
                        continue
                aggregated.append({
                    "bookmaker": bk,
                    "price": outcome.get("price"),
                    "point": outcome.get("point"),
                    "description": outcome.get("description")
                })
    return aggregated

# Main processing function for all odds
from datetime import datetime, timezone

def process_all_odds(data):
    ev_plays = []
    # Get current time in UTC. Make sure your event times are also in UTC.
    current_time = datetime.now(timezone.utc)
    
    for event in data:
        # Skip events if they have already started.
        # Assumes that the event dictionary has a "commence_time" key with an ISO 8601 string.
        commence_time_str = event.get("commence_time")
        if commence_time_str:
            try:
                # Convert the commence_time to a datetime object.
                # If your time string ends with "Z" indicating UTC, replace it with "+00:00".
                event_start_time = datetime.fromisoformat(commence_time_str.replace("Z", "+00:00"))
                if event_start_time <= current_time:
                    # Skip this event because it has already started.
                    continue
            except Exception as e:
                # Optionally log this exception if the time format is unexpected.
                print(f"Error parsing commence_time for event {event.get('id')}: {e}")
                continue

        # Proceed with processing the event if it hasn't started yet.
        event_id = event.get("id")
        sport = event.get("sport_label")
        home_team = event.get("home_team")
        away_team = event.get("away_team")
        
        # Iterate over each bookmaker and its markets
        for bookmaker in event.get("odds", {}).get("bookmakers", []):
            book_key = bookmaker.get("key")
            for market in bookmaker.get("markets", []):
                market_key = market.get("key")
                for outcome in market.get("outcomes", []):
                    team = outcome.get("name")
                    price = outcome.get("price")
                    point = outcome.get("point")
                    description = outcome.get("description")

                    if price is None:
                        continue

                    # Fetch corresponding Pinnacle odds for fair probability calculation...
                    # (your existing logic here)

                    # If fair probability and market width are satisfactory, construct the play:
                    if ev > 0 and (market_width is None or market_width <= 25):
                        outcome_name = outcome.get("name", "")
                        outcome_desc = description or ""
                        outcome_point_str = str(point) if point is not None else "NA"
                        unique_id = f"{event_id}_{market_key.upper()}_{outcome_name}_{outcome_desc}_{outcome_point_str}"
                        
                        fair_american_odds = no_vig_american_odds(fair_prob)
                        
                        aggregated_odds = aggregate_odds_for_play(event, market_key, team, point, description)
                        
                        ev_plays.append({
                            "unique_id": unique_id,
                            "event_id": event_id,
                            "sport": sport,
                            "home_team": home_team,
                            "away_team": away_team,
                            "market": market_key,
                            "bookmaker": book_key,
                            "team": team,
                            "point": point,
                            "description": description,
                            "sportsbook_odds": price,
                            "fair_prob": round(fair_prob, 4),
                            "fair_american_odds": fair_american_odds,
                            "ev": round(ev, 2),
                            "market_width": market_width,
                            "aggregated_odds": aggregated_odds
                        })
    return ev_plays


def main():
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)
    results = process_all_odds(data)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print(f"Saved {len(results)} positive EV plays to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

