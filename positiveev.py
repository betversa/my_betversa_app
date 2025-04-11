import json
import pandas as pd
import os
from datetime import datetime, timezone
import math

# ----- Odds Conversion Helpers -----
def american_to_implied_prob(odds):
    odds = float(odds)
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)

def american_to_profit(odds):
    odds = float(odds)
    if odds > 0:
        return odds
    else:
        return 10000 / abs(odds)

def no_vig_american_odds(prob):
    if prob == 0:
        return None
    dec = 1 / prob
    if dec >= 2:
        return int((dec - 1) * 100)
    else:
        return int(-100 / (dec - 1))

def calculate_no_vig_probabilities(outcomes):
    # Requires exactly 2 outcomes to proceed.
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

def calculate_market_width(pinnacle_outcomes):
    if len(pinnacle_outcomes) != 2:
        return None
    try:
        odds_1 = abs(float(pinnacle_outcomes[0].get("price")))
        odds_2 = abs(float(pinnacle_outcomes[1].get("price")))
        return round(abs(odds_1 - odds_2), 2)
    except Exception:
        return None

def calculate_ev(fair_prob, sportsbook_odds):
    profit_if_win = american_to_profit(sportsbook_odds)
    return round((fair_prob * profit_if_win) - (1 - fair_prob) * 100, 4)

def aggregate_odds_for_play(event, market_key, team, point, description):
    aggregated = []
    for bookmaker in event.get("odds", {}).get("bookmakers", []):
        bk = bookmaker.get("key")
        for market in bookmaker.get("markets", []):
            if market.get("key") != market_key:
                continue
            for outcome in market.get("outcomes", []):
                if outcome.get("name") != team:
                    continue
                if market_key.startswith(("spreads", "alternate_spreads", "totals", "alternate_totals")):
                    if outcome.get("point") != point:
                        continue
                if market_key.startswith(("player", "batter", "pitcher", "team")):
                    if outcome.get("description") != description or outcome.get("point") != point:
                        continue
                aggregated.append({
                    "bookmaker": bk,
                    "price": outcome.get("price"),
                    "point": outcome.get("point"),
                    "description": outcome.get("description")
                })
    return aggregated

# ----- Helper: Determine Fair Probability & Market Width from Pinnacle -----
def determine_fair_prob_and_width(event, market_key, team, point, description, outcome_name):
    """
    Loop through the event's bookmakers to find the Pinnacle odds for the given market.
    If Pinnacle is unavailable, calculate the fair probability and market width using the aggregated odds.
    Returns a tuple (fair_prob, market_width) if successfully determined, or (None, None) otherwise.
    """
    fair_prob = None
    market_width = None
    pinnacle_outcomes = None
    aggregated_outcomes = []

    # Look for the Pinnacle bookmaker
    for pin_book in event.get("odds", {}).get("bookmakers", []):
        if pin_book.get("key") == "pinnacle":
            for pin_market in pin_book.get("markets", []):
                if pin_market.get("key") == market_key:
                    pinnacle_outcomes = pin_market.get("outcomes", [])
                    break

    # If Pinnacle is available, use its odds
    if pinnacle_outcomes:
        # For head-to-head markets
        if market_key.startswith("h2h") and len(pinnacle_outcomes) == 2:
            fair_prob1, fair_prob2, _ = calculate_no_vig_probabilities(pinnacle_outcomes)
            fair_prob = fair_prob1 if pinnacle_outcomes[0].get("name") == team else fair_prob2
            market_width = calculate_market_width(pinnacle_outcomes)
        # For spreads or alternate spreads
        elif market_key in {"spreads", "alternate_spreads"}:
            valid_outcomes = [o for o in pinnacle_outcomes if o.get("point") == point]
            if len(valid_outcomes) == 2:
                fair_prob1, fair_prob2, _ = calculate_no_vig_probabilities(valid_outcomes)
                fair_prob = fair_prob1 if valid_outcomes[0].get("name") == team else fair_prob2
                market_width = calculate_market_width(valid_outcomes)
        # For totals or alternate totals
        elif market_key.startswith(("totals", "alternate_totals")):
            valid_outcomes = [o for o in pinnacle_outcomes if o.get("point") == point]
            if len(valid_outcomes) == 2:
                fair_prob1, fair_prob2, _ = calculate_no_vig_probabilities(valid_outcomes)
                fair_prob = fair_prob1 if valid_outcomes[0].get("name") == outcome_name else fair_prob2
                market_width = calculate_market_width(valid_outcomes)
        # For player-specific markets
        elif market_key.startswith(("player", "batter", "pitcher", "team")):
            valid_outcomes = [o for o in pinnacle_outcomes if o.get("point") == point and o.get("description") == description]
            if len(valid_outcomes) == 2:
                fair_prob1, fair_prob2, _ = calculate_no_vig_probabilities(valid_outcomes)
                fair_prob = fair_prob1 if valid_outcomes[0].get("name") == outcome_name else fair_prob2
                market_width = calculate_market_width(valid_outcomes)

    # If Pinnacle is not available, calculate using average odds
    if fair_prob is None:
        for bookmaker in event.get("odds", {}).get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                if market.get("key") != market_key:
                    continue
                for outcome in market.get("outcomes", []):
                    if outcome.get("name") == team:
                        aggregated_outcomes.append(outcome)

        # Calculate fair probability and market width from aggregated outcomes
        if aggregated_outcomes:
            total_prob = sum(american_to_implied_prob(outcome.get("price")) for outcome in aggregated_outcomes)
            fair_prob = total_prob / len(aggregated_outcomes)

            # Calculate market width as the difference between the highest and lowest odds
            prices = [abs(outcome.get("price")) for outcome in aggregated_outcomes if outcome.get("price") is not None]
            if prices:
                market_width = round(max(prices) - min(prices), 2)

    return fair_prob, market_width

# ----- Main Processing Function for All Odds -----
INPUT_FILE = "data/all_odds.json"
OUTPUT_FILE = "data/positive_ev_plays.json"

def process_all_odds(data):
    ev_plays = []
    current_time = datetime.now(timezone.utc)
    
    for event in data:
        # Skip events that have already started.
        commence_time_str = event.get("commence_time")
        if commence_time_str:
            try:
                event_start_time = datetime.fromisoformat(commence_time_str.replace("Z", "+00:00"))
                if event_start_time <= current_time:
                    continue
            except Exception as e:
                print(f"Error parsing commence_time for event {event.get('id')}: {e}")
                continue

        event_id = event.get("id")
        sport = event.get("sport_label")
        home_team = event.get("home_team")
        away_team = event.get("away_team")
        
        for bookmaker in event.get("odds", {}).get("bookmakers", []):
            book_key = bookmaker.get("key")
            if book_key in {"lowvig", "pinnacle"}:
                continue
            for market in bookmaker.get("markets", []):
                market_key = market.get("key")
                for outcome in market.get("outcomes", []):
                    team = outcome.get("name")
                    price = outcome.get("price")
                    point = outcome.get("point")
                    description = outcome.get("description")
                    if price is None:
                        continue

                    # Determine fair probability and market width from Pinnacle data.
                    fair_prob, market_width = determine_fair_prob_and_width(
                        event, market_key, team, point, description, outcome.get("name")
                    )

                    # If fair_prob could not be determined, skip this outcome.
                    if fair_prob is None:
                        continue

                    # Now calculate EV using the calculated fair_prob and sportsbook odds (price).
                    ev = calculate_ev(fair_prob, price)
                    
                    # Only process the outcome if EV is positive and market width is acceptable.
                    if 0.5 < ev < 10 and (market_width is None or market_width <= 25):
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
