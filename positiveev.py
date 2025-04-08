import json

INPUT_FILE = "all_odds.json"
OUTPUT_FILE = "positive_ev_plays.json"

# Convert American odds to implied probability
def american_to_implied_prob(odds):
    odds = float(odds)
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)

# Convert American odds to profit on $100 stake
def american_to_profit(odds):
    odds = float(odds)
    if odds > 0:
        return odds
    else:
        return 10000 / abs(odds)

# Convert decimal probability to no vig American odds
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
    except:
        return None, None, None

# Calculate market width between Pinnacle odds
def calculate_market_width(pinnacle_outcomes):
    if len(pinnacle_outcomes) != 2:
        return None
    try:
        odds_1 = abs(float(pinnacle_outcomes[0].get("price")))
        odds_2 = abs(float(pinnacle_outcomes[1].get("price")))
        return round(abs(odds_1 - odds_2), 2)
    except:
        return None

# EV calculation using no vig probability from Pinnacle
def calculate_ev(fair_prob, sportsbook_odds):
    profit_if_win = american_to_profit(sportsbook_odds)
    return round((fair_prob * profit_if_win) - (1 - fair_prob) * 100, 4)

# Main processing function
def process_all_odds(data):
    ev_plays = []
    for event in data:
        event_id = event.get("id")
        home_team = event.get("home_team")
        away_team = event.get("away_team")
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

                    pinnacle_outcomes = []
                    match = None

                    for pin_book in event.get("odds", {}).get("bookmakers", []):
                        if pin_book.get("key") != "pinnacle":
                            continue
                        for pin_market in pin_book.get("markets", []):
                            if pin_market.get("key") != market_key:
                                continue
                            pin_outcomes = pin_market.get("outcomes", [])
                            if market_key.startswith("h2h"):
                                match = [o for o in pin_outcomes if o.get("name") == team]
                                pinnacle_outcomes = pin_outcomes
                            elif market_key in {"spreads", "alternate_spreads"}:
                                match = [o for o in pin_outcomes if o.get("name") == team and o.get("point") == point]
                                pinnacle_outcomes = [o for o in pin_outcomes if o.get("point") == point]
                            elif market_key in {"totals", "alternate_totals"}:
                                match = [o for o in pin_outcomes if o.get("name") == outcome.get("name") and o.get("point") == point]
                                pinnacle_outcomes = [o for o in pin_outcomes if o.get("point") == point]
                            elif market_key.startswith(("player", "batter", "pitcher")):
                                match = [o for o in pin_outcomes if o.get("name") == outcome.get("name") and o.get("description") == description and o.get("point") == point]
                                pinnacle_outcomes = [o for o in pin_outcomes if o.get("description") == description and o.get("point") == point]
                            if match:
                                break

                    if not pinnacle_outcomes or len(pinnacle_outcomes) != 2:
                        continue

                    no_vig_probs = calculate_no_vig_probabilities(pinnacle_outcomes)
                    if no_vig_probs is None:
                        continue
                    fair_prob_1, fair_prob_2, outcomes_used = no_vig_probs
                    fair_prob = fair_prob_1 if match and match[0] == outcomes_used[0] else fair_prob_2

                    ev = calculate_ev(fair_prob, price)
                    market_width = calculate_market_width(pinnacle_outcomes)

                    if ev > 0 and (market_width is None or market_width <= 25):
                        outcome_name = outcome.get("name", "")
                        outcome_desc = description or ""
                        outcome_point_str = str(point) if point is not None else "NA"
                        unique_id = f"{event_id}_{market_key.upper()}_{outcome_name}_{outcome_desc}_{outcome_point_str}"

                        fair_american_odds = no_vig_american_odds(fair_prob)

                        ev_plays.append({
                            "unique_id": unique_id,
                            "event_id": event_id,
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
                            "market_width": market_width
                        })
    return ev_plays

# Main script
def main():
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)
    results = process_all_odds(data)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print(f"Saved {len(results)} positive EV plays to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
