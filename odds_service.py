import os
import requests
import json
import time
import cProfile
import pstats
import streamlit as st
import sqlite3
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler

# ===== Common Configuration =====
DEBUG = True

# Retrieve the API key from the environment variable or st.secrets
try:
    API_KEY = st.secrets["API_KEY"]
    print("API key loaded from st.secrets.")
except KeyError:
    API_KEY = os.getenv("API_KEY")
    print("st.secrets does not have 'API_KEY'. Falling back to environment variable.")

if not API_KEY:
    raise ValueError("API_KEY is not set! Please add it to your secrets or set the API_KEY environment variable.")

# ===== Sports Configuration =====
SPORTS_CONFIG = {
    "NBA": {
        "sport_key": "basketball_nba",
        "player_prop_markets": {"player_points", "player_assists", "player_rebounds", "player_threes", "player_points_rebounds_assists"}
    },
    "MLB": {
        "sport_key": "baseball_mlb",
        "player_prop_markets": {"batter_hits", "batter_total_bases", "pitcher_strikeouts"}
    },
    "NHL": {
        "sport_key": "icehockey_nhl",
        "player_prop_markets": {"player_points", "player_shots_on_goal", "player_assists", "player_goals"}
    },
    "NCAAM": {
        "sport_key": "basketball_ncaab",
        "player_prop_markets": {"player_points", "player_assists", "player_rebounds", "player_threes"}
    },
    "EPL": {
        "sport_key": "soccer_epl",
        "player_prop_markets": {"player_shots", "player_shots_on_target", "player_assists"}
    },
    "Bundesliga": {
        "sport_key": "soccer_germany_bundesliga",
        "player_prop_markets": {"player_shots", "player_shots_on_target", "player_assists"}
    },
    "Serie A": {
        "sport_key": "soccer_italy_serie_a",
        "player_prop_markets": {"player_shots", "player_shots_on_target", "player_assists"}
    },
    "La Liga": {
        "sport_key": "soccer_spain_la_liga",
        "player_prop_markets": {"player_shots", "player_shots_on_target", "player_assists"}
    },
    "MLS": {
        "sport_key": "soccer_usa_mls",
        "player_prop_markets": {"player_shots", "player_shots_on_target", "player_assists"}
    },
    "MMA": {
        "sport_key": "mma_mixed_martial_arts",
        "player_prop_markets": {"player_points", "player_shots_on_goal", "player_assists", "player_goals"}
    },
    # Add additional sports as needed.
}
STANDARD_MARKETS = {"h2h", "spreads", "totals", "alternate_spreads", "alternate_totals",
                    "h2h_h1", "h2h_q1", "h2h_p1", "h2h_1st_3_innings"}
DATE_FORMAT = 'iso'
BOOKMAKERS = 'pinnacle,fanduel,draftkings,betmgm,espnbet,williamhill_us,betonlinag,lowvig,betrivers,hardrockbet'
EVENT_ODDS_FLAG = 'true'

# ===== Configuration for Line Movement (Historical Snapshots) =====
ODDS_FORMAT_LINE = 'american'
MAX_SNAPSHOTS = 6

# ===== Configuration for Positive EV Bets =====
ODDS_FORMAT_EV = 'decimal'
BANKROLL = 1000.0
EPSILON = 0.005
MAX_EV = 0.10
AVERAGE_SPORTBOOKS = {'fanduel', 'draftkings', 'betmgm', 'espnbet', 'williamhill_us',
                      'betonlinag', 'lowvig', 'betrivers', 'hardrockbet'}
FINAL_SPORTBOOKS = {'fanduel', 'draftkings', 'betmgm'}
OUTPUT_FILE = "data/positive_ev_bets.json"

# ===== SQLite Setup for Line Movement Snapshots =====
# Create (or open) an SQLite database file for storing snapshots.
conn = sqlite3.connect("line_movement.db")
cursor = conn.cursor()

# Debug: Check if the database file exists
if os.path.exists(DB_FILENAME):
    print(f"DEBUG: Database {DB_FILENAME} exists.")
else:
    print(f"DEBUG: Database {DB_FILENAME} does not exist!")

cursor.execute('''
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    unique_key TEXT NOT NULL,
    snapshot_time TEXT NOT NULL,
    snapshot_data TEXT NOT NULL
)
''')
conn.commit()

def insert_snapshot(unique_key, snapshot):
    """Insert a new snapshot record into the database."""
    snapshot_time = datetime.now(timezone.utc).isoformat() + "Z"
    snapshot_json = json.dumps(snapshot)
    cursor.execute('''
        INSERT INTO snapshots (unique_key, snapshot_time, snapshot_data)
        VALUES (?, ?, ?)
    ''', (unique_key, snapshot_time, snapshot_json))
    conn.commit()

def get_snapshot_count(unique_key):
    """Return the number of snapshots stored for the given unique_key."""
    cursor.execute('''
        SELECT COUNT(*) FROM snapshots WHERE unique_key = ?
    ''', (unique_key,))
    return cursor.fetchone()[0]

def delete_oldest_snapshots(unique_key, keep=MAX_SNAPSHOTS):
    """Delete the oldest snapshots so that only the most recent 'keep' remain."""
    count = get_snapshot_count(unique_key)
    if count > keep:
        cursor.execute('''
            DELETE FROM snapshots 
            WHERE id IN (
                SELECT id FROM snapshots
                WHERE unique_key = ?
                ORDER BY snapshot_time ASC
                LIMIT ?
            )
        ''', (unique_key, count - keep))
        conn.commit()

def get_last_n_snapshots(unique_key, n=6):
    """Retrieve the last n snapshots for a given unique key, newest first."""
    cursor.execute('''
        SELECT snapshot_time, snapshot_data
        FROM snapshots
        WHERE unique_key = ?
        ORDER BY snapshot_time DESC
        LIMIT ?
    ''', (unique_key, n))
    snapshots = [(ts, json.loads(data)) for ts, data in cursor.fetchall()]
    return snapshots

# ===== Functions for Line Movement =====
def event_has_not_started(event):
    """Returns True if the event has not yet started (based on 'commence_time')."""
    commence_time_str = event.get("commence_time")
    if not commence_time_str:
        return True
    try:
        commence_time = datetime.fromisoformat(commence_time_str.replace("Z", "+00:00"))
        if commence_time.tzinfo is None:
            commence_time = commence_time.replace(tzinfo=timezone.utc)
        else:
            commence_time = commence_time.astimezone(timezone.utc)
        now = datetime.now(timezone.utc)
        return commence_time > now
    except Exception as e:
        if DEBUG:
            print(f"Error parsing commence_time: {e}")
        return True

def get_event_odds(event, sport_key, accepted_markets, odds_format):
    """Retrieves odds data for a given event."""
    odds_url = f'https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event["id"]}/odds'
    odds_params = {
        'api_key': API_KEY,
        'regions': 'us,eu',
        'markets': ",".join(accepted_markets),
        'oddsFormat': odds_format,
        'dateFormat': DATE_FORMAT,
        'bookmakers': BOOKMAKERS,
        'eventOdds': EVENT_ODDS_FLAG
    }
    response = requests.get(odds_url, params=odds_params)
    if response.status_code == 422:
        odds_params['markets'] = 'h2h,spreads,totals'
        response = requests.get(odds_url, params=odds_params)
    if response.status_code != 200:
        if DEBUG:
            print(f"Failed to get odds for event {event['id']}: status_code {response.status_code}")
        return None
    odds_data = response.json()
    if isinstance(odds_data, list):
        return odds_data[0]
    elif isinstance(odds_data, dict):
        return odds_data
    else:
        if DEBUG:
            print("Unexpected odds data format")
        return None

def compile_aggregated_snapshots(event, sport_label, sport_key, accepted_markets):
    """Aggregate bookmaker odds into snapshots per unique bet for line movement tracking."""
    aggregated_snapshots = {}
    event_odds = get_event_odds(event, sport_key, accepted_markets, ODDS_FORMAT_LINE)
    if not event_odds:
        return []

    for bookmaker in event_odds.get("bookmakers", []):
        bk_key = bookmaker.get("key")
        bk_title = bookmaker.get("title")
        bk_data = {"key": bk_key, "title": bk_title}
        for market in bookmaker.get("markets", []):
            market_key = market.get("key")
            if market_key not in accepted_markets:
                continue
            for outcome in market.get("outcomes", []):
                bet_name = outcome.get("name", "NA")
                description = outcome.get("description", "NA")
                offered_odds = outcome.get("price")
                offered_point = outcome.get("point")
                point_str = str(offered_point) if offered_point is not None else "NA"
                unique_key = f"{event['id']}_{market_key.upper()}_{bet_name}_{description}_{point_str}"
                if unique_key not in aggregated_snapshots:
                    aggregated_snapshots[unique_key] = {
                        "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
                        "sport": sport_label,
                        "event_id": event.get("id"),
                        "home_team": event.get("home_team"),
                        "away_team": event.get("away_team"),
                        "commence_time": event.get("commence_time"),
                        "market": market_key,
                        "outcome": f"{bet_name} {description}",
                        "bookmakers": []
                    }
                aggregated_snapshots[unique_key]["bookmakers"].append({
                    "offered_odds": offered_odds,
                    "offered_point": offered_point,
                    "bookmaker": bk_data
                })
    return list(aggregated_snapshots.items())

def run_line_movement():
    """Processes events for all sports and updates the line movement snapshots in SQLite."""
    print("DEBUG: Using SQLite for snapshot storage.")
    
    for sport_label, config in SPORTS_CONFIG.items():
        sport_key = config["sport_key"]
        player_prop_markets = config["player_prop_markets"]
        accepted_markets = STANDARD_MARKETS.union(player_prop_markets)
        print(f"\n====== Processing Sport: {sport_label} for Line Movement ======")
        events_url = f'https://api.the-odds-api.com/v4/sports/{sport_key}/events'
        events_params = {'api_key': API_KEY}
        response = requests.get(events_url, params=events_params)
        if response.status_code != 200:
            print(f"DEBUG: Failed to get events for {sport_label}: status_code {response.status_code}")
            continue
        events = response.json()
        if not events:
            print(f"DEBUG: No events found for {sport_label}")
            continue
        print(f"DEBUG: Found {len(events)} events for {sport_label}.")
        for event in events:
            if not event_has_not_started(event):
                print(f"DEBUG: Skipping event {event.get('id')} - already started.")
                continue
            play_snapshots = compile_aggregated_snapshots(event, sport_label, sport_key, accepted_markets)
            for unique_key, snap in play_snapshots:
                count_before = get_snapshot_count(unique_key)
                print(f"DEBUG: Before update - {unique_key} has {count_before} snapshot(s).")
                # Insert new snapshot for this unique key
                insert_snapshot(unique_key, snap)
                # Delete oldest snapshots if count exceeds MAX_SNAPSHOTS
                delete_oldest_snapshots(unique_key, keep=MAX_SNAPSHOTS)
                count_after = get_snapshot_count(unique_key)
                print(f"DEBUG: After update - {unique_key} now has {count_after} snapshot(s).")
            time.sleep(1)  # Pause to avoid API rate limiting

    print("DEBUG: Line movement snapshots updated in SQLite.")

def debug_print_snapshots():
    cursor.execute("SELECT unique_key, snapshot_time FROM snapshots ORDER BY snapshot_time DESC LIMIT 10")
    rows = cursor.fetchall()
    print("DEBUG: Latest snapshot entries:")
    for row in rows:
        print(f"   unique_key: {row[0]}, snapshot_time: {row[1]}")

# Call this function at the end of run_line_movement()
debug_print_snapshots()



# ===== Functions for Positive EV Bets =====
def convert_to_american(decimal_odds):
    if decimal_odds == 1:
        return 0
    if decimal_odds >= 2.0:
        return round((decimal_odds - 1) * 100)
    else:
        return round(-100 / (decimal_odds - 1))

def calculate_ev(fair_prob, offered_odds):
    return fair_prob * (offered_odds - 1) - (1 - fair_prob)

def kelly_fraction(fair_prob, offered_odds):
    b = offered_odds - 1
    if b == 0:
        return 0
    return (fair_prob * b - (1 - fair_prob)) / b

def update_best_play(best_plays, key, details):
    if key not in best_plays or details['ev'] > best_plays[key]['ev']:
        best_plays[key] = details

def compute_average_fair_prob_h2h(event_odds, outcome_name):
    implied = {}
    for bookmaker in event_odds.get('bookmakers', []):
        if bookmaker.get('key') not in AVERAGE_SPORTBOOKS:
            continue
        for market in bookmaker.get('markets', []):
            if market.get('key') != "h2h":
                continue
            for outcome in market.get('outcomes', []):
                name = outcome.get('name')
                price = outcome.get('price')
                if price and price > 0:
                    implied.setdefault(name, []).append(1 / price)
    avg_implied = {name: sum(lst) / len(lst) for name, lst in implied.items() if lst}
    total = sum(avg_implied.values())
    if total == 0:
        return None
    return avg_implied.get(outcome_name, 0) / total

def compute_average_fair_prob_spreads(event_odds, outcome_name, offered_point):
    norm_probs = []
    for bookmaker in event_odds.get('bookmakers', []):
        if bookmaker.get('key') not in AVERAGE_SPORTBOOKS:
            continue
        for market in bookmaker.get('markets', []):
            if market.get('key') not in {'spreads', 'alternate_spreads'}:
                continue
            group = [o for o in market.get('outcomes', []) 
                     if o.get('point') is not None and abs(o.get('point')) == abs(offered_point)]
            if len(group) != 2:
                continue
            try:
                opp = {o.get('name'): o for o in group}
                if outcome_name not in opp:
                    continue
                inv1 = 1 / opp[outcome_name]['price']
                other = [name for name in opp if name != outcome_name]
                if not other:
                    continue
                inv2 = 1 / opp[other[0]]['price']
                total = inv1 + inv2
                norm_probs.append(inv1 / total)
            except Exception as e:
                if DEBUG:
                    print(f"DEBUG SPREADS (avg): Error: {e}")
                continue
    return sum(norm_probs) / len(norm_probs) if norm_probs else None

def compute_average_fair_prob_totals(event_odds, outcome_name, point):
    norm_probs = []
    for bookmaker in event_odds.get('bookmakers', []):
        if bookmaker.get('key') not in AVERAGE_SPORTBOOKS:
            continue
        for market in bookmaker.get('markets', []):
            if market.get('key') not in {'totals', 'alternate_totals'}:
                continue
            group = [o for o in market.get('outcomes', []) if o.get('point') == point]
            if len(group) != 2:
                continue
            try:
                opp = {o.get('name').lower(): o for o in group}
                if outcome_name.lower() not in opp:
                    continue
                inv1 = 1 / opp[outcome_name.lower()]['price']
                other = [name for name in opp if name.lower() != outcome_name.lower()]
                if not other:
                    continue
                inv2 = 1 / opp[other[0]]['price']
                total = inv1 + inv2
                norm_probs.append(inv1 / total)
            except Exception as e:
                if DEBUG:
                    print(f"DEBUG TOTALS (avg): Error: {e}")
                continue
    return sum(norm_probs) / len(norm_probs) if norm_probs else None

def compute_average_fair_prob_player_props(event_odds, market_key, outcome_name, player_name, offered_point):
    norm_probs = []
    for bookmaker in event_odds.get('bookmakers', []):
        if bookmaker.get('key') not in AVERAGE_SPORTBOOKS:
            continue
        for market in bookmaker.get('markets', []):
            key = market.get('key', '').lower()
            if key != market_key.lower() and not key.startswith(("player", "batter", "pitcher")):
                continue
            group = [o for o in market.get('outcomes', [])
                     if o.get('point') == offered_point and (o.get('player') or o.get('description')) == player_name]
            if len(group) != 2:
                continue
            try:
                opp = {o.get('name').lower(): o for o in group}
                if outcome_name.lower() not in opp:
                    continue
                inv1 = 1 / opp[outcome_name.lower()]['price']
                other = [n for n in opp if n.lower() != outcome_name.lower()]
                if not other:
                    continue
                inv2 = 1 / opp[other[0]]['price']
                total = inv1 + inv2
                norm_probs.append(inv1 / total)
            except Exception as e:
                if DEBUG:
                    print(f"DEBUG PLAYER_PROPS (avg): Error: {e}")
                continue
    return sum(norm_probs) / len(norm_probs) if norm_probs else None

def get_odds_breakdown(event_odds, market_key, outcome_name, offered_point=None, player_name=None):
    breakdown = []
    for bookmaker in event_odds.get('bookmakers', []):
        if bookmaker.get('key') not in AVERAGE_SPORTBOOKS:
            continue
        for market in bookmaker.get('markets', []):
            if market_key == "h2h":
                if market.get('key') != "h2h":
                    continue
                for out in market.get('outcomes', []):
                    if out.get('name') == outcome_name:
                        offered = out.get('price')
                        outcomes = market.get('outcomes', [])
                        if len(outcomes) == 2:
                            imp_this = 1 / offered
                            others = [x for x in outcomes if x.get('name') != outcome_name]
                            if others:
                                imp_other = 1 / others[0].get('price')
                                normalized = imp_this / (imp_this + imp_other)
                                fair_decimal = 1 / normalized
                                fair_american = convert_to_american(fair_decimal)
                            else:
                                fair_american = None
                        else:
                            fair_american = None
                        breakdown.append({
                            "bookmaker": bookmaker.get('title'),
                            "offered_odds_american": convert_to_american(offered),
                            "fair_odds_american": fair_american
                        })
            elif market_key in {"spreads", "alternate_spreads"}:
                if market.get('key') not in {"spreads", "alternate_spreads"}:
                    continue
                for out in market.get('outcomes', []):
                    if out.get('point') is None:
                        continue
                    if abs(out.get('point')) == abs(offered_point) and out.get('name') == outcome_name:
                        offered = out.get('price')
                        outcomes = [o for o in market.get('outcomes', []) if o.get('point') is not None and abs(o.get('point')) == abs(offered_point)]
                        if len(outcomes) == 2:
                            imp_this = 1 / offered
                            others = [x for x in outcomes if x.get('name') != outcome_name]
                            if others:
                                imp_other = 1 / others[0].get('price')
                                normalized = imp_this / (imp_this + imp_other)
                                fair_decimal = 1 / normalized
                                fair_american = convert_to_american(fair_decimal)
                            else:
                                fair_american = None
                        else:
                            fair_american = None
                        breakdown.append({
                            "bookmaker": bookmaker.get('title'),
                            "offered_odds_american": convert_to_american(offered),
                            "fair_odds_american": fair_american,
                            "offered_point": offered_point
                        })
            elif market_key in {"totals", "alternate_totals"}:
                if market.get('key') not in {"totals", "alternate_totals"}:
                    continue
                for out in market.get('outcomes', []):
                    if out.get('point') != offered_point:
                        continue
                    if out.get('name').lower() != outcome_name.lower():
                        continue
                    offered = out.get('price')
                    outcomes = [o for o in market.get('outcomes', []) if o.get('point') == offered_point]
                    if len(outcomes) == 2:
                        imp_this = 1 / offered
                        others = [x for x in outcomes if x.get('name').lower() != outcome_name.lower()]
                        if others:
                            imp_other = 1 / others[0].get('price')
                            normalized = imp_this / (imp_this + imp_other)
                            fair_decimal = 1 / normalized
                            fair_american = convert_to_american(fair_decimal)
                        else:
                            fair_american = None
                    else:
                        fair_american = None
                    breakdown.append({
                        "bookmaker": bookmaker.get('title'),
                        "offered_odds_american": convert_to_american(offered),
                        "fair_odds_american": fair_american,
                        "offered_point": offered_point
                    })
            elif market_key.lower().startswith(("player", "batter", "pitcher")):
                if offered_point is None or player_name is None:
                    continue
                if market.get('key').lower() != market_key.lower() and not market.get('key').lower().startswith(("player", "batter", "pitcher")):
                    continue
                for out in market.get('outcomes', []):
                    if out.get('point') != offered_point:
                        continue
                    if (out.get('player') or out.get('description')) != player_name:
                        continue
                    if out.get('name').lower() != outcome_name.lower():
                        continue
                    offered = out.get('price')
                    outcomes = [o for o in market.get('outcomes', []) if o.get('point') == offered_point and (o.get('player') or o.get('description')) == player_name]
                    if len(outcomes) == 2:
                        imp_this = 1 / offered
                        others = [x for x in outcomes if x.get('name').lower() != outcome_name.lower()]
                        if others:
                            imp_other = 1 / others[0].get('price')
                            normalized = imp_this / (imp_this + imp_other)
                            fair_decimal = 1 / normalized
                            fair_american = convert_to_american(fair_decimal)
                        else:
                            fair_american = None
                    else:
                        fair_american = None
                    breakdown.append({
                        "bookmaker": bookmaker.get('title'),
                        "offered_odds_american": convert_to_american(offered),
                        "fair_odds_american": fair_american,
                        "offered_point": offered_point,
                        "player": player_name
                    })
    return breakdown

def process_event(event, sport_label, sport_key, accepted_markets, player_prop_markets):
    event_id = event.get('id')
    if not event_id:
        if DEBUG:
            print("DEBUG: Event ID missing.")
        return {}
    sport_title = event.get('sport_title', sport_label)
    home_team = event.get('home_team')
    away_team = event.get('away_team')
    print(f"\nProcessing Event ID: {event_id} ({home_team} vs {away_team}) - Sport: {sport_title}")
    
    odds_url = f'https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds'
    odds_params = {
        'api_key': API_KEY,
        'regions': 'us,eu',
        'markets': ",".join(accepted_markets),
        'oddsFormat': ODDS_FORMAT_EV,
        'dateFormat': DATE_FORMAT,
        'bookmakers': BOOKMAKERS,
        'eventOdds': EVENT_ODDS_FLAG
    }
    odds_response = requests.get(odds_url, params=odds_params)
    if odds_response.status_code == 422:
        print(f"  Received 422 error for event {event_id}; trying basic markets")
        odds_params['markets'] = 'h2h,spreads,totals'
        odds_response = requests.get(odds_url, params=odds_params)
    if odds_response.status_code != 200:
        print(f"  Failed to get odds for event {event_id}: status_code {odds_response.status_code}")
        return {}
    odds_data = odds_response.json()
    if isinstance(odds_data, list):
        event_odds = odds_data[0]
    elif isinstance(odds_data, dict):
        event_odds = odds_data
    else:
        print("  Unexpected odds data format")
        return {}
    
    # Extract Pinnacle Data
    pinnacle_bookmaker = next((b for b in event_odds.get('bookmakers', []) if b.get('key') == 'pinnacle'), None)
    if not pinnacle_bookmaker:
        print("  Pinnacle odds not found. Skipping event.")
        return {}
    pinnacle_data = {}
    for market in pinnacle_bookmaker.get('markets', []):
        key = market.get('key')
        if key not in accepted_markets:
            continue
        outcomes = market.get('outcomes', [])
        if not outcomes:
            continue
        market_dict = {}
        for outcome in outcomes:
            name = outcome.get('name')
            price = outcome.get('price')
            point = outcome.get('point')
            candidate = {"price": price, "point": point}
            if key in player_prop_markets or key.lower().startswith(("player", "batter", "pitcher")):
                candidate["player"] = outcome.get("player") or outcome.get("description")
            market_dict.setdefault(name, []).append(candidate)
        for candidates in market_dict.values():
            for candidate in candidates:
                candidate['implied'] = 1 / candidate['price']
        pinnacle_data[key] = market_dict
        if DEBUG:
            print(f"  Pinnacle {key.upper()} Data:")
            print(json.dumps(market_dict, indent=4))
    
    best_plays = {}
    # Process non-Pinnacle plays from FINAL_SPORTBOOKS (excluding Pinnacle)
    for bookmaker in event_odds.get('bookmakers', []):
        book_key = bookmaker.get('key')
        if book_key not in FINAL_SPORTBOOKS or book_key == 'pinnacle':
            continue
        book_title = bookmaker.get('title')
        book_markets = {m.get('key'): m for m in bookmaker.get('markets', []) if m.get('key') in accepted_markets}
        
        # Process H2H
        if 'h2h' in book_markets:
            ph = pinnacle_data.get('h2h', {})
            if len(ph) == 2:
                total_inv = sum(1 / ph[out][0]['price'] for out in ph)
                for outcome in book_markets['h2h'].get('outcomes', []):
                    name = outcome.get('name')
                    offered_odds = outcome.get('price')
                    sportsbook_american = convert_to_american(offered_odds)
                    if name in ph:
                        candidate = ph[name][0]
                        fair_prob = (1 / candidate['price']) / total_inv
                        ev = calculate_ev(fair_prob, offered_odds)
                    else:
                        fair_prob = 1 / offered_odds
                        ev = calculate_ev(fair_prob, offered_odds)
                    avg_fair_prob = compute_average_fair_prob_h2h(event_odds, name)
                    avg_ev = calculate_ev(avg_fair_prob, offered_odds) if avg_fair_prob is not None else None
                    avg_fair_american = convert_to_american(1 / avg_fair_prob) if avg_fair_prob is not None else None
                    if ev <= EPSILON or ev > MAX_EV:
                        continue
                    kelly_full = kelly_fraction(fair_prob, offered_odds)
                    kelly = 0.5 * kelly_full
                    dollar_kelly = kelly * BANKROLL
                    fair_decimal = 1 / fair_prob
                    fair_american = convert_to_american(fair_decimal)
                    odds_breakdown = get_odds_breakdown(event_odds, "h2h", name)
                    key = (event_id, "H2H", name)
                    unique_key = "_".join(str(x) for x in key)
                    details = {
                        "unique_key": unique_key,
                        "home_team": home_team,
                        "away_team": away_team,
                        "sport": sport_title,
                        "market": "H2H",
                        "bookmaker": book_title,
                        "outcome": name,
                        "sportsbook_american": sportsbook_american,
                        "offered_point": None,
                        "pinnacle_fair_american": fair_american,
                        "no_vig_american": avg_fair_american,
                        "pinnacle_point": None,
                        "fair_prob": fair_prob,
                        "ev": ev,
                        "average_ev": avg_ev,
                        "kelly_dollar": dollar_kelly,
                        "calc_method": "Pinnacle / Average",
                        "odds_breakdown": odds_breakdown
                    }
                    update_best_play(best_plays, key, details)
        
        # Process Spreads / Alternate Spreads
        for m_key in ['spreads', 'alternate_spreads']:
            if m_key not in book_markets:
                continue
            kelly_multiplier = 0.5 if m_key == 'spreads' else 0.25
            groups = {}
            for outcome in book_markets[m_key].get('outcomes', []):
                pt = outcome.get('point')
                if pt is None:
                    continue
                groups.setdefault(abs(pt), []).append(outcome)
            for group_key, outcomes_group in groups.items():
                if len(outcomes_group) != 2:
                    if DEBUG:
                        print(f"DEBUG SPREADS: Group for point {group_key} does not have exactly 2 outcomes.")
                    continue
                for outcome in outcomes_group:
                    name = outcome.get('name')
                    offered_odds = outcome.get('price')
                    offered_point = outcome.get('point')
                    sportsbook_american = convert_to_american(offered_odds)
                    if m_key in pinnacle_data and name in pinnacle_data[m_key]:
                        candidates = pinnacle_data[m_key][name]
                    elif 'spreads' in pinnacle_data and name in pinnacle_data['spreads']:
                        candidates = pinnacle_data['spreads'][name]
                    else:
                        if DEBUG:
                            print(f"DEBUG SPREADS: No Pinnacle data for {name}.")
                        continue
                    candidate = None
                    for cand in candidates:
                        if offered_point is not None and cand.get('point') is not None and cand.get('point') == offered_point:
                            candidate = cand
                            break
                    if candidate is None:
                        if DEBUG:
                            print(f"DEBUG SPREADS: No matching Pinnacle candidate for {name} at {offered_point}.")
                        continue
                    group_candidates = []
                    for other in outcomes_group:
                        other_name = other.get('name')
                        other_pt = other.get('point')
                        if m_key in pinnacle_data and other_name in pinnacle_data[m_key]:
                            cand_list = pinnacle_data[m_key][other_name]
                        elif 'spreads' in pinnacle_data and other_name in pinnacle_data['spreads']:
                            cand_list = pinnacle_data['spreads'][other_name]
                        else:
                            continue
                        for cand in cand_list:
                            if other_pt is not None and cand.get('point') is not None and cand.get('point') == other_pt:
                                group_candidates.append((other_name, 1 / cand['price']))
                                break
                    if len(group_candidates) != 2:
                        if DEBUG:
                            print(f"DEBUG SPREADS: Group for {group_key} not normalized properly.")
                        continue
                    total_inv = sum(imp for (_, imp) in group_candidates)
                    imp = next((imp for (n, imp) in group_candidates if n == name), None)
                    if imp is None:
                        continue
                    fair_prob = imp / total_inv
                    ev = calculate_ev(fair_prob, offered_odds)
                    avg_fair_prob = compute_average_fair_prob_spreads(event_odds, name, offered_point)
                    avg_ev = calculate_ev(avg_fair_prob, offered_odds) if avg_fair_prob is not None else None
                    avg_fair_american = convert_to_american(1 / avg_fair_prob) if avg_fair_prob is not None else None
                    if ev <= EPSILON or ev > MAX_EV:
                        continue
                    kelly_full = kelly_fraction(fair_prob, offered_odds)
                    kelly = kelly_multiplier * kelly_full
                    dollar_kelly = kelly * BANKROLL
                    fair_decimal = 1 / fair_prob
                    fair_american = convert_to_american(fair_decimal)
                    odds_breakdown = get_odds_breakdown(event_odds, m_key, name, offered_point)
                    key = (event_id, m_key.upper(), name, offered_point)
                    unique_key = "_".join(str(x) for x in key)
                    details = {
                        "unique_key": unique_key,
                        "home_team": home_team,
                        "away_team": away_team,
                        "sport": sport_title,
                        "market": m_key.upper(),
                        "bookmaker": book_title,
                        "outcome": name,
                        "sportsbook_american": sportsbook_american,
                        "offered_point": offered_point,
                        "pinnacle_fair_american": fair_american,
                        "no_vig_american": avg_fair_american,
                        "pinnacle_point": offered_point,
                        "fair_prob": fair_prob,
                        "ev": ev,
                        "average_ev": avg_ev,
                        "kelly_dollar": dollar_kelly,
                        "calc_method": "Pinnacle / Average",
                        "odds_breakdown": odds_breakdown
                    }
                    update_best_play(best_plays, key, details)
        
        # Process Totals / Alternate Totals
        for m_key in ['totals', 'alternate_totals']:
            if m_key not in book_markets:
                continue
            kelly_multiplier = 0.5 if m_key == 'totals' else 0.25
            groups = {}
            for outcome in book_markets[m_key].get('outcomes', []):
                pt = outcome.get('point')
                if pt is None:
                    continue
                groups.setdefault(pt, []).append(outcome)
            for pt, outcomes_group in groups.items():
                over_offer = next((o for o in outcomes_group if o.get('name').lower() == "over"), None)
                under_offer = next((o for o in outcomes_group if o.get('name').lower() == "under"), None)
                if over_offer is None or under_offer is None:
                    if DEBUG:
                        print(f"DEBUG TOTALS: Incomplete outcomes at {pt}.")
                    continue
                pk_key = m_key if m_key in pinnacle_data else ('totals' if 'totals' in pinnacle_data else None)
                if not pk_key:
                    continue
                def find_candidate(outcome_name, offered_pt):
                    candidates = pinnacle_data[pk_key].get(outcome_name, [])
                    for cand in candidates:
                        if cand.get('point') == offered_pt:
                            return cand
                    return None
                over_candidate = find_candidate("Over", pt)
                under_candidate = find_candidate("Under", pt)
                if over_candidate is None or under_candidate is None:
                    if DEBUG:
                        print(f"DEBUG TOTALS: No Pinnacle candidates for {pt}.")
                    continue
                imp_over = 1 / over_candidate['price']
                imp_under = 1 / under_candidate['price']
                total_inv = imp_over + imp_under
                fair_prob_over = imp_over / total_inv
                fair_prob_under = imp_under / total_inv
                over_offered_odds = over_offer.get('price')
                under_offered_odds = under_offer.get('price')
                over_ev = calculate_ev(fair_prob_over, over_offered_odds)
                under_ev = calculate_ev(fair_prob_under, under_offered_odds)
                avg_fair_prob_over = compute_average_fair_prob_totals(event_odds, "Over", pt)
                avg_fair_prob_under = compute_average_fair_prob_totals(event_odds, "Under", pt)
                avg_over_ev = calculate_ev(avg_fair_prob_over, over_offered_odds) if avg_fair_prob_over is not None else None
                avg_under_ev = calculate_ev(avg_fair_prob_under, under_offered_odds) if avg_fair_prob_under is not None else None
                avg_over_american = convert_to_american(1 / avg_fair_prob_over) if avg_fair_prob_over is not None else None
                avg_under_american = convert_to_american(1 / avg_fair_prob_under) if avg_fair_prob_under is not None else None
                if over_ev > EPSILON and over_ev <= MAX_EV:
                    kelly_over_full = kelly_fraction(fair_prob_over, over_offered_odds)
                    kelly_over = kelly_multiplier * kelly_over_full
                    dollar_kelly_over = kelly_over * BANKROLL
                    fair_decimal_over = 1 / fair_prob_over
                    fair_american_over = convert_to_american(fair_decimal_over)
                    odds_breakdown = get_odds_breakdown(event_odds, m_key, "Over", pt)
                    key = (event_id, m_key.upper(), "Over", pt)
                    unique_key = "_".join(str(x) for x in key)
                    details = {
                        "unique_key": unique_key,
                        "home_team": home_team,
                        "away_team": away_team,
                        "sport": sport_title,
                        "market": m_key.upper(),
                        "bookmaker": book_title,
                        "outcome": "Over",
                        "sportsbook_american": convert_to_american(over_offered_odds),
                        "offered_point": pt,
                        "pinnacle_fair_american": fair_american_over,
                        "no_vig_american": avg_over_american,
                        "pinnacle_point": pt,
                        "fair_prob": fair_prob_over,
                        "ev": over_ev,
                        "average_ev": avg_over_ev,
                        "kelly_dollar": dollar_kelly_over,
                        "calc_method": "Pinnacle / Average",
                        "odds_breakdown": odds_breakdown
                    }
                    update_best_play(best_plays, key, details)
                if under_ev > EPSILON and under_ev <= MAX_EV:
                    kelly_under_full = kelly_fraction(fair_prob_under, under_offered_odds)
                    kelly_under = kelly_multiplier * kelly_under_full
                    dollar_kelly_under = kelly_under * BANKROLL
                    fair_decimal_under = 1 / fair_prob_under
                    fair_american_under = convert_to_american(fair_decimal_under)
                    odds_breakdown = get_odds_breakdown(event_odds, m_key, "Under", pt)
                    key = (event_id, m_key.upper(), "Under", pt)
                    unique_key = "_".join(str(x) for x in key)
                    details = {
                        "unique_key": unique_key,
                        "home_team": home_team,
                        "away_team": away_team,
                        "sport": sport_title,
                        "market": m_key.upper(),
                        "bookmaker": book_title,
                        "outcome": "Under",
                        "sportsbook_american": convert_to_american(under_offered_odds),
                        "offered_point": pt,
                        "pinnacle_fair_american": fair_american_under,
                        "no_vig_american": avg_under_american,
                        "pinnacle_point": pt,
                        "fair_prob": fair_prob_under,
                        "ev": under_ev,
                        "average_ev": avg_under_ev,
                        "kelly_dollar": dollar_kelly_under,
                        "calc_method": "Pinnacle / Average",
                        "odds_breakdown": odds_breakdown
                    }
                    update_best_play(best_plays, key, details)
        
        # Process Player Props
        all_prop_keys = set(prop.lower() for prop in player_prop_markets)
        all_prop_keys = all_prop_keys.union({m for m in book_markets if m.lower().startswith(("player", "batter", "pitcher"))})
        for market_key in all_prop_keys:
            if market_key not in book_markets:
                continue
            kelly_multiplier = 0.25
            groups = {}
            for outcome in book_markets[market_key].get('outcomes', []):
                pt = outcome.get('point')
                player_name = outcome.get('player') or outcome.get('description')
                if pt is None or not player_name:
                    continue
                group_key = (player_name, pt)
                groups.setdefault(group_key, []).append(outcome)
            for group_key, outcomes_group in groups.items():
                player_name, pt = group_key
                over_offer = next((o for o in outcomes_group if o.get('name').lower() == "over"), None)
                under_offer = next((o for o in outcomes_group if o.get('name').lower() == "under"), None)
                if over_offer is None or under_offer is None:
                    if DEBUG:
                        print(f"DEBUG PLAYER_PROPS: Incomplete outcomes for {player_name} at {pt}.")
                    continue
                if market_key in pinnacle_data and "Over" in pinnacle_data[market_key]:
                    candidates_over = pinnacle_data[market_key]["Over"]
                    candidates_under = pinnacle_data[market_key]["Under"]
                else:
                    if DEBUG:
                        print(f"DEBUG PLAYER_PROPS: No Pinnacle data for {market_key}.")
                    continue
                def find_candidate(candidates, offered_pt, player_name):
                    for cand in candidates:
                        if cand.get('point') == offered_pt and cand.get('player') == player_name:
                            return cand
                    return None
                over_candidate = find_candidate(candidates_over, pt, player_name)
                under_candidate = find_candidate(candidates_under, pt, player_name)
                if over_candidate is None or under_candidate is None:
                    if DEBUG:
                        print(f"DEBUG PLAYER_PROPS: No matching Pinnacle candidate for {player_name} at {pt}.")
                    continue
                imp_over = 1 / over_candidate['price']
                imp_under = 1 / under_candidate['price']
                total_inv = imp_over + imp_under
                fair_prob_over = imp_over / total_inv
                fair_prob_under = imp_under / total_inv
                over_offered_odds = over_offer.get('price')
                under_offered_odds = under_offer.get('price')
                over_ev = calculate_ev(fair_prob_over, over_offered_odds)
                under_ev = calculate_ev(fair_prob_under, under_offered_odds)
                if over_ev > MAX_EV or under_ev > MAX_EV:
                    if DEBUG:
                        print(f"DEBUG PLAYER_PROPS: {player_name} at {pt} EV too high, skipping.")
                    continue
                avg_fair_prob_over = compute_average_fair_prob_player_props(event_odds, market_key, "Over", player_name, pt)
                avg_fair_prob_under = compute_average_fair_prob_player_props(event_odds, market_key, "Under", player_name, pt)
                avg_over_ev = calculate_ev(avg_fair_prob_over, over_offered_odds) if avg_fair_prob_over is not None else None
                avg_under_ev = calculate_ev(avg_fair_prob_under, under_offered_odds) if avg_fair_prob_under is not None else None
                avg_over_american = convert_to_american(1 / avg_fair_prob_over) if avg_fair_prob_over is not None else None
                avg_under_american = convert_to_american(1 / avg_fair_prob_under) if avg_fair_prob_under is not None else None
                if over_ev > EPSILON:
                    kelly_over_full = kelly_fraction(fair_prob_over, over_offered_odds)
                    kelly_over = kelly_multiplier * kelly_over_full
                    dollar_kelly_over = kelly_over * BANKROLL
                    fair_decimal_over = 1 / fair_prob_over
                    fair_american_over = convert_to_american(fair_decimal_over)
                    odds_breakdown = get_odds_breakdown(event_odds, market_key, "Over", pt, player_name)
                    key = (event_id, market_key.upper(), "Over", player_name, pt)
                    unique_key = "_".join(str(x) for x in key)
                    details = {
                        "unique_key": unique_key,
                        "home_team": home_team,
                        "away_team": away_team,
                        "sport": sport_title,
                        "market": market_key.upper(),
                        "bookmaker": book_title,
                        "outcome": "Over",
                        "sportsbook_american": convert_to_american(over_offered_odds),
                        "offered_point": pt,
                        "pinnacle_fair_american": fair_american_over,
                        "no_vig_american": avg_over_american,
                        "pinnacle_point": pt,
                        "fair_prob": fair_prob_over,
                        "ev": over_ev,
                        "average_ev": avg_over_ev,
                        "kelly_dollar": dollar_kelly_over,
                        "calc_method": "Pinnacle / Average",
                        "player": player_name,
                        "odds_breakdown": odds_breakdown
                    }
                    update_best_play(best_plays, key, details)
                if under_ev > EPSILON:
                    kelly_under_full = kelly_fraction(fair_prob_under, under_offered_odds)
                    kelly_under = kelly_multiplier * kelly_under_full
                    dollar_kelly_under = kelly_under * BANKROLL
                    fair_decimal_under = 1 / fair_prob_under
                    fair_american_under = convert_to_american(fair_decimal_under)
                    odds_breakdown = get_odds_breakdown(event_odds, market_key, "Under", pt, player_name)
                    key = (event_id, market_key.upper(), "Under", player_name, pt)
                    unique_key = "_".join(str(x) for x in key)
                    details = {
                        "unique_key": unique_key,
                        "home_team": home_team,
                        "away_team": away_team,
                        "sport": sport_title,
                        "market": market_key.upper(),
                        "bookmaker": book_title,
                        "outcome": "Under",
                        "sportsbook_american": convert_to_american(under_offered_odds),
                        "offered_point": pt,
                        "pinnacle_fair_american": fair_american_under,
                        "no_vig_american": avg_under_american,
                        "pinnacle_point": pt,
                        "fair_prob": fair_prob_under,
                        "ev": under_ev,
                        "average_ev": avg_under_ev,
                        "kelly_dollar": dollar_kelly_under,
                        "calc_method": "Pinnacle / Average",
                        "player": player_name,
                        "odds_breakdown": odds_breakdown
                    }
                    update_best_play(best_plays, key, details)
    
    if best_plays:
        print("\n  Best positive EV plays for this event:")
        for key in sorted(best_plays.keys()):
            details = best_plays[key]
            print(f"    [{details['market']}] {details['outcome']} from {details['bookmaker']} "
                  f"(EV: {details['ev']*100:.2f}%, Kelly: ${details['kelly_dollar']:.2f})")
    else:
        print("  No positive EV plays found for this event.")
    
    return best_plays

def run_positive_ev_script():
    """Processes events for all sports and writes positive EV bets to a file."""
    all_best_plays = []
    for sport_label, config in SPORTS_CONFIG.items():
        sport_key = config["sport_key"]
        player_prop_markets = config["player_prop_markets"]
        accepted_markets = STANDARD_MARKETS.union(player_prop_markets)
        print(f"\n====== Processing Sport: {sport_label} for Positive EV Bets ======")
        events_url = f'https://api.the-odds-api.com/v4/sports/{sport_key}/events'
        events_params = {'api_key': API_KEY}
        events_response = requests.get(events_url, params=events_params)
        if events_response.status_code != 200:
            print(f"Failed to get events for {sport_label}: status_code {events_response.status_code}, response: {events_response.text}")
            continue
        events_json = events_response.json()
        if not events_json:
            print(f"No events found for {sport_label}")
            continue
        print(f"Found {len(events_json)} events for {sport_label}.")
        for event in events_json:
            best = process_event(event, sport_label, sport_key, accepted_markets, player_prop_markets)
            if best:
                all_best_plays.extend(list(best.values()))
            time.sleep(1)
    filtered_best_plays = [
        play for play in all_best_plays
        if not (play.get("sportsbook_american", 0) > 200 or play.get("sportsbook_american", 0) < -200)
    ]
    with open(OUTPUT_FILE, "w") as f:
        json.dump(filtered_best_plays, f, indent=4)
    print(f"\nWrote {len(filtered_best_plays)} positive EV plays to {OUTPUT_FILE}")

# ===== Main Execution =====
def run_all_tasks():
    print("Starting run_all_tasks()")
    run_line_movement()
    print("Finished run_line_movement(), starting run_positive_ev_script()")
    run_positive_ev_script()
    print("Finished run_positive_ev_script()")

if __name__ == "__main__":
    # If running in GitHub Actions, run tasks once and exit.
    if os.getenv("GITHUB_ACTIONS") == "true":
        print("Running tasks once (detected GitHub Actions environment)...")
        run_all_tasks()
    else:
        # Otherwise, use the BlockingScheduler to run tasks every 5 minutes.
        scheduler = BlockingScheduler()
        scheduler.add_job(run_all_tasks, 'interval', minutes=5)
        print("Starting scheduler to run both tasks every 5 minutes...")
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass
    # Optionally close the database connection when done
    conn.close()
