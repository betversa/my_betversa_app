import os
import json
import sqlite3
from datetime import datetime, timedelta, timezone

# Configuration: Path to the JSON file and the SQLite database file.
JSON_FILE = "all_odds.json"
DB_FILE = "odds_data.db"

def create_table(conn):
    """Creates the odds_snapshots table if it does not exist."""
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS odds_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            unique_id TEXT NOT NULL,
            snapshot_time TEXT NOT NULL,
            snapshot_data TEXT NOT NULL
        )
    ''')
    conn.commit()

def insert_snapshot(conn, unique_id, snapshot_data):
    """Insert a new snapshot record with the current UTC timestamp."""
    cursor = conn.cursor()
    snapshot_time = datetime.now(timezone.utc).isoformat()
    cursor.execute('''
        INSERT INTO odds_snapshots (unique_id, snapshot_time, snapshot_data)
        VALUES (?, ?, ?)
    ''', (unique_id, snapshot_time, json.dumps(snapshot_data)))
    conn.commit()

def prune_snapshots(conn):
    """Prune snapshots to:
       1. Keep at most 10 records per unique_id (delete oldest ones).
       2. Delete any records older than 24 hours.
    """
    cursor = conn.cursor()
    
    # Delete snapshots older than 24 hours.
    cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cursor.execute('''
        DELETE FROM odds_snapshots WHERE snapshot_time < ?
    ''', (cutoff_time,))
    conn.commit()
    
    # For each unique_id, keep only the 10 most recent snapshots.
    cursor.execute('''
        SELECT unique_id, COUNT(*) as cnt FROM odds_snapshots GROUP BY unique_id HAVING cnt > 10
    ''')
    rows = cursor.fetchall()
    for unique_id, cnt in rows:
        to_delete = cnt - 10
        cursor.execute('''
            DELETE FROM odds_snapshots
            WHERE id IN (
                SELECT id FROM odds_snapshots
                WHERE unique_id = ?
                ORDER BY snapshot_time ASC
                LIMIT ?
            )
        ''', (unique_id, to_delete))
        conn.commit()

def load_odds_from_json(json_file):
    """Load odds data from the provided JSON file.
       Now expected as a list of event objects.
    """
    with open(json_file, "r") as f:
        return json.load(f)

def main():
    conn = sqlite3.connect(DB_FILE)
    create_table(conn)
    
    try:
        events = load_odds_from_json(JSON_FILE)
    except Exception as e:
        print("Error loading JSON file:", e)
        conn.close()
        return
    
    # Process each event in the JSON list.
    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue
        
        # Check that odds data exists.
        odds_data = event.get("odds")
        if not odds_data:
            print(f"Event {event_id} has no odds data.")
            continue
        
        # Iterate over bookmakers, markets, and outcomes.
        bookmakers = odds_data.get("bookmakers", [])
        for bookmaker in bookmakers:
            markets = bookmaker.get("markets", [])
            for market in markets:
                market_key = market.get("key", "NA")
                for outcome in market.get("outcomes", []):
                    outcome_name = outcome.get("name", "NA")
                    outcome_desc = outcome.get("description", "NA")
                    outcome_point = outcome.get("point")
                    outcome_point_str = str(outcome_point) if outcome_point is not None else "NA"
                    
                    # Create unique id using event id, market (upper case), outcome name, description, and point.
                    unique_id = f"{event_id}_{market_key.upper()}_{outcome_name}_{outcome_desc}_{outcome_point_str}"
                    
                    # Prepare a snapshot that includes the full event and the specific odds details.
                    snapshot = {
                        "event": event,
                        "bookmaker": bookmaker,
                        "market": market,
                        "outcome": outcome
                    }
                    insert_snapshot(conn, unique_id, snapshot)
                    print(f"Inserted snapshot for {unique_id}")
    
    prune_snapshots(conn)
    print("Pruning complete.")
    
    conn.close()
    print("Database connection closed.")

if __name__ == "__main__":
    main()

