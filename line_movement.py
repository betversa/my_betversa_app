import os
import json
import boto3
from datetime import datetime, timezone

# Configuration
JSON_FILE = "data/all_odds.json"
BUCKET_NAME = "betversa-odds-data"
SNAPSHOT_PREFIX = "snapshots/"

# Initialize S3 client (boto3 will automatically pick up AWS credentials from the environment)
s3 = boto3.client("s3")

def load_odds_from_json(json_file):
    """Load odds data from the provided JSON file."""
    with open(json_file, "r") as f:
        return json.load(f)

def upload_snapshot_to_s3(unique_key, snapshot_data):
    """Upload a snapshot to S3 using a key composed from the unique key and a timestamp."""
    # Create a compact timestamp string (ISO formatted without colons)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    s3_key = f"{SNAPSHOT_PREFIX}{unique_key}/{timestamp}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(snapshot_data),
        ContentType="application/json"
    )
    print(f"Uploaded snapshot for {unique_key} to {s3_key}")

def main():
    try:
        events = load_odds_from_json(JSON_FILE)
    except Exception as e:
        print("Error loading JSON file:", e)
        return

    # Create a dictionary to group snapshots by unique key.
    snapshots = {}

    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue

        odds_data = event.get("odds")
        if not odds_data:
            print(f"Event {event_id} has no odds data.")
            continue

        # Iterate over bookmakers
        for bookmaker in odds_data.get("bookmakers", []):
            bookmaker_title = bookmaker.get("title", "Unknown Sportsbook")
            # Iterate over the markets for this bookmaker
            for market in bookmaker.get("markets", []):
                market_key = market.get("key", "NA")
                # Iterate over outcomes in this market
                for outcome in market.get("outcomes", []):
                    outcome_name = outcome.get("name", "NA")
                    outcome_desc = outcome.get("description", "")
                    outcome_point = outcome.get("point")
                    outcome_point_str = str(outcome_point) if outcome_point is not None else "NA"

                    # Create the unique key based on your logic.
                    unique_key = f"{event_id}_{market_key.upper()}_{outcome_name}_{outcome_desc}_{outcome_point_str}"

                    # If this unique key isn’t already in our snapshots dictionary, create a new entry.
                    if unique_key not in snapshots:
                        snapshots[unique_key] = {
                            "unique_key": unique_key,
                            "event_id": event_id,
                            "market_key": market_key,
                            "bet_details": {
                                "outcome_name": outcome_name,
                                "outcome_description": outcome_desc,
                                "point": outcome_point
                            },
                            "sportsbook_odds": [],   # We'll append all odds here
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                    # Append this bookmaker’s odds to the aggregated list for the unique key.
                    snapshots[unique_key]["sportsbook_odds"].append({
                        "bookmaker": bookmaker_title,
                        "price": outcome.get("price")
                    })

    # Now upload one snapshot per unique key
    for unique_key, snapshot_data in snapshots.items():
        try:
            upload_snapshot_to_s3(unique_key, snapshot_data)
        except Exception as e:
            print(f"Error uploading snapshot for {unique_key}: {e}")

    print("All aggregated snapshots uploaded to S3.")

if __name__ == "__main__":
    main()
