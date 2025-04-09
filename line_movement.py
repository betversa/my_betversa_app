import os
import json
import boto3
from datetime import datetime, timedelta, timezone

# Configuration
JSON_FILE = "data/all_odds.json"
BUCKET_NAME = "betversa-odds-data"
SNAPSHOT_PREFIX = "snapshots"

# Initialize S3 client using credentials from environment variables
s3 = boto3.client("s3")

def load_odds_from_json(json_file):
    """Load odds data from the provided JSON file."""
    with open(json_file, "r") as f:
        return json.load(f)

def upload_snapshot_to_s3(unique_id, snapshot_data):
    """Upload a snapshot to S3 with timestamped key."""
    timestamp = datetime.now(timezone.utc).isoformat()
    key = f"{SNAPSHOT_PREFIX}/{unique_id}/{timestamp}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(snapshot_data),
        ContentType="application/json"
    )
    print(f"Uploaded snapshot: {key}")

def main():
    try:
        events = load_odds_from_json(JSON_FILE)
    except Exception as e:
        print("❌ Error loading JSON file:", e)
        return

    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue

        odds_data = event.get("odds")
        if not odds_data:
            print(f"⚠️ Event {event_id} has no odds data.")
            continue

        for bookmaker in odds_data.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                market_key = market.get("key", "NA")
                for outcome in market.get("outcomes", []):
                    outcome_name = outcome.get("name", "NA")
                    outcome_desc = outcome.get("description", "NA")
                    outcome_point = outcome.get("point")
                    outcome_point_str = str(outcome_point) if outcome_point is not None else "NA"

                    unique_id = f"{event_id}_{market_key.upper()}_{outcome_name}_{outcome_desc}_{outcome_point_str}"

                    snapshot = {
                        "event": event,
                        "bookmaker": bookmaker,
                        "market": market,
                        "outcome": outcome
                    }
                    upload_snapshot_to_s3(unique_id, snapshot)

    print("✅ All snapshots uploaded to S3.")

if __name__ == "__main__":
    main()
