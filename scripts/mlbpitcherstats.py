from mlb_data import get_baseball_data
import pandas as pd
import json

# Get pitcher stats for the 2025 season using standard pitching stats.
# Note that IP, H, ER, BB, and SO have been converted to per game started (GS).
stats_df = get_baseball_data.single_pitching(2025, "standard")
print(stats_df.head())

# Also write a pretty-printed JSON file
json_str = stats_df.to_json(orient='records')
data = json.loads(json_str)
with open('mlb_pitcher_stats_2025_pretty.json', 'w') as f:
    json.dump(data, f, indent=4)
