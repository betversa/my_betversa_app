from module.mlb_data import get_baseball_data
import pandas as pd
import json

# Get batter stats for the 2025 season using standard batting stats.
# Note that R, H, 2B, 3B, HR, and RBI have been converted to per-game values.
stats_df = get_baseball_data.single(2025, "standard")
print(stats_df.head())

# Also write a pretty-printed JSON file
json_str = stats_df.to_json(orient='records')
data = json.loads(json_str)
with open('mlb_batter_stats_2025_pretty.json', 'w') as f:
    json.dump(data, f, indent=4)
