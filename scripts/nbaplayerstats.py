from modules.basketball_data import get_data
import pandas as pd
import json

# Example: Get player stats for the 2025 season with additional data
stats_df = get_data.single(2025, "per_game", additional_data=True)
print(stats_df.head())

# Alternatively, write a pretty-printed JSON file
json_str = stats_df.to_json(orient='records')
data = json.loads(json_str)
with open('data/nba_stats_2025_pretty.json', 'w') as f:
    json.dump(data, f, indent=4)
