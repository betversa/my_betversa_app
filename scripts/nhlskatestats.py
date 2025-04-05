from modules.nhl_data import get_hockey_data
import pandas as pd
import json

# Get NHL skater stats for the 2025 season.
# The stats for G, A, PTS, and SOG are now represented on a per-game basis.
stats_df = get_hockey_data.single_skaters(2025)
print(stats_df.head())

# Also write a pretty-printed JSON file
json_str = stats_df.to_json(orient='records')
data = json.loads(json_str)
with open('nhl_skater_stats_2025_pretty.json', 'w') as f:
    json.dump(data, f, indent=4)
