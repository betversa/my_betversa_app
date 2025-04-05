import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import re
import pandas as pd
from io import StringIO
import unicodedata
import numpy as np

pd.options.mode.chained_assignment = None

def normalize_name(name):
    """Normalize player names by removing accents and extra spaces."""
    normalized = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')
    return normalized.strip()

class get_hockey_data:
    @staticmethod
    def single_skaters(season):
        """
        Fetch NHL skater stats from Hockey-Reference for the given season.
        The URL is constructed as: 
            https://www.hockey-reference.com/leagues/NHL_{season}_skaters.html
        Then, select the following columns:
            Player, GP, G, A, PTS, SOG
        Finally, convert G, A, PTS, and SOG into per-game values by dividing by GP.
        """
        url = f"https://www.hockey-reference.com/leagues/NHL_{season}_skaters.html"
        html = urllib.request.urlopen(url)
        soup = BeautifulSoup(html, 'html.parser')
        
        # Attempt to locate the main stats table.
        table = soup.find("table", {"id": "skaters"})
        if table is None:
            # If not found, fallback to the first table on the page
            table = soup.find("table")
        
        # Read the table into a DataFrame.
        df = pd.read_html(StringIO(str(table)))[0]
        
        # Flatten the columns if they are a MultiIndex.
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1)
        
        # Remove any repeated header rows (rows where "Player" is repeated).
        df = df[df["Player"] != "Player"]
        
        # Clean up player names.
        df["Player"] = df["Player"].str.replace("*", "", regex=False)
        df["Player"] = df["Player"].apply(normalize_name)
        
        # Convert columns to numeric where possible.
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass
        
        # Select only the columns of interest.
        cols_to_keep = ["Player", "GP", "G", "A", "PTS", "SOG", "ATOI"]
        available_cols = [col for col in cols_to_keep if col in df.columns]
        df = df[available_cols]
        
        # Convert G, A, PTS, and SOG to per-game values by dividing by GP.
        if "GP" in df.columns:
            for stat in ["G", "A", "PTS", "SOG"]:
                if stat in df.columns:
                    df[stat] = df.apply(lambda row: row[stat] / row["GP"] if row["GP"] else np.nan, axis=1)
        
        return df

