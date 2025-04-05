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

class get_baseball_data:
    @staticmethod
    def single(season, stats_type="standard"):
        """
        Fetch batter stats from Baseball Reference for the given season.
        stats_type: "standard" (for standard batting stats) or "advanced" (for advanced stats).
        """
        url = f"https://www.baseball-reference.com/leagues/MLB/{season}-{stats_type}-batting.shtml"
        html = urllib.request.urlopen(url)
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for the table with id "players_standard_batting"
        table = soup.find("table", {"id": "players_standard_batting"})
        if table is None:
            table = soup.find("table")
        
        df = pd.read_html(StringIO(str(table)))[0]
        # Remove repeated header rows
        df = df[df["Player"] != "Player"]
        # Clean up player names
        df["Player"] = df["Player"].str.replace("*", "", regex=False)
        df["Player"] = df["Player"].apply(normalize_name)
        
        # Convert columns to numeric where possible
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass
        
        # Select only the columns of interest
        cols_to_keep = ["Player", "G", "AB", "R", "H", "2B", "3B", "HR", "RBI", "SB", "CS", "BA", "OBP", "SLG", "OPS"]
        available_cols = [col for col in cols_to_keep if col in df.columns]
        df = df[available_cols]
        
        # Normalize hitting counting stats by games played (G)
        if "G" in df.columns:
            for stat in ["R", "H", "2B", "3B", "HR", "RBI"]:
                if stat in df.columns:
                    df[stat] = df.apply(lambda row: row[stat] / row["G"] if row["G"] else np.nan, axis=1)
        
        return df

    @staticmethod
    def single_pitching(season, stats_type="standard"):
        """
        Fetch pitcher stats from Baseball Reference for the given season.
        stats_type: "standard" (for standard pitching stats) or "advanced" (for advanced stats).
        """
        url = f"https://www.baseball-reference.com/leagues/MLB/{season}-{stats_type}-pitching.shtml"
        html = urllib.request.urlopen(url)
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for the pitching table by id "players_standard_pitching"
        table = soup.find("table", {"id": "players_standard_pitching"})
        if table is None:
            table = soup.find("table")
        
        df = pd.read_html(StringIO(str(table)))[0]
        # Remove repeated header rows
        df = df[df["Player"] != "Player"]
        # Clean up player names
        df["Player"] = df["Player"].str.replace("*", "", regex=False)
        df["Player"] = df["Player"].apply(normalize_name)
        
        # Convert columns to numeric where possible
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass
        
        # Select only the columns of interest
        cols_to_keep = ["Player", "W", "L", "ERA", "G", "GS", "SV", "IP", "H", "ER", "BB", "SO", "WHIP"]
        available_cols = [col for col in cols_to_keep if col in df.columns]
        df = df[available_cols]
        
        # Normalize pitching stats by games started (GS)
        if "GS" in df.columns:
            for stat in ["IP", "H", "ER", "BB", "SO"]:
                if stat in df.columns:
                    df[stat] = df.apply(lambda row: row[stat] / row["GS"] if row["GS"] else np.nan, axis=1)
        
        return df

