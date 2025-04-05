import json
import pandas as pd
import streamlit as st
import base64
import matplotlib.pyplot as plt
from functools import reduce
from operator import mul
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from datetime import datetime
import plotly.express as px  # For interactive charts
import ijson

###############################################################################
# Page Config & Basic Functions
###############################################################################
st.set_page_config(
    page_title="BetVersa.app",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon=":money_with_wings:"
)

def get_base64_image(file_path):
    try:
        with open(file_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")
    except Exception:
        return ""

def parse_query_params():
    query_params = st.query_params
    page_val = query_params.get("page", "overview")
    st.session_state.page = page_val[0] if isinstance(page_val, list) else page_val

def set_query_params(page_name):
    st.experimental_set_query_params(page=page_name)

# Odds conversion helpers.
def american_to_decimal(odds):
    try:
        odds = float(odds)
    except:
        return None
    return 1 + odds / 100.0 if odds > 0 else 1 + 100.0 / abs(odds)

def decimal_to_american(decimal_odds):
    try:
        decimal_odds = float(decimal_odds)
    except:
        return None
    return round((decimal_odds - 1) * 100) if decimal_odds >= 2 else round(-100 / (decimal_odds - 1))

###############################################################################
# Function to Add Combo NV Odds and Combo EV Columns (Weighted 65/35)
###############################################################################
def add_combo_columns(df):
    df = df.copy()
    combo_nv = []
    combo_ev = []
    for idx, row in df.iterrows():
        try:
            pin = row["Pin NV Odds"]
            avg = row["Avg NV Odds"]
            dec_pin = american_to_decimal(pin)
            dec_avg = american_to_decimal(avg)
            if dec_pin and dec_avg:
                prob_pin = 1 / dec_pin
                prob_avg = 1 / dec_avg
                weighted_prob = 0.65 * prob_pin + 0.35 * prob_avg
                combo_decimal = 1 / weighted_prob
                combo_american = decimal_to_american(combo_decimal)
            else:
                combo_american = None
        except Exception:
            combo_american = None
        combo_nv.append(combo_american)
        try:
            ev_str = row["EV"]
            avg_ev_str = row["Avg EV"]
            ev_val = float(ev_str.replace("%", "")) / 100 if isinstance(ev_str, str) else ev_str
            avg_ev_val = float(avg_ev_str.replace("%", "")) / 100 if isinstance(avg_ev_str, str) else avg_ev_str
            weighted_ev = 0.65 * ev_val + 0.35 * avg_ev_val
            combo_str = f"{weighted_ev * 100:.2f}%"
        except Exception:
            combo_str = ""
        combo_ev.append(combo_str)
    
    df["Combo NV Odds"] = combo_nv
    df["Combo EV"] = combo_ev
    df = df.drop(columns=["Pin NV Odds", "Avg NV Odds", "EV", "Avg EV"])
    return df

###############################################################################
# Data Loading Functions with TTL (60 seconds caching)
###############################################################################
@st.cache_data(ttl=60)
def load_data():
    with st.spinner("Loading bets data..."):
        try:
            with open("data/positive_ev_bets.json", "r") as file:
                data = json.load(file)
            if isinstance(data, dict):
                data = [data]
            df = pd.DataFrame(data)
            df = df.rename(columns={
                "home_team": "Home Team",
                "away_team": "Away Team",
                "sport": "Sport",
                "market": "Market",
                "bookmaker": "Book",
                "outcome": "Outcome",
                "sportsbook_american": "Odds",
                "offered_point": "Line",
                "pinnacle_fair_american": "Pin NV Odds",
                "no_vig_american": "Avg NV Odds",
                "ev": "EV",
                "average_ev": "Avg EV",
                "kelly_dollar": "Kelly $",
                "calc_method": "Calculation Method",
                "player": "Player"
            })
            # We assume unique_key is provided.
            df["Game"] = df["Away Team"] + " @ " + df["Home Team"]
            if "Market" in df.columns:
                df["Market"] = df["Market"].apply(lambda x: x.replace("_", " ").title() if pd.notnull(x) else x)
            selected_columns = [
                "Sport", "Game", "unique_key", "Player", "Market", "Book", "Outcome", "Line", "Odds",
                "Pin NV Odds", "Avg NV Odds", "EV", "Avg EV", "Kelly $", "odds_breakdown"
            ]
            df = df[selected_columns]
            df["EV"] = df["EV"].apply(lambda x: f"{x * 100:.2f}%" if pd.notnull(x) else x)
            df["Avg EV"] = df["Avg EV"].apply(lambda x: f"{x * 100:.2f}%" if pd.notnull(x) else x)
            df["Line"] = df["Line"].apply(lambda x: f"{float(x):.1f}" if pd.notnull(x) else x)
            if "Kelly $" in df.columns:
                df["Kelly $"] = df["Kelly $"].apply(lambda x: f"${float(x):.2f}" if pd.notnull(x) else x)
            return df
        except Exception as e:
            st.error("Error loading data: " + str(e))
            return pd.DataFrame()

@st.cache_data(ttl=60)
def load_nba_stats():
    with st.spinner("Loading NBA stats..."):
        try:
            with open("data/nba_stats_2025_pretty.json", "r") as f:
                nba_data = json.load(f)
            return pd.DataFrame(nba_data)
        except Exception as e:
            st.error("Error loading NBA stats data: " + str(e))
            return pd.DataFrame()

@st.cache_data(ttl=60)
def load_nba_stats_2024():
    try:
        with open("data/nba_stats_2024_pretty.json", "r") as f:
            nba_data = json.load(f)
        return pd.DataFrame(nba_data)
    except Exception as e:
        st.error("Error loading NBA 2024 stats: " + str(e))
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_mlb_batter_stats_2024():
    try:
        with open("data/mlb_batter_stats_pretty.json", "r") as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except Exception as e:
        st.error("Error loading MLB batter stats 2024: " + str(e))
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_mlb_batter_stats_2025():
    try:
        with open("data/mlb_batter_stats_2025_pretty.json", "r") as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except Exception as e:
        st.error("Error loading MLB batter stats 2025: " + str(e))
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_mlb_pitcher_stats_2024():
    try:
        with open("data/mlb_pitcher_stats_pretty.json", "r") as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except Exception as e:
        st.error("Error loading MLB pitcher stats 2024: " + str(e))
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_mlb_pitcher_stats_2025():
    try:
        with open("data/mlb_pitcher_stats_2025_pretty.json", "r") as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except Exception as e:
        st.error("Error loading MLB pitcher stats 2025: " + str(e))
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_nhl_skater_stats_2024():
    try:
        with open("data/nhl_skater_stats_2024_pretty.json", "r") as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except Exception as e:
        st.error("Error loading NHL skater stats 2024: " + str(e))
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_nhl_skater_stats_2025():
    try:
        with open("data/nhl_skater_stats_2025_pretty.json", "r") as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except Exception as e:
        st.error("Error loading NHL skater stats 2025: " + str(e))
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_history_odds_for_ev(ev_key_tails, file_path="data/line_movement.json"):
    history_data = {}
    try:
        with open(file_path, "r") as f:
            # Use ijson to iterate over key-value pairs from the top-level JSON object
            parser = ijson.kvitems(f, "")
            for key, value in parser:
                # Use your tail_key logic here (or inline) to check if this key is relevant
                key_tail = key.split("_", 1)[-1].lower()
                if key_tail in ev_key_tails:
                    history_data[key] = value
        return history_data
    except Exception as e:
        st.error("Error lazy loading history odds for EV bets: " + str(e))
        return {}


df_full = load_data()
df_unique = df_full.drop_duplicates(subset=["unique_key"])
df_processed = add_combo_columns(df_unique)

###############################################################################
# Helper: Extract tail from unique key (everything after the first underscore)
###############################################################################
def tail_key(u_key):
    parts = u_key.split("_", 1)
    return parts[1].lower() if len(parts) == 2 else u_key.lower()

###############################################################################
# Formatting Functions for Stats
###############################################################################
def format_nba_stat(col, value):
    try:
        value = float(value)
    except:
        return value
    if col in ["FG%", "3P%"]:
        return f"{value*100:.1f}%"
    else:
        return f"{value:.1f}"

def format_mlb_stat(col, value):
    try:
        value = float(value)
    except:
        return value
    if col in ["AVG", "OPS"]:
        formatted = f"{value:.3f}"
        return formatted[1:] if formatted.startswith("0") else formatted
    elif col in ["H", "2B", "HR", "AB", "IP", "ER", "SO", "BB", "WHIP"]:
        return f"{value:.2f}"
    else:
        return value

def format_nhl_stat(col, value):
    try:
        value = float(value)
    except:
        return value
    if col == "GP":
        return f"{int(value)}"
    elif col in ["G", "A", "PTS", "SOG"]:
        return f"{value:.2f}"
    elif col == "ATOI":
        minutes = int(value // 60)
        seconds = int(value % 60)
        return f"{minutes}:{seconds:02d}"
    else:
        return value

###############################################################################
# Custom CSS & Layout Enhancements
###############################################################################
custom_css = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;600&display=swap');

:root {{
    --bg-color: #1e1e1e;
    --text-color: #ffffff;
    --accent-color: #FDB827;
}}

html, body, [data-testid="stAppViewContainer"] {{
    height: 100%;
    width: 100%;
    margin: 0;
    padding: 0;
    font-family: 'Oswald', sans-serif;
    background-color: var(--bg-color);
    color: var(--text-color);
}}

.stApp {{
    display: flex;
    flex-direction: column;
    min-height: 100vh;
}}

main.block-container {{
    flex: 1 0 auto;
    width: 100%;
    padding: 2rem;
    min-height: calc(100vh - 150px);
}}

.navbar {{
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    z-index: 999999;
    background-color: #2a2a2a;
    padding: 1rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
    box-shadow: 0 2px 4px rgba(0,0,0,0.6);
}}

.navbar-left {{
    display: flex;
    align-items: center;
}}

.navbar-left img {{
    margin-right: 1rem;
    width: 100px;
}}

.navbar-left a {{
    position: relative;
    color: #FDB827;
    text-decoration: none;
    margin: 0 1rem;
    font-family: 'Oswald', sans-serif;
    font-weight: 600;
    font-size: 1.1rem;
    cursor: pointer;
    transition: color 0.3s ease;
}}

.navbar-left a::after {{
    content: "";
    position: absolute;
    left: 0;
    bottom: -3px;
    width: 0;
    height: 2px;
    background: #FDB827;
    transition: width 0.3s ease;
}}

.navbar-left a:hover::after {{
    width: 100%;
}}

.hero {{
    text-align: center;
    padding: 3rem 2rem;
    background: linear-gradient(135deg, #555555, #333333);
    background-attachment: fixed;
    margin-bottom: 2rem;
    border: 1px solid #444444;
    border-radius: 4px;
    box-shadow: 0 4px 8px rgba(0,0,0,0.5);
    color: #ffffff;
}}

.hero h1 {{
    color: #000000;
    font-family: 'Oswald', sans-serif;
    font-size: 3rem;
    margin: 0;
}}

.hero p {{
    color: #ffffff;
    font-family: 'Oswald', sans-serif;
    font-size: 1.5rem;
}}

.custom-header {{
    text-align: center;
    color: #FDB827;
    font-family: 'Oswald', sans-serif;
    font-size: 2.5rem;
    margin: 20px 0;
    font-weight: 600;
}}

/* Custom table styling for improved visuals */
.custom-table-wrapper table {{
    margin: auto;
    border-collapse: collapse;
    width: 90%;
    font-size: 0.9rem;
}}
.custom-table-wrapper th, .custom-table-wrapper td {{
    padding: 8px;
    border: 1px solid #444444;
    text-align: center;
}}
.custom-table-wrapper th {{
    background-color: #2a2a2a;
    color: #FDB827;
    text-align: center;
}}
.custom-table-wrapper tr:nth-child(even) {{
    background-color: #333333;
}}
.custom-table-wrapper tr:nth-child(odd) {{
    background-color: #2a2a2a;
}}

table, th, td {{
    text-align: center;
    border: 1px solid #444444;
}}

.footer {{
    margin-top: auto;
    width: 100%;
    text-align: center;
    padding: 1rem;
    background-color: #272727;
    color: #FDB827;
    border-top: 2px solid #FDB827;
    font-family: 'Oswald', sans-serif;
    font-weight: 600;
    font-size: 1.1rem;
}}

.button-primary {{
    background-color: #FDB827;
    color: #2a2a2a;
    padding: 0.5rem 1rem;
    border: none;
    border-radius: 4px;
    font-weight: 600;
    cursor: pointer;
    transition: background-color 0.3s ease;
}}

.button-primary:hover {{
    background-color: #ffcb00;
}}

/* Fade-in animation */
.fade-in {{
    animation: fadeInAnimation 1.5s ease-in both;
}}

@keyframes fadeInAnimation {{
    from {{ opacity: 0; transform: translateY(20px); }}
    to {{ opacity: 1; transform: translateY(0); }}
}}

/* Enhanced hover effects for images and cards */
.image-hover:hover {{
    transform: scale(1.05);
    transition: transform 0.3s ease;
}}

.card {{
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    transition: box-shadow 0.3s ease;
}}

.card:hover {{
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
}}

/* Responsive adjustments */
@media only screen and (max-width: 600px) {{
    .navbar-left a {{
        font-size: 0.9rem;
        margin: 0 0.5rem;
    }}
    .hero h1 {{
        font-size: 2rem;
    }}
    .hero p {{
        font-size: 1.2rem;
    }}
    /* Stack columns vertically */
    .streamlit-expanderHeader {{
        flex-direction: column;
        align-items: flex-start;
    }}
}}

</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

###############################################################################
# Navigation Bar (with target="_self")
###############################################################################
navbar_html = f"""
<div class="navbar">
  <div class="navbar-left">
    <img src="data:image/png;base64,{get_base64_image('assets/bvlogo2.png')}" alt="BetVersa Logo">
    <a href="?page=overview" target="_self" title="View top EV plays">Home</a>
    <a href="?page=ev" target="_self" title="Filter and inspect EV data">EV Data</a>
    <a href="?page=parlay" target="_self" title="Build your custom parlay">Parlay</a>
    <a href="?page=about" target="_self" title="Learn more about BetVersa">About</a>
  </div>
</div>
"""
st.markdown(navbar_html, unsafe_allow_html=True)

###############################################################################
# Page Navigation
###############################################################################
if "page" not in st.session_state:
    st.session_state.page = "overview"
parse_query_params()

###############################################################################
# Page Definitions
###############################################################################
def show_overview():
    st.markdown("""
        <div class="hero fade-in">
            <h1>Discover Positive EV Plays</h1>
            <p>Your source for valuable betting insights.</p>
        </div>
    """, unsafe_allow_html=True)
    st.markdown("""
        <p style="text-align: center; font-size: 1.1rem;">
            Welcome to BetVersa! On this page, you'll find a curated list of top plays with positive expected value (EV).
            Use the table below to browse through the latest betting insights.
        </p>
    """, unsafe_allow_html=True)
    st.markdown("<div class='custom-header'>BetVersa Prop Shop</div>", unsafe_allow_html=True)
    
    search_query = st.text_input("Search bets (by team, player, or market):", "")
    
    def is_positive(percent_str):
        try:
            return float(percent_str.replace("%", "")) > 0
        except:
            return False

    filtered = df_unique[(df_full["EV"].apply(is_positive)) & (df_full["Avg EV"].apply(is_positive))]
    filtered = pd.merge(filtered, df_processed[["unique_key", "Combo NV Odds", "Combo EV"]], on="unique_key", how="left")
    filtered = filtered[filtered["Combo EV"].apply(lambda x: float(x.replace("%", "")) > 0)]

    allowed_markets = ("player", "batter", "pitcher")
    filtered = filtered[filtered["Market"].str.lower().str.startswith(allowed_markets)]
  
    if search_query:
        filtered = filtered[
            filtered["Game"].str.contains(search_query, case=False, na=False) |
            filtered["Player"].str.contains(search_query, case=False, na=False) |
            filtered["Market"].str.contains(search_query, case=False, na=False)
        ]
    
    nba_stats_df = load_nba_stats()
    def stat_condition(row):
        sport = row["Sport"].lower()
        market = row["Market"].lower()
        outcome = row["Outcome"].lower()
        try:
            line_value = float(row["Line"])
        except Exception:
            return False
        if sport == "nba" and market.startswith("player"):
            stats_row = nba_stats_df[nba_stats_df["Player"] == row["Player"]]
            if stats_row.empty:
                return False
            stats_row = stats_row.iloc[0]
            if "points rebounds assists" in market or ("points" in market and "rebounds" in market and "assists" in market):
                stat_value = stats_row["PTS"] + stats_row["TRB"] + stats_row["AST"]
            elif "points" in market:
                stat_value = stats_row["PTS"]
            elif "rebounds" in market:
                stat_value = stats_row["TRB"]
            elif "assists" in market:
                stat_value = stats_row["AST"]
            elif "threes" in market:
                stat_value = stats_row.get("3P", None)
            else:
                stat_value = None
            if stat_value is None:
                return False
            return (stat_value > line_value) if outcome.startswith("over") else (stat_value < line_value)
        elif sport == "mlb":
            if market.startswith("batter"):
                mlb_batter_df = load_mlb_batter_stats_2025()
                stats_row = mlb_batter_df[mlb_batter_df["Player"] == row["Player"]]
                if stats_row.empty:
                    return False
                stat_value = stats_row.iloc[0]["H"]
                return (stat_value > line_value) if outcome.startswith("over") else (stat_value < line_value)
            elif market.startswith("pitcher"):
                mlb_pitcher_df = load_mlb_pitcher_stats_2025()
                stats_row = mlb_pitcher_df[mlb_pitcher_df["Player"] == row["Player"]]
                if stats_row.empty:
                    return False
                stat_value = stats_row.iloc[0]["SO"]
                return (stat_value > line_value) if outcome.startswith("over") else (stat_value < line_value)
            else:
                return True
        elif sport == "nhl" and market.startswith("player"):
            nhl_df = load_nhl_skater_stats_2025()
            stats_row = nhl_df[nhl_df["Player"] == row["Player"]]
            if stats_row.empty:
                return False
            stat_value = stats_row.iloc[0]["SOG"]
            return (stat_value > line_value) if outcome.startswith("over") else (stat_value < line_value)
        else:
            return True

    filtered = filtered[filtered.apply(stat_condition, axis=1)]
    
    st.dataframe(
        filtered[["Sport", "Game", "Player", "Market", "Book", "Outcome", "Line", "Odds", "Combo NV Odds", "Combo EV", "Kelly $"]]
        .reset_index(drop=True),
        height=400
    )

def show_ev_page():
    st.markdown("<div class='custom-header'>EV Data</div>", unsafe_allow_html=True)
    st.markdown("""
        <p style="text-align: center; font-size: 1.1rem;">
            Explore detailed betting metrics and historical trends.
            Use the filter below to select a sport, and click on a row for a deeper dive into odds breakdowns and player stats.
        </p>
    """, unsafe_allow_html=True)
    sports_raw = sorted(df_full["Sport"].unique())
    if "selected_sport" not in st.session_state:
        st.session_state.selected_sport = "All"
    selected_sport = st.radio("Select Sport:", options=["All"] + sports_raw, index=0, horizontal=True)
    st.session_state.selected_sport = selected_sport

    merged_ev = pd.merge(df_full, df_processed[["unique_key", "Combo NV Odds", "Combo EV"]], on="unique_key", how="left")
    if st.session_state.selected_sport != "All":
        merged_ev = merged_ev[merged_ev["Sport"] == st.session_state.selected_sport]
    
    ev_display_cols = ["Sport", "Game", "Player", "Market", "Book", "Outcome", "Line", "Odds", "Pin NV Odds", "EV", "Kelly $", "odds_breakdown", "unique_key"]
    ev_display = merged_ev[ev_display_cols].reset_index(drop=True)
    ev_display.index = [''] * len(ev_display)

    gb = GridOptionsBuilder.from_dataframe(ev_display)
    gb.configure_default_column(resizable=True, autoSize=True)
    gb.configure_selection("single", use_checkbox=False)
    gb.configure_column("odds_breakdown", hide=True)
    grid_options = gb.build()
    grid_response = AgGrid(
        ev_display,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme="blue",
        fit_columns_on_grid_load=True
    )
    selected = grid_response.get("selected_rows")
    selected_row = None
    if selected is not None:
        if isinstance(selected, list) and len(selected) > 0:
            selected_row = selected[0]
        elif isinstance(selected, pd.DataFrame) and not selected.empty:
            selected_row = selected.iloc[0].to_dict()

    if selected_row is not None and "odds_breakdown" in selected_row and selected_row["odds_breakdown"]:
        tabs = st.tabs(["Interactive Chart", "Details"])
        with tabs[0]:
            debug_chart = st.checkbox("Show chart debug info", value=False)
            # INTERACTIVE GRAPH SECTION (UPDATED)
            pos_ev_unique = selected_row.get("unique_key", "")
            bet_tail = tail_key(pos_ev_unique)
            if debug_chart:
                st.write("Debug: Bet unique_key tail:", bet_tail)
            trends_data = []
            # Create a set of EV keys (and tails) from your EV bets data
            ev_keys = set(df_full["unique_key"].tolist())
            ev_key_tails = {key.split("_", 1)[-1].lower() for key in ev_keys}
            history_records = load_history_odds_for_ev(ev_key_tails)# Dict with keys as unique ids.
            matching_count = 0
            for hist_key, rec_list in history_records.items():
                hist_tail = tail_key(hist_key)
                if bet_tail == hist_tail:
                    matching_count += 1
                    if debug_chart:
                        st.write("Debug: Found matching history key:", hist_key)
                    for rec in rec_list:
                        ts_str = rec.get("timestamp", "")
                        # Remove trailing "Z" if present with "+00:00"
                        if ts_str.endswith("Z") and "+00:00" in ts_str:
                            ts_str = ts_str.replace("+00:00Z", "+00:00")
                        try:
                            ts = pd.to_datetime(ts_str)
                            if ts.tzinfo is None:
                                ts = ts.tz_localize("UTC")
                            ts = ts.tz_convert("US/Central")
                        except Exception as e:
                            if debug_chart:
                                st.write("Debug: Timestamp conversion error:", e)
                            ts = None
                        for entry in rec.get("bookmakers", []):
                            bk = entry.get("bookmaker", {}).get("title", "")
                            offered = entry.get("offered_odds")
                            trends_data.append({"Time": ts, "Odds": offered, "Book": bk})
            if debug_chart:
                st.write("Debug: Matching records count:", matching_count)
                if trends_data:
                    st.write("Debug: Matched snapshots:")
                    st.dataframe(pd.DataFrame(trends_data).sort_values("Time"))
            if trends_data:
                trends_df = pd.DataFrame(trends_data)
                if not pd.api.types.is_datetime64_any_dtype(trends_df["Time"]):
                    trends_df["Time"] = pd.to_datetime(trends_df["Time"], errors='coerce')
                fig = px.line(trends_df, x="Time", y="Odds", color="Book", markers=True,
                              title="Interactive Betting Trends by Sportsbook (US Central Time)")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.write("No interactive trends available.")
        with tabs[1]:
            # ODDS BREAKDOWN SECTION (UNCHANGED)
            col_left, col_right = st.columns([1, 1.5])
            with col_left:
                odds_df = pd.DataFrame(selected_row["odds_breakdown"])
                if "player" in odds_df.columns and "offered_point" in odds_df.columns and "Line" in selected_row:
                    odds_df = odds_df[
                        (odds_df["player"] == selected_row["Player"]) & 
                        (odds_df["offered_point"].astype(str) == selected_row["Line"])
                    ]
                odds_df = odds_df.drop_duplicates(subset=["bookmaker"])
                odds_df = odds_df.rename(columns={
                    "bookmaker": "Book",
                    "offered_odds_american": "Odds",
                    "offered_point": "Line"
                })
                for col in ["fair_odds_american", "player"]:
                    if col in odds_df.columns:
                        odds_df = odds_df.drop(columns=[col])
                if "Line" in odds_df.columns:
                    odds_df["Line"] = odds_df["Line"].apply(lambda x: f"{float(x):.1f}" if pd.notnull(x) else x)
                logo_dict = {
                    "DraftKings": get_base64_image("assets/draftkings.png"),
                    "BetMGM": get_base64_image("assets/betmgm.png"),
                    "Hard Rock Bet": get_base64_image("assets/hardrockbet.png"),
                    "ESPN BET": get_base64_image("assets/espnbet.png"),
                    "Caesars": get_base64_image("assets/williamhill_us.png"),
                    "BetRivers": get_base64_image("assets/betrivers.png"),
                    "Bet Online": get_base64_image("assets/betonlineag.png"),
                    "Low Vig": get_base64_image("assets/lowvig.png"),
                    "FanDuel": get_base64_image("assets/fanduel.png")
                }
                odds_df["Logo"] = odds_df["Book"].map(lambda book: f'<img src="data:image/png;base64,{logo_dict.get(book, "")}" width="30" style="border-radius:4px; border:1px solid #ddd;">' if logo_dict.get(book, "") else "")
                display_cols = ["Book", "Odds"]
                if "Line" in odds_df.columns:
                    display_cols.append("Line")
                odds_html = odds_df[["Logo"] + display_cols].to_html(escape=False, index=False)
                st.markdown("<div class='custom-table-wrapper'>" + odds_html + "</div>", unsafe_allow_html=True)
            with col_right:
                # PLAYER STATS SECTION (UNCHANGED)
                sport = selected_row["Sport"].lower()
                market = selected_row["Market"].lower()
                selected_player = selected_row["Player"]
                stats_last = pd.DataFrame()
                stats_this = pd.DataFrame()
                if sport == "nba" and market.startswith("player"):
                    name_mapping = {"Bub Carrington": "Carlton Carrington"}
                    canonical_player = name_mapping.get(selected_player, selected_player)
                    nba_last = load_nba_stats_2024()
                    nba_this = load_nba_stats()
                    stats_last = nba_last[nba_last["Player"] == canonical_player].copy()
                    stats_this = nba_this[nba_this["Player"] == canonical_player].copy()
                    for df in [stats_last, stats_this]:
                        if not df.empty:
                            for col in df.columns:
                                if col == "Player":
                                    continue
                                df[col] = format_nba_stat(col, df[col].iloc[0])
                elif sport == "mlb":
                    if market.startswith("batter"):
                        mlb_last = load_mlb_batter_stats_2024()
                        mlb_this = load_mlb_batter_stats_2025()
                        stats_last = mlb_last[mlb_last["Player"] == selected_player].copy() if not mlb_last.empty else pd.DataFrame()
                        stats_this = mlb_this[mlb_this["Player"] == selected_player].copy() if not mlb_this.empty else pd.DataFrame()
                        for df in [stats_last, stats_this]:
                            if not df.empty:
                                for col in df.columns:
                                    if col == "Player":
                                        continue
                                    df[col] = format_mlb_stat(col, df[col].iloc[0])
                    elif market.startswith("pitcher"):
                        mlb_last = load_mlb_pitcher_stats_2024()
                        mlb_this = load_mlb_pitcher_stats_2025()
                        stats_last = mlb_last[mlb_last["Player"] == selected_player].copy() if not mlb_last.empty else pd.DataFrame()
                        stats_this = mlb_this[mlb_this["Player"] == selected_player].copy() if not mlb_this.empty else pd.DataFrame()
                        for df in [stats_last, stats_this]:
                            if not df.empty:
                                for col in df.columns:
                                    if col == "Player":
                                        continue
                                    df[col] = format_mlb_stat(col, df[col].iloc[0])
                elif sport == "nhl" and market.startswith("player"):
                    nhl_last = load_nhl_skater_stats_2024()
                    nhl_this = load_nhl_skater_stats_2025()
                    stats_last = nhl_last[nhl_last["Player"] == selected_player].copy() if not nhl_last.empty else pd.DataFrame()
                    stats_this = nhl_this[nhl_this["Player"] == selected_player].copy() if not nhl_this.empty else pd.DataFrame()
                    for df in [stats_last, stats_this]:
                        if not df.empty:
                            for col in df.columns:
                                if col == "Player":
                                    continue
                                df[col] = format_nhl_stat(col, df[col].iloc[0])
                if not stats_last.empty:
                    stats_last["Season"] = "Last Season"
                if not stats_this.empty:
                    stats_this["Season"] = "This Season"
                combined_stats = pd.concat([stats_last, stats_this], ignore_index=True)
                if "Player" in combined_stats.columns:
                    combined_stats.drop(columns=["Player"], inplace=True)
                if not combined_stats.empty:
                    cols = ["Season"] + [c for c in combined_stats.columns if c != "Season"]
                    combined_stats = combined_stats[cols]
                    stats_html = combined_stats.to_html(index=False)
                    st.markdown("<div class='custom-table-wrapper'>" + stats_html + "</div>", unsafe_allow_html=True)
                else:
                    st.write("No stats available for this player.")
    else:
        st.write("No odds breakdown available for this play.")

def show_parlay():
    st.markdown("<div class='custom-header'>Parlay Builder</div>", unsafe_allow_html=True)
    st.markdown("""
        <p style="text-align: center; font-size: 1.1rem;">
            Use this tool to combine multiple EV bets into one parlay.
            Select your desired number of legs, filter by sport and sportsbook,
            and let the tool calculate the combined odds.
        </p>
    """, unsafe_allow_html=True)
    legs = st.number_input("Number of Legs:", min_value=2, max_value=10, value=2, step=1)
    available_sports = sorted(df_full["Sport"].unique())
    selected_sports = st.multiselect("Select Sports (leave empty for all):", options=available_sports)
    available_books = sorted(df_full["Book"].unique())
    selected_books = st.multiselect("Select Sportsbooks (leave empty for all):", options=available_books)
    build_clicked = st.button("Build Parlay", key="build_parlay", help="Click to generate a parlay with the selected criteria.")
    if build_clicked:
        parlay_df = df_full.copy()
        if selected_sports:
            parlay_df = parlay_df[parlay_df["Sport"].isin(selected_sports)]
        if selected_books:
            parlay_df = parlay_df[parlay_df["Book"].isin(selected_books)]
        if len(parlay_df) < legs:
            st.error("Not enough bets available to build a parlay with the selected criteria.")
        else:
            parlay_df["EV_float"] = parlay_df["EV"].apply(lambda x: float(x.replace("%", "")) / 100.0 if pd.notnull(x) else 0)
            parlay_df = parlay_df.sort_values("EV_float", ascending=False)
            selected_bets = parlay_df.head(legs)
            processed_bets = add_combo_columns(selected_bets)
            try:
                final_combo_decimal = reduce(mul, [american_to_decimal(x) for x in processed_bets["Combo NV Odds"] if x is not None], 1)
                final_combo_american = decimal_to_american(final_combo_decimal)
            except Exception:
                final_combo_american = None
            try:
                no_vig_decimals = [american_to_decimal(x) for x in selected_bets["Avg NV Odds"] if x is not None]
                parlay_decimal_no_vig = reduce(mul, no_vig_decimals, 1)
                parlay_american_no_vig = decimal_to_american(parlay_decimal_no_vig)
            except Exception:
                parlay_american_no_vig = None

            bet_amount = 250

            display_columns = ["Sport", "Game", "Player", "Market", "Book", "Outcome", "Line", "Odds", "Combo NV Odds", "Combo EV", "Kelly $"]
            temp = processed_bets[display_columns].reset_index(drop=True)
            temp.index = [''] * len(temp)
            st.dataframe(temp, height=400)

            st.markdown("#### Parlay Summary", unsafe_allow_html=True)
            st.write(f"**Combined American Odds:** {final_combo_american if final_combo_american is not None else 'N/A'}")
            st.write(f"**No-Vig American Odds:** {parlay_american_no_vig if parlay_american_no_vig is not None else 'N/A'}")
            st.write(f"**Bet Amount (Quarter Kelly):** ${bet_amount}")
            
def show_about():
    st.markdown("<div class='custom-header'>About BetVersa</div>", unsafe_allow_html=True)
    st.markdown("""
        <p style="text-align: center; font-size: 1.1rem;">
            BetVersa.app provides in-depth analysis of positive EV bets across multiple sports.
            Our mission is to deliver actionable insights backed by data and advanced statistical analysis.
            Please remember to gamble responsibly.
        </p>
    """, unsafe_allow_html=True)
    st.markdown("""
        <div style="text-align: center; margin: 1rem 0;">
            <a href="#" target="_self" title="Share on X">
                <img src="https://upload.wikimedia.org/wikipedia/commons/c/ce/X_logo_2023.svg" alt="X" width="30">
            </a>
            <a href="#" target="_self" title="Share on Facebook">
                <img src="https://upload.wikimedia.org/wikipedia/commons/5/51/Facebook_f_logo_%282019%29.svg" alt="Facebook" width="30">
            </a>
            <a href="#" target="_self" title="Share on Pikki">
                <img src="https://framerusercontent.com/images/2kbJ9ZRenf530fkFtV9h3qcdhLM.png" alt="Pikkit" width="50">
            </a>
        </div>
    """, unsafe_allow_html=True)
    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown("<p style='font-size:0.9rem; text-align:center;'>For inquiries or feedback, contact us at <a href='mailto:support@betversa.app' style='color:#FDB827;'>support@betversa.app</a></p>", unsafe_allow_html=True)

###############################################################################
# Render Page Based on Query Parameter
###############################################################################
if st.session_state.page == "overview":
    show_overview()
elif st.session_state.page == "ev":
    show_ev_page()
elif st.session_state.page == "parlay":
    show_parlay()
elif st.session_state.page == "about":
    show_about()

# Spacer for Footer
st.markdown("<div style='height: 150px'></div>", unsafe_allow_html=True)

# Footer Section with additional links (target="_self")
footer_html = """
<div class="footer">
    <p>&copy; 2025 BetVersa. All rights reserved.</p>
    <p style="font-size:0.8rem; margin-top: 0.5rem;">
        Disclaimer: Betting carries risk. Please gamble responsibly and within your limits.
    </p>
    <p style="font-size:0.8rem; margin-top: 0.5rem;">
        <a href="https://betversa.app/terms" target="_self" style="color:#FDB827; text-decoration:none;">Terms of Service</a> | 
        <a href="https://betversa.app/privacy" target="_self" style="color:#FDB827; text-decoration:none;">Privacy Policy</a>
    </p>
</div>
"""
st.markdown(footer_html, unsafe_allow_html=True)
