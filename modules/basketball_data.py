import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import re
import pandas as pd
from fuzzywuzzy import fuzz
from io import StringIO
import unicodedata

pd.options.mode.chained_assignment = None 

def normalize_name(name):
    """Normalize player names by removing accents and extra spaces."""
    normalized = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')
    return normalized.strip()

class awards:
    @staticmethod
    def mvp(season):
        season = str(season)
        url = 'https://www.basketball-reference.com/awards/mvp.html'
        html = urllib.request.urlopen(url)
        soup = BeautifulSoup(html, 'html.parser')
        table_html = str(soup.findAll('table', id='mvp_NBA')[0])
        df = pd.read_html(StringIO(table_html))[0]
        df.columns = df.columns.droplevel(0)
        df = df[['Season', 'Player']]
        df['Season'] = df['Season'].str[:2] + df['Season'].str[5:7]
        mvp_lists = df.values.tolist()
        for data in mvp_lists:
            if season in data[0]:
                return data[1]

    @staticmethod
    def allstar(season):
        if season == 1999:
            print('No All Star game in 1999 season')
        else:
            url = f'https://www.basketball-reference.com/allstar/NBA_{season}.html'
            html = urllib.request.urlopen(url)
            soup = BeautifulSoup(html, 'html.parser')
            name_html = soup.findAll('a', string=re.compile('[a-z]'),
                                       href=re.compile('^/players/.+'),
                                       title=False)
            names = [tag.text for tag in name_html]
            names = list(set(names))
            return names

class get_data:
    @staticmethod
    def team_records(season):
        url = f'https://www.basketball-reference.com/leagues/NBA_{season}.html'
        html = urllib.request.urlopen(url)
        soup = BeautifulSoup(html, 'html.parser')
        table_html_links = soup.findAll('table', id=re.compile('advanced-team'))[0].findAll('a')
        team_name = []
        team_abrv = []
        for tag in table_html_links:
            abrv = tag.get('href')
            pattern = re.compile(r'([A-Z]{3})')
            match = pattern.search(abrv)
            if match:
                team_abrv.append(match.group())
                team_name.append(tag.text)
        team_abrv = list(zip(team_name, team_abrv))
        
        table = soup.findAll('table', id=re.compile('advanced-team'))
        team_records = pd.read_html(StringIO(str(table)))[0]
        team_records = team_records.apply(pd.to_numeric, errors='coerce').fillna(team_records)
        team_records.columns = team_records.columns.droplevel(0)
        team_records = team_records.drop('Rk', axis=1)
        team_records = team_records.loc[:, ['Team', 'W', 'L']]
        team_records['Team'] = team_records['Team'].str.replace('*','', regex=False)
        team_names = [i[0] for i in team_abrv]
        team_abbrevation = [i[1] for i in team_abrv]
        for i in team_records.index:
            team = team_records.loc[i, 'Team']
            if team in team_names:
                idx = team_names.index(team)
                team_records.loc[i, 'Team'] = team_abbrevation[idx]
        return team_records

    @staticmethod
    def single(season, stats, additional_data=False, salary=False):
        print('Loading', season, 'data...')
        url = f'https://www.basketball-reference.com/leagues/NBA_{season}_{stats}.html'
        html = urllib.request.urlopen(url)
        soup = BeautifulSoup(html, 'html.parser')
        table_html = soup.findAll('table')
        df = pd.read_html(StringIO(str(table_html)))[0]
        
        # Remove duplicate header rows and drop the ranking column.
        df = df.drop(df[df.Player == 'Player'].index)
        df = df.drop('Rk', axis=1)
        
        # Clean up and normalize player names.
        df['Player'] = df['Player'].str.replace('*', '', regex=False)
        df['Player'] = df['Player'].apply(normalize_name)
        # Replace "Bub Carrington" with "Carlton Carrington" and, for any name ending with "Jr.",
        # remove the trailing period.
        df['Player'] = df['Player'].apply(
            lambda x: "Carlton Carrington" if x.strip() == "Bub Carrington"
            else ("R.J. Barrett" if x.strip() == "RJ Barrett"
            else (x[:-1] if x.strip().endswith("Jr.") else x))
        )
        
        # Create a helper key column for duplicate resolution.
        df['Player_key'] = df['Player']
        
        df.insert(0, 'Season', season)
        df = df.apply(pd.to_numeric, errors='coerce').fillna(df)
        
        if additional_data:
            df['All_Star'] = 0
            if season != 1999:
                all_star = awards.allstar(season)
                df.loc[df['Player'].isin(all_star), 'All_Star'] = 1

            df['MVP'] = 0
            mvp = awards.mvp(season)
            df.loc[df['Player'] == mvp, 'MVP'] = 1

            team_records_df = get_data.team_records(season)
            team_records_list = team_records_df.values.tolist()
            df['team_win'] = 0
            df['team_lose'] = 0
            teams = [i[0] for i in team_records_list]
            win = [i[1] for i in team_records_list]
            lose = [i[2] for i in team_records_list]

            if 'Tm' in df.columns:
                team_col = 'Tm'
            elif 'Team' in df.columns:
                team_col = 'Team'
            else:
                raise KeyError("No team column found in the stats DataFrame.")

            for i in df.index:
                team = df.loc[i, team_col]
                if team in teams:
                    idx = teams.index(team)
                    df.loc[i, 'team_win'] = win[idx]
                    df.loc[i, 'team_lose'] = lose[idx]
                else:
                    df.loc[i, 'team_win'] = 0
                    df.loc[i, 'team_lose'] = 0

        if salary:
            def get_salary(season):
                season_str = str(int(season) - 1) + '-' + str(int(season))
                url = f'https://hoopshype.com/salaries/players/{season_str}/'
                html = urllib.request.urlopen(url)
                soup = BeautifulSoup(html, 'html.parser')
                salary_df = pd.read_html(StringIO(str(soup)))[0]
                salary_df = salary_df.iloc[:, [1, 3]]
                salary_df = salary_df.rename(columns={salary_df.columns[1]: 'Salary'})
                return salary_df

            stats_df = df.copy()
            salary_df = get_salary(season)
            salary_names = salary_df['Player'].tolist()
            for salary_name in salary_names:
                stats_df['Player'] = stats_df['Player'].apply(
                    lambda stats_name: salary_name if fuzz.ratio(stats_name, salary_name) >= 80 else stats_name
                )
            df = pd.merge(stats_df, salary_df, how='left', on='Player')
        
        # Duplicate resolution: group by the helper key "Player_key"
        if 'Tm' in df.columns:
            team_col = 'Tm'
        elif 'Team' in df.columns:
            team_col = 'Team'
        else:
            team_col = None

        if team_col:
            def select_row(group):
                # Look for rows where the team column matches the pattern "^\d+TM$"
                matches = group[group[team_col].astype(str).str.match(r'^\d+TM$')]
                if not matches.empty:
                    return matches.iloc[0]
                else:
                    return group.iloc[0]
            df = df.groupby('Player_key', as_index=False).apply(select_row).reset_index(drop=True)
            df = df.rename(columns={'Player_key': 'Player'})
        else:
            df = df.drop(columns=['Player_key'])
        
        # Ensure columns are unique.
        if not df.columns.is_unique:
            df = df.loc[:, ~df.columns.duplicated()]
        
        # Keep only the desired columns.
        cols_to_keep = ['Player', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%', 'TRB', 'AST', 'PTS']
        df = df[cols_to_keep]
        return df

    @staticmethod
    def multiple(start_year, end_year, stats, additional_data=False, salary=False):
        if additional_data:
            df = get_data.single(start_year, stats, additional_data=True)
        elif salary:
            df = get_data.single(start_year, stats, salary=True)
        else:
            df = get_data.single(start_year, stats)
        
        while start_year < end_year:
            start_year += 1
            if additional_data:
                df_next = get_data.single(start_year, stats, additional_data=True)
            elif salary:
                df_next = get_data.single(start_year, stats, salary=True)
            else:
                df_next = get_data.single(start_year, stats)
            df = pd.concat([df, df_next])
        return df
