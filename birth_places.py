#!/usr/bin/env python3
"""
Simple web scraper for Pro Football Reference birth places data.
Scrapes player data from https://www.pro-football-reference.com/friv/birthplaces.cgi
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import time

def scrape_birthplaces(
    country: str,
    state: str,
    offset: int = 0
    ) -> pd.DataFrame:

    
    # Construct the URL
    url = f"https://www.pro-football-reference.com/friv/birthplaces.cgi?country={country}&state={state}"
    if offset > 0:
        url += f"&offset={offset}"
    
    # Headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # Make the request
        print(f"Fetching data from: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse the HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the table - looking for the main stats table
        table = soup.find('table', {'id': 'birthplaces'})
        
        if not table:
            print("Could not find the birthplaces table")
            return pd.DataFrame()
        
        # Extract table headers
        thead = table.find('thead') # type: ignore
        if thead:
            headers_row = thead.find('tr') # type: ignore
            headers = []
            if headers_row:
                for th in headers_row.find_all(['th', 'td']): # type: ignore
                    # Use data-stat attribute if available, otherwise use text
                    header = th.get('data-stat', th.get_text(strip=True)) # type: ignore
                    headers.append(header)
            else:
                print("Could not find header row")
                return pd.DataFrame()
        else:
            print("Could not find table header")
            return pd.DataFrame()
        
        print(f"Found headers: {headers}")
        
        # Extract table rows
        tbody = table.find('tbody') # type: ignore
        rows_data = []
        
        if tbody:
            for row in tbody.find_all('tr'): # type: ignore
                # Skip header rows that might appear in tbody
                if row.find('th') and 'thead' in str(row.get('class', [])): # type: ignore
                    continue
                    
                row_data = []
                for cell in row.find_all(['th', 'td']): # type: ignore
                    # Extract text, handling links
                    if cell.find('a'): # type: ignore
                        text = cell.find('a').get_text(strip=True) # type: ignore
                    else:
                        text = cell.get_text(strip=True)
                    
                    # Convert empty strings to None for better data handling
                    if text == '':
                        text = None
                        
                    row_data.append(text)
                
                if row_data:  # Only add non-empty rows
                    rows_data.append(row_data)
        else:
            print("Could not find table body")
            return pd.DataFrame()
        
        # Create DataFrame
        df = pd.DataFrame(rows_data, columns=headers)
        
        # Clean up the DataFrame
        df = clean_dataframe(df)
        
        print(f"Successfully scraped {len(df)} rows of data")
        return df
        
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error parsing data: {e}")
        return pd.DataFrame()





def clean_dataframe(df):

    # Convert numeric columns
    numeric_columns = [
        'year_min', 'year_max', 'all_pros_first_team', 'pro_bowls',
        'years_as_primary_starter', 'career_av', 'g', 'pass_cmp', 'pass_att',
        'pass_yds', 'pass_td', 'pass_long', 'pass_int', 'pass_sacked',
        'pass_sacked_yds', 'rush_att', 'rush_yds', 'rush_td', 'rush_long',
        'rec', 'rec_yds', 'rec_td', 'rec_long',
        "# of Pros","# Active", "# of HOF", "G"
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def save_data(df, location):
    print(f"Saving data for {location} with {len(df)} rows")
    filename = f"./data/nfl_players_birthplaces_{location.lower()}.csv"
    
    df.to_csv(filename, index=False)
    print(f"Data saved to: {filename}")


def load_locations_df() -> pd.DataFrame:
    try:
        # TODO: Update Scraping to pull birthlocations from web.
        locations_df = pd.read_csv('/data/birthlocations.csv')
        locations_df = locations_df[['Country', 'State', '# of Pros', '# Active', '# of HOF', 'G']].dropna()
        locations_df = clean_dataframe(locations_df)
        locations_df = locations_df.sort_values(by='# of Pros', ascending=False)
        print(f"Loaded {len(locations_df)} locations from birthlocations.csv")
        return locations_df
    except FileNotFoundError:
        print("Error: birthlocations.csv not found.")
        return pd.DataFrame()


def load_location_df(row: pd.Series) -> pd.DataFrame:
    
    time.sleep(3)  # Respectful scraping delay
    
    _country = row.get('Country', 'Unknown')
    _state = row.get('State', '')
    _location = f"{_country}_{_state}"
    
    print(f"Scraping data for {_location}")
    
    _number_of_pros = row.get('# of Pros', 0)
    if _number_of_pros < 1:
        print(f"Skipping {_location} as it has no players")
        return pd.DataFrame()
    offset = 0
    out_df = pd.DataFrame()

    while _number_of_pros >= offset:
        if offset > 0:
            
            print(f"Scraping {_location} - {_number_of_pros} remaining")
        
            df = scrape_birthplaces(
                country=_country.replace(" ", "%20"), 
                state=_state.replace(" ", "%20"),
                offset=offset
                )
            if df.empty:
                print(f"No data found for {_location}")
                break

            out_df = pd.concat([out_df, df], ignore_index=True)

            # Save to CSV
            save_data(df, location=_location)

            # # Basic statistics
            # if 'player' in df.columns:
            #     print(f"\n{_location} players found: {len(df)}")
            
            # if 'pos' in df.columns:
            #     print("\nPositions breakdown:")
            #     print(df['pos'].value_counts())
            offset += 200

    return out_df

def load_birthplaces():
    locations_df = load_locations_df()
    consolidated  = pd.DataFrame()
    for _, row in locations_df.iterrows():
        # if _ >= 1:
        #     break
        location_df = load_location_df(row)
            
        consolidated = pd.concat([consolidated, location_df], ignore_index=True)

    # Save the consolidated DataFrame to a CSV file
    save_data(consolidated, location="consolidated")



if __name__ == "__main__":
    load_birthplaces()
