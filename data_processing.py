import pandas as pd
import requests
import re
from io import StringIO
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define the API URLs
AUTH_URL = "https://api.echomtg.com/api/user/auth/"
EXPORT_URL = "https://api.echomtg.com/api/stores/export/"
final_output_file = 'readyformox.csv'

def get_auth_token():
    """
    Authenticate with EchoMTG API and return the access token
    """
    email = os.getenv('ECHO_EMAIL')
    password = os.getenv('ECHO_PASSWORD')

    if not email or not password:
        raise Exception("Email or password not found. Please set ECHO_EMAIL and ECHO_PASSWORD in your .env file.")

    # Prepare the authentication request
    auth_data = {
        "email": email,
        "password": password,
        "type": "curl"  # To receive plain text token
    }

    response = requests.post(AUTH_URL, json=auth_data)

    if response.status_code == 200:
        # The response should be a plain text token
        return response.text.strip()
    else:
        raise Exception(f"Authentication failed: {response.status_code} - {response.text}")

def fetch_and_process_data():
    # First, get the authentication token
    try:
        auth_token = get_auth_token()
    except Exception as e:
        raise Exception(f"Failed to authenticate: {str(e)}")

    # Make the API request with the new token
    headers = {
        "Authorization": f"Bearer {auth_token}"
    }
    
    response = requests.get(EXPORT_URL, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Read the CSV data directly into a DataFrame using StringIO
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)

        # Process the DataFrame in memory
        # Drop the specific columns
        columns_to_drop = [
            'Set', 'Rarity', 'Date Acquired', 'Marked as Trade', 'note', 'tcg_market', 
            'tcg_mid', 'tcg_low', 'foil_price', 'echo_inventory_id', 'tcgid', 'echoid'
        ]
        df = df.drop(columns=columns_to_drop, errors='ignore')

        # Rename the specific columns
        columns_to_rename = {
            'Acquired': 'Purchased Price',
            'Reg Qty': 'Count',
            'Foil Qty': 'Foil',
            'Name': 'Name',
            'Set Code': 'Edition',
            'Condition': 'Condition',
            'Language': 'Language',
            'Collector Number': 'Collector Number'
        }
        df = df.rename(columns=columns_to_rename)

        # Modify the 'Foil' column
        df['Foil'] = df['Foil'].replace({1: 'foil', 0: ''})

        # Modify the 'Count' column
        df['Count'] = df['Count'].replace(0, 1)

        # Data processing functions
        def remove_parentheses(text):
            return re.sub(r'\s*\(.*?\)\s*', '', text).strip()

        def remove_jp_full_art(name):
            return name.replace('- JP Full Art', '').strip()

        def rename_cards(name):
            card_renames = {
                "Enlightened Tutor - 2000 Nicolas Labarre": "Enlightened Tutor",
                "Scroll Rack - 1998 Brian Selden": "Scroll Rack"
            }
            return card_renames.get(name, name)

        def update_edition(original_name, edition):
            # Apply specific card edition changes
            # Note: Global PLIST->PLST conversion is handled separately
            edition_changes = {
                "Acidic Soil": "PLST",
                "Aethersphere Harvester": "PAER",
                "Arena": "PHPR",
                "Clash of Wills": "ORI",
                "Convention Maro": "SLP",
                "Enlightened Tutor": "WC00",
                "Everflowing Chalice": "PLST",
                "Fabled Passage": "PW21",
                "Field of the Dead": "PM20",
                "Gilt-Leaf Palace": "PLST",
                "Mishra's Factory": "PLST",
                "Nadir Kraken": "PTHB",
                "Oubliette": "PLST",
                "Ratchet Bomb": "PM14",
                "Rip Apart": "STX",
                "Salivating Gremlins": "PLST",
                "Sanctum Gargoyle": "PLST",
                "Scourge of the Throne": "PLST",
                "Scroll Rack": "WC98",
                "Shrapnel Blast": "F08",
                "Slaying Fire": "ELD",
                "Solemnity": "PHOU",
                "Temur Battle Rage": "PLST",
                "Thrill of Possibility": "PLST",
                "Xiahou Dun, the One-Eyed": "J12",
                "Voja, Jaws of the Conclave": "MKM"
            }
            return edition_changes.get(original_name, edition)

        changes = 0
        changed_rows = []

        # Process the DataFrame
        for index, row in df.iterrows():
            original_name = row['Name']
            original_edition = row['Edition']
            
            # Clean the name
            cleaned_name = remove_jp_full_art(remove_parentheses(original_name))

            # Rename specific cards
            new_name = rename_cards(cleaned_name)
            
            # First handle global PLIST to PLST conversion
            new_edition = original_edition
            if original_edition == "PLIST":
                new_edition = "PLST"

            # Then apply any specific card edition changes
            new_edition = update_edition(new_name, new_edition)

            # Check if there were changes
            if original_name != new_name or original_edition != new_edition:
                changes += 1
                changed_rows.append(f'Original: {original_name} (Edition: {original_edition}) -> New: {new_name} (Edition: {new_edition})')

            # Update the DataFrame with the modified values
            df.at[index, 'Name'] = new_name
            df.at[index, 'Edition'] = new_edition

        # Save the modified DataFrame to a final CSV file
        df.to_csv(final_output_file, index=False)

        # Log summary of changes
        log_file = 'summary.txt'
        with open(log_file, 'w') as logfile:
            logfile.write(f'Total changes: {changes}\n\n')
            logfile.write('\n'.join(changed_rows))

        return final_output_file
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

if __name__ == "__main__":
    try:
        output_file = fetch_and_process_data()
        print(f"Successfully processed data and saved to {output_file}")
    except Exception as e:
        print(f"Error: {str(e)}")