import pandas as pd
import requests
import re
from io import StringIO

# Define the API URL
api_url = "https://api.echomtg.com/api/stores/export/"
final_output_file = 'readyformox.csv'  # Final output file name

# Read the authentication token from the local file
def get_auth_token(file_path):
    with open(file_path, 'r') as file:
        return file.read().strip()  # Read the token and strip any extra whitespace

def fetch_and_process_data():
    auth_token_file = 'auth_token'  # Path to the authentication token file
    auth_token = get_auth_token(auth_token_file)  # Fetch the token

    # Make the API request
    response = requests.get(api_url, params={'auth': auth_token})

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
                # Add more renames as needed
            }
            return card_renames.get(name, name)

        def update_edition(original_name, edition):
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

        changes = 0  # Count the number of changes made
        changed_rows = []  # List to track changes

        # Process the DataFrame
        for index, row in df.iterrows():
            original_name = row['Name']
            original_edition = row['Edition']
            
            # Clean the name
            cleaned_name = remove_jp_full_art(remove_parentheses(original_name))

            # Rename specific cards
            new_name = rename_cards(cleaned_name)

            # Update the edition using the new name
            new_edition = update_edition(new_name, original_edition)

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
        log_file = 'summary.txt'  # Log file name
        with open(log_file, 'w') as logfile:
            logfile.write(f'Total changes: {changes}\n\n')
            logfile.write('\n'.join(changed_rows))

        return final_output_file  # Return the path of the saved file
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")
