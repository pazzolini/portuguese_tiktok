import requests
import json
import pandas as pd
from datetime import datetime
import os
import time

# Configuration
API_URL = "https://open.tiktokapis.com/v2/research/user/following/"
ACCESS_TOKEN = "" # Add the access token (valid for 2 hours)
CONFIG_FILE = os.path.join("config", "portuguese_political_personalities.json")

def load_config(file_path):
    """Loads the configuration from the JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        return config_data
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {file_path}.")
        return None


def ensure_directories():
    """Ensure the required directories exist"""
    # Get current date for raw data organization
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Use the specified directories
    raw_dir = os.path.join("data", "raw", "political_personalities_following", current_date)
    processed_dir = os.path.join("data", "processed", "political_personalities_following")

    # Create directories if they don't exist
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    return raw_dir, processed_dir


def get_personality_following(username, max_count=100, cursor=None):
    """
    Get the accounts that a personality is following
    """
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    # Build request body
    request_body = {
        "username": username,
        "max_count": max_count
    }

    # Add cursor if provided
    if cursor:
        request_body["cursor"] = cursor

    try:
        response = requests.post(API_URL, headers=headers, json=request_body)

        # Print status code for debugging
        print(f"Status code: {response.status_code}")

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error for {username}: {response.status_code}")
            print(f"Response: {response.text}")
            return None
    except Exception as e:
        print(f"Exception for {username}: {str(e)}")
        return None


def get_all_following(username, max_count=100):
    """
    Get all accounts that a personality is following by handling pagination
    """
    all_following = []
    cursor = None
    has_more = True

    while has_more:
        # Get batch of following
        response = get_personality_following(username, max_count, cursor)

        if not response:
            break

        # Extract following accounts
        following_data = response.get('data', {})
        following_accounts = following_data.get('user_following', [])

        # Add to our list
        all_following.extend(following_accounts)

        # Update cursor and has_more flag
        cursor = following_data.get('cursor')
        has_more = following_data.get('has_more', False)

        if has_more:
            print(f"Retrieved {len(following_accounts)} accounts. Fetching more...")
            time.sleep(1)  # 1-second delay between requests

    return all_following


def main():
    # Ensure directories exist
    raw_dir, processed_dir = ensure_directories()

    # Load config and extract personality mapping
    loaded_config = load_config(CONFIG_FILE)
    if loaded_config is None:
        print("Failed to load config. Exiting.")
        return
    if "political_personalities" not in loaded_config:
        print("Error: 'political_personalities' key not found in config file.")
        return
    # This dictionary contains username -> {"name": "...", "party": "..."}
    actual_personality_mapping = loaded_config["political_personalities"]

    # Current timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Store all following data
    all_personalities_following = {}

    # Store data for creating a DataFrame
    df_data = []

    # Loop through usernames from the loaded config
    for username in actual_personality_mapping.keys():
        print(f"\nFetching following accounts for {username}...")

        # Get all accounts the personality is following
        following_accounts = get_all_following(username)

        if following_accounts:
            print(f"Successfully retrieved {len(following_accounts)} accounts followed by {username}")

            # Store results
            all_personalities_following[username] = following_accounts

            # Use loaded config data for personality details
            personality_details = actual_personality_mapping.get(username) # Get the {"name": ..., "party": ...} object
            person_name = personality_details.get("name", "Unknown") if personality_details else "Unknown"
            person_party = personality_details.get("party", "Unknown") if personality_details else "Unknown"

            # Add personality and following info to each row for the DataFrame
            for account in following_accounts:
                df_data.append({
                    'personality_username': username, # Renamed key for clarity
                    'personality_name': person_name, # Use name from config object
                    'personality_party': person_party, # Use party from config object
                    'following_username': account.get('username', ''),
                    'following_display_name': account.get('display_name', '')
                })

            # Save individual JSON file
            json_filename = os.path.join(raw_dir, f"{username}_following_{timestamp}.json")
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(following_accounts, f, indent=4, ensure_ascii=False)
        else:
            print(f"No following data retrieved for {username}")

    # Create and save DataFrame if we have data
    if df_data:
        df = pd.DataFrame(df_data)

        # Save to CSV
        csv_filename = os.path.join(processed_dir, f"political_personalities_following_{timestamp}.csv")
        df.to_csv(csv_filename, index=False, encoding='utf-8')

        print(f"\nData saved to {csv_filename}")

    else:
        print("No following data was collected")


if __name__ == "__main__":
    main()