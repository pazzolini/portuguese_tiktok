import requests
import json
import pandas as pd
from datetime import datetime
import os

# Configuration
API_URL = "https://open.tiktokapis.com/v2/research/user/info/?fields=display_name,bio_description,avatar_url,is_verified,follower_count,following_count,likes_count,video_count"
ACCESS_TOKEN = ""  # Only valid for two hours
CONFIG_FILE = os.path.join("config", "portuguese_political_parties.json") # Path to the config file

def ensure_directories():
    """Ensure the required directories exist"""
    current_date = datetime.now().strftime("%Y-%m-%d")
    raw_dir = os.path.join("data", "raw", "political_parties_info", current_date)
    processed_dir = os.path.join("data", "processed", "political_parties_info")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    return raw_dir, processed_dir

def load_config(file_path):
    """Loads the party configuration from the JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        print(f"Successfully loaded configuration from {file_path}")
        return config_data
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {file_path}. Check file formatting.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while loading config: {str(e)}")
        return None

def main():
    # Ensure directories exist
    raw_dir, processed_dir = ensure_directories()

    # Load configuration from JSON
    loaded_config_data = load_config(CONFIG_FILE)
    if loaded_config_data is None:
        print("Exiting due to configuration loading failure.")
        return # Exit if config couldn't be loaded

    # --- Access the 'political_parties' dictionary
    if "political_parties" not in loaded_config_data:
        print(f"Error: Key 'political_parties' not found in {CONFIG_FILE}")
        return
    party_mapping = loaded_config_data["political_parties"]

    # Check if ACCESS_TOKEN is set
    if not ACCESS_TOKEN:
        print("Error: ACCESS_TOKEN is not set in the script. Please add your token.")
        return

    results = []

    # Request headers
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    # Current timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Loop through accounts using the extracted party_mapping
    for username in party_mapping.keys(): # Iterate through the keys of the nested dictionary
        print(f"Fetching data for {username}...")

        # Request data payload
        data_payload = {"username": username}

        try:
            # Make the request
            response = requests.post(API_URL, headers=headers, json=data_payload)

            # Print status code for debugging
            print(f"Status code: {response.status_code}")

            # Check if successful
            if response.status_code == 200:
                # Get the data
                user_data_response = response.json()

                # Check for API errors within the response structure
                if user_data_response.get('error', {}).get('code') != 'ok':
                     print(f"API Error for {username}: {user_data_response.get('error',{}).get('message','Unknown API error')}")
                     continue

                # Use the extracted party_mapping for party name
                user_data_response['party_name'] = party_mapping.get(username, "Unknown")

                # Add the original username
                user_data_response['original_username'] = username

                # Add to results
                results.append(user_data_response)

                # Save individual JSON file
                json_filename = os.path.join(raw_dir, f"{username}_{timestamp}.json")
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(user_data_response, f, indent=4, ensure_ascii=False)

                print(f"Successfully collected data for {username}")
            else:
                print(f"HTTP Error for {username}: {response.status_code}")
                print(f"Response: {response.text}")

        except requests.exceptions.RequestException as e:
             print(f"Request Exception for {username}: {str(e)}")
        except Exception as e:
            print(f"General Exception for {username}: {str(e)}")

    # Create a DataFrame from collected results
    if results:
        data_for_df = []
        for result in results:
            data_content = result.get('data', {})
            original_username = result.get('original_username', '')

            data_for_df.append({
                'username': original_username,
                'display_name': data_content.get('display_name', ''),
                'follower_count': data_content.get('follower_count', 0),
                'following_count': data_content.get('following_count', 0),
                'likes_count': data_content.get('likes_count', 0),
                'video_count': data_content.get('video_count', 0),
                'bio': data_content.get('bio_description', ''),
                'verified': data_content.get('is_verified', False),
                'party_name': result.get('party_name', 'Unknown')
            })

        df = pd.DataFrame(data_for_df)

        # Save aggregated data to CSV
        csv_filename = os.path.join(processed_dir, f"political_parties_data_{timestamp}.csv")
        try:
            df.to_csv(csv_filename, index=False, encoding='utf-8')
            print(f"\nAggregated data saved to {csv_filename}")
            print(f"Total accounts successfully processed and saved: {len(results)}")
        except Exception as e:
            print(f"Error saving CSV file: {str(e)}")

    else:
        print("\nNo data was successfully collected to save to CSV.")

if __name__ == "__main__":
    main()