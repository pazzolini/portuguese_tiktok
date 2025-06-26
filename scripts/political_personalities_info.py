import requests
import json
import pandas as pd
from datetime import datetime
import os

# Configuration
API_URL = "https://open.tiktokapis.com/v2/research/user/info/?fields=display_name,bio_description,avatar_url,is_verified,follower_count,following_count,likes_count,video_count"
ACCESS_TOKEN = ""  # Add the access token (valid for 2 hours)

# Path to the personalities JSON file
PERSONALITIES_JSON_PATH = os.path.join("config", "portuguese_political_personalities.json")


def load_personalities():
    """Load political personalities from JSON file"""
    try:
        with open(PERSONALITIES_JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"Loaded {len(data['political_personalities'])} personalities from {PERSONALITIES_JSON_PATH}")

        # Extract usernames
        tiktok_accounts = list(data['political_personalities'].keys())

        # Create formatted mapping dictionary
        personalities_mapping = {}
        for username, info in data['political_personalities'].items():
            personalities_mapping[username] = f"{info['name']} ({info['party']})"

        return tiktok_accounts, personalities_mapping
    except Exception as e:
        print(f"Error loading personalities from JSON: {e}")
        return [], {}


def ensure_directories():
    """Ensure the required directories exist"""
    # Get current date for raw data organization
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Create more organized directory structure
    raw_dir = os.path.join("data", "raw", "political_personalities_info", current_date)
    processed_dir = os.path.join("data", "processed", "political_personalities_info")

    # Create directories if they don't exist
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    return raw_dir, processed_dir


def main():
    # Load personalities data
    tiktok_accounts, personalities_mapping = load_personalities()

    # Ensure directories exist
    raw_dir, processed_dir = ensure_directories()

    results = []

    # Request headers - exactly as in the cURL command
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    # Current timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Loop through each account
    for username in tiktok_accounts:
        print(f"Fetching data for {username}...")

        # Request data
        data = {"username": username}

        try:
            # Make the request
            response = requests.post(API_URL, headers=headers, json=data)

            # Print status code for debugging
            print(f"Status code: {response.status_code}")

            # Check if successful
            if response.status_code == 200:
                # Get the data
                user_data = response.json()

                # Add personality name with proper encoding
                user_data['personality_name'] = personalities_mapping.get(username, "Unknown")

                # Add the original username since the API does not return it
                user_data['original_username'] = username

                # Add to results
                results.append(user_data)

                # Save individual JSON file with proper encoding for Portuguese characters
                json_filename = os.path.join(raw_dir, f"{username}_{timestamp}.json")
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(user_data, f, indent=4, ensure_ascii=False)

                print(f"Successfully collected data for {username}")
            else:
                print(f"Error for {username}: {response.status_code}")
                print(f"Response: {response.text}")

        except Exception as e:
            print(f"Exception for {username}: {str(e)}")

    # Create a DataFrame
    if results:
        # Extract the fields
        data_for_df = []
        for result in results:
            data = result.get('data', {})
            original_username = result.get('original_username', '')

            data_for_df.append({
                'username': original_username,
                'display_name': data.get('display_name', ''),
                'follower_count': data.get('follower_count', 0),
                'following_count': data.get('following_count', 0),
                'likes_count': data.get('likes_count', 0),
                'video_count': data.get('video_count', 0),
                'bio': data.get('bio_description', ''),
                'verified': data.get('is_verified', False)
            })

        # Create DataFrame
        df = pd.DataFrame(data_for_df)

        # Save to CSV with proper encoding
        csv_filename = os.path.join(processed_dir, f"political_personalities_data_{timestamp}.csv")
        df.to_csv(csv_filename, index=False, encoding='utf-8')

        print(f"\nData saved to {csv_filename}")
        print(f"Total accounts processed: {len(results)}")

    else:
        print("No data was collected.")


if __name__ == "__main__":
    main()