import requests
import json
import pandas as pd
from datetime import datetime
import os
import time

# Configuration
API_URL = "https://open.tiktokapis.com/v2/research/user/reposted_videos/"
ACCESS_TOKEN = ""  # Add the access token (valid for 2 hours)
CONFIG_FILE = os.path.join("config", "portuguese_political_parties.json")

# Fields to request
FIELDS = "id,create_time,username,region_code,video_description,music_id,like_count,comment_count,share_count,view_count,hashtag_names,video_duration,favorites_count,is_stem_verified,hashtag_info_list,sticker_info_list,effect_info_list,video_mention_list,video_label"

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
    raw_dir = os.path.join("data", "raw", "political_parties_reposted", current_date)
    processed_dir = os.path.join("data", "processed", "political_parties_reposted")

    # Create directories if they don't exist
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    return raw_dir, processed_dir


def get_party_reposted_videos(username, max_count=100, cursor=None):
    """
    Get reposted videos for a party
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
        formatted_fields = FIELDS.replace(" ", "").replace("\n", "")
        url = f"{API_URL}?fields={formatted_fields}"

        response = requests.post(url, headers=headers, json=request_body)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error for {username}: {response.status_code}")
            print(f"Response: {response.text}")
            return None
    except Exception as e:
        print(f"Exception for {username}: {str(e)}")
        return None


def get_all_reposted_videos(username, max_count=100):
    """
    Get all reposted videos by a party by handling pagination
    """
    all_videos = []
    cursor = None
    has_more = True

    while has_more:
        # Get batch of reposted videos
        response = get_party_reposted_videos(username, max_count, cursor)

        if not response:
            break

        # Extract reposted videos
        video_data = response.get('data', {})
        videos = []

        if 'reposted_videos' in video_data:
            videos = video_data.get('reposted_videos', [])
        elif 'user_reposted_videos' in video_data:
            videos = video_data.get('user_reposted_videos', [])

        # Add to our list
        all_videos.extend(videos)

        # Update cursor and has_more flag
        cursor = video_data.get('cursor')
        has_more = video_data.get('has_more', False)

        if has_more:
            time.sleep(1)  # 1-second delay between requests

    return all_videos


def format_datetime(unix_timestamp):
    """Convert unix timestamp to readable datetime"""
    if unix_timestamp:
        return datetime.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return None


def main():
    raw_dir, processed_dir = ensure_directories()

    # Load config and extract mapping
    loaded_config = load_config(CONFIG_FILE)
    if loaded_config is None:
        print("Failed to load config. Exiting.")
        return
    if "political_parties" not in loaded_config:
        print("Error: 'political_parties' key not found in config file.")
        return
    actual_party_mapping = loaded_config["political_parties"]


    # Current timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Store data for creating a DataFrame
    df_data = []

    # Loop through usernames from the loaded config
    for username in actual_party_mapping.keys():
        print(f"Fetching reposted videos for {username}...")

        # Get all reposted videos by the party
        reposted_videos = get_all_reposted_videos(username)

        if reposted_videos:
            print(f"Retrieved {len(reposted_videos)} reposted videos for {username}")

            # Add party info to each video for the DataFrame
            party_full_name = actual_party_mapping.get(username, "Unknown")
            for video in reposted_videos:
                # Format hashtags as a string if they exist
                hashtags = ", ".join(video.get('hashtag_names', [])) if 'hashtag_names' in video else ""

                # Convert creation time to readable format
                create_time_formatted = format_datetime(video.get('create_time'))

                # Convert complex objects to JSON strings for CSV storage
                hashtag_info = json.dumps(video.get('hashtag_info_list', []), ensure_ascii=False) if 'hashtag_info_list' in video else ""
                sticker_info = json.dumps(video.get('sticker_info_list', []), ensure_ascii=False) if 'sticker_info_list' in video else ""
                effect_info = json.dumps(video.get('effect_info_list', []), ensure_ascii=False) if 'effect_info_list' in video else ""
                video_mentions = json.dumps(video.get('video_mention_list', []), ensure_ascii=False) if 'video_mention_list' in video else ""
                video_label = json.dumps(video.get('video_label', {}), ensure_ascii=False) if 'video_label' in video else ""

                df_data.append({
                    'party_username': username,
                    'party_name': party_full_name, # Use name from loaded config
                    'video_id': video.get('id', ''),
                    'create_time': create_time_formatted,
                    'creator_username': video.get('username', ''),
                    'region_code': video.get('region_code', ''),
                    'video_description': video.get('video_description', ''),
                    'like_count': video.get('like_count', 0),
                    'comment_count': video.get('comment_count', 0),
                    'share_count': video.get('share_count', 0),
                    'view_count': video.get('view_count', 0),
                    'favorites_count': video.get('favorites_count', 0),
                    'hashtags': hashtags,
                    'hashtag_info_list': hashtag_info,
                    'sticker_info_list': sticker_info,
                    'effect_info_list': effect_info,
                    'video_mention_list': video_mentions,
                    'video_label': video_label,
                    'video_duration': video.get('video_duration', 0),
                    'is_stem_verified': video.get('is_stem_verified', False),
                    'music_id': video.get('music_id', '')
                })

            # Save individual JSON file with raw data
            json_filename = os.path.join(raw_dir, f"{username}_reposted_{timestamp}.json")
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(reposted_videos, f, indent=4, ensure_ascii=False)
        else:
            print(f"No reposted videos retrieved for {username}")

    # Create and save DataFrame if we have data
    if df_data:
        df = pd.DataFrame(df_data)

        # Save to CSV
        csv_filename = os.path.join(processed_dir, f"political_parties_reposted_{timestamp}.csv")
        df.to_csv(csv_filename, index=False, encoding='utf-8')

        print(f"\nData saved to {csv_filename}")
    else:
        print("No reposted videos data was collected")


if __name__ == "__main__":
    main()