import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os
import time

# Configuration
API_URL = "https://open.tiktokapis.com/v2/research/video/query/"
ACCESS_TOKEN = ""  # ADD ACCESS TOKEN

# All available fields from the documentation
FIELDS = "id,video_description,create_time,region_code,share_count,view_count,like_count,comment_count,music_id,hashtag_names,username,effect_ids,playlist_id,voice_to_text,is_stem_verified,favorites_count,video_duration,hashtag_info_list,sticker_info_list,effect_info_list,video_mention_list,video_label,video_tag"

# Path to the accounts JSON file
ACCOUNTS_JSON_PATH = os.path.join("config", "all_portuguese_accounts_for_model.json")

# Year to extract (change this manually to extract different years)
YEAR_TO_EXTRACT = 2025


def load_accounts_config():
    """Load political account configurations from JSON file"""
    try:
        with open(ACCOUNTS_JSON_PATH, 'r', encoding='utf-8') as f:
            accounts_data = json.load(f)  # Expecting a list of account objects

        if not isinstance(accounts_data, list):
            print(f"Error: Expected a list of accounts in {ACCOUNTS_JSON_PATH}, found {type(accounts_data)}")
            return []

        print(f"Loaded {len(accounts_data)} account configurations from {ACCOUNTS_JSON_PATH}")
        return accounts_data
    except Exception as e:
        print(f"Error loading accounts configuration from JSON: {e}")
        return []


def ensure_directories():
    """Ensure the required directories exist"""
    current_date = datetime.now().strftime("%Y-%m-%d")
    # Generic directory names
    raw_dir_base = "political_accounts_videos"

    raw_dir = os.path.join("data", "raw", raw_dir_base, current_date)
    processed_dir = os.path.join("data", "processed", raw_dir_base)

    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    return raw_dir, processed_dir


def get_videos_for_account(account_username, start_date, end_date, max_count=100, cursor=None, search_id=None):
    """
    Get videos for a specific account using the video query endpoint
    """
    if not ACCESS_TOKEN:
        print("Error: ACCESS_TOKEN is not set. Please add your access token to the script.")
        return None

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    query = {
        "and": [
            {
                "operation": "EQ",
                "field_name": "username",
                "field_values": [account_username]
            }
        ]
    }

    request_body = {
        "query": query,
        "start_date": start_date,
        "end_date": end_date,
        "max_count": max_count,
        "is_random": False
    }

    if cursor:
        request_body["cursor"] = cursor
    if search_id:
        request_body["search_id"] = search_id

    try:
        formatted_fields = FIELDS.replace(" ", "").replace("\n", "")
        url = f"{API_URL}?fields={formatted_fields}"
        response = requests.post(url, headers=headers, json=request_body)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error for account {account_username} ({start_date} to {end_date}): {response.status_code}")
            print(f"Response: {response.text}")
            # Attempt to parse error for more details
            try:
                error_details = response.json()
                print(f"Error details: {error_details}")
            except json.JSONDecodeError:
                pass  # Already printed the text
            return None
    except Exception as e:
        print(f"Exception for account {account_username} ({start_date} to {end_date}): {str(e)}")
        return None


def get_all_videos_for_account(account_username, start_date, end_date, max_count=100):
    """
    Get all videos for an account by handling pagination
    """
    all_videos = []
    cursor = None
    search_id = None
    has_more = True
    retries = 3  # Number of retries for failed requests
    retry_delay = 3  # Seconds to wait before retrying

    while has_more:
        current_retry = 0
        response = None
        while current_retry < retries:
            response = get_videos_for_account(account_username, start_date, end_date, max_count, cursor, search_id)
            if response:
                break
            current_retry += 1
            print(
                f"Request failed for {account_username}, attempt {current_retry}/{retries}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)

        if not response:
            print(
                f"Failed to retrieve data for {account_username} after {retries} retries for period {start_date}-{end_date}.")
            break  # Stop trying for this period if all retries fail

        video_data = response.get('data', {})
        videos = video_data.get('videos', [])

        if videos:
            all_videos.extend(videos)
            print(
                f"Retrieved {len(videos)} videos for {account_username}, total so far for this period: {len(all_videos)}")
        else:
            print(f"No new videos found in this batch for {account_username} for period {start_date}-{end_date}.")
            # The API's has_more flag is the source of truth.

        cursor = video_data.get('cursor')
        search_id = video_data.get('search_id', search_id)  # Persist search_id if returned
        has_more = video_data.get('has_more', False)

        if not cursor or not has_more:  # Stop if no cursor or API says no more data
            has_more = False

        if has_more:
            print(f"Pausing before next request for {account_username}...")
            time.sleep(1)
        else:
            print(f"No more videos available for {account_username} in this date range or iteration.")

    return all_videos


def format_datetime_from_unix(unix_timestamp):
    """Convert unix timestamp to readable datetime string"""
    if unix_timestamp is not None:
        try:
            return datetime.fromtimestamp(int(unix_timestamp)).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return str(unix_timestamp)  # Return as string if conversion fails
    return None


def generate_year_date_ranges(year):
    """
    Generate date ranges for a specific year, split into 30-day chunks.
    The API documentation specifies a maximum of 30 days per query.
    """
    start_of_year = datetime(year, 1, 1)

    if year == datetime.now().year:
        end_of_period = datetime.now()
    elif year < datetime.now().year:
        end_of_period = datetime(year, 12, 31)
    else:  # Future year
        print(
            f"Warning: YEAR_TO_EXTRACT ({year}) is a future year. Data collection will be up to end of that year if run in the future.")
        end_of_period = datetime(year, 12, 31)

    date_ranges = []
    current_start = start_of_year

    while current_start <= end_of_period:
        current_end = min(current_start + timedelta(days=29), end_of_period)  # 30 days inclusive means +29 days
        date_ranges.append((
            current_start.strftime('%Y%m%d'),
            current_end.strftime('%Y%m%d')
        ))
        current_start = current_end + timedelta(days=1)

    return date_ranges


def process_videos_to_dataframe_rows(videos, account_config):
    """
    Process video data into DataFrame rows, prepending account config information.
    """
    df_data_rows = []
    for video_api_data in videos:
        # Prepend data from the configuration file
        row_data = {
            'config_account_username': account_config.get('account_username'),
            'config_account_name': account_config.get('account_name'),
            'config_account_type': account_config.get('account_type'),
            'config_associated_party': account_config.get('associated_party'),
            'config_party_lrecon_label': account_config.get('party_lrecon_label'),
            'config_party_galtan_label': account_config.get('party_galtan_label'),
        }

        # Add fields from the API response
        row_data.update({
            'video_id': video_api_data.get('id'),
            'api_username': video_api_data.get('username'),  # Username returned by API for the video
            'create_time': format_datetime_from_unix(video_api_data.get('create_time')),
            'region_code': video_api_data.get('region_code'),
            'video_description': video_api_data.get('video_description'),
            'like_count': video_api_data.get('like_count', 0),
            'comment_count': video_api_data.get('comment_count', 0),
            'share_count': video_api_data.get('share_count', 0),
            'view_count': video_api_data.get('view_count', 0),
            'favorites_count': video_api_data.get('favorites_count', 0),
            'video_duration': video_api_data.get('video_duration', 0),
            'music_id': video_api_data.get('music_id'),
            'playlist_id': video_api_data.get('playlist_id'),
            'voice_to_text': video_api_data.get('voice_to_text'),
            'is_stem_verified': video_api_data.get('is_stem_verified', False),
            'hashtags': ", ".join(video_api_data.get('hashtag_names', [])),
            'effect_ids_str': json.dumps(video_api_data.get('effect_ids', []), ensure_ascii=False),
            'hashtag_info_list_str': json.dumps(video_api_data.get('hashtag_info_list', []), ensure_ascii=False),
            'sticker_info_list_str': json.dumps(video_api_data.get('sticker_info_list', []), ensure_ascii=False),
            'effect_info_list_str': json.dumps(video_api_data.get('effect_info_list', []), ensure_ascii=False),
            'video_mention_list_str': json.dumps(video_api_data.get('video_mention_list', []), ensure_ascii=False),
            'video_label_str': json.dumps(video_api_data.get('video_label', {}), ensure_ascii=False),
            'video_tag_str': json.dumps(video_api_data.get('video_tag', []), ensure_ascii=False)  # Added video_tag
        })
        df_data_rows.append(row_data)
    return df_data_rows


def main():
    print("Starting TikTok video collection script...")
    if not ACCESS_TOKEN:
        print("CRITICAL: ACCESS_TOKEN is not set in the script. Please add your token and restart.")
        return

    target_accounts_config = load_accounts_config()
    if not target_accounts_config:
        print("No accounts loaded. Exiting.")
        return

    raw_dir, processed_dir = ensure_directories()
    timestamp_for_files = datetime.now().strftime("%Y%m%d_%H%M%S")
    year_to_process = YEAR_TO_EXTRACT

    print(f"\n{'=' * 80}")
    print(f"Processing videos for year {year_to_process}")
    print(f"{'=' * 80}")

    date_ranges_for_year = generate_year_date_ranges(year_to_process)
    if not date_ranges_for_year:
        print(
            f"No date ranges generated for year {year_to_process}. This might happen if the year is far in the future.")
        return
    print(f"Split year {year_to_process} into {len(date_ranges_for_year)} date ranges for API queries.")

    all_collected_videos_for_year_df_rows = []

    for account_config in target_accounts_config:
        account_username = account_config.get('account_username')
        account_display_name = account_config.get('account_name', "Unknown Account")  # Use account_name for display

        if not account_username:
            print(f"Skipping account due to missing 'account_username': {account_config}")
            continue

        print(f"\nFetching videos for {account_username} ({account_display_name}) for year {year_to_process}...")

        account_yearly_raw_videos = []

        for start_date_str, end_date_str in date_ranges_for_year:
            print(f"Querying from {start_date_str} to {end_date_str} for {account_username}...")

            videos_in_period = get_all_videos_for_account(account_username, start_date_str, end_date_str)

            if videos_in_period:
                account_yearly_raw_videos.extend(videos_in_period)
                print(
                    f"Retrieved {len(videos_in_period)} videos for {account_username} in period {start_date_str}-{end_date_str}.")

                # Save intermediate raw JSON for this period and account
                period_raw_json_filename = os.path.join(
                    raw_dir,
                    f"{account_username}_{year_to_process}_{start_date_str}_to_{end_date_str}_{timestamp_for_files}.json"
                )
                try:
                    with open(period_raw_json_filename, 'w', encoding='utf-8') as f:
                        json.dump(videos_in_period, f, indent=4, ensure_ascii=False)
                    print(f"Saved raw data for period to {period_raw_json_filename}")
                except IOError as e:
                    print(f"Error saving raw data for period: {e}")

                time.sleep(1)
            else:
                print(f"No videos found for {account_username} in period {start_date_str}-{end_date_str}.")
                time.sleep(1)

        if account_yearly_raw_videos:
            total_videos_for_account = len(account_yearly_raw_videos)
            print(f"Total raw videos retrieved for {account_username} in {year_to_process}: {total_videos_for_account}")

            # Save combined raw JSON for this account and year
            account_year_raw_json_filename = os.path.join(
                raw_dir,
                f"{account_username}_{year_to_process}_all_videos_{timestamp_for_files}.json"
            )
            try:
                with open(account_year_raw_json_filename, 'w', encoding='utf-8') as f:
                    json.dump(account_yearly_raw_videos, f, indent=4, ensure_ascii=False)
                print(
                    f"Saved all raw videos for {account_username} in {year_to_process} to {account_year_raw_json_filename}")
            except IOError as e:
                print(f"Error saving all raw videos for account: {e}")

            # Process these videos and add to the year's total collection
            processed_rows = process_videos_to_dataframe_rows(account_yearly_raw_videos, account_config)
            all_collected_videos_for_year_df_rows.extend(processed_rows)

            # Save individual processed CSV per account per year
            if processed_rows:
                account_df = pd.DataFrame(processed_rows)
                individual_csv_filename = os.path.join(
                    processed_dir,
                    f"{account_username}_{year_to_process}_processed_{timestamp_for_files}.csv"
                )
                try:
                    account_df.to_csv(individual_csv_filename, index=False,
                                      encoding='utf-8-sig')  # utf-8-sig for Excel compatibility
                    print(
                        f"Processed data for {account_username} in {year_to_process} saved to {individual_csv_filename}")
                except IOError as e:
                    print(f"Error saving processed CSV for account: {e}")
        else:
            print(f"No videos retrieved for {account_username} in {year_to_process} after checking all periods.")

        print(f"Pausing for a few seconds before processing next account...")
        time.sleep(2)  # Pause between accounts

    if all_collected_videos_for_year_df_rows:
        year_df = pd.DataFrame(all_collected_videos_for_year_df_rows)
        combined_year_csv_filename = os.path.join(
            processed_dir,
            f"all_accounts_{year_to_process}_videos_{timestamp_for_files}.csv"
        )
        try:
            year_df.to_csv(combined_year_csv_filename, index=False, encoding='utf-8-sig')
            print(f"\nAll processed data for year {year_to_process} saved to {combined_year_csv_filename}")
            print(
                f"Total videos collected and processed for {year_to_process}: {len(all_collected_videos_for_year_df_rows)}")
        except IOError as e:
            print(f"Error saving combined processed CSV for year: {e}")
    else:
        print(f"\nNo video data was collected or processed for any account in year {year_to_process}.")

    print("\nScript finished.")


if __name__ == "__main__":
    if not ACCESS_TOKEN:
        print("Set your ACCESS_TOKEN variable in the script before running.")
    else:
        main()