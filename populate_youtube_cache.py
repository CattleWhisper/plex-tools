#!/usr/bin/env python3
"""
Script to extract YouTube metadata from yt_info directory and populate the youtube_metadata_cache.json file.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path


def format_timestamp(timestamp):
    """Convert timestamp to ISO format string"""
    if not timestamp:
        return None
    
    try:
        # If timestamp is already a string, return as is
        if isinstance(timestamp, str):
            return timestamp
        
        # If timestamp is a number, convert to ISO format
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except (ValueError, TypeError):
        return None


def load_existing_cache(cache_file_path):
    """Load existing cache file if it exists"""
    if os.path.exists(cache_file_path):
        try:
            with open(cache_file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            print(f"Warning: Could not read existing cache file {cache_file_path}")
            return {}
    return {}


def extract_metadata_from_json(file_path):
    """Extract relevant metadata from a YouTube info JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract video ID from the JSON data
        video_id = data.get('id')
        if not video_id:
            print(f"Warning: No 'id' field found in {file_path}")
            return None, None
        
        # Extract relevant fields
        metadata = {
            'title': data.get('title', ''),
            'channel_name': data.get('channel') or data.get('uploader', ''),
            'description': data.get('description', ''),
            'published_at': format_timestamp(data.get('timestamp')) or data.get('upload_date', '')
        }
        
        return video_id, metadata
    
    except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
        print(f"Error reading {file_path}: {e}")
        return None, None


def process_yt_info_directory(yt_info_dir, cache_file_path):
    """Process all JSON files in the yt_info directory and populate the cache"""
    
    # Load existing cache
    cache = load_existing_cache(cache_file_path)
    
    # Get all .info.json files
    yt_info_path = Path(yt_info_dir)
    if not yt_info_path.exists():
        print(f"Error: Directory {yt_info_dir} does not exist")
        return False
    
    json_files = list(yt_info_path.glob('*.info.json'))
    
    if not json_files:
        print(f"No .info.json files found in {yt_info_dir}")
        return False
    
    print(f"Found {len(json_files)} JSON files to process...")
    
    processed_count = 0
    updated_count = 0
    error_count = 0
    
    for json_file in json_files:
        # Extract video ID and metadata from JSON file
        video_id, metadata = extract_metadata_from_json(json_file)
        
        if video_id is None or metadata is None:
            error_count += 1
            continue
        
        # Check if this video ID already exists in cache
        if video_id in cache:
            print(f"Updating existing entry for {video_id}: {metadata['title'][:50]}...")
        else:
            print(f"Adding new entry for {video_id}: {metadata['title'][:50]}...")
            updated_count += 1
        
        # Add/update in cache
        cache[video_id] = metadata
        processed_count += 1
    
    # Save updated cache
    try:
        # Create backup of existing cache
        if os.path.exists(cache_file_path):
            backup_path = f"{cache_file_path}.backup"
            with open(cache_file_path, 'r', encoding='utf-8') as src, \
                 open(backup_path, 'w', encoding='utf-8') as dst:
                dst.write(src.read())
            print(f"Created backup at {backup_path}")
        
        # Write updated cache
        with open(cache_file_path, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
        
        print(f"\nSummary:")
        print(f"- Total files processed: {processed_count}")
        print(f"- New entries added: {updated_count}")
        print(f"- Errors encountered: {error_count}")
        print(f"- Total entries in cache: {len(cache)}")
        print(f"- Cache file updated: {cache_file_path}")
        
        return True
        
    except Exception as e:
        print(f"Error writing cache file: {e}")
        return False


def main():
    """Main function"""
    # Define paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    yt_info_dir = os.path.join(script_dir, 'yt_info')
    cache_file_path = os.path.join(script_dir, 'youtube_metadata_cache.json')
    
    print("YouTube Metadata Cache Populator")
    print("=================================")
    print(f"Source directory: {yt_info_dir}")
    print(f"Cache file: {cache_file_path}")
    print()
    
    # Process the directory
    success = process_yt_info_directory(yt_info_dir, cache_file_path)
    
    if success:
        print("\nProcess completed successfully!")
        return 0
    else:
        print("\nProcess completed with errors!")
        return 1


if __name__ == '__main__':
    sys.exit(main())
