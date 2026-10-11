#!/usr/bin/env python3
"""
Script to refresh all YouTube metadata in the cache file by fetching updated data from YouTube API.
This script goes through all video IDs in the cache and updates their metadata.
"""

import json
import os
import sys
import time
import logging
from typing import Dict, Optional
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def get_cache_file_path() -> str:
    """Get the path for the YouTube metadata cache file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, "youtube_metadata_cache.json")


def load_cache() -> Dict[str, Dict[str, str]]:
    """Load YouTube metadata cache from file."""
    cache_file = get_cache_file_path()
    
    if not os.path.exists(cache_file):
        logging.error(f"Cache file not found: {cache_file}")
        return {}
    
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            cache = json.load(f)
        logging.info(f"Loaded cache with {len(cache)} entries from {cache_file}")
        return cache
    except (json.JSONDecodeError, IOError) as e:
        logging.error(f"Failed to load cache file: {e}")
        return {}


def save_cache(cache: Dict[str, Dict[str, str]]) -> bool:
    """Save YouTube metadata cache to file."""
    cache_file = get_cache_file_path()
    
    try:
        # Create backup of existing cache
        if os.path.exists(cache_file):
            backup_path = f"{cache_file}.backup"
            with open(cache_file, 'r', encoding='utf-8') as src, \
                 open(backup_path, 'w', encoding='utf-8') as dst:
                dst.write(src.read())
            logging.info(f"Created backup at {backup_path}")
        
        # Write updated cache
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
        logging.info(f"Saved cache with {len(cache)} entries to {cache_file}")
        return True
    except IOError as e:
        logging.error(f"Failed to save cache file: {e}")
        return False


def get_youtube_metadata_fresh(youtube_service, video_id: str) -> Optional[Dict[str, str]]:
    """
    Get fresh video metadata from YouTube API (bypassing cache).
    Returns dict with metadata fields or None if not found.
    """
    try:
        # Request video details from YouTube API
        request = youtube_service.videos().list(
            part="snippet",
            id=video_id
        )
        response = request.execute()
        
        if not response.get('items'):
            logging.warning(f"No video found for ID: {video_id}")
            return None
        
        video_info = response['items'][0]['snippet']
        
        metadata = {
            'title': video_info.get('title', ''),
            'channel_name': video_info.get('channelTitle', ''),
            'description': video_info.get('description', ''),
            'published_at': video_info.get('publishedAt', '')
        }
        
        logging.debug(f"Retrieved fresh metadata for {video_id}: {metadata['channel_name']} - {metadata['title']}")
        return metadata
        
    except HttpError as e:
        if e.resp.status == 403:
            logging.error("YouTube API quota exceeded or invalid API key")
            return None
        elif e.resp.status == 404:
            logging.warning(f"Video not found (may have been deleted): {video_id}")
            return None
        else:
            logging.error(f"YouTube API error for video {video_id}: {e}")
            return None
    except Exception as e:
        logging.error(f"Error getting YouTube metadata for {video_id}: {e}")
        return None


def refresh_cache_entries(youtube_service, cache: Dict[str, Dict[str, str]], 
                         batch_size: int = 10, delay: float = 1.0) -> Dict[str, str]:
    """
    Refresh all entries in the cache with fresh metadata from YouTube.
    
    Args:
        youtube_service: YouTube API service object
        cache: Current cache dictionary
        batch_size: Number of requests before a longer pause
        delay: Delay between requests in seconds
    
    Returns:
        Dict with statistics about the refresh operation
    """
    video_ids = list(cache.keys())
    total_videos = len(video_ids)
    
    stats = {
        'total': total_videos,
        'updated': 0,
        'failed': 0,
        'not_found': 0,
        'errors': 0
    }
    
    logging.info(f"Starting refresh of {total_videos} cache entries...")
    
    with tqdm(total=total_videos, desc="Refreshing cache") as pbar:
        for i, video_id in enumerate(video_ids):
            try:
                # Get fresh metadata from YouTube
                fresh_metadata = get_youtube_metadata_fresh(youtube_service, video_id)
                
                if fresh_metadata:
                    # Compare with existing metadata
                    old_metadata = cache[video_id]
                    
                    # Check if anything changed
                    changed = False
                    for key in ['title', 'channel_name', 'description', 'published_at']:
                        if old_metadata.get(key) != fresh_metadata.get(key):
                            changed = True
                            break
                    
                    # Update cache with fresh metadata
                    cache[video_id] = fresh_metadata
                    stats['updated'] += 1
                    
                    if changed:
                        logging.info(f"Updated metadata for {video_id}: {fresh_metadata['title'][:50]}...")
                    else:
                        logging.debug(f"No changes for {video_id}: {fresh_metadata['title'][:50]}...")
                        
                else:
                    # Video not found or other error
                    if video_id in cache:
                        # Keep the old metadata but mark as potentially stale
                        cache[video_id]['_refresh_failed'] = True
                        stats['not_found'] += 1
                        logging.warning(f"Could not refresh {video_id}, keeping old metadata")
                
            except Exception as e:
                logging.error(f"Error processing video {video_id}: {e}")
                stats['errors'] += 1
                
            # Update progress bar
            pbar.update(1)
            pbar.set_postfix({
                'Updated': stats['updated'], 
                'Failed': stats['failed'] + stats['not_found'] + stats['errors']
            })
            
            # Rate limiting - small delay between requests
            if i < total_videos - 1:  # Don't delay after the last request
                time.sleep(delay)
                
                # Longer pause every batch_size requests
                if (i + 1) % batch_size == 0:
                    logging.info(f"Processed {i + 1}/{total_videos} videos, taking a short break...")
                    time.sleep(2.0)
    
    return stats


def main():
    """Main function"""
    # Setup
    setup_logging("INFO")
    load_dotenv()
    
    print("YouTube Cache Refresher")
    print("======================")
    
    # Check for YouTube API key
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    if not youtube_api_key:
        logging.error("YOUTUBE_API_KEY environment variable not set")
        print("Please set your YouTube API key in the .env file:")
        print("YOUTUBE_API_KEY=your_api_key_here")
        return 1
    
    # Initialize YouTube API service
    try:
        youtube_service = build('youtube', 'v3', developerKey=youtube_api_key)
        logging.info("YouTube API service initialized successfully")
    except Exception as e:
        logging.error(f"Failed to initialize YouTube API service: {e}")
        return 1
    
    # Load cache
    cache = load_cache()
    if not cache:
        logging.error("No cache found or cache is empty")
        return 1
    
    cache_file = get_cache_file_path()
    print(f"Cache file: {cache_file}")
    print(f"Total entries to refresh: {len(cache)}")
    print()
    
    # Ask for confirmation
    response = input("Do you want to refresh all cache entries? This will make API calls to YouTube. (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Operation cancelled.")
        return 0
    
    # Refresh all entries
    try:
        stats = refresh_cache_entries(youtube_service, cache)
        
        # Save updated cache
        if save_cache(cache):
            print(f"\nRefresh completed successfully!")
            print(f"Statistics:")
            print(f"- Total entries processed: {stats['total']}")
            print(f"- Successfully updated: {stats['updated']}")
            print(f"- Videos not found: {stats['not_found']}")
            print(f"- Errors encountered: {stats['errors']}")
            print(f"- Cache file updated: {cache_file}")
            return 0
        else:
            logging.error("Failed to save updated cache")
            return 1
            
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
        
        # Save partial results
        response = input("Do you want to save the partial results? (y/N): ")
        if response.lower() in ['y', 'yes']:
            if save_cache(cache):
                print("Partial results saved.")
            else:
                print("Failed to save partial results.")
        return 1
        
    except Exception as e:
        logging.error(f"Unexpected error during refresh: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
