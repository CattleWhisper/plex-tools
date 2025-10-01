#!/usr/bin/env python3
"""
YouTube Metadata Hydrator for Plex
Extract YouTube IDs from file paths and update Plex items using YouTube metadata.
Updates title, summary/description, and publish date from YouTube data.
"""

import os
import re
import logging
import time
import json
from datetime import datetime
from typing import List, Optional, Tuple, Dict
from dotenv import load_dotenv
from plexapi.server import PlexServer
from plexapi.library import LibrarySection
from plexapi.video import Movie, Episode
from tqdm import tqdm
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def get_cache_file_path() -> str:
    """Get the path for the YouTube metadata cache file."""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, "youtube_metadata_cache.json")


def load_cache() -> Dict[str, Dict[str, str]]:
    """Load YouTube metadata cache from file."""
    cache_file = get_cache_file_path()
    
    if not os.path.exists(cache_file):
        logging.info("No cache file found, starting with empty cache")
        return {}
    
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            cache = json.load(f)
        logging.info(f"Loaded cache with {len(cache)} entries from {cache_file}")
        return cache
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"Failed to load cache file: {e}. Starting with empty cache.")
        return {}


def save_cache(cache: Dict[str, Dict[str, str]]) -> None:
    """Save YouTube metadata cache to file."""
    cache_file = get_cache_file_path()
    
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
        logging.debug(f"Saved cache with {len(cache)} entries to {cache_file}")
    except IOError as e:
        logging.error(f"Failed to save cache file: {e}")


def get_cached_metadata(cache: Dict[str, Dict[str, str]], video_id: str) -> Optional[Dict[str, str]]:
    """Get metadata from cache if available."""
    if video_id in cache:
        logging.debug(f"Cache hit for video ID: {video_id}")
        return cache[video_id]
    return None


def cache_metadata(cache: Dict[str, Dict[str, str]], video_id: str, metadata: Dict[str, str]) -> None:
    """Add metadata to cache."""
    cache[video_id] = metadata
    logging.debug(f"Cached metadata for video ID: {video_id}")


def extract_youtube_id(file_path: str) -> Optional[str]:
    """
    Extract YouTube ID from file path.
    Looks for patterns like [video_id] or (video_id) in the filename.
    """
    if not file_path:
        return None
    
    # Get just the filename from the full path
    filename = os.path.basename(file_path)
    
    # Pattern to match YouTube video IDs in square brackets or parentheses
    # YouTube IDs are 11 characters long, alphanumeric plus - and _
    patterns = [
        r'\[([a-zA-Z0-9_-]{11})\]',  # [video_id]
        r'\(([a-zA-Z0-9_-]{11})\)',  # (video_id)
        r'_([a-zA-Z0-9_-]{11})\.mp4',  # _video_id.mp4
        r'_([a-zA-Z0-9_-]{11})\.mkv',  # _video_id.mkv
        r'_([a-zA-Z0-9_-]{11})\.avi',  # _video_id.avi
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            video_id = match.group(1)
            logging.debug(f"Extracted YouTube ID '{video_id}' from: {filename}")
            return video_id
    
    logging.warning(f"No YouTube ID found in: {filename}")
    return None


def get_youtube_metadata(youtube_service, video_id: str, cache: Dict[str, Dict[str, str]]) -> Optional[Dict[str, str]]:
    """
    Get video metadata from YouTube API or cache.
    Returns dict with 'title' and 'channel_name' or None if not found.
    """
    # Check cache first
    cached_metadata = get_cached_metadata(cache, video_id)
    if cached_metadata:
        return cached_metadata
    
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
        
        # Cache the metadata
        cache_metadata(cache, video_id, metadata)
        
        logging.debug(f"Retrieved and cached metadata for {video_id}: {metadata['channel_name']} - {metadata['title']}")
        return metadata
        
    except HttpError as e:
        if e.resp.status == 403:
            logging.error("YouTube API quota exceeded or invalid API key")
        else:
            logging.error(f"YouTube API error for video {video_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error getting YouTube metadata for {video_id}: {e}")
        return None


def sanitize_filename(text: str) -> str:
    """
    Sanitize text to be safe for use as a filename/title.
    Remove or replace problematic characters.
    """
    # Replace problematic characters
    replacements = {
        '/': '-',
        '\\': '-',
        ':': ' -',
        '*': '',
        '?': '',
        '"': "'",
        '<': '(',
        '>': ')',
        '|': '-',
        '\n': ' ',
        '\r': ' ',
        '\t': ' '
    }
    
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    # Remove multiple spaces and trim
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Limit length
    if len(text) > 200:
        text = text[:197] + "..."
    
    return text


def create_new_title(channel_name: str, video_title: str) -> str:
    """Create new title in format: <channel name> - <video title>"""
    channel_clean = sanitize_filename(channel_name)
    title_clean = sanitize_filename(video_title)
    
    new_title = f"{channel_clean} - {title_clean}"
    return new_title


def parse_youtube_date(published_at: str) -> Optional[datetime]:
    """
    Parse YouTube's published date format to datetime object.
    YouTube returns dates in ISO 8601 format: 2023-12-25T10:30:00Z
    """
    if not published_at:
        return None
    
    try:
        # Remove 'Z' suffix and parse
        if published_at.endswith('Z'):
            published_at = published_at[:-1] + '+00:00'
        
        # Parse the datetime
        dt = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
        logging.debug(f"Parsed YouTube date: {published_at} -> {dt}")
        return dt
    except (ValueError, TypeError) as e:
        logging.warning(f"Failed to parse YouTube date '{published_at}': {e}")
        return None


def connect_to_plex(url: str, token: str) -> PlexServer:
    """Connect to Plex server."""
    try:
        logging.info(f"Connecting to Plex server at {url}")
        # Disable SSL verification for self-signed certificates
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        session = None
        if url.startswith('https://'):
            import requests
            session = requests.Session()
            session.verify = False
        
        plex = PlexServer(url, token, session=session)
        logging.info(f"Successfully connected to Plex server: {plex.friendlyName}")
        return plex
    except Exception as e:
        logging.error(f"Failed to connect to Plex server: {e}")
        raise


def get_library(plex: PlexServer, library_name: Optional[str] = None) -> LibrarySection:
    """Get a specific library or let user choose."""
    libraries = plex.library.sections()
    
    if not libraries:
        raise ValueError("No libraries found on Plex server")
    
    # If library name is specified, try to find it
    if library_name:
        for library in libraries:
            if library.title.lower() == library_name.lower():
                logging.info(f"Found library: {library.title} ({library.type})")
                return library
        raise ValueError(f"Library '{library_name}' not found")
    
    # Otherwise, show available libraries and let user choose
    print("\nAvailable libraries:")
    for i, library in enumerate(libraries, 1):
        print(f"{i}. {library.title} ({library.type}) - {library.totalSize} items")
    
    while True:
        try:
            choice = int(input(f"\nSelect library (1-{len(libraries)}): ")) - 1
            if 0 <= choice < len(libraries):
                selected_library = libraries[choice]
                logging.info(f"Selected library: {selected_library.title}")
                return selected_library
            else:
                print(f"Please enter a number between 1 and {len(libraries)}")
        except ValueError:
            print("Please enter a valid number")
        except KeyboardInterrupt:
            print("\nOperation cancelled")
            exit(0)


def process_library_items(library: LibrarySection, youtube_service, dry_run: bool = True, verbose: bool = False) -> None:
    """
    Process all items in library, extract YouTube IDs, get metadata, and update titles, summaries, and publish dates.
    """
    print(f"\n📚 Processing library: {library.title} ({library.type})")
    print(f"📊 Total items: {library.totalSize}")
    print(f"🧪 Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}")
    print("-" * 80)
    
    # Load cache
    cache = load_cache()
    cache_saves = 0
    
    try:
        # Get all items
        logging.info("Fetching library items...")
        items = library.all()
        
        if not items:
            print("No items found in this library.")
            return
        
        processed_count = 0
        updated_count = 0
        failed_count = 0
        cache_hits = 0
        api_calls = 0
        
        # Process each item
        for item in tqdm(items, desc="Processing items", unit="item"):
            try:
                # Get file path
                if not item.media or not item.media[0].parts:
                    logging.warning(f"No media file found for: {item.title}")
                    failed_count += 1
                    continue
                
                file_path = item.media[0].parts[0].file
                
                # Extract YouTube ID
                youtube_id = extract_youtube_id(file_path)
                if not youtube_id:
                    logging.warning(f"No YouTube ID found for: {item.title}")
                    failed_count += 1
                    continue
                
                # Check if we already have this in cache
                cached_metadata = get_cached_metadata(cache, youtube_id)
                if cached_metadata:
                    metadata = cached_metadata
                    cache_hits += 1
                else:
                    # Get YouTube metadata from API
                    metadata = get_youtube_metadata(youtube_service, youtube_id, cache)
                    if metadata:
                        api_calls += 1
                
                if not metadata:
                    logging.warning(f"Failed to get YouTube metadata for: {youtube_id}")
                    failed_count += 1
                    continue
                
                # Create new title
                new_title = create_new_title(metadata['channel_name'], metadata['title'])
                
                # Get current summary for comparison
                current_summary = getattr(item, 'summary', '') or ''
                video_description = metadata.get('description', '')
                
                # Parse YouTube publish date
                youtube_publish_date = parse_youtube_date(metadata.get('published_at', ''))
                current_date = getattr(item, 'originallyAvailableAt', None)
                
                # Display what would happen
                if verbose:
                    print(f"\n📹 Current: {item.title}")
                    print(f"🆔 YouTube ID: {youtube_id}")
                    print(f"📺 Channel: {metadata['channel_name']}")
                    print(f"🎬 Video Title: {metadata['title']}")
                    print(f"✨ New Title: {new_title}")
                    if video_description:
                        print(f"📝 Description: {video_description[:100]}{'...' if len(video_description) > 100 else ''}")
                    if youtube_publish_date:
                        print(f"📅 YouTube Published: {youtube_publish_date.strftime('%Y-%m-%d')}")
                        if current_date:
                            print(f"📅 Current Plex Date: {current_date.strftime('%Y-%m-%d')}")
                        else:
                            print(f"📅 Current Plex Date: Not set")
                
                # Check what needs updating
                title_changed = item.title != new_title
                summary_changed = current_summary != video_description
                date_changed = False
                
                if youtube_publish_date:
                    if current_date is None:
                        date_changed = True
                        logging.debug(f"Date missing for {item.title}, will set to {youtube_publish_date.date()}")
                    else:
                        # Compare dates (ignoring time)
                        youtube_date_only = youtube_publish_date.date()
                        current_date_only = current_date.date()
                        date_changed = youtube_date_only != current_date_only
                        if date_changed:
                            logging.debug(f"Date mismatch for {item.title}: {current_date_only} -> {youtube_date_only}")
                else:
                    logging.debug(f"No YouTube publish date available for {item.title}")
                
                if not dry_run and (title_changed or summary_changed or date_changed):
                    try:
                        changes = []
                        if title_changed:
                            item.editTitle(new_title)
                            changes.append("title")
                        if summary_changed and video_description:
                            item.editSummary(video_description)
                            changes.append("summary")
                        if date_changed and youtube_publish_date:
                            date_only = youtube_publish_date.date()
                            item.editOriginallyAvailable(date_only)
                            changes.append("publish date")                        
                        if changes:
                            print(f"✅ Updated {', '.join(changes)} successfully!")
                            updated_count += 1
                    except Exception as e:
                        print(f"❌ Failed to update: {e}")
                        logging.error(f"Update error for {item.title}: {e}")
                        failed_count += 1
                elif dry_run and (title_changed or summary_changed or date_changed):
                    changes = []
                    if title_changed:
                        changes.append("title")
                    if summary_changed and video_description:
                        changes.append("summary")
                    if date_changed and youtube_publish_date:
                        changes.append("publish date")
                    print(f"🧪 Would update {', '.join(changes)} (dry run)")
                    updated_count += 1
                elif verbose and not (title_changed or summary_changed or date_changed):
                    print(f"ℹ️  No changes needed")
                
                processed_count += 1
                
                # Save cache every 10 items
                if processed_count % 10 == 0:
                    save_cache(cache)
                    cache_saves += 1
                
                # Small delay to be nice to YouTube API (only if we made an API call)
                if not cached_metadata:
                    time.sleep(0.1)
                
            except Exception as e:
                logging.error(f"Error processing item {item.title}: {e}")
                failed_count += 1
                continue
        
        # Final cache save
        save_cache(cache)
        cache_saves += 1
        
        # Summary
        print(f"\n" + "="*80)
        print(f"📊 PROCESSING SUMMARY")
        print(f"📋 Items processed: {processed_count}")
        print(f"✅ Items updated: {updated_count}")
        print(f"❌ Items failed: {failed_count}")
        print(f"💾 Cache hits: {cache_hits}")
        print(f"🌐 API calls: {api_calls}")
        print(f"💾 Cache saves: {cache_saves}")
        print(f"🧪 Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}")
        print("="*80)
        
    except Exception as e:
        logging.error(f"Error processing library items: {e}")
        # Save cache before raising
        save_cache(cache)
        raise


def main():
    """Main function."""
    # Load environment variables
    load_dotenv()
    
    # Setup logging
    log_level = os.getenv("LOG_LEVEL", "INFO")
    setup_logging(log_level)
    
    # Get configuration
    plex_url = os.getenv("PLEX_URL")
    plex_token = os.getenv("PLEX_TOKEN")
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    library_name = os.getenv("LIBRARY_NAME")
    
    if not plex_url or not plex_token:
        print("❌ Error: PLEX_URL and PLEX_TOKEN must be set in .env file")
        return 1
    
    if not youtube_api_key:
        print("❌ Error: YOUTUBE_API_KEY must be set in .env file")
        print("Get your API key from: https://console.developers.google.com/")
        return 1
    
    try:
        # Initialize YouTube API with SSL verification disabled
        logging.info("Initializing YouTube API...")
        import ssl
        import httplib2
        
        # Create an httplib2.Http object with SSL verification disabled
        http = httplib2.Http()
        http.disable_ssl_certificate_validation = True
        
        # Also disable SSL verification globally for urllib3 (used by googleapiclient)
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        youtube = build('youtube', 'v3', developerKey=youtube_api_key, http=http)
        
        # Connect to Plex
        plex = connect_to_plex(plex_url, plex_token)
        
        # Get library
        library = get_library(plex, library_name)
        
        # Ask for execution mode
        print("\n🤔 Choose execution mode:")
        print("1. Dry run (preview changes only)")
        print("2. Live execution (actually update items)")
        
        while False:
            try:
                choice = input("\nSelect mode (1-2): ").strip()
                if choice == "1":
                    dry_run = True
                    verbose = True
                    break
                elif choice == "2":
                    dry_run = False
                    verbose = False
                    confirmation = input("⚠️  This will actually update item titles and summaries. Are you sure? (yes/no): ")
                    if confirmation.lower() in ['yes', 'y']:
                        break
                    else:
                        print("Operation cancelled.")
                        return 0
                else:
                    print("Please enter 1 or 2")
            except KeyboardInterrupt:
                print("\n\n👋 Operation cancelled by user")
                return 0
        dry_run = False
        verbose = False

        # Process library items
        process_library_items(library, youtube, dry_run, verbose)
        
        print(f"\n✅ Successfully processed library: {library.title}")
        
    except KeyboardInterrupt:
        print("\n\n👋 Operation cancelled by user")
        return 0
    except Exception as e:
        logging.error(f"Application error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
