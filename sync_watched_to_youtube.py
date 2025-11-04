#!/usr/bin/env python3
"""
Sync Watched Videos to YouTube
Fetches watched videos from a Plex library and marks them as watched on YouTube using yt-dlp.
"""

import os
import re
import logging
import subprocess
import json
from typing import List, Optional, Tuple, Set
from datetime import datetime
from dotenv import load_dotenv
from plexapi.server import PlexServer
from plexapi.library import LibrarySection
from plexapi.video import Movie, Episode
from tqdm import tqdm


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def connect_to_plex(url: str, token: str) -> PlexServer:
    """Connect to Plex server."""
    try:
        logging.info(f"Connecting to Plex server at {url}")
        # Create a session that doesn't verify SSL certificates
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        session = requests.Session()
        session.verify = False
        
        # Disable SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        plex = PlexServer(url, token, session=session)
        logging.info(f"Successfully connected to Plex server: {plex.friendlyName}")
        return plex
    except Exception as e:
        logging.error(f"Failed to connect to Plex server: {e}")
        raise


def get_synced_videos_file() -> str:
    """Get the path for the synced videos tracking file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, "youtube_synced_videos.json")


def load_synced_videos() -> Set[str]:
    """Load the set of video IDs that have already been synced to YouTube."""
    synced_file = get_synced_videos_file()
    
    if not os.path.exists(synced_file):
        logging.info("No synced videos file found, starting fresh")
        return set()
    
    try:
        with open(synced_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            synced_ids = set(data.get('synced_videos', []))
            logging.info(f"Loaded {len(synced_ids)} previously synced video IDs")
            return synced_ids
    except Exception as e:
        logging.warning(f"Error loading synced videos file: {e}")
        return set()


def save_synced_videos(synced_videos: Set[str]) -> None:
    """Save the set of synced video IDs to file."""
    synced_file = get_synced_videos_file()
    
    try:
        data = {
            'synced_videos': list(synced_videos),
            'last_updated': datetime.now().isoformat()
        }
        
        with open(synced_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Saved {len(synced_videos)} synced video IDs to {synced_file}")
    except Exception as e:
        logging.error(f"Error saving synced videos file: {e}")


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


def extract_youtube_id(filename: str) -> Optional[str]:
    """
    Extract YouTube video ID from filename.
    Supports various common formats like [video_id].ext or _video_id.ext.
    """
    # Common patterns for YouTube video IDs in filenames
    patterns = [
        r'\[([a-zA-Z0-9_-]{11})\]',  # [video_id]
        r'\(([a-zA-Z0-9_-]{11})\)',  # (video_id)
        r'_([a-zA-Z0-9_-]{11})\.mp4',  # _video_id.mp4
        r'_([a-zA-Z0-9_-]{11})\.mkv',  # _video_id.mkv
        r'_([a-zA-Z0-9_-]{11})\.avi',  # _video_id.avi
        r'_([a-zA-Z0-9_-]{11})\.webm',  # _video_id.webm
        r'-([a-zA-Z0-9_-]{11})\.mp4',  # -video_id.mp4
        r'-([a-zA-Z0-9_-]{11})\.mkv',  # -video_id.mkv
        r'-([a-zA-Z0-9_-]{11})\.avi',  # -video_id.avi
        r'-([a-zA-Z0-9_-]{11})\.webm',  # -video_id.webm
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            video_id = match.group(1)
            logging.debug(f"Extracted YouTube ID '{video_id}' from: {filename}")
            return video_id
    
    logging.warning(f"No YouTube ID found in: {filename}")
    return None


def get_watched_videos(library: LibrarySection) -> List[Tuple[str, str]]:
    """
    Get all watched videos from a Plex library.
    Returns a list of tuples: (video_title, youtube_id)
    """
    logging.info(f"Fetching watched videos from library: {library.title}")
    watched_videos = []
    
    try:
        # Get all items in the library
        items = library.all()
        
        logging.info(f"Scanning {len(items)} items for watched videos...")
        
        for item in tqdm(items, desc="Scanning items", unit="item"):
            # Check if the item has been watched
            if item.isWatched:
                # Try to get the file path
                try:
                    if item.media and item.media[0].parts:
                        file_path = item.media[0].parts[0].file
                        filename = os.path.basename(file_path)
                        
                        # Extract YouTube ID from filename
                        youtube_id = extract_youtube_id(filename)
                        
                        if youtube_id:
                            watched_videos.append((item.title, youtube_id))
                            logging.debug(f"Found watched video: {item.title} - {youtube_id}")
                        else:
                            logging.warning(f"Could not extract YouTube ID from watched item: {item.title}")
                except Exception as e:
                    logging.warning(f"Error processing item '{item.title}': {e}")
        
        logging.info(f"Found {len(watched_videos)} watched videos with YouTube IDs")
        return watched_videos
        
    except Exception as e:
        logging.error(f"Error fetching watched videos: {e}")
        raise


def mark_video_watched_on_youtube(video_id: str, cookies_file: str) -> bool:
    """
    Mark a video as watched on YouTube using yt-dlp.
    Returns True if successful, False otherwise.
    """
    try:
        # yt-dlp command to mark video as watched
        # Using --mark-watched flag with cookies for authentication
        cmd = [
            'yt-dlp',
            '--cookies', cookies_file,
            '--mark-watched',
            '--no-download',
            '--quiet',
            f'https://www.youtube.com/watch?v={video_id}'
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            logging.debug(f"Successfully marked video {video_id} as watched")
            return True
        else:
            logging.warning(f"Failed to mark video {video_id} as watched: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error(f"Timeout while marking video {video_id} as watched")
        return False
    except Exception as e:
        logging.error(f"Error marking video {video_id} as watched: {e}")
        return False


def sync_watched_videos(
    watched_videos: List[Tuple[str, str]], 
    cookies_file: str,
    dry_run: bool = False
) -> Tuple[int, int]:
    """
    Sync watched videos to YouTube.
    Returns tuple of (successful_count, failed_count)
    """
    if not watched_videos:
        logging.info("No watched videos to sync")
        return 0, 0
    
    # Load previously synced videos
    synced_videos = load_synced_videos()
    
    # Filter out already synced videos
    videos_to_sync = [(title, vid) for title, vid in watched_videos if vid not in synced_videos]
    already_synced_count = len(watched_videos) - len(videos_to_sync)
    
    if already_synced_count > 0:
        print(f"\n⏭️  Skipping {already_synced_count} videos that were already synced")
    
    if not videos_to_sync:
        print("\n✅ All watched videos have already been synced to YouTube")
        return 0, 0
    
    if dry_run:
        print(f"\n🔍 DRY RUN MODE - Would mark {len(videos_to_sync)} new videos as watched on YouTube:")
        for title, video_id in videos_to_sync:
            print(f"  - {title} (https://www.youtube.com/watch?v={video_id})")
        if already_synced_count > 0:
            print(f"\n(Skipping {already_synced_count} already synced videos)")
        return 0, 0
    
    successful = 0
    failed = 0
    
    print(f"\n🔄 Marking {len(videos_to_sync)} new videos as watched on YouTube...")
    
    for title, video_id in tqdm(videos_to_sync, desc="Syncing videos", unit="video"):
        logging.info(f"Processing: {title}")
        
        if mark_video_watched_on_youtube(video_id, cookies_file):
            successful += 1
            synced_videos.add(video_id)
            # Save after each successful sync to avoid losing progress
            save_synced_videos(synced_videos)
        else:
            failed += 1
            logging.error(f"Failed to sync: {title} ({video_id})")
    
    return successful, failed


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
    library_name = os.getenv("LIBRARY_NAME")
    cookies_file = os.getenv("COOKIES_FILE", "cookies.txt")
    
    if not plex_url or not plex_token:
        print("❌ Error: PLEX_URL and PLEX_TOKEN must be set in .env file")
        print("Please copy .env.example to .env and configure your Plex settings")
        return 1
    
    # Check if cookies file exists
    if not os.path.exists(cookies_file):
        print(f"❌ Error: Cookies file '{cookies_file}' not found")
        print("Please export your YouTube cookies to this file")
        print("You can use browser extensions like 'Get cookies.txt' to export cookies")
        return 1
    
    # Check if yt-dlp is installed
    try:
        result = subprocess.run(['yt-dlp', '--version'], capture_output=True, text=True)
        logging.info(f"Using yt-dlp version: {result.stdout.strip()}")
    except FileNotFoundError:
        print("❌ Error: yt-dlp is not installed")
        print("Please install it using: pip install yt-dlp")
        return 1
    
    try:
        # Connect to Plex
        plex = connect_to_plex(plex_url, plex_token)
        
        # Get library
        library = get_library(plex, library_name)
        
        # Get watched videos
        watched_videos = get_watched_videos(library)
        
        if not watched_videos:
            print("\n✅ No watched videos found in the library")
            return 0
        
        # Ask for confirmation
        print(f"\n📊 Found {len(watched_videos)} watched videos in Plex")
        print(f"🍪 Using cookies file: {cookies_file}")
        
        # Ask for dry run or actual sync
        response = input("\nDo you want to perform a dry run first? (y/n): ").lower()
        dry_run = response.startswith('y')
        
        # Sync videos
        successful, failed = sync_watched_videos(watched_videos, cookies_file, dry_run)
        
        if dry_run:
            print(f"\n✅ Dry run completed - no videos were marked as watched")
        else:
            print(f"\n✅ Sync completed!")
            print(f"   ✓ Successfully marked: {successful} videos")
            if failed > 0:
                print(f"   ✗ Failed: {failed} videos")
            if successful > 0:
                print(f"   📝 Synced videos tracked in: {get_synced_videos_file()}")
        
        return 0 if failed == 0 else 1
        
    except KeyboardInterrupt:
        print("\n\n👋 Operation cancelled by user")
        return 0
    except Exception as e:
        logging.error(f"Application error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
