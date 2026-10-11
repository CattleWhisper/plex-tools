#!/usr/bin/env python3
"""
Plex Media Lister
A script to list all media items from a specific Plex library.
"""

import os
import logging
from typing import List, Optional
from dotenv import load_dotenv
from plexapi.server import PlexServer
from plexapi.library import LibrarySection
from plexapi.video import Movie, Episode
from plexapi.audio import Track
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
        plex = PlexServer(url, token)
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


def format_media_info(item) -> str:
    """Format media information for display."""
    if hasattr(item, 'year') and item.year:
        year_str = f" ({item.year})"
    else:
        year_str = ""
    
    if isinstance(item, Movie):
        duration = f" - {item.duration // 60000}min" if item.duration else ""
        rating = f" - {item.contentRating}" if item.contentRating else ""
        return f"🎬 {item.title}{year_str}{duration}{rating}"
    
    elif isinstance(item, Episode):
        season_ep = f"S{item.seasonNumber:02d}E{item.episodeNumber:02d}"
        show_title = item.grandparentTitle
        return f"📺 {show_title} - {season_ep} - {item.title}{year_str}"
    
    elif isinstance(item, Track):
        artist = item.grandparentTitle or "Unknown Artist"
        album = item.parentTitle or "Unknown Album"
        return f"🎵 {artist} - {album} - {item.title}"
    
    else:
        # Generic fallback
        return f"📄 {item.title}{year_str}"


def list_library_media(library: LibrarySection, show_details: bool = False) -> None:
    """List all media in a library."""
    print(f"\n📚 Library: {library.title} ({library.type})")
    print(f"📊 Total items: {library.totalSize}")
    print("-" * 80)
    
    try:
        # Get all items with progress bar
        logging.info("Fetching media items...")
        items = library.all()
        
        if not items:
            print("No items found in this library.")
            return
        
        # Sort items by title
        items.sort(key=lambda x: x.title.lower())
        
        # Display items with progress bar
        print("\n📋 Media Items:")
        for item in tqdm(items, desc="Processing items", unit="item"):
            print(format_media_info(item))
            
            if show_details:
                print(f"   📁 Path: {item.media[0].parts[0].file if item.media else 'N/A'}")
                print(f"   📅 Added: {item.addedAt.strftime('%Y-%m-%d') if item.addedAt else 'N/A'}")
                if hasattr(item, 'summary') and item.summary:
                    summary = item.summary[:100] + "..." if len(item.summary) > 100 else item.summary
                    print(f"   📝 Summary: {summary}")
                print()
    
    except Exception as e:
        logging.error(f"Error fetching library items: {e}")
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
    library_name = os.getenv("LIBRARY_NAME")
    
    if not plex_url or not plex_token:
        print("❌ Error: PLEX_URL and PLEX_TOKEN must be set in .env file")
        print("Please copy .env.example to .env and configure your Plex settings")
        return 1
    
    try:
        # Connect to Plex
        plex = connect_to_plex(plex_url, plex_token)
        
        # Get library
        library = get_library(plex, library_name)
        
        # Ask for detail level
        show_details = input("\nShow detailed information? (y/n): ").lower().startswith('y')
        
        # List media
        list_library_media(library, show_details)
        
        print(f"\n✅ Successfully listed all media from '{library.title}' library")
        
    except KeyboardInterrupt:
        print("\n\n👋 Operation cancelled by user")
        return 0
    except Exception as e:
        logging.error(f"Application error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
