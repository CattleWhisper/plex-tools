#!/usr/bin/env python3
"""
Find Similar Titles in Plex Library
Identify videos with similar titles to help find duplicates or related content.
"""

import os
import re
import logging
from typing import List, Dict, Tuple, Set
from collections import defaultdict
from difflib import SequenceMatcher
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


def get_library(plex: PlexServer, library_name: str = None) -> LibrarySection:
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


def normalize_title(title: str) -> str:
    """
    Normalize title for comparison by removing common patterns and standardizing text.
    """
    if not title:
        return ""
    
    # Convert to lowercase
    normalized = title.lower()
    
    # Remove common prefixes/suffixes that might differ between videos
    patterns_to_remove = [
        r'\[.*?\]',  # Remove anything in square brackets
        r'\(.*?\)',  # Remove anything in parentheses
        r'【.*?】',   # Remove anything in Japanese brackets
        r'｜.*',     # Remove everything after Japanese pipe
        r'\|.*',     # Remove everything after pipe
        r'part\s*\d+',  # Remove "part 1", "part 2", etc.
        r'episode\s*\d+',  # Remove "episode 1", etc.
        r'ep\s*\d+',  # Remove "ep 1", etc.
        r'\s*-\s*manhwa\s*recap.*',  # Remove manhwa recap suffixes
        r'\s*-\s*recap.*',  # Remove recap suffixes
        r'\bfull\b',  # Remove "full"
        r'\bhd\b',    # Remove "hd"
        r'\b4k\b',    # Remove "4k"
        r'\d{4}p',    # Remove resolution like "1080p"
    ]
    
    for pattern in patterns_to_remove:
        normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)
    
    # Remove extra whitespace and special characters
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized)
    normalized = normalized.strip()
    
    return normalized


def extract_keywords(title: str) -> Set[str]:
    """
    Extract meaningful keywords from a title for comparison.
    """
    normalized = normalize_title(title)
    
    # Split into words and filter out common stopwords
    stopwords = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 
        'of', 'with', 'by', 'he', 'she', 'it', 'his', 'her', 'their', 'this',
        'that', 'these', 'those', 'is', 'was', 'are', 'were', 'be', 'been',
        'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
        'should', 'may', 'might', 'must', 'can', 'now', 'new', 'old', 'big',
        'small', 'good', 'bad', 'best', 'worst', 'first', 'last', 'next'
    }
    
    words = normalized.split()
    keywords = {word for word in words if len(word) > 2 and word not in stopwords}
    
    return keywords


def calculate_similarity(title1: str, title2: str) -> float:
    """
    Calculate similarity between two titles using multiple methods.
    Returns a score between 0 and 1.
    """
    # Method 1: Direct string similarity
    direct_similarity = SequenceMatcher(None, normalize_title(title1), normalize_title(title2)).ratio()
    
    # Method 2: Keyword overlap
    keywords1 = extract_keywords(title1)
    keywords2 = extract_keywords(title2)
    
    if not keywords1 and not keywords2:
        keyword_similarity = 1.0 if title1 == title2 else 0.0
    elif not keywords1 or not keywords2:
        keyword_similarity = 0.0
    else:
        intersection = keywords1.intersection(keywords2)
        union = keywords1.union(keywords2)
        keyword_similarity = len(intersection) / len(union) if union else 0.0
    
    # Combine both methods (weighted average)
    combined_similarity = (direct_similarity * 0.4) + (keyword_similarity * 0.6)
    
    return combined_similarity


def find_similar_titles(items: List, similarity_threshold: float = 0.6) -> List[Tuple[List, float]]:
    """
    Find groups of items with similar titles.
    Returns list of tuples: (list_of_similar_items, similarity_score)
    """
    print(f"\n🔍 Analyzing {len(items)} items for similar titles...")
    print(f"📊 Similarity threshold: {similarity_threshold:.2f}")
    
    similar_groups = []
    processed_indices = set()
    
    # Compare each item with every other item
    for i in tqdm(range(len(items)), desc="Finding similarities", unit="item"):
        if i in processed_indices:
            continue
            
        current_item = items[i]
        similar_items = [current_item]
        group_similarities = []
        
        # Find all items similar to the current one
        for j in range(i + 1, len(items)):
            if j in processed_indices:
                continue
                
            other_item = items[j]
            similarity = calculate_similarity(current_item.title, other_item.title)
            
            if similarity >= similarity_threshold:
                similar_items.append(other_item)
                group_similarities.append(similarity)
                processed_indices.add(j)
        
        # If we found similar items, add to results
        if len(similar_items) > 1:
            avg_similarity = sum(group_similarities) / len(group_similarities) if group_similarities else 0.0
            similar_groups.append((similar_items, avg_similarity))
            processed_indices.add(i)
    
    # Sort by similarity score (highest first)
    similar_groups.sort(key=lambda x: x[1], reverse=True)
    
    return similar_groups


def display_similar_groups(similar_groups: List[Tuple[List, float]], show_details: bool = False) -> None:
    """
    Display the groups of similar titles in a formatted way.
    """
    if not similar_groups:
        print("\n✅ No similar titles found!")
        return
    
    print(f"\n🎯 Found {len(similar_groups)} groups of similar titles:")
    print("=" * 80)
    
    total_duplicates = 0
    
    for group_idx, (items, similarity) in enumerate(similar_groups, 1):
        print(f"\n📂 Group {group_idx} (Similarity: {similarity:.2f})")
        print(f"📊 {len(items)} items")
        print("-" * 60)
        
        total_duplicates += len(items) - 1  # Count all but one as duplicates
        
        for item_idx, item in enumerate(items, 1):
            # Get file info if available
            file_info = ""
            if hasattr(item, 'media') and item.media and item.media[0].parts:
                file_path = item.media[0].parts[0].file
                file_size = getattr(item.media[0].parts[0], 'size', 0)
                if file_size:
                    file_size_mb = file_size / (1024 * 1024)
                    file_info = f" ({file_size_mb:.1f} MB)"
                if show_details:
                    file_info += f"\n    📁 {file_path}"
            
            # Get duration if available
            duration_info = ""
            if hasattr(item, 'duration') and item.duration:
                duration_min = item.duration // 60000  # Convert from ms to minutes
                duration_info = f" [{duration_min} min]"
            
            print(f"  {item_idx}. 🎬 {item.title}{duration_info}{file_info}")
            
            if show_details:
                # Show additional details
                if hasattr(item, 'summary') and item.summary:
                    summary = item.summary[:100] + "..." if len(item.summary) > 100 else item.summary
                    print(f"    📝 {summary}")
                
                if hasattr(item, 'year') and item.year:
                    print(f"    📅 Year: {item.year}")
                
                print()  # Extra spacing for detailed view
    
    print("=" * 80)
    print(f"📈 Summary:")
    print(f"  • {len(similar_groups)} groups found")
    print(f"  • {total_duplicates} potential duplicates")
    print(f"  • Consider reviewing these groups for cleanup")


def export_results(similar_groups: List[Tuple[List, float]], output_file: str = "similar_titles_report.txt") -> None:
    """
    Export the results to a text file for further analysis.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, output_file)
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("Similar Titles Report\n")
            f.write("=" * 50 + "\n\n")
            
            if not similar_groups:
                f.write("No similar titles found.\n")
                return
            
            total_duplicates = 0
            
            for group_idx, (items, similarity) in enumerate(similar_groups, 1):
                f.write(f"Group {group_idx} (Similarity: {similarity:.2f})\n")
                f.write(f"Items: {len(items)}\n")
                f.write("-" * 30 + "\n")
                
                total_duplicates += len(items) - 1
                
                for item_idx, item in enumerate(items, 1):
                    f.write(f"  {item_idx}. {item.title}\n")
                    
                    # Add file path if available
                    if hasattr(item, 'media') and item.media and item.media[0].parts:
                        file_path = item.media[0].parts[0].file
                        f.write(f"     File: {file_path}\n")
                    
                    # Add duration if available
                    if hasattr(item, 'duration') and item.duration:
                        duration_min = item.duration // 60000
                        f.write(f"     Duration: {duration_min} minutes\n")
                
                f.write("\n")
            
            f.write("=" * 50 + "\n")
            f.write(f"Summary:\n")
            f.write(f"  Groups found: {len(similar_groups)}\n")
            f.write(f"  Potential duplicates: {total_duplicates}\n")
        
        print(f"\n💾 Results exported to: {output_path}")
        
    except Exception as e:
        logging.error(f"Failed to export results: {e}")


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
        return 1
    
    try:
        # Connect to Plex
        plex = connect_to_plex(plex_url, plex_token)
        
        # Get library
        library = get_library(plex, library_name)
        
        print(f"\n📚 Library: {library.title} ({library.type})")
        print(f"📊 Total items: {library.totalSize}")
        
        # Get similarity threshold from user
        print("\n🎯 Set similarity threshold:")
        print("  0.5 - Very loose matching (may include false positives)")
        print("  0.6 - Balanced matching (recommended)")
        print("  0.7 - Strict matching (may miss some duplicates)")
        print("  0.8 - Very strict matching")
        
        while True:
            try:
                threshold_input = input("\nEnter similarity threshold (0.5-0.9) [default: 0.6]: ").strip()
                if not threshold_input:
                    threshold = 0.6
                    break
                threshold = float(threshold_input)
                if 0.5 <= threshold <= 0.9:
                    break
                else:
                    print("Please enter a value between 0.5 and 0.9")
            except ValueError:
                print("Please enter a valid number")
            except KeyboardInterrupt:
                print("\n\n👋 Operation cancelled by user")
                return 0
        
        # Ask for detail level
        print("\n📋 Choose detail level:")
        print("1. Summary only")
        print("2. Detailed view (includes file paths, descriptions)")
        
        while True:
            try:
                choice = input("\nSelect detail level (1-2) [default: 1]: ").strip()
                if not choice or choice == "1":
                    show_details = False
                    break
                elif choice == "2":
                    show_details = True
                    break
                else:
                    print("Please enter 1 or 2")
            except KeyboardInterrupt:
                print("\n\n👋 Operation cancelled by user")
                return 0
        
        # Get all items from library
        print(f"\n📥 Fetching items from library...")
        items = library.all()
        
        if not items:
            print("❌ No items found in this library.")
            return 1
        
        # Find similar titles
        similar_groups = find_similar_titles(items, threshold)
        
        # Display results
        display_similar_groups(similar_groups, show_details)
        
        # Ask if user wants to export results
        if similar_groups:
            try:
                export_choice = input("\n💾 Export results to file? (y/n) [default: n]: ").strip().lower()
                if export_choice in ['y', 'yes']:
                    export_results(similar_groups)
            except KeyboardInterrupt:
                pass
        
        print(f"\n✅ Analysis complete!")
        
    except KeyboardInterrupt:
        print("\n\n👋 Operation cancelled by user")
        return 0
    except Exception as e:
        logging.error(f"Application error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
