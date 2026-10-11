#!/usr/bin/env python3
"""
Orphan Sidecar Cleanup
When Plex deletes a video it only removes the media file, leaving the yt-dlp sidecars
(.info.json, .jpg/.webp thumbnails, .vtt/.srt subtitles, ...) behind. This script finds
files carrying a YouTube ID ("... [id].ext") whose video no longer exists in the same
directory and deletes them.

Dry-run by default; pass --delete to actually remove files.
"""

import argparse
import logging
import os
import re
import sys
from collections import defaultdict
from typing import Dict, List, Set, Tuple

VIDEO_EXTENSIONS = {
    '.mp4', '.mkv', '.webm', '.m4v', '.mov', '.avi', '.flv', '.ts', '.wmv', '.mpg', '.mpeg',
}
# Leftovers of a download still in progress: they keep their siblings alive and are never deleted
IN_PROGRESS_SUFFIXES = ('.part', '.ytdl', '.temp')

# "<anything> [<11-char id>].<ext...>" -> the ID is the last [...] before the first extension dot
YTID_IN_NAME_RE = re.compile(r'\[([A-Za-z0-9_-]{11})\](?=\.[^\[\]]*$)')


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def extract_id(filename: str) -> str:
    """Return the YouTube ID embedded in a yt-dlp file name, or '' if there is none."""
    match = YTID_IN_NAME_RE.search(filename)
    return match.group(1) if match else ''


def is_video(filename: str) -> bool:
    return os.path.splitext(filename)[1].lower() in VIDEO_EXTENSIONS


def is_in_progress(filename: str) -> bool:
    return filename.lower().endswith(IN_PROGRESS_SUFFIXES)


def find_orphans_in_dir(filenames: List[str]) -> List[str]:
    """Given the file names of one directory, return those whose video is gone."""
    alive_ids: Set[str] = set()
    candidates: Dict[str, List[str]] = defaultdict(list)

    for name in filenames:
        ytid = extract_id(name)
        if not ytid:
            continue
        if is_video(name) or is_in_progress(name):
            alive_ids.add(ytid)
        else:
            candidates[ytid].append(name)

    return sorted(name for ytid, names in candidates.items() if ytid not in alive_ids for name in names)


def find_orphans(roots: List[str]) -> List[str]:
    """Walk the given roots and return absolute paths of orphaned sidecar files."""
    orphans: List[str] = []
    for root in roots:
        for dirpath, _dirnames, filenames in os.walk(root):
            orphans.extend(os.path.join(dirpath, name) for name in find_orphans_in_dir(filenames))
    return orphans


def delete_files(paths: List[str]) -> Tuple[int, int]:
    """Delete the given files. Returns (deleted, bytes_freed)."""
    deleted = freed = 0
    for path in paths:
        try:
            size = os.path.getsize(path)
            os.remove(path)
            deleted += 1
            freed += size
            logging.debug(f"Deleted {path}")
        except OSError as e:
            logging.error(f"Failed to delete {path}: {e}")
    return deleted, freed


def remove_empty_dirs(roots: List[str]) -> int:
    """Remove directories left empty below the roots (the roots themselves are kept)."""
    removed = 0
    for root in roots:
        for dirpath, _dirnames, _filenames in os.walk(root, topdown=False):
            if os.path.abspath(dirpath) == os.path.abspath(root):
                continue
            try:
                if not os.listdir(dirpath):
                    os.rmdir(dirpath)
                    removed += 1
                    logging.info(f"Removed empty directory {dirpath}")
            except OSError as e:
                logging.error(f"Failed to remove directory {dirpath}: {e}")
    return removed


def format_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ('B', 'KB', 'MB', 'GB'):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Delete yt-dlp sidecar files (.info.json, .jpg, .vtt, ...) whose video was removed"
    )
    parser.add_argument('roots', nargs='+', help="Library directories to scan (recursively)")
    parser.add_argument('--delete', action='store_true', help="Actually delete files (default: dry run)")
    parser.add_argument('--remove-empty-dirs', action='store_true',
                        help="With --delete, also remove directories left empty")
    parser.add_argument('--log-level', default=os.getenv('LOG_LEVEL', 'INFO'))
    args = parser.parse_args()

    setup_logging(args.log_level)

    for root in args.roots:
        if not os.path.isdir(root):
            logging.error(f"Not a directory: {root}")
            return 1

    orphans = find_orphans(args.roots)
    total_size = sum(os.path.getsize(p) for p in orphans if os.path.exists(p))

    for path in orphans:
        print(path)

    if not orphans:
        logging.info("No orphaned files found")
    elif not args.delete:
        logging.info(f"DRY RUN: {len(orphans)} orphaned files ({format_size(total_size)}). "
                     f"Re-run with --delete to remove them.")
    else:
        deleted, freed = delete_files(orphans)
        logging.info(f"Deleted {deleted}/{len(orphans)} files, freed {format_size(freed)}")

    if args.delete and args.remove_empty_dirs:
        remove_empty_dirs(args.roots)

    return 0


if __name__ == "__main__":
    sys.exit(main())
