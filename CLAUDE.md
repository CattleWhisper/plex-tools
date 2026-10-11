# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A collection of standalone Python scripts for managing a Plex library of downloaded YouTube videos. There is no package, shared module, test suite, or linter: each script at the repo root is self-contained and run directly. Helpers like `setup_logging`, `connect_to_plex`, `get_library`, `extract_youtube_id` and the cache load/save functions are **copy-pasted across scripts**. When changing one, check whether the same function in the other scripts needs the same change.

## Running

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in values
python youtube_hydrator.py
```

The scripts read configuration from `.env` via `python-dotenv`: `PLEX_URL`, `PLEX_TOKEN`, `LIBRARY_NAME`, `YOUTUBE_API_KEY`, `COOKIES_FILE`, `LOG_LEVEL`. If `LIBRARY_NAME` is unset, `get_library` prompts interactively for a library, so most scripts are not fully non-interactive.

## Scripts

- `youtube_hydrator.py`: the main tool and the default command of the Docker image. For every item in a Plex library it extracts the 11-char YouTube ID from the media file path (patterns like `[id]`, `(id)`, `_id.ext`, `-id.ext`), fetches metadata from the YouTube Data API v3, and edits the Plex item. The title becomes `<channel> - <title>`, and it also sets the summary and the originally-available date. The dry-run/live prompt in `main()` is disabled (`while False:`), so it **always runs live** against Plex. It also disables SSL verification for the YouTube client.
- `sync_watched_to_youtube.py`: finds watched items in Plex and marks them watched on YouTube by shelling out to `yt-dlp --mark-watched` with a cookies file. Synced IDs are tracked in `youtube_synced_videos.json` so they are skipped later.
- `populate_youtube_cache.py`: builds or merges the metadata cache from yt-dlp `yt_info/*.info.json` files, which avoids spending API quota.
- `refresh_youtube_cache.py`: re-fetches every cached ID from the YouTube API and overwrites its entry.
- `cleanup_orphans.py`: filesystem only, no Plex connection. Walks the given directories and removes yt-dlp sidecars (`.info.json`, thumbnails, subtitles) whose `[id]` has no video file in the same directory, which is what's left after Plex deletes a video. It is a dry run unless `--delete` is passed.
- `list_media.py` / `find_similar_titles.py`: read-only Plex reporting. The second one does fuzzy title grouping to spot duplicates.

## Shared state

`youtube_metadata_cache.json` sits next to the scripts and is keyed by video ID, with `title`, `channel_name`, `description` and `published_at`. The hydrator, populate and refresh scripts all read and write this same format. The hydrator checks the cache before calling the API (quota is the main constraint). The populate and refresh scripts write a `.backup` copy before saving. Cache files, `yt_info/*.info.json`, `*cookies.txt`, `.env` and `sandbox/` are gitignored and contain user data.

## CI / Docker

`.github/workflows/docker-build.yml` builds a multi-arch (amd64/arm64) image on pushes and PRs to `master` and on `v*` tags. It pushes the image to `ghcr.io/<repo>` and signs it with cosign, except on PRs. The `Dockerfile` copies only `youtube_hydrator.py` (the default command) and `cleanup_orphans.py`, so other scripts aren't available in the container unless the `COPY` line changes.
