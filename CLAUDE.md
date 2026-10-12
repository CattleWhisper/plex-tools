# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A collection of standalone Python scripts for managing a Plex library of downloaded YouTube videos. There is no package or linter: each script at the repo root is self-contained and run directly. Only `manhwa_organizer.py` has tests (see below). Helpers like `setup_logging`, `connect_to_plex`, `get_library`, `extract_youtube_id` and the cache load/save functions are **copy-pasted across scripts**. When changing one, check whether the same function in the other scripts needs the same change.

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

## manhwa_organizer.py

Organizes YouTube manhwa recaps into the "Manhwa" Plex TV library: one show per story, one episode per part. yt-dlp downloads into an inbox folder outside Plex (`/data/Manhwa-temp`, files named `%(channel)s - %(title)s [%(id)s].ext`). `sync` moves each finished download into `/data/Manhwa/<Show>/Season NN/<Show> - SxxEyy [id].ext`, taking its `.info.json`, subtitles and thumbnail along. The tests (`tests/`) are the reference for the intended behavior.

Pipeline (CLI subcommands): `catalog` → `plan` → `apply` → `refresh`, with `export-watch`, `collections`, and `sync`. `sync` runs catalog, plan, a watch export of renumbered episodes, apply, then refresh of new and moved videos only. Every command that writes has `--dry-run`, and `apply_plan`/`refresh_library` default to dry run.

```bash
.venv/bin/python -m pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python -m pytest -q                          # all tests
.venv/bin/python -m pytest -q tests/test_plan.py -k title
.venv/bin/python manhwa_organizer.py plan              # rebuild state/plan.csv from state/catalog.json
.venv/bin/python manhwa_organizer.py sync --dry-run
```

How it decides things:
- **Catalog** (`catalog_from_disk`): every video in the library tree plus the inbox top level. Inbox videos only count when their ID is in the yt-dlp `--download-archive` file (`download_archive`, a file name inside `inbox_dir`), so unfinished downloads are skipped. Metadata comes from the YouTube Data API through `youtube_hydrator.get_youtube_metadata` and a JSON cache (`metadata_cache`, same format as `youtube_metadata_cache.json`). The `.info.json` is the fallback. When an ID exists twice, the library copy wins.
- **Grouping** (`group_videos`): union-find over videos linked by labelled YouTube links in descriptions (`Part N <url>`; unlabelled links are ignored on purpose), plus same channel + same normalized title (`title_key`). Overrides in `overrides.yaml` (`show_names`, `parts`, `merge`, `detach`) are applied on top.
- **Part number** (`collect_votes`/`decide_part`): weighted votes. Explicit self-claim and timestamp chapters weigh 3, citations by other videos 2, title formats (`Part N`, `(N)`, `[N]`, `[N-M]`, `#N+M`, `Manhwa Recap N`) 1.5, gap inference 1. Description evidence deliberately outranks titles.
- **Episodes** (`assign_episodes`): a compilation covering parts that no individual video has takes those parts. If none exist, it becomes a multi-episode file (`S01E01-E05`); if all exist, it goes to Season 00. Timestamp chapters only count when other videos of the series are numbered (many standalone videos have internal "Parte 1/2" sections). An old unnumbered video before all numbered ones becomes part 1.
- **Stability**: `state/applied.json` pins show names and current paths. Without it, `load_applied` rebuilds it from the library tree, so a fresh state volume does not reapply everything.

Files and safety (`link_mode: move`): `apply` renames on the same filesystem. It only moves known companions (`MANAGED_SIDECAR_SUFFIXES`); yt-dlp leftovers such as `.part`, `.f140.m4a` or `.temp.mp4` stay in the inbox. An inbox file that is a hardlink of its library copy (same inode) is adopted, meaning deleted, and a different file with the same ID is never touched. Deleting a video in Plex is final, and `cleanup_orphans` (inside the library) drops its leftover sidecars and empty folders. The older `hardlink` mode, with a separate Plex library as `source_library`, is still supported. macOS writes `._*` AppleDouble files on the NFS share, and `is_os_junk` ignores them.

`refresh` locks episode title ("Parte N"; specials "Partes 1-16"), summary and date, plus show title and Studio (= channel), and restores watch state. The `collections` command creates one smart collection per channel, filtering on Studio. Plex's API cannot set watched dates, so restored views get the current date.

Config is in `config.yaml` (gitignored; `config.example.yaml` documents it). Plex URL and token fall back to `.env` (`PLEX_URL`, `PLEX_TOKEN`), and so does `YOUTUBE_API_KEY`. Runtime state lives in `state/` (gitignored), and watch-state backups go to `state/backups/`. In production `sync` runs as a Kubernetes CronJob from the Docker image, as uid 1000 with the NFS share mounted at `/data` (the same path Plex sees, so `path_map` is empty) and `state_dir` on a PVC. The manifest lives outside this repo. `youtube_hydrator.py` must not target the "Manhwa" library: it would overwrite the "Parte N" titles.

## Shared state

`youtube_metadata_cache.json` sits next to the scripts and is keyed by video ID, with `title`, `channel_name`, `description` and `published_at`. The hydrator, populate and refresh scripts all read and write this same format. The hydrator checks the cache before calling the API (quota is the main constraint). The populate and refresh scripts write a `.backup` copy before saving. Cache files, `yt_info/*.info.json`, `*cookies.txt`, `.env` and `sandbox/` are gitignored and contain user data.

## CI / Docker

`.github/workflows/docker-build.yml` builds a multi-arch (amd64/arm64) image on pushes and PRs to `master` and on `v*` tags. It pushes the image to `ghcr.io/<repo>` and signs it with cosign, except on PRs. The `Dockerfile` copies only `youtube_hydrator.py` (the default command), `cleanup_orphans.py` and `manhwa_organizer.py` (which needs the `fonts-dejavu-core` package for posters), so other scripts aren't available in the container unless the `COPY` lines change.
