#!/usr/bin/env python3
"""
Manhwa Organizer for Plex
Group YouTube manhwa recap videos (from a Plex "Other Videos" library) into TV shows,
one show per story and one episode per part, preserving watch state.

Usage: manhwa_organizer.py [--config config.yaml] [--overrides overrides.yaml] <command> [--dry-run]
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import yaml
from dotenv import load_dotenv


# --------------------------------------------------------------------------- #
# Regexes
# --------------------------------------------------------------------------- #

URL_RE = re.compile(r'https?://\S+')
YT_ID_RE = re.compile(
    r'(?:youtu\.be/|youtube\.com/(?:watch\?(?:\S*?&)?v=|shorts/|live/|embed/))([A-Za-z0-9_-]{11})'
)
FILE_YTID_RE = re.compile(r'\[([A-Za-z0-9_-]{11})\]\.[^.]+$')
PART_LABEL_RE = re.compile(
    r'(?i)\b(?:part|parte|pt|ep|eps|episode|epis[oó]dio|cap|chapter|cap[ií]tulo)s?\.?\s*#?\s*'
    r'(\d{1,4})(?:\s*(?:-|–|—|~|to|até|a)\s*(\d{1,4}))?\b'
)
NAV_LABEL_RE = re.compile(
    r'(?i)\b(?:previous|prev|next|anterior|pr[oó]xim[oa]|continua[cç][aã]o)\b'
)
SELF_PART_RE = re.compile(
    r'(?i)\b(?:this is|this video is|esta [ée]|essa [ée]|este [ée])\s+(?:the\s+|a\s+)?'
    r'(?:part|parte|ep\w*)\s*#?\s*(\d{1,4})'
)
RECAP_SUFFIX_RE = re.compile(
    r'(?i)\s*(?:(?:[-|｜–—:]\s*)?(?:manhwa\s+recap|resumo\s+manhwa|manhwa\s+cap)!?'
    r'|\(\s*manhwa\s+recap\s*\)|\[\s*manhwa\s+recap\s*\])\s*$'
)
# Channel-specific part numbering at the start of titles:
# "(13) Title", "[13] Title", "[1-17] Title", "(1-4) Title", "#1 Title", "#1+2 Title"
TITLE_LEADING_PART_RE = re.compile(
    r'^\s*(?:[\(\[]\s*(\d{1,4})(?:\s*[-~–—]\s*(\d{1,4}))?\s*[\)\]]'
    r'|#(\d{1,4})((?:\s*\+\s*\d{1,4})*)(?=\s))\s*'
)
# Whole story in one video; removed from names ("Final" is removed but not treated as a compilation)
FULL_MARKER_RE = re.compile(r'(?i)[\[\(]\s*(?:full|complet[oa]|completar)\s*[\]\)]')
END_MARKER_RE = re.compile(r'(?i)[\[\(]\s*(?:full|complet[oa]|completar|final(?:\s+chapter)?)\s*[\]\)]')
TIMESTAMP_RE = re.compile(r'\b\d{1,2}:\d{2}(?::\d{2})?\b')
TITLE_YTID_RE = re.compile(r'\s*\[[A-Za-z0-9_-]{11}\]')
HASHTAG_RE = re.compile(r',?\s*#[^\W\d]\w*')
TITLE_TRAILING_PART_RE = re.compile(r'(?i)(\bmanhwa\s+recap)\s+(\d{1,4})\s*$')
TITLE_SEPARATORS = '-|｜:–—'
INVALID_PATH_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

MAX_RANGE_SPAN = 2000
MIN_TITLE_KEY_LEN = 8
MAX_SHOW_NAME_LEN = 150

Part = Tuple[int, int]  # (start, end); a single part has start == end


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

DEFAULT_CONFIG = {
    'plex': {'url': None, 'token': None},
    'source_library': None,
    'target_library': None,
    'target_root': None,
    'path_map': {},
    'info_json_dir': None,
    'link_mode': 'hardlink',
    'users': 'all',
    'strip_title_patterns': [],
    'state_dir': './state',
    'scan_timeout_minutes': 30,
}


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def load_yaml(path: Optional[str]) -> dict:
    """Load a YAML file, returning {} when it does not exist."""
    if not path or not os.path.exists(path):
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def load_config(path: Optional[str]) -> dict:
    """Load config.yaml merged over the defaults; Plex URL/token/library fall back to .env."""
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
    config = dict(DEFAULT_CONFIG)
    config.update(load_yaml(path))
    plex = dict(DEFAULT_CONFIG['plex'])
    plex.update(config.get('plex') or {})
    plex['url'] = plex['url'] or os.getenv('PLEX_URL')
    plex['token'] = plex['token'] or os.getenv('PLEX_TOKEN')
    config['plex'] = plex
    config['source_library'] = config['source_library'] or os.getenv('LIBRARY_NAME')
    return config


def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(path: str, data) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


# --------------------------------------------------------------------------- #
# Description / title parsing
# --------------------------------------------------------------------------- #

@dataclass
class ParsedDescription:
    links: List[Tuple[str, Part]] = field(default_factory=list)  # YouTube IDs cited with a part label
    nav_links: List[str] = field(default_factory=list)            # YouTube IDs cited with prev/next
    known_numbers: Set[int] = field(default_factory=set)          # every labelled number (yt + non-yt)
    self_part: Optional[int] = None                               # "This is part N"
    timestamp_parts: Set[int] = field(default_factory=set)        # "0:00 - Part 1" chapters (compilation)


def _to_part(match: re.Match) -> Optional[Part]:
    start = int(match.group(1))
    end = int(match.group(2)) if match.group(2) else start
    if end < start or end - start > MAX_RANGE_SPAN:
        return None
    return (start, end)


def parse_part_label(text: str) -> Optional[Part]:
    """Return the last part label in text (closest to whatever follows it)."""
    matches = list(PART_LABEL_RE.finditer(text))
    return _to_part(matches[-1]) if matches else None


def parse_description(summary: str) -> ParsedDescription:
    """Extract labelled links, navigation links and self-identification from a description."""
    parsed = ParsedDescription()
    for line in (summary or '').splitlines():
        if parsed.self_part is None:
            self_match = SELF_PART_RE.search(line)
            if self_match:
                parsed.self_part = int(self_match.group(1))

        if not URL_RE.search(line) and TIMESTAMP_RE.search(line):
            part = parse_part_label(TIMESTAMP_RE.sub(' ', line))
            if part:
                parsed.timestamp_parts.update(range(part[0], part[1] + 1))
            continue

        # The label for a URL is the text between the previous URL and this one
        segment_start = 0
        for url_match in URL_RE.finditer(line):
            segment = line[segment_start:url_match.start()]
            segment_start = url_match.end()

            part = parse_part_label(segment)
            yt_match = YT_ID_RE.search(url_match.group(0))

            if part:
                parsed.known_numbers.update(range(part[0], part[1] + 1))
                if yt_match:
                    parsed.links.append((yt_match.group(1), part))
            elif yt_match and NAV_LABEL_RE.search(segment):
                parsed.nav_links.append(yt_match.group(1))
            # Unlabelled links are ignored on purpose ("see also", trailers, other shows)

    return parsed


def strip_accents(text: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', text) if not unicodedata.combining(c))


def _loose(text: str) -> str:
    """Casefolded alphanumerics only (ignores spacing and fullwidth/halfwidth differences)."""
    return ''.join(c for c in unicodedata.normalize('NFKC', text).casefold() if c.isalnum())


def strip_channel_prefix(title: str, channel: str) -> str:
    """Remove '<channel> <sep>' from the start of title, tolerating spacing/punctuation differences."""
    target = _loose(channel or '')
    if not target:
        return title
    matched, index = 0, 0
    while index < len(title) and matched < len(target):
        piece = _loose(title[index])
        if piece:
            if not target.startswith(piece, matched):
                return title
            matched += len(piece)
        index += 1
    rest = title[index:].lstrip()
    if matched == len(target) and rest[:1] and rest[0] in TITLE_SEPARATORS:
        return rest[1:].lstrip()
    return title


def parse_leading_title_part(text: str) -> Optional[Part]:
    match = TITLE_LEADING_PART_RE.match(text)
    if not match:
        return None
    if match.group(1):
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) else start
    else:
        numbers = [int(match.group(3))] + [int(n) for n in re.findall(r'\d+', match.group(4) or '')]
        start, end = numbers[0], numbers[-1]
    if end < start or end - start > MAX_RANGE_SPAN:
        return None
    return (start, end)


def parse_title_part(title: str, channel: str = '') -> Optional[Part]:
    """Part number from a title: 'Part N' labels, leading '(N)'/'[N-M]'/'#N+M', or '... Manhwa Recap N'."""
    text = strip_channel_prefix(title or '', channel)
    part = parse_part_label(text) or parse_leading_title_part(text)
    if part:
        return part
    match = TITLE_TRAILING_PART_RE.search(text)
    if match:
        number = int(match.group(2))
        return (number, number)
    return None


def is_full_story(title: str) -> bool:
    return bool(FULL_MARKER_RE.search(title or ''))


def clean_title(title: str, channel: str = '', extra_patterns: Optional[List[str]] = None) -> str:
    """Remove channel prefix, recap suffixes and part labels from a video title."""
    original = (title or '').strip()
    text = strip_channel_prefix(original, channel)

    text = TITLE_LEADING_PART_RE.sub('', text)
    text = TITLE_TRAILING_PART_RE.sub(r'\1', text)
    text = END_MARKER_RE.sub('', text)
    text = TITLE_YTID_RE.sub('', text)
    text = HASHTAG_RE.sub('', text)
    text = PART_LABEL_RE.sub('', text)
    text = re.sub(r'[\(\[]\s*[\)\]]', '', text).rstrip(' ' + TITLE_SEPARATORS)  # "(Part 1)" leaves "()"
    previous = None
    while previous != text:
        previous = text
        text = RECAP_SUFFIX_RE.sub('', text)

    for pattern in extra_patterns or []:
        text = re.sub(pattern, '', text)

    text = re.sub(r'\s+', ' ', text).strip(' ' + TITLE_SEPARATORS)
    return text or original


def title_key(title: str, channel: str = '', extra_patterns: Optional[List[str]] = None) -> str:
    """Normalized title used to group videos with identical titles."""
    cleaned = strip_accents(clean_title(title, channel, extra_patterns)).lower()
    return re.sub(r'[^a-z0-9]', '', cleaned)


def sanitize_folder_name(name: str) -> str:
    name = INVALID_PATH_CHARS_RE.sub('', name)
    name = re.sub(r'\s+', ' ', name).strip()
    name = name[:MAX_SHOW_NAME_LEN].rstrip(' .')
    return name or 'Unknown'


def extract_ytid_from_path(path: str) -> Optional[str]:
    match = FILE_YTID_RE.search(path or '')
    return match.group(1) if match else None


# --------------------------------------------------------------------------- #
# Grouping (union-find)
# --------------------------------------------------------------------------- #

class UnionFind:
    def __init__(self):
        self.parent: Dict[str, str] = {}

    def find(self, node: str) -> str:
        self.parent.setdefault(node, node)
        root = node
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[node] != root:
            self.parent[node], node = root, self.parent[node]
        return root

    def union(self, a: str, b: str) -> None:
        root_a, root_b = self.find(a), self.find(b)
        if root_a != root_b:
            # Deterministic root regardless of union order
            if root_b < root_a:
                root_a, root_b = root_b, root_a
            self.parent[root_b] = root_a


def group_videos(videos: Dict[str, dict], parsed: Dict[str, ParsedDescription],
                 overrides: dict, extra_patterns: List[str]) -> List[List[str]]:
    """Return connected components of library videos (phantom IDs excluded)."""
    detached = set(overrides.get('detach') or [])
    uf = UnionFind()

    for ytid in videos:
        uf.find(ytid)

    for ytid, info in parsed.items():
        if ytid in detached:
            continue
        cited = [target for target, _ in info.links] + info.nav_links
        for target in cited:
            if target not in detached:
                uf.union(ytid, target)

    by_key: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for ytid, video in videos.items():
        if ytid in detached:
            continue
        key = title_key(video.get('title', ''), video.get('channel', ''), extra_patterns)
        if len(key) >= MIN_TITLE_KEY_LEN:
            by_key[(video.get('channel', ''), key)].append(ytid)
    for members in by_key.values():
        for other in members[1:]:
            uf.union(members[0], other)

    for group in overrides.get('merge') or []:
        group = [g for g in group if g in videos]
        for other in group[1:]:
            uf.union(group[0], other)

    components: Dict[str, List[str]] = defaultdict(list)
    for ytid in videos:
        components[uf.find(ytid)].append(ytid)
    return list(components.values())


# --------------------------------------------------------------------------- #
# Part number voting
# --------------------------------------------------------------------------- #

@dataclass
class PartDecision:
    part: Optional[Part]
    source: str = ''
    conflict: bool = False
    votes: List[Tuple[Part, float, str]] = field(default_factory=list)
    strength: float = 0  # heaviest single piece of evidence for the chosen part


def infer_part_from_gap(ytid: str, info: ParsedDescription) -> Optional[int]:
    if any(target == ytid for target, _ in info.links) or not info.known_numbers:
        return None
    highest = max(info.known_numbers)
    gaps = [n for n in range(1, highest + 1) if n not in info.known_numbers]
    if not gaps:
        return highest + 1
    if len(gaps) == 1:
        return gaps[0]
    return None


# Description evidence outranks titles (titles vary and are sometimes wrong)
WEIGHT_SELF = 3
WEIGHT_CITED = 2
WEIGHT_TITLE = 1.5
WEIGHT_GAP = 1
STRONG_EVIDENCE = 2


def collect_votes(videos: Dict[str, dict], parsed: Dict[str, ParsedDescription],
                  extra_patterns: List[str]) -> Dict[str, List[Tuple[Part, float, str]]]:
    votes: Dict[str, List[Tuple[Part, float, str]]] = defaultdict(list)

    for ytid, info in parsed.items():
        # "This is part N" is a copy-paste leftover when the same list labels another video as part N
        if info.self_part is not None and not any(
                target != ytid and part[0] <= info.self_part <= part[1] for target, part in info.links):
            votes[ytid].append(((info.self_part, info.self_part), WEIGHT_SELF, 'self'))
        if len(info.timestamp_parts) >= 2:
            span = (min(info.timestamp_parts), max(info.timestamp_parts))
            votes[ytid].append((span, WEIGHT_SELF, 'timestamps'))

        # A description that labels the same video with different numbers is unreliable for it
        numbers_by_target: Dict[str, Set[Part]] = defaultdict(set)
        for target, part in info.links:
            numbers_by_target[target].add(part)
        for target, parts in numbers_by_target.items():
            if target in videos and len(parts) == 1:
                source = 'self-link' if target == ytid else f'cited:{ytid}'
                votes[target].append((next(iter(parts)), WEIGHT_CITED, source))

        gap = infer_part_from_gap(ytid, info)
        if gap is not None:
            votes[ytid].append(((gap, gap), WEIGHT_GAP, 'gap'))

    for ytid, video in videos.items():
        part = parse_title_part(video.get('title', ''), video.get('channel', ''))
        if part:
            votes[ytid].append((part, WEIGHT_TITLE, 'title'))

    return votes


def decide_part(votes: List[Tuple[Part, float, str]], forced=None) -> PartDecision:
    if forced is not None:
        part = parse_forced_part(forced)
        return PartDecision(part=part, source='override', votes=votes, strength=WEIGHT_SELF)
    if not votes:
        return PartDecision(part=None, votes=votes)

    totals: Dict[Part, float] = defaultdict(float)
    best_single: Dict[Part, float] = defaultdict(float)
    sources: Dict[Part, List[str]] = defaultdict(list)
    for part, weight, source in votes:
        totals[part] += weight
        best_single[part] = max(best_single[part], weight)
        sources[part].append(source)

    ranked = sorted(totals, key=lambda p: (-totals[p], -best_single[p], p))
    winner = ranked[0]
    strength = best_single[winner]
    # A title "... Recap 5" on a video cited as "Part 1-5" agrees with the range: not a conflict
    def agrees_with_winner(part: Part) -> bool:
        return (part[0] == part[1] == winner[1] and winner[0] < winner[1]
                and all(s == 'title' for s in sources[part]))

    # Only a close contest is a conflict: one wrong reference against many agreeing ones is noise
    conflict = any(
        p != winner and totals[p] >= STRONG_EVIDENCE and totals[p] * 2 >= totals[winner]
        and not agrees_with_winner(p)
        for p in totals
    )
    source = '+'.join(sorted(set(s.split(':')[0] for s in sources[winner])))
    return PartDecision(part=winner, source=source, conflict=conflict, votes=votes, strength=strength)


def parse_forced_part(value) -> Optional[Part]:
    if isinstance(value, int):
        return (value, value)
    match = re.fullmatch(r'\s*(\d+)\s*(?:-\s*(\d+))?\s*', str(value))
    if not match:
        raise ValueError(f"Invalid part override: {value!r}")
    start = int(match.group(1))
    return (start, int(match.group(2)) if match.group(2) else start)


# --------------------------------------------------------------------------- #
# Episode assignment
# --------------------------------------------------------------------------- #

def _date_key(video: dict) -> Tuple[str, str]:
    return (video.get('date') or '', video['ytid'])


def assign_episodes(members: List[dict]) -> None:
    """
    Fill season/episode/episode_end/flags on each member dict (in place).
    Each member has 'ytid', 'date', 'part' (Part or None) and optionally 'full' (whole story in one video)
    and 'part_strength' (weight of the strongest evidence for its part).
    """
    for member in members:
        member.setdefault('flags', [])
    occupied: Set[int] = set()
    specials: List[dict] = []

    def place(member, start, end, *flags):
        member.update(season=1, episode=start, episode_end=end)
        member['flags'].extend(flags)
        occupied.update(range(start, end + 1))

    weak_duplicates: List[dict] = []
    singles = sorted((m for m in members if m['part'] and m['part'][0] == m['part'][1]),
                     key=lambda m: (-m.get('part_strength', STRONG_EVIDENCE), _date_key(m)))
    for member in singles:
        number = member['part'][0]
        if number not in occupied:
            place(member, number, number)
        elif member.get('part_strength', STRONG_EVIDENCE) >= STRONG_EVIDENCE:
            member['flags'].append('duplicada')
            specials.append(member)
        else:
            # Weak evidence (gap/title) colliding with a taken number: place it by date instead
            weak_duplicates.append(member)

    # The first part often has no reference saying it is part 1: an unnumbered video older than
    # every numbered one takes part 1 when no individual video has it
    unnumbered_first = sorted((m for m in members if not m['part']), key=_date_key)
    numbered_all = [m for m in members if m['part']]
    if (unnumbered_first and numbered_all and 1 not in occupied
            and _date_key(unnumbered_first[0]) < min(_date_key(m) for m in numbered_all)):
        place(unnumbered_first[0], 1, 1, 'parte 1 inferida')

    # Smallest compilations first; a compilation takes the numbers no individual video covers
    # (e.g. "Part 1-5" posted as part 5 goes to E05), or S00 when it adds nothing new
    compilations = sorted(
        (m for m in members if m['part'] and m['part'][0] < m['part'][1]),
        key=lambda m: (m['part'][1] - m['part'][0], _date_key(m))
    )
    for member in compilations:
        start, end = member['part']
        free = [n for n in range(start, end + 1) if n not in occupied]
        if len(free) == end - start + 1:
            place(member, start, end, 'multi-episodio')
        elif free and free[-1] - free[0] == len(free) - 1:
            place(member, free[0], free[-1], 'compilacao parcial')
        else:
            member['flags'].append('compilacao')
            specials.append(member)

    unnumbered = sorted([m for m in members if not m['part'] and 'season' not in m] + weak_duplicates,
                        key=_date_key)
    numbered = [m for m in members if m.get('season') == 1]
    for member in unnumbered:
        if member.get('full') and numbered:
            member['flags'].append('compilacao')
            specials.append(member)
            continue
        if not numbered:
            number = 1
            while number in occupied:
                number += 1
            place(member, number, number)
            continue
        earlier = [m for m in numbered if _date_key(m) <= _date_key(member)]
        after = max(earlier, key=_date_key)['episode_end'] if earlier else 0
        number = after + 1
        while number in occupied:
            number += 1
        place(member, number, number, 'inferido pela data')

    for index, member in enumerate(sorted(specials, key=_date_key), start=1):
        member.update(season=0, episode=index, episode_end=index)


# --------------------------------------------------------------------------- #
# Plan
# --------------------------------------------------------------------------- #

def episode_code(season: int, episode: int, episode_end: int) -> str:
    code = f"S{season:02d}E{episode:02d}"
    if episode_end != episode:
        code += f"-E{episode_end:02d}"
    return code


def target_relpath(show: str, member: dict) -> str:
    ext = os.path.splitext(member.get('file') or '')[1] or '.mp4'
    code = episode_code(member['season'], member['episode'], member['episode_end'])
    season_dir = f"Season {member['season']:02d}"
    return os.path.join(show, season_dir, f"{show} - {code} [{member['ytid']}]{ext}")


def dedupe_catalog(catalog: List[dict]) -> Tuple[Dict[str, dict], Dict[str, List[dict]]]:
    """
    Index catalog by YouTube ID. When one ID has several files (video re-downloaded after a
    title change), keep the most recently added one and return the others as ignored.
    """
    by_id: Dict[str, List[dict]] = defaultdict(list)
    for entry in catalog:
        if entry.get('ytid'):
            by_id[entry['ytid']].append(entry)

    videos, ignored = {}, {}
    for ytid, entries in by_id.items():
        entries.sort(key=lambda e: (e.get('addedAt') or '', e.get('ratingKey') or 0), reverse=True)
        videos[ytid] = entries[0]
        if len(entries) > 1:
            ignored[ytid] = entries[1:]
            logging.warning(f"Duplicate YouTube ID {ytid}: keeping {entries[0].get('file')}")
    return videos, ignored


def build_plan(catalog: List[dict], overrides: Optional[dict] = None,
               applied: Optional[dict] = None, extra_patterns: Optional[List[str]] = None) -> dict:
    """
    Build the reorganization plan from catalog entries.
    applied: previous state ({ytid: {'show': name, ...}}) used to keep show names stable.
    """
    overrides = overrides or {}
    applied = applied or {}
    extra_patterns = extra_patterns or []

    videos, ignored_duplicates = dedupe_catalog(catalog)
    parsed = {ytid: parse_description(video.get('summary', '')) for ytid, video in videos.items()}
    votes = collect_votes(videos, parsed, extra_patterns)
    forced_parts = overrides.get('parts') or {}
    show_overrides = overrides.get('show_names') or {}

    series = []
    for component in group_videos(videos, parsed, overrides, extra_patterns):
        component_votes = {ytid: votes.get(ytid, []) for ytid in component}
        # "00:00 - Parte 1" chapters are often just sections of a standalone video; they only
        # count as a compilation range when other videos of the series carry real part numbers
        if not any(source != 'timestamps' for vs in component_votes.values() for _, _, source in vs):
            component_votes = {ytid: [v for v in vs if v[2] != 'timestamps'] for ytid, vs in component_votes.items()}

        members = []
        for ytid in component:
            video = videos[ytid]
            decision = decide_part(component_votes[ytid], forced_parts.get(ytid))
            flags = ['conflito'] if decision.conflict else []
            if ytid in ignored_duplicates:
                flags.append('id duplicado')
            members.append({
                'ytid': ytid,
                'title': video.get('title', ''),
                'channel': video.get('channel', ''),
                'date': video.get('date') or '',
                'file': video.get('file', ''),
                'part': decision.part,
                'part_source': decision.source,
                'part_strength': decision.strength,
                'full': is_full_story(video.get('title', '')),
                'votes': [[list(p), w, s] for p, w, s in decision.votes],
                'flags': flags,
            })
        assign_episodes(members)
        members.sort(key=lambda m: (m['season'] == 0, m['season'], m['episode'], _date_key(m)))
        series.append({'members': members, 'name_source': ''})

    for show in series:
        members = show['members']
        ids = [m['ytid'] for m in members]
        override = next((show_overrides[i] for i in ids if i in show_overrides), None)
        pinned = next((applied[i]['show'] for i in ids if applied.get(i, {}).get('show')), None)
        if override:
            name, source = override, 'override'
        elif pinned:
            name, source = pinned, 'estado'
        else:
            season_one = [m for m in members if m['season'] == 1]
            base = min(season_one, key=lambda m: m['episode']) if season_one else min(members, key=_date_key)
            name, source = clean_title(base['title'], base['channel'], extra_patterns), 'titulo'
        show['name'] = sanitize_folder_name(name)
        show['name_source'] = source
        show['channel'] = members[0]['channel']
        show['first_date'] = min(m['date'] for m in members)

    resolve_name_collisions(series)

    for show in series:
        for member in show['members']:
            member['show'] = show['name']
            member['code'] = episode_code(member['season'], member['episode'], member['episode_end'])
            member['target'] = target_relpath(show['name'], member)
            if member['part']:
                member['part'] = list(member['part'])

    series.sort(key=lambda s: s['name'].casefold())
    summary = summarize(series)
    summary['ignored_duplicate_files'] = sum(len(v) for v in ignored_duplicates.values())
    return {'series': series, 'summary': summary,
            'ignored_duplicates': {k: [e.get('file') for e in v] for k, v in ignored_duplicates.items()}}


def resolve_name_collisions(series: List[dict]) -> None:
    """Disambiguate identical show names: keep the pinned/oldest, then '(Channel)', then '(2)'."""
    priority = {'override': 0, 'estado': 1, 'titulo': 2}
    by_name: Dict[str, List[dict]] = defaultdict(list)
    for show in series:
        by_name[show['name'].casefold()].append(show)

    for shows in by_name.values():
        if len(shows) < 2:
            continue
        shows.sort(key=lambda s: (priority[s['name_source']], s['first_date']))
        for show in shows[1:]:
            if show['channel']:
                show['name'] = sanitize_folder_name(f"{show['name']} ({show['channel']})")

    used: Set[str] = set()
    for show in sorted(series, key=lambda s: (priority[s['name_source']], s['first_date'])):
        name, counter = show['name'], 2
        while name.casefold() in used:
            name = sanitize_folder_name(f"{show['name']} ({counter})")
            counter += 1
        show['name'] = name
        used.add(name.casefold())


def summarize(series: List[dict]) -> dict:
    members = [m for s in series for m in s['members']]
    return {
        'videos': len(members),
        'series': len(series),
        'series_with_multiple_videos': sum(1 for s in series if len(s['members']) > 1),
        'multi_episode_files': sum(1 for m in members if m['season'] == 1 and m['episode_end'] != m['episode']),
        'specials': sum(1 for m in members if m['season'] == 0),
        'conflicts': sum(1 for m in members if 'conflito' in m['flags']),
        'inferred_by_date': sum(1 for m in members if 'inferido pela data' in m['flags']),
        'duplicates': sum(1 for m in members if 'duplicada' in m['flags']),
    }


PLAN_CSV_FIELDS = ['show', 'code', 'ytid', 'title', 'date', 'part', 'part_source', 'flags', 'channel', 'file', 'target']


def write_plan(plan: dict, state_dir: str) -> Tuple[str, str]:
    json_path = os.path.join(state_dir, 'plan.json')
    csv_path = os.path.join(state_dir, 'plan.csv')
    save_json(json_path, plan)
    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=PLAN_CSV_FIELDS, extrasaction='ignore')
        writer.writeheader()
        for show in plan['series']:
            for member in show['members']:
                row = dict(member)
                row['part'] = '-'.join(str(n) for n in dict.fromkeys(member['part'])) if member['part'] else ''
                row['flags'] = '; '.join(member['flags'])
                writer.writerow(row)
    return json_path, csv_path


def print_plan_summary(summary: dict) -> None:
    print("\n📋 Plan summary")
    print(f"   🎞️  Videos:                    {summary['videos']}")
    print(f"   📺 Series:                    {summary['series']} "
          f"({summary['series_with_multiple_videos']} with more than one video)")
    print(f"   🧩 Multi-episode files:       {summary['multi_episode_files']}")
    print(f"   ⭐ Specials (Season 00):      {summary['specials']} ({summary['duplicates']} duplicates)")
    print(f"   ⚠️  Conflicts:                 {summary['conflicts']}")
    print(f"   📅 Inferred by date:          {summary['inferred_by_date']}")
    print(f"   👯 Ignored duplicate files:   {summary.get('ignored_duplicate_files', 0)}")


# --------------------------------------------------------------------------- #
# Plex: catalog and watch-state export (read-only)
# --------------------------------------------------------------------------- #

OWNER = '__owner__'


def connect_to_plex(config: dict):
    """Connect to Plex server using URL and token from config/.env."""
    from plexapi.server import PlexServer

    url, token = config['plex']['url'], config['plex']['token']
    if not url or not token:
        raise ValueError("Plex URL and token must be set in config.yaml or .env (PLEX_URL / PLEX_TOKEN)")
    logging.info(f"Connecting to Plex server at {url}")
    plex = PlexServer(url, token, timeout=120)
    logging.info(f"Connected to Plex server: {plex.friendlyName} ({plex.version})")
    return plex


def item_file(item) -> Optional[str]:
    if not item.media or not item.media[0].parts:
        return None
    return item.media[0].parts[0].file


def channel_from_path(path: str) -> str:
    """Channel is the basename part before the first ' - ' (yt-dlp template '%(channel)s - %(title)s [%(id)s]')."""
    base = os.path.basename(path)
    return base.split(' - ', 1)[0].strip() if ' - ' in base else ''


def item_date(item) -> str:
    value = getattr(item, 'originallyAvailableAt', None) or getattr(item, 'addedAt', None)
    return value.strftime('%Y-%m-%d') if value else ''


def plex_to_local(path: str, path_map: Dict[str, str]) -> str:
    """Translate a path seen by Plex into the local path (longest matching prefix wins)."""
    for prefix in sorted(path_map or {}, key=len, reverse=True):
        if path == prefix or path.startswith(prefix.rstrip('/') + '/'):
            return os.path.expanduser(path_map[prefix]) + path[len(prefix.rstrip('/')):]
    return path


def info_json_path(plex_path: str, config: dict) -> str:
    """yt-dlp .info.json next to the video, or in info_json_dir when configured."""
    base = os.path.splitext(os.path.basename(plex_path))[0] + '.info.json'
    if config.get('info_json_dir'):
        return os.path.join(os.path.expanduser(config['info_json_dir']), base)
    return os.path.join(os.path.dirname(plex_to_local(plex_path, config.get('path_map'))), base)


def channel_from_info_json(path: str) -> Optional[str]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            channel = json.load(f).get('channel')
        return channel.strip() if channel else None
    except (OSError, json.JSONDecodeError):
        return None


def catalog_entry(item, config: Optional[dict] = None, known_channels: Optional[Dict[str, str]] = None) -> Optional[dict]:
    """known_channels: {ytid: channel} already read from .info.json (avoids re-reading every file over NFS)."""
    path = item_file(item)
    ytid = extract_ytid_from_path(path or '')
    if not ytid:
        return None
    channel = (known_channels or {}).get(ytid)
    if channel is None and config is not None:
        channel = channel_from_info_json(info_json_path(path, config))
    added = getattr(item, 'addedAt', None)
    return {
        'ytid': ytid,
        'channel': channel or channel_from_path(path),
        'channel_source': 'info.json' if channel else 'filename',
        'title': item.title or '',
        'summary': item.summary or '',
        'date': item_date(item),
        'addedAt': added.isoformat() if added else '',
        'ratingKey': item.ratingKey,
        'thumb': item.thumb,
        'file': path,
    }


def build_catalog(section, config: Optional[dict] = None,
                  previous: Optional[List[dict]] = None) -> Tuple[List[dict], List[str]]:
    known_channels = {e['ytid']: e['channel'] for e in previous or [] if e.get('channel_source') == 'info.json'}
    entries, skipped = [], []
    for item in section.all():
        entry = catalog_entry(item, config, known_channels)
        if entry:
            entries.append(entry)
        else:
            skipped.append(item_file(item) or item.title)
            logging.warning(f"No YouTube ID in file path, skipping: {skipped[-1]}")
    return entries, skipped


def plex_user_names(plex, users_setting) -> List[str]:
    """Other Plex users whose watch state must be migrated ('all' | [] | [names])."""
    if users_setting == 'all':
        return [user.title for user in plex.myPlexAccount().users()]
    return list(users_setting or [])


def watch_state_of_section(section, ids: Optional[Set[str]] = None) -> Dict[str, dict]:
    """{ytid: {'played': bool, 'offset': ms}} for items with any watch state."""
    items = section.searchEpisodes() if section.type == 'show' else section.all()
    state: Dict[str, dict] = {}
    for item in items:
        ytid = extract_ytid_from_path(item_file(item) or '')
        if not ytid or (ids is not None and ytid not in ids):
            continue
        played = bool(item.viewCount)
        offset = getattr(item, 'viewOffset', 0) or 0
        if not played and not offset:
            continue
        # A multi-episode file maps one ID to several episodes: keep the most advanced state
        previous = state.get(ytid, {'played': False, 'offset': 0})
        state[ytid] = {'played': played or previous['played'], 'offset': max(offset, previous['offset'])}
    return state


def export_watch(plex, library: str, users_setting, ids: Optional[Set[str]] = None) -> Dict[str, Dict[str, dict]]:
    """Export watch state of a library for the owner and every configured user."""
    result = {OWNER: watch_state_of_section(plex.library.section(library), ids)}
    for name in plex_user_names(plex, users_setting):
        try:
            user_server = plex.switchUser(name)
            result[name] = watch_state_of_section(user_server.library.section(library), ids)
        except Exception as e:
            logging.warning(f"Could not export watch state for user '{name}': {e}")
    return result


# --------------------------------------------------------------------------- #
# Apply: target tree (links, thumbs, posters)
# --------------------------------------------------------------------------- #

LINK_MODES = ('hardlink', 'symlink', 'copy')
POSTER_SIZE = (1000, 1500)
POSTER_MAX_LINES = 5
POSTER_FONT_CANDIDATES = [
    'DejaVuSans-Bold.ttf',
    '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
    '/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf',
    '/System/Library/Fonts/Supplemental/Arial Bold.ttf',
    '/Library/Fonts/Arial Bold.ttf',
]
SERIES_ASSETS = ('poster.jpg', 'background.jpg')
# Created by macOS on network shares; never ours, only removed together with an orphan folder
def is_os_junk(name: str) -> bool:
    return name.startswith('._') or name in ('.DS_Store', 'Thumbs.db')


VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.webm', '.avi', '.mov', '.m4v', '.ts'}


@dataclass
class Action:
    kind: str          # link | thumb | poster | background | remove | rmdir | error
    path: str
    detail: str = ''


@dataclass
class ApplyResult:
    actions: List[Action] = field(default_factory=list)
    new: List[str] = field(default_factory=list)
    moved: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    applied: Dict[str, dict] = field(default_factory=dict)

    def add(self, kind: str, path: str, detail: str = '') -> None:
        self.actions.append(Action(kind, path, detail))

    def count(self, kind: str) -> int:
        return sum(1 for a in self.actions if a.kind == kind)


def is_inside(path: str, root: str) -> bool:
    path, root = os.path.realpath(path), os.path.realpath(root)
    return path == root or path.startswith(root.rstrip(os.sep) + os.sep)


def thumb_path_for(video_path: str) -> str:
    return os.path.splitext(video_path)[0] + '.jpg'


def same_file(a: str, b: str) -> bool:
    try:
        return os.path.samefile(a, b)
    except OSError:
        return False


def nearest_existing_dir(path: str) -> str:
    while not os.path.exists(path):
        parent = os.path.dirname(path)
        if parent == path:
            break
        path = parent
    return path


def validate_apply_config(config: dict, source_dirs: Set[str]) -> List[str]:
    """Return a list of fatal problems with the target configuration."""
    problems = []
    target_root = config.get('target_root')
    if not target_root:
        return ["target_root is not set in config.yaml"]
    if config.get('link_mode') not in LINK_MODES:
        problems.append(f"link_mode must be one of {LINK_MODES}")
    for source_dir in source_dirs:
        if is_inside(target_root, source_dir) or is_inside(source_dir, target_root):
            problems.append(f"target_root {target_root} overlaps the source library folder {source_dir}")
    return problems


def target_writable_problem(target_root: str) -> Optional[str]:
    """Try to create and delete a file in target_root (or its nearest existing parent)."""
    directory = nearest_existing_dir(target_root)
    probe = os.path.join(directory, f".manhwa_organizer_write_test_{os.getpid()}")
    try:
        with open(probe, 'w') as f:
            f.write('ok')
        os.remove(probe)
        return None
    except OSError as e:
        return f"cannot write to {directory}: {e.strerror}"


def save_jpeg(data: bytes, path: str) -> None:
    """Convert any image to JPEG and write it atomically."""
    from io import BytesIO
    from PIL import Image

    image = Image.open(BytesIO(data)).convert('RGB')
    tmp = f"{path}.tmp"
    image.save(tmp, 'JPEG', quality=90)
    os.replace(tmp, path)


def load_poster_font(size: int):
    from PIL import ImageFont

    for candidate in POSTER_FONT_CANDIDATES:
        try:
            return ImageFont.truetype(candidate, size)
        except OSError:
            continue
    return ImageFont.load_default(size=size)


def wrap_text(draw, text: str, font, max_width: int) -> List[str]:
    """Greedy word wrap by rendered pixel width; very long words are split by characters."""
    lines: List[str] = []
    current = ''
    for word in text.split():
        candidate = f"{current} {word}".strip()
        if draw.textlength(candidate, font=font) <= max_width:
            current = candidate
            continue
        if current:
            lines.append(current)
        current = ''
        for char in word:
            if draw.textlength(current + char, font=font) > max_width and current:
                lines.append(current)
                current = ''
            current += char
    if current:
        lines.append(current)
    return lines


def fit_poster_text(draw, text: str, max_width: int, max_height: int):
    """Largest font size whose wrapped text fits in the box (at most POSTER_MAX_LINES lines)."""
    for size in range(96, 27, -4):
        font = load_poster_font(size)
        lines = wrap_text(draw, text, font, max_width)
        line_height = int(size * 1.2)
        if len(lines) <= POSTER_MAX_LINES and len(lines) * line_height <= max_height:
            return font, lines, line_height
    font = load_poster_font(28)
    lines = wrap_text(draw, text, font, max_width)
    if len(lines) > POSTER_MAX_LINES:
        lines = lines[:POSTER_MAX_LINES]
        lines[-1] = lines[-1].rstrip(' .') + '…'
    return font, lines, int(28 * 1.2)


def make_poster(thumb_path: str, title: str, out_path: str) -> None:
    """2:3 poster: blurred dark cover background, full-width 16:9 thumb, show name below."""
    from PIL import Image, ImageDraw, ImageEnhance, ImageFilter

    width, height = POSTER_SIZE
    thumb = Image.open(thumb_path).convert('RGB')

    scale = max(width / thumb.width, height / thumb.height)
    background = thumb.resize((round(thumb.width * scale) + 1, round(thumb.height * scale) + 1), Image.LANCZOS)
    left, top = (background.width - width) // 2, (background.height - height) // 2
    background = background.crop((left, top, left + width, top + height))
    background = background.filter(ImageFilter.GaussianBlur(40))
    background = ImageEnhance.Brightness(background).enhance(0.45)

    front_height = round(thumb.height * width / thumb.width)
    front = thumb.resize((width, front_height), Image.LANCZOS)
    front_top = round(height * 0.37 - front_height / 2)
    background.paste(front, (0, front_top))

    draw = ImageDraw.Draw(background)
    margin = 60
    text_top = front_top + front_height + 50
    font, lines, line_height = fit_poster_text(draw, title, width - 2 * margin, height - text_top - margin)
    y = text_top
    for line in lines:
        x = (width - draw.textlength(line, font=font)) / 2
        draw.text((x + 3, y + 3), line, font=font, fill=(0, 0, 0))
        draw.text((x, y), line, font=font, fill=(255, 255, 255))
        y += line_height

    tmp = f"{out_path}.tmp"
    background.save(tmp, 'JPEG', quality=90)
    os.replace(tmp, out_path)


SIDECAR_IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.webp', '.png')


def sidecar_image(source: str) -> Optional[str]:
    """Thumbnail written by yt-dlp next to the video (same name, image extension)."""
    base = os.path.splitext(source)[0]
    return next((base + ext for ext in SIDECAR_IMAGE_EXTENSIONS if os.path.exists(base + ext)), None)


def make_image_fetcher(connect_plex, catalog_by_id: Dict[str, dict], timeout: int = 30):
    """
    Episode thumb bytes: yt-dlp sidecar image first (the same file Plex shows), then the Plex
    thumb, then YouTube maxres/hq. connect_plex is called only if a sidecar is missing.
    """
    import requests
    plex_holder = {}

    def fetch(ytid: str, source: str = '') -> Optional[bytes]:
        sidecar = sidecar_image(source) if source else None
        if sidecar:
            with open(sidecar, 'rb') as f:
                return f.read()

        urls = []
        entry = catalog_by_id.get(ytid) or {}
        if entry.get('thumb'):
            if 'plex' not in plex_holder:
                plex_holder['plex'] = connect_plex()
            urls.append(plex_holder['plex'].url(entry['thumb'], includeToken=True))
        urls += [f"https://i.ytimg.com/vi/{ytid}/maxresdefault.jpg",
                 f"https://i.ytimg.com/vi/{ytid}/hqdefault.jpg"]
        for url in urls:
            try:
                response = requests.get(url, timeout=timeout)
                if response.ok and response.content:
                    return response.content
            except requests.RequestException as e:
                logging.debug(f"Thumb download failed for {ytid}: {e}")
        return None

    return fetch


def link_file(source: str, target: str, mode: str) -> None:
    if mode == 'hardlink':
        os.link(source, target)
    elif mode == 'symlink':
        os.symlink(source, target)
    else:
        import shutil
        shutil.copy2(source, target)


def link_is_current(source: str, target: str, mode: str) -> bool:
    if mode == 'symlink':
        return os.path.islink(target) and os.readlink(target) == source
    if mode == 'copy':
        return os.path.exists(target) and os.path.getsize(target) == os.path.getsize(source)
    return same_file(source, target)


def remove_managed_file(path: str, target_root: str, result: ApplyResult, dry_run: bool, detail: str) -> None:
    """Delete a file we created; refuses anything outside target_root."""
    if not os.path.lexists(path):
        return
    if not is_inside(os.path.dirname(path), target_root):
        result.add('error', path, 'refusing to delete outside target_root')
        return
    result.add('remove', path, detail)
    if not dry_run:
        os.remove(path)


def rebuild_applied_from_tree(target_root: str) -> Dict[str, dict]:
    """Recover applied.json from the target tree ('<Show>/Season NN/<Show> - SxxEyy [ytid].ext')."""
    applied: Dict[str, dict] = {}
    if not os.path.isdir(target_root):
        return applied
    for dirpath, _, filenames in os.walk(target_root):
        for filename in filenames:
            if is_os_junk(filename) or os.path.splitext(filename)[1].lower() not in VIDEO_EXTENSIONS:
                continue
            ytid = extract_ytid_from_path(filename)
            if not ytid:
                continue
            target = os.path.relpath(os.path.join(dirpath, filename), target_root)
            if ytid in applied:
                logging.warning(f"{ytid} found twice in target tree: {applied[ytid]['target']} and {target}")
                continue
            applied[ytid] = {'show': target.split(os.sep, 1)[0], 'target': target}
    return applied


def load_applied(config: dict) -> Dict[str, dict]:
    """applied.json, or (empty state, e.g. a fresh volume) rebuilt from the existing target tree and saved."""
    path = os.path.join(config['state_dir'], 'applied.json')
    if os.path.exists(path):
        return load_json(path, {})
    applied = rebuild_applied_from_tree(os.path.expanduser(config['target_root'] or ''))
    if applied:
        save_json(path, applied)
        print(f"♻️  Rebuilt {path} from the target tree ({len(applied)} videos)")
    return applied


def plan_members(plan: dict) -> List[Tuple[dict, dict]]:
    return [(show, member) for show in plan['series'] for member in show['members']]


def apply_plan(plan: dict, config: dict, applied: Dict[str, dict], fetch_image,
               dry_run: bool = True, force_posters: bool = False) -> ApplyResult:
    """
    Build/update the target tree from the plan. Original files are only ever read.
    applied: previous state {ytid: {'show', 'target', ...}}; returns the new one in result.applied.
    """
    result = ApplyResult()
    path_map = config.get('path_map') or {}
    mode = config.get('link_mode', 'hardlink')
    target_root = os.path.expanduser(config['target_root'])
    desired = {member['ytid']: (show, member) for show, member in plan_members(plan)}

    # 1. Files of videos that left the plan or changed place
    for ytid, previous in applied.items():
        current = desired.get(ytid)
        if current and current[1]['target'] == previous.get('target'):
            continue
        (result.moved if current else result.removed).append(ytid)
        old_video = os.path.join(target_root, previous['target'])
        remove_managed_file(old_video, target_root, result, dry_run, f"{'moved' if current else 'removed'} {ytid}")
        remove_managed_file(thumb_path_for(old_video), target_root, result, dry_run, f"thumb of {ytid}")

    # 2. Links and episode thumbs
    for ytid, (show, member) in desired.items():
        if ytid not in applied:
            result.new.append(ytid)
        source = plex_to_local(member['file'], path_map)
        target = os.path.join(target_root, member['target'])
        result.applied[ytid] = {'show': show['name'], 'season': member['season'], 'episode': member['episode'],
                                'episode_end': member['episode_end'], 'target': member['target']}

        if not os.path.exists(source):
            result.add('error', target, f"source missing: {source}")
            result.applied.pop(ytid)
            continue
        if os.path.lexists(target):
            if not link_is_current(source, target, mode):
                result.add('error', target, f"exists and is not a {mode} of {source}")
        else:
            result.add('link', target, source)
            if not dry_run:
                try:
                    os.makedirs(os.path.dirname(target), exist_ok=True)
                    link_file(source, target, mode)
                except OSError as e:
                    result.add('error', target, f"link failed: {e}")
                    result.applied.pop(ytid)
                    continue

        thumb = thumb_path_for(target)
        if not os.path.exists(thumb):
            result.add('thumb', thumb, ytid)
            if not dry_run:
                try:
                    data = fetch_image(ytid, source)
                    if data:
                        save_jpeg(data, thumb)
                    else:
                        result.add('error', thumb, f"no thumbnail found for {ytid}")
                except (OSError, ValueError) as e:
                    result.add('error', thumb, f"thumbnail failed: {e}")

    # 3. Series poster (2:3) and background, from the first part's thumb
    for show in plan['series']:
        members = [m for m in show['members'] if m['ytid'] in result.applied]
        if not members:
            continue
        first = min(members, key=lambda m: (m['season'] == 0, m['episode']))
        first_thumb = thumb_path_for(os.path.join(target_root, first['target']))
        show_dir = os.path.join(target_root, show['name'])
        poster, background = os.path.join(show_dir, 'poster.jpg'), os.path.join(show_dir, 'background.jpg')

        try:
            if force_posters or not os.path.exists(poster):
                result.add('poster', poster, first['ytid'])
                if not dry_run and os.path.exists(first_thumb):
                    make_poster(first_thumb, show['name'], poster)
            if not os.path.exists(background):
                result.add('background', background, first['ytid'])
                if not dry_run and os.path.exists(first_thumb):
                    import shutil
                    shutil.copyfile(first_thumb, background)
        except (OSError, ValueError) as e:
            result.add('error', show_dir, f"poster/background failed: {e}")

    # 4. Show folders left without videos: drop our assets and empty folders
    if os.path.isdir(target_root):
        cleanup_orphans(target_root, result, dry_run)
    return result


def cleanup_orphans(target_root: str, result: ApplyResult, dry_run: bool) -> None:
    """Remove orphan episode thumbs, series assets and empty folders inside target_root."""
    removed_paths = {a.path for a in result.actions if a.kind == 'remove'}
    linked_paths = {a.path for a in result.actions if a.kind == 'link'}

    def alive(path: str) -> bool:
        return (os.path.exists(path) and path not in removed_paths) or path in linked_paths

    for dirpath, dirnames, filenames in os.walk(target_root, topdown=False):
        if dirpath == target_root:
            continue
        junk = [os.path.join(dirpath, f) for f in filenames if is_os_junk(f)]
        files = [os.path.join(dirpath, f) for f in filenames if not is_os_junk(f)]
        videos = [f for f in files if os.path.splitext(f)[1].lower() in VIDEO_EXTENSIONS and alive(f)]
        for path in files:
            if path.endswith('.jpg') and path not in removed_paths and os.path.basename(path) not in SERIES_ASSETS:
                siblings = [v for v in videos if os.path.splitext(v)[0] == os.path.splitext(path)[0]]
                if not siblings:
                    remove_managed_file(path, target_root, result, dry_run, 'orphan thumb')

        subdirs_alive = any(os.path.isdir(os.path.join(dirpath, d)) and
                            os.path.join(dirpath, d) not in {a.path for a in result.actions if a.kind == 'rmdir'}
                            for d in dirnames)
        if videos or subdirs_alive:
            continue
        for asset in SERIES_ASSETS:
            asset_path = os.path.join(dirpath, asset)
            if asset_path in files and asset_path not in removed_paths:
                remove_managed_file(asset_path, target_root, result, dry_run, 'orphan series asset')
        leftovers = [f for f in files if f not in {a.path for a in result.actions if a.kind == 'remove'}]
        if leftovers:
            result.add('error', dirpath, f"not removed, unknown files: {[os.path.basename(f) for f in leftovers]}")
            continue
        for path in junk:
            remove_managed_file(path, target_root, result, dry_run, 'os junk')
        result.add('rmdir', dirpath)
        if not dry_run:
            os.rmdir(dirpath)


def print_apply_report(result: ApplyResult, dry_run: bool, report_path: Optional[str]) -> None:
    title = "🧪 DRY RUN: nothing was changed" if dry_run else "✅ Apply finished"
    print(f"\n{title}")
    labels = [('link', '🔗 Video links'), ('thumb', '🖼️  Episode thumbs'), ('poster', '🎨 Posters'),
              ('background', '🌄 Backgrounds'), ('remove', '🗑️  Removed files'), ('rmdir', '📁 Removed folders'),
              ('error', '❌ Errors')]
    for kind, label in labels:
        print(f"   {label + ':':22} {result.count(kind)}")
    print(f"   🆕 New videos:          {len(result.new)}")
    print(f"   🔀 Moved videos:        {len(result.moved)}")
    print(f"   ➖ Removed videos:      {len(result.removed)}")
    for action in [a for a in result.actions if a.kind == 'error'][:10]:
        print(f"   ❌ {action.path}: {action.detail}")
    if report_path:
        with open(report_path, 'w', encoding='utf-8') as f:
            for action in result.actions:
                f.write(f"{action.kind}\t{action.path}\t{action.detail}\n")
        print(f"\n📝 Full action list: {report_path}")


# --------------------------------------------------------------------------- #
# Refresh: scan target library, write metadata, restore watch state
# --------------------------------------------------------------------------- #

SCAN_POLL_SECONDS = 10


def local_to_plex(path: str, path_map: Dict[str, str]) -> str:
    """Inverse of plex_to_local (longest matching local prefix wins)."""
    pairs = sorted(((os.path.expanduser(local).rstrip('/'), prefix.rstrip('/'))
                    for prefix, local in (path_map or {}).items()), key=lambda p: len(p[0]), reverse=True)
    for local, prefix in pairs:
        if path == local or path.startswith(local + '/'):
            return prefix + path[len(local):]
    return path


def expected_plex_paths(applied: Dict[str, dict], ids: Set[str], config: dict) -> Dict[str, str]:
    target_root = os.path.expanduser(config['target_root'])
    return {ytid: local_to_plex(os.path.join(target_root, applied[ytid]['target']), config.get('path_map'))
            for ytid in ids if ytid in applied}


def select_refresh_ids(applied: Dict[str, dict], last_apply: dict, all_ids: bool,
                       only: Optional[List[str]]) -> Set[str]:
    """--all: everything applied; --only: whole series of the given IDs; default: last apply's new+moved."""
    if all_ids:
        return set(applied)
    if only:
        shows = {applied[ytid]['show'] for ytid in only if ytid in applied}
        return {ytid for ytid, entry in applied.items() if entry['show'] in shows}
    return set(last_apply.get('new', [])) | set(last_apply.get('moved', []))


def index_episodes_by_path(section) -> Dict[str, list]:
    index: Dict[str, list] = defaultdict(list)
    for episode in section.searchEpisodes():
        # Unset fields (e.g. no date yet) would otherwise trigger one full reload request per episode
        episode._autoReload = False
        path = item_file(episode)
        if path:
            index[path].append(episode)
    return index


def wait_for_episodes(section, expected: Dict[str, str], timeout_seconds: float, interval: float = SCAN_POLL_SECONDS,
                      sleep=None, clock=None) -> Tuple[Dict[str, list], Set[str]]:
    """Poll until every expected ID exists as episode(s) at its expected path, or timeout."""
    import time
    sleep = sleep or time.sleep
    clock = clock or time.monotonic
    deadline = clock() + timeout_seconds
    while True:
        index = index_episodes_by_path(section)
        found = {ytid: index[path] for ytid, path in expected.items() if path in index}
        missing = set(expected) - set(found)
        if not missing or clock() >= deadline:
            return found, missing
        logging.info(f"Waiting for Plex to scan {len(missing)} episode(s)...")
        sleep(interval)


def episode_title(season: int, index: int, part: Optional[List[int]]) -> str:
    """'Parte N' (the show name already carries the story title); specials show their part range."""
    if season != 0:
        return f"Parte {index}"
    if part and part[0] != part[1]:
        return f"Partes {part[0]}-{part[1]}"
    if part:
        return f"Parte {part[0]}"
    return f"Especial {index}"


def episode_metadata(entry: dict, episode, part: Optional[List[int]] = None) -> Dict[str, str]:
    return {
        'title': episode_title(episode.seasonNumber, episode.index, part),
        'summary': entry.get('summary', ''),
        'date': entry.get('date', ''),
    }


def metadata_is_current(episode, metadata: Dict[str, str]) -> bool:
    current_date = episode.originallyAvailableAt.strftime('%Y-%m-%d') if episode.originallyAvailableAt else ''
    return (episode.title == metadata['title'] and (episode.summary or '') == metadata['summary']
            and current_date == metadata['date'])


def write_episode_metadata(episode, metadata: Dict[str, str]) -> None:
    edits = {'title.value': metadata['title'], 'title.locked': 1,
             'summary.value': metadata['summary'], 'summary.locked': 1}
    if metadata['date']:
        edits.update({'originallyAvailableAt.value': metadata['date'], 'originallyAvailableAt.locked': 1})
    episode.edit(**edits)


def write_show_metadata(plex, found: Dict[str, list], applied: Dict[str, dict], show_channels: Dict[str, str],
                        dry_run: bool, refresh_artwork: bool = False,
                        shows_by_key: Optional[Dict[int, object]] = None) -> Tuple[int, int, List[str]]:
    """
    Force and lock the planned show name (the series scanner mangles folder names: '0.01%' -> '0 01%')
    and the YouTube channel as Studio. With refresh_artwork, shows still without poster get a metadata
    refresh so the Local Media Assets agent picks up poster.jpg/background.jpg (Plex also does this on
    its own in the background, slowly, after a big scan).
    Returns (shows edited, shows refreshed for artwork, errors).
    """
    show_keys: Dict[str, Set[int]] = defaultdict(set)
    for ytid, episodes in found.items():
        for episode in episodes:
            show_keys[applied[ytid]['show']].add(episode.grandparentRatingKey)

    edited, refreshed, errors = 0, 0, []
    for name, keys in sorted(show_keys.items()):
        channel = show_channels.get(name, '')
        for key in keys:
            try:
                show = (shows_by_key or {}).get(key) or plex.fetchItem(key)
                edits = {}
                if show.title != name:
                    edits.update({'title.value': name, 'title.locked': 1})
                if channel and (show.studio or '') != channel:
                    edits.update({'studio.value': channel, 'studio.locked': 1})
                if edits:
                    edited += 1
                    if not dry_run:
                        show.edit(**edits)
                if refresh_artwork and not show.thumb:
                    refreshed += 1
                    if not dry_run:
                        show.refresh()
            except Exception as e:
                errors.append(f"show '{name}': {e}")
    return edited, refreshed, errors


def restore_watch_state(server, found: Dict[str, list], state: Dict[str, dict], dry_run: bool,
                        refetch: bool = True) -> Dict[str, int]:
    """
    Mark played / set progress as one user. found maps ytid -> episodes; other users must refetch them
    (watch state is per user), the owner can use the scanned objects as they are.
    """
    counts = {'played': 0, 'progress': 0, 'already': 0}
    for ytid, watch in state.items():
        if ytid not in found:
            continue
        episodes = [server.fetchItem(ep.ratingKey) for ep in found[ytid]] if refetch else found[ytid]
        if watch.get('played'):
            pending = [ep for ep in episodes if not ep.isPlayed]
            if not pending:
                counts['already'] += 1
                continue
            counts['played'] += 1
            if not dry_run:
                for episode in pending:
                    episode.markPlayed()
        elif watch.get('offset'):
            episode = episodes[0]
            if (getattr(episode, 'viewOffset', 0) or 0) == watch['offset']:
                counts['already'] += 1
                continue
            counts['progress'] += 1
            if not dry_run:
                episode.updateProgress(watch['offset'], state='stopped')
    return counts


def show_scan_paths(applied: Dict[str, dict], ids: Set[str], config: dict) -> List[str]:
    """Plex paths of the show folders containing the given IDs."""
    target_root = os.path.expanduser(config['target_root'])
    shows = sorted({applied[ytid]['show'] for ytid in ids if ytid in applied})
    return [local_to_plex(os.path.join(target_root, show), config.get('path_map')) for show in shows]


def refresh_library(plex, config: dict, applied: Dict[str, dict], catalog_by_id: Dict[str, dict],
                    watch: Dict[str, Dict[str, dict]], ids: Set[str], dry_run: bool = True,
                    sleep=None, clock=None, scan_paths: Optional[List[str]] = None,
                    parts: Optional[Dict[str, List[int]]] = None,
                    show_channels: Optional[Dict[str, str]] = None, refresh_artwork: bool = False) -> dict:
    """scan_paths: scan only these folders (None = whole library). parts: {ytid: [start, end]} from the plan."""
    section = plex.library.section(config['target_library'])
    expected = expected_plex_paths(applied, ids, config)

    if dry_run:
        found, missing = wait_for_episodes(section, expected, 0, sleep=sleep, clock=clock)
    else:
        if scan_paths is None:
            logging.info(f"Scanning library {section.title}...")
            section.update()
        else:
            for path in scan_paths:
                logging.info(f"Scanning {path}")
                section.update(path=path)
        timeout = float(config.get('scan_timeout_minutes', 30)) * 60
        found, missing = wait_for_episodes(section, expected, timeout, sleep=sleep, clock=clock)

    metadata_pending, metadata_errors = 0, []
    for ytid, episodes in found.items():
        for episode in episodes:
            metadata = episode_metadata(catalog_by_id.get(ytid, {}), episode, (parts or {}).get(ytid))
            if metadata_is_current(episode, metadata):
                continue
            metadata_pending += 1
            if not dry_run:
                try:
                    write_episode_metadata(episode, metadata)
                except Exception as e:
                    metadata_errors.append(f"{ytid}: {e}")

    try:
        shows_by_key = {show.ratingKey: show for show in section.search(libtype='show')}
    except Exception as e:
        logging.warning(f"Could not list shows at once, fetching one by one: {e}")
        shows_by_key = {}
    shows_pending, shows_refreshed, show_errors = write_show_metadata(plex, found, applied, show_channels or {},
                                                                      dry_run, refresh_artwork, shows_by_key)
    metadata_errors += show_errors

    watch_counts = {}
    for user, state in watch.items():
        try:
            server = plex if user == OWNER else plex.switchUser(user)
            watch_counts[user] = restore_watch_state(server, found, {k: v for k, v in state.items() if k in ids},
                                                     dry_run, refetch=user != OWNER)
        except Exception as e:
            logging.warning(f"Could not restore watch state for user '{user}': {e}")
            watch_counts[user] = {'error': str(e)}

    trash_emptied = False
    if not dry_run and not missing:
        section.emptyTrash()
        trash_emptied = True

    return {'expected': len(expected), 'found': len(found), 'missing': sorted(missing),
            'metadata_pending': metadata_pending, 'metadata_errors': metadata_errors,
            'show_titles_pending': shows_pending, 'shows_refreshed': shows_refreshed,
            'watch': watch_counts, 'trash_emptied': trash_emptied}


def print_refresh_report(report: dict, dry_run: bool) -> None:
    print(f"\n{'🧪 DRY RUN: nothing was changed' if dry_run else '✅ Refresh finished'}")
    print(f"   📺 Episodes expected:     {report['expected']}")
    print(f"   🔎 Found in Plex:         {report['found']}")
    print(f"   ⏳ Missing:               {len(report['missing'])}")
    verb = 'to write' if dry_run else 'written'
    print(f"   📝 Metadata {verb}:      {report['metadata_pending']}")
    print(f"   🏷️  Shows title/studio {verb}: {report['show_titles_pending']}")
    print(f"   🖼️  Shows {'to refresh' if dry_run else 'refreshed'} for poster: {report['shows_refreshed']}")
    for user, counts in report['watch'].items():
        label = 'owner' if user == OWNER else user
        if 'error' in counts:
            print(f"   👤 {label}: ❌ {counts['error']}")
        else:
            print(f"   👤 {label}: {counts['played']} played, {counts['progress']} in progress, "
                  f"{counts['already']} already restored")
    if report['trash_emptied']:
        print("   🗑️  Library trash emptied")
    for error in report['metadata_errors'][:10]:
        print(f"   ❌ {error}")
    if report['missing']:
        print(f"   ⏳ Missing IDs (first 10): {report['missing'][:10]}")


# --------------------------------------------------------------------------- #
# Sync helpers
# --------------------------------------------------------------------------- #

def diff_plan(plan: dict, applied: Dict[str, dict]) -> Tuple[List[str], List[str], List[str]]:
    """(new, moved, removed) video IDs between the plan and what is already applied."""
    desired = {member['ytid']: member['target'] for _, member in plan_members(plan)}
    new = sorted(ytid for ytid in desired if ytid not in applied)
    moved = sorted(ytid for ytid in desired if ytid in applied and applied[ytid].get('target') != desired[ytid])
    removed = sorted(ytid for ytid in applied if ytid not in desired)
    return new, moved, removed


def filter_watch(watch: Dict[str, Dict[str, dict]], ids: Set[str]) -> Dict[str, Dict[str, dict]]:
    return {user: {k: v for k, v in state.items() if k in ids} for user, state in watch.items()}


def forward_watch_changes(old_watch: Dict[str, Dict[str, dict]], new_watch: Dict[str, Dict[str, dict]],
                          ids: Set[str]) -> Dict[str, Dict[str, dict]]:
    """
    Watch progress made in the old library after migration, to carry into the new one. Forward only:
    played wins, a larger offset wins; nothing is ever unmarked or rewound in the new library.
    """
    changes: Dict[str, Dict[str, dict]] = {}
    for user, old_state in old_watch.items():
        new_state = new_watch.get(user, {})
        for ytid, old in old_state.items():
            if ytid not in ids:
                continue
            new = new_state.get(ytid, {'played': False, 'offset': 0})
            if new['played']:
                continue
            if old['played']:
                change = {'played': True, 'offset': 0}
            elif old['offset'] > new['offset']:
                change = {'played': False, 'offset': old['offset']}
            else:
                continue
            changes.setdefault(user, {})[ytid] = change
    return changes


def push_watch_forward(plex, config: dict, applied: Dict[str, dict], changes: Dict[str, Dict[str, dict]],
                       dry_run: bool = True) -> Dict[str, dict]:
    """Apply forward_watch_changes to the target library episodes (all episodes of multi-episode files)."""
    ids = {ytid for state in changes.values() for ytid in state}
    if not ids:
        return {}
    section = plex.library.section(config['target_library'])
    expected = expected_plex_paths(applied, ids, config)
    index = index_episodes_by_path(section)
    found = {ytid: index[path] for ytid, path in expected.items() if path in index}
    counts: Dict[str, dict] = {}
    for user, state in changes.items():
        try:
            server = plex if user == OWNER else plex.switchUser(user)
            counts[user] = restore_watch_state(server, found, state, dry_run, refetch=user != OWNER)
        except Exception as e:
            logging.warning(f"Could not forward watch state for user '{user}': {e}")
            counts[user] = {'error': str(e)}
    return counts


def print_watch_forward(changes: Dict[str, Dict[str, dict]], counts: Dict[str, dict], dry_run: bool) -> None:
    print(f"\n👁️  Watch progress from the old library ({'dry run' if dry_run else 'applied'}):")
    if not any(changes.values()):
        print("   ✅ Nothing new")
        return
    for user, counted in counts.items():
        label = 'owner' if user == OWNER else user
        if 'error' in counted:
            print(f"   👤 {label}: ❌ {counted['error']}")
        else:
            print(f"   👤 {label}: {counted['played']} to played, {counted['progress']} progress advanced, "
                  f"{counted['already']} already up to date")


def merge_watch(*exports: Dict[str, Dict[str, dict]]) -> Dict[str, Dict[str, dict]]:
    merged: Dict[str, Dict[str, dict]] = {}
    for export in exports:
        for user, state in export.items():
            merged.setdefault(user, {}).update(state)
    return merged


# --------------------------------------------------------------------------- #
# Channel collections
# --------------------------------------------------------------------------- #

def ensure_channel_collections(section, channels: Set[str], dry_run: bool = True) -> Tuple[List[str], int, List[str]]:
    """One smart collection of shows per channel (Studio is exactly the channel). Existing ones are kept."""
    existing = {collection.title for collection in section.collections()}
    created, errors = [], []
    for channel in sorted(c for c in channels if c):
        if channel in existing:
            continue
        created.append(channel)
        if not dry_run:
            try:
                section.createCollection(title=channel, smart=True, libtype='show',
                                         filters={'studio==': channel}, sort='titleSort:asc')
            except Exception as e:
                errors.append(f"{channel}: {e}")
    return created, len(channels & existing), errors


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def cmd_catalog(args, config: dict) -> int:
    plex = connect_to_plex(config)
    section = plex.library.section(config['source_library'])
    print(f"\n📚 Reading library: {section.title} ({section.type}, {section.totalSize} items)")

    path = os.path.join(config['state_dir'], 'catalog.json')
    entries, skipped = build_catalog(section, config, load_json(path, []))
    save_json(path, entries)

    channels = {e['channel'] for e in entries}
    print(f"   🎞️  Videos with YouTube ID: {len(entries)}")
    print(f"   📡 Channels:               {len(channels)}")
    print(f"   ⏭️  Skipped (no ID):        {len(skipped)}")
    from_filename = sum(1 for e in entries if e['channel_source'] == 'filename')
    print(f"   🏷️  Channel from filename:  {from_filename} (no .info.json found)")
    print(f"\n💾 Saved {path}")
    return 0


def cmd_export_watch(args, config: dict) -> int:
    plex = connect_to_plex(config)
    library = config['target_library'] if args.library == 'target' else config['source_library']
    watch = export_watch(plex, library, config.get('users'))
    path = args.output or os.path.join(config['state_dir'], 'watch.json')
    save_json(path, watch)
    from datetime import datetime
    backup = os.path.join(config['state_dir'], 'backups',
                          f"watch-{args.library}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json")
    save_json(backup, watch)

    print(f"\n👁️  Watch state exported from library: {library}")
    for user, state in watch.items():
        played = sum(1 for s in state.values() if s['played'])
        label = 'owner' if user == OWNER else user
        print(f"   👤 {label}: {played} played, {len(state) - played} in progress")
    print(f"\n💾 Saved {path}")
    print(f"🛟 Backup {backup}")
    return 0


def cmd_plan(args, config: dict) -> int:
    state_dir = config['state_dir']
    catalog_path = args.catalog or os.path.join(state_dir, 'catalog.json')
    if not os.path.exists(catalog_path):
        print(f"❌ Error: catalog not found at {catalog_path} (run 'catalog' first)")
        return 1

    catalog = load_json(catalog_path, [])
    overrides = load_yaml(args.overrides)
    applied = load_applied(config)
    plan = build_plan(catalog, overrides, applied, config.get('strip_title_patterns') or [])

    json_path, csv_path = write_plan(plan, state_dir)
    print_plan_summary(plan['summary'])
    print(f"\n💾 Saved {json_path}")
    print(f"💾 Saved {csv_path}")
    return 0


def cmd_apply(args, config: dict) -> int:
    state_dir = config['state_dir']
    plan = load_json(os.path.join(state_dir, 'plan.json'), None)
    if plan is None:
        print(f"❌ Error: plan not found in {state_dir} (run 'plan' first)")
        return 1
    applied_path = os.path.join(state_dir, 'applied.json')
    applied = load_applied(config)
    path_map = config.get('path_map') or {}

    sources = [plex_to_local(m['file'], path_map) for _, m in plan_members(plan)]
    problems = validate_apply_config(config, {os.path.dirname(p) for p in sources})
    if problems:
        for problem in problems:
            print(f"❌ Error: {problem}")
        return 1

    target_root = os.path.expanduser(config['target_root'])
    mode = config['link_mode']
    print(f"\n📂 Target root: {target_root} ({mode})")
    if mode == 'hardlink':
        existing_source = next((p for p in sources if os.path.exists(p)), None)
        if existing_source and os.stat(existing_source).st_dev != os.stat(nearest_existing_dir(target_root)).st_dev:
            print("❌ Error: target_root is not on the same filesystem as the videos (hardlinks impossible)")
            return 1

    write_problem = target_writable_problem(target_root)
    if write_problem:
        print(f"{'⚠️  Warning' if args.dry_run else '❌ Error'}: {write_problem}")
        if not args.dry_run:
            return 1

    fetch_image = None
    if not args.dry_run:
        catalog = load_json(os.path.join(state_dir, 'catalog.json'), [])
        fetch_image = make_image_fetcher(lambda: connect_to_plex(config), {e['ytid']: e for e in catalog})

    result = apply_plan(plan, config, applied, fetch_image, dry_run=args.dry_run,
                        force_posters=args.force_posters)
    report = os.path.join(state_dir, 'apply_dryrun.tsv' if args.dry_run else 'apply_actions.tsv')
    print_apply_report(result, args.dry_run, report)

    if not args.dry_run:
        save_json(applied_path, result.applied)
        save_json(os.path.join(state_dir, 'last_apply.json'),
                  {'new': result.new, 'moved': result.moved, 'removed': result.removed})
        print(f"💾 Saved {applied_path}")
    return 1 if result.count('error') else 0


def cmd_refresh(args, config: dict) -> int:
    state_dir = config['state_dir']
    applied = load_applied(config)
    if not applied:
        print("❌ Error: nothing applied yet (run 'apply' first)")
        return 1
    last_apply = load_json(os.path.join(state_dir, 'last_apply.json'), {})
    ids = select_refresh_ids(applied, last_apply, args.all, args.only)
    unknown = [ytid for ytid in args.only or [] if ytid not in applied]
    if unknown:
        print(f"❌ Error: not in applied.json: {unknown}")
        return 1
    if not ids:
        print("✅ Nothing to refresh")
        return 0

    watch_path = args.watch or os.path.join(state_dir, 'watch.json')
    if not os.path.exists(watch_path):
        print(f"❌ Error: watch state not found at {watch_path} (run 'export-watch' first)")
        return 1
    watch = load_json(watch_path, {})
    catalog = {e['ytid']: e for e in load_json(os.path.join(state_dir, 'catalog.json'), [])}

    shows = sorted({applied[ytid]['show'] for ytid in ids})
    print(f"\n🔄 Refreshing {len(ids)} video(s) in {len(shows)} show(s) of library '{config['target_library']}'")
    for show in shows[:5]:
        print(f"   📺 {show}")
    plex = connect_to_plex(config)
    scan_paths = None if args.all else show_scan_paths(applied, ids, config)
    plan = load_json(os.path.join(state_dir, 'plan.json'), {'series': []})
    parts = {member['ytid']: member['part'] for _, member in plan_members(plan)}
    show_channels = {show['name']: show.get('channel', '') for show in plan['series']}
    report = refresh_library(plex, config, applied, catalog, watch, ids, dry_run=args.dry_run,
                             scan_paths=scan_paths, parts=parts, show_channels=show_channels,
                             refresh_artwork=args.refresh_artwork)
    print_refresh_report(report, args.dry_run)
    return 1 if report['missing'] or report['metadata_errors'] else 0


def cmd_collections(args, config: dict) -> int:
    plan = load_json(os.path.join(config['state_dir'], 'plan.json'), None)
    if plan is None:
        print("❌ Error: plan not found (run 'plan' first)")
        return 1
    channels = {show.get('channel', '') for show in plan['series']} - {''}
    plex = connect_to_plex(config)
    section = plex.library.section(config['target_library'])
    created, existing, errors = ensure_channel_collections(section, channels, dry_run=args.dry_run)

    print(f"\n{'🧪 DRY RUN: nothing was changed' if args.dry_run else '✅ Collections updated'}")
    print(f"   📡 Channels:            {len(channels)}")
    print(f"   ➕ {'To create' if args.dry_run else 'Created'}:           {len(created) - len(errors)}")
    print(f"   ✔️  Already existing:    {existing}")
    for channel in created[:10]:
        print(f"      • {channel}")
    for error in errors:
        print(f"   ❌ {error}")
    return 1 if errors else 0


def cmd_sync(args, config: dict) -> int:
    """Incremental run after new downloads: catalog -> plan -> watch export -> apply -> refresh new/moved."""
    state_dir = config['state_dir']
    if cmd_catalog(args, config):
        return 1
    if cmd_plan(argparse.Namespace(catalog=None, overrides=args.overrides), config):
        return 1

    plan = load_json(os.path.join(state_dir, 'plan.json'), {'series': []})
    applied = load_applied(config)
    new, moved, removed = diff_plan(plan, applied)
    print(f"\n🔁 Sync: {len(new)} new, {len(moved)} moved, {len(removed)} removed")

    # Watch state is read before files move: moved videos from the new library, new ones from the old
    plex = connect_to_plex(config)
    users = config.get('users')
    old_watch = export_watch(plex, config['source_library'], users)
    new_watch = export_watch(plex, config['target_library'], users) if applied else {}

    status = 0
    if new or moved or removed:
        watch = merge_watch(filter_watch(old_watch, set(new)), filter_watch(new_watch, set(moved)))
        watch_path = os.path.join(state_dir, 'watch_sync.json')
        save_json(watch_path, watch)
        from datetime import datetime
        save_json(os.path.join(state_dir, 'backups', f"watch-sync-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"),
                  watch)

        status = cmd_apply(argparse.Namespace(dry_run=args.dry_run, force_posters=False), config)
        if status and not args.dry_run:
            return status
        if args.dry_run:
            print("\n🧪 Refresh skipped in dry run (the new files are not in Plex yet)")
        elif new or moved:
            status = cmd_refresh(argparse.Namespace(dry_run=False, all=False, only=None, watch=watch_path,
                                                    refresh_artwork=False), config)
    else:
        print("✅ No file changes")

    # Videos already migrated and still in place: carry over progress made in the old library
    migrated = set(applied) - set(moved) - set(removed)
    changes = forward_watch_changes(old_watch, new_watch, migrated)
    counts = push_watch_forward(plex, config, applied, changes, dry_run=args.dry_run)
    print_watch_forward(changes, counts, args.dry_run)
    return status or (1 if any('error' in c for c in counts.values()) else 0)


def cmd_sync_watch(args, config: dict) -> int:
    """Only the forward watch-state step of sync."""
    applied = load_applied(config)
    if not applied:
        print("❌ Error: nothing applied yet (run 'apply' first)")
        return 1
    plex = connect_to_plex(config)
    users = config.get('users')
    changes = forward_watch_changes(export_watch(plex, config['source_library'], users),
                                    export_watch(plex, config['target_library'], users), set(applied))
    counts = push_watch_forward(plex, config, applied, changes, dry_run=args.dry_run)
    print_watch_forward(changes, counts, args.dry_run)
    return 1 if any('error' in c for c in counts.values()) else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Organize manhwa recap videos into Plex TV shows")
    parser.add_argument('--config', default='config.yaml', help="Path to config.yaml")
    parser.add_argument('--overrides', default='overrides.yaml', help="Path to overrides.yaml")
    subparsers = parser.add_subparsers(dest='command', required=True)

    catalog_parser = subparsers.add_parser('catalog', help="Read the source library into catalog.json (read-only)")
    catalog_parser.set_defaults(func=cmd_catalog)

    watch_parser = subparsers.add_parser('export-watch', help="Export watch state to watch.json (read-only)")
    watch_parser.add_argument('--library', choices=['source', 'target'], default='source')
    watch_parser.add_argument('--output', help="Output file (default: <state_dir>/watch.json)")
    watch_parser.set_defaults(func=cmd_export_watch)

    plan_parser = subparsers.add_parser('plan', help="Build plan.json / plan.csv from the catalog")
    plan_parser.add_argument('--catalog', help="Catalog JSON (default: <state_dir>/catalog.json)")
    plan_parser.set_defaults(func=cmd_plan)

    apply_parser = subparsers.add_parser('apply', help="Create/update the target tree (links, thumbs, posters)")
    apply_parser.add_argument('--dry-run', action='store_true', help="Only print what would be done")
    apply_parser.add_argument('--force-posters', action='store_true', help="Regenerate existing poster.jpg files")
    apply_parser.set_defaults(func=cmd_apply)

    refresh_parser = subparsers.add_parser('refresh', help="Scan target library, write metadata, restore watch state")
    refresh_parser.add_argument('--dry-run', action='store_true', help="Only report what would be done (no scan/edits)")
    refresh_parser.add_argument('--all', action='store_true', help="Every applied video (default: last apply's new+moved)")
    refresh_parser.add_argument('--only', nargs='+', metavar='YTID', help="Only the series containing these video IDs")
    refresh_parser.add_argument('--watch', help="Watch state JSON to restore (default: <state_dir>/watch.json)")
    refresh_parser.add_argument('--refresh-artwork', action='store_true',
                                help="Ask Plex to refresh metadata of shows still without poster")
    refresh_parser.set_defaults(func=cmd_refresh)

    sync_parser = subparsers.add_parser('sync', help="Incremental: catalog, plan, apply and refresh new/moved videos")
    sync_parser.add_argument('--dry-run', action='store_true', help="Catalog and plan, then only report file changes")
    sync_parser.set_defaults(func=cmd_sync)

    sync_watch_parser = subparsers.add_parser('sync-watch',
                                              help="Carry watch progress made in the old library into the new one")
    sync_watch_parser.add_argument('--dry-run', action='store_true', help="Only report what would change")
    sync_watch_parser.set_defaults(func=cmd_sync_watch)

    collections_parser = subparsers.add_parser('collections', help="Create one smart collection per channel")
    collections_parser.add_argument('--dry-run', action='store_true', help="Only list collections to create")
    collections_parser.set_defaults(func=cmd_collections)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    config = load_config(args.config)
    return args.func(args, config)


if __name__ == "__main__":
    sys.exit(main())
