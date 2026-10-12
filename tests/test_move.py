"""Move mode: inbox folder outside Plex, organized library as the only Plex library."""
import json
import os
from io import BytesIO

import pytest
from PIL import Image

import manhwa_organizer as mo


def image_bytes(fmt='WEBP'):
    buffer = BytesIO()
    Image.new('RGB', (1280, 720), (10, 120, 200)).save(buffer, fmt)
    return buffer.getvalue()


def no_fetch(ytid, source=''):
    raise AssertionError('thumbnails must come from the moved sidecar')


METADATA = {
    'SWORDPART01': {'title': 'Sword Saint Returns', 'channel_name': 'Chan', 'description': 'This is part 1',
                    'published_at': '2026-01-01T10:00:00Z'},
    'SWORDPART02': {'title': 'Sword Saint Returns', 'channel_name': 'Chan', 'description': 'This is part 2',
                    'published_at': '2026-02-01T10:00:00Z'},
    'LONELONELON': {'title': 'Lone Story Here | Manhwa Recap', 'channel_name': 'Chan', 'description': '',
                    'published_at': '2026-03-01T10:00:00Z'},
}


def download(inbox, ytid, title, with_info=True):
    """Files as yt-dlp leaves them: video, info.json, subtitles, webp thumbnail, plus temp junk."""
    base = inbox / f'Chan - {title} [{ytid}]'
    base.with_name(base.name + '.mp4').write_bytes(b'video ' + ytid.encode())
    if with_info:
        base.with_name(base.name + '.info.json').write_text(json.dumps(
            {'id': ytid, 'title': title, 'channel': 'Chan', 'description': 'info', 'upload_date': '20260105'}))
    base.with_name(base.name + '.pt.srt').write_text('1\n00:00:01,000 --> 00:00:02,000\nOi\n')
    base.with_name(base.name + '.webp').write_bytes(image_bytes())
    base.with_name(base.name + '.f140.m4a').write_bytes(b'leftover audio of an earlier attempt')


@pytest.fixture
def env(tmp_path):
    inbox, library = tmp_path / 'Manhwa-temp', tmp_path / 'Manhwa'
    inbox.mkdir()
    download(inbox, 'SWORDPART01', 'Sword Saint Returns P1')
    download(inbox, 'SWORDPART02', 'Sword Saint Returns P2')
    download(inbox, 'LONELONELON', 'Lone Story Here')
    (inbox / 'Chan - Partial [PARTIALPART].f137.mp4').write_bytes(b'partial')   # yt-dlp temp file
    (inbox / 'Chan - Partial [PARTIALPART].mp4.part').write_bytes(b'partial')
    config = dict(mo.DEFAULT_CONFIG, inbox_dir=str(inbox), target_root=str(library), link_mode='move',
                  state_dir=str(tmp_path / 'state'))
    return {'inbox': inbox, 'library': library, 'config': config}


def fetch_metadata(ytid):
    return METADATA.get(ytid)


def run(env, applied=None, overrides=None, dry_run=False):
    catalog, _ = mo.build_disk_catalog(str(env['inbox']), str(env['library']), fetch_metadata)
    plan = mo.build_plan(catalog, overrides, applied)
    return mo.apply_plan(plan, env['config'], applied or {}, no_fetch, dry_run=dry_run), plan


def files(root):
    return sorted(os.path.relpath(os.path.join(d, f), root) for d, _, fs in os.walk(root) for f in fs)


def test_disk_catalog_uses_youtube_metadata_then_info_json(env):
    download(env['inbox'], 'NOMETADATA1', 'No Metadata Video')
    download(env['inbox'], 'NOTHINGATAL', 'Nothing At All', with_info=False)
    entries, _ = mo.build_disk_catalog(str(env['inbox']), str(env['library']), fetch_metadata)
    catalog = {e['ytid']: e for e in entries}
    assert set(catalog) == {'SWORDPART01', 'SWORDPART02', 'LONELONELON', 'NOMETADATA1', 'NOTHINGATAL'}
    assert catalog['SWORDPART02']['summary'] == 'This is part 2' and catalog['SWORDPART02']['date'] == '2026-02-01'
    assert (catalog['NOMETADATA1']['channel_source'], catalog['NOMETADATA1']['date']) == ('info.json', '2026-01-05')
    assert catalog['NOTHINGATAL']['channel_source'] == 'filename'
    assert catalog['NOTHINGATAL']['title'] == 'Chan - Nothing At All'
    assert all(e['location'] == 'inbox' for e in catalog.values())


def test_youtube_fetcher_uses_cache_without_api_key():
    cache = {'SWORDPART01': METADATA['SWORDPART01']}
    fetch = mo.make_youtube_metadata_fetcher(None, cache)
    assert fetch('SWORDPART01')['title'] == 'Sword Saint Returns'
    assert fetch('UNKNOWNVIDE') is None


def test_dry_run_moves_nothing(env):
    before = files(env['inbox'])
    result, _ = run(env, dry_run=True)
    assert result.count('error') == 0
    assert result.count('thumb') == 0           # the webp sidecar becomes the thumb
    assert files(env['inbox']) == before and not env['library'].exists()


def test_move_puts_video_and_sidecars_in_library(env):
    result, _ = run(env)
    assert result.count('error') == 0
    season = env['library'] / 'Sword Saint Returns' / 'Season 01'
    assert sorted(os.listdir(season)) == [
        'Sword Saint Returns - S01E01 [SWORDPART01].info.json', 'Sword Saint Returns - S01E01 [SWORDPART01].jpg',
        'Sword Saint Returns - S01E01 [SWORDPART01].mp4', 'Sword Saint Returns - S01E01 [SWORDPART01].pt.srt',
        'Sword Saint Returns - S01E02 [SWORDPART02].info.json', 'Sword Saint Returns - S01E02 [SWORDPART02].jpg',
        'Sword Saint Returns - S01E02 [SWORDPART02].mp4', 'Sword Saint Returns - S01E02 [SWORDPART02].pt.srt',
    ]
    assert (season / 'Sword Saint Returns - S01E02 [SWORDPART02].mp4').read_bytes() == b'video SWORDPART02'
    with Image.open(season / 'Sword Saint Returns - S01E01 [SWORDPART01].jpg') as thumb:
        assert thumb.format == 'JPEG'
    assert (env['library'] / 'Sword Saint Returns' / 'poster.jpg').exists()
    assert (env['library'] / 'Lone Story Here' / 'Season 01' / 'Lone Story Here - S01E01 [LONELONELON].mp4').exists()
    # only yt-dlp temp files and leftovers stay in the inbox
    assert files(env['inbox']) == [
        'Chan - Lone Story Here [LONELONELON].f140.m4a', 'Chan - Partial [PARTIALPART].f137.mp4',
        'Chan - Partial [PARTIALPART].mp4.part', 'Chan - Sword Saint Returns P1 [SWORDPART01].f140.m4a',
        'Chan - Sword Saint Returns P2 [SWORDPART02].f140.m4a']


def test_second_run_is_a_no_op_and_catalog_reads_library(env):
    first, _ = run(env)
    second, plan = run(env, applied=first.applied)
    assert second.actions == [] and second.new == []
    assert {m['ytid'] for _, m in mo.plan_members(plan)} == {'SWORDPART01', 'SWORDPART02', 'LONELONELON'}


def test_renumbered_episode_is_renamed_with_its_sidecars(env):
    first, _ = run(env)
    second, _ = run(env, applied=first.applied, overrides={'merge': [['SWORDPART01', 'LONELONELON']],
                                                            'parts': {'LONELONELON': 3}})
    assert second.moved == ['LONELONELON'] and second.count('error') == 0
    season = env['library'] / 'Sword Saint Returns' / 'Season 01'
    for suffix in ('.mp4', '.jpg', '.info.json', '.pt.srt'):
        assert (season / f'Sword Saint Returns - S01E03 [LONELONELON]{suffix}').exists()
    assert not (env['library'] / 'Lone Story Here').exists()


def test_video_deleted_in_plex_is_not_recreated_and_sidecars_are_cleaned(env):
    first, _ = run(env)
    lone = env['library'] / 'Lone Story Here'
    (lone / 'Season 01' / 'Lone Story Here - S01E01 [LONELONELON].mp4').unlink()   # Plex "delete"
    second, _ = run(env, applied=first.applied)
    assert second.removed == ['LONELONELON'] and second.count('error') == 0
    assert not lone.exists()
    assert 'LONELONELON' not in second.applied


def test_inbox_hardlink_twins_are_adopted_only_when_same_inode(env):
    first, _ = run(env)
    library_video = env['library'] / 'Sword Saint Returns' / 'Season 01' / 'Sword Saint Returns - S01E01 [SWORDPART01].mp4'
    # Migration from the hardlink setup: the old originals are now in the inbox
    twin = env['inbox'] / 'Chan - Sword Saint Returns P1 [SWORDPART01].mp4'
    os.link(library_video, twin)
    (env['inbox'] / 'Chan - Sword Saint Returns P1 [SWORDPART01].en.srt').write_text('extra subtitle')
    (env['inbox'] / 'Chan - Sword Saint Returns P1 [SWORDPART01].jpg').write_bytes(image_bytes('JPEG'))
    impostor = env['inbox'] / 'Chan - Sword Saint Returns P2 other title [SWORDPART02].mp4'
    impostor.write_bytes(b'different content')

    second, _ = run(env, applied=first.applied)
    assert second.count('adopt') == 1 and second.count('error') == 0
    assert not twin.exists() and library_video.read_bytes() == b'video SWORDPART01'
    assert (library_video.parent / 'Sword Saint Returns - S01E01 [SWORDPART01].en.srt').exists()
    assert not (env['inbox'] / 'Chan - Sword Saint Returns P1 [SWORDPART01].jpg').exists()
    assert impostor.exists()                      # a different file with the same ID is never deleted


def test_library_copy_wins_duplicate_ids():
    inbox = {'ytid': 'A', 'location': 'inbox', 'addedAt': '2026-09-01', 'file': '/in/A.mp4'}
    library = {'ytid': 'A', 'location': 'library', 'addedAt': '2026-01-01', 'file': '/lib/A.mp4'}
    videos, ignored = mo.dedupe_catalog([inbox, library])
    assert videos['A']['file'] == '/lib/A.mp4' and ignored['A'] == [inbox]


def test_download_archive_gates_inbox_videos(env, tmp_path):
    archive = tmp_path / 'ytdlp_archive.txt'
    archive.write_text('youtube SWORDPART01\nyoutube LONELONELON\n\nyoutube\n')
    assert mo.load_download_archive(str(archive)) == {'SWORDPART01', 'LONELONELON'}
    assert mo.load_download_archive(None) is None

    entries, skipped = mo.build_disk_catalog(str(env['inbox']), str(env['library']), fetch_metadata,
                                             mo.load_download_archive(str(archive)))
    assert {e['ytid'] for e in entries} == {'SWORDPART01', 'LONELONELON'}
    assert [os.path.basename(p) for p in skipped] == ['Chan - Sword Saint Returns P2 [SWORDPART02].mp4']


def test_catalog_reads_archive_by_name_inside_inbox(env, monkeypatch):
    (env['inbox'] / 'ytdlp_archive.txt').write_text('youtube SWORDPART01\n')
    config = dict(env['config'], download_archive='ytdlp_archive.txt', youtube_api_key=None)
    assert mo.catalog_from_disk(config) == 0
    catalog = json.loads((env['inbox'].parent / 'state' / 'catalog.json').read_text())
    assert [e['ytid'] for e in catalog] == ['SWORDPART01']
