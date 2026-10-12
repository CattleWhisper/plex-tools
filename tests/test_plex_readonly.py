from datetime import datetime
from types import SimpleNamespace

import manhwa_organizer as mo


def fake_item(path, title='T', viewCount=0, viewOffset=0, date=datetime(2026, 1, 2), rating_key=1):
    part = SimpleNamespace(file=path)
    return SimpleNamespace(media=[SimpleNamespace(parts=[part])], title=title, summary='S',
                           originallyAvailableAt=date, addedAt=datetime(2026, 5, 5), ratingKey=rating_key,
                           thumb=f'/library/metadata/{rating_key}/thumb/1', viewCount=viewCount,
                           viewOffset=viewOffset)


class FakeSection:
    def __init__(self, items, type_='movie'):
        self.items, self.type = items, type_

    def all(self):
        return self.items

    def searchEpisodes(self):
        return self.items


def test_catalog_entry_extracts_id_channel_and_date():
    entry = mo.catalog_entry(fake_item('/data/Manhwa/Haru Senpai - (13) Forced To Army [TSAwCIuQrFE].mp4'))
    assert entry['ytid'] == 'TSAwCIuQrFE'
    assert entry['channel'] == 'Haru Senpai'
    assert entry['date'] == '2026-01-02'


def test_catalog_entry_falls_back_to_added_at_and_skips_missing_id():
    entry = mo.catalog_entry(fake_item('/data/M/Chan - T [abcdefghijk].mkv', date=None))
    assert entry['date'] == '2026-05-05'
    assert mo.catalog_entry(fake_item('/data/M/no id.mkv')) is None


def test_watch_state_keeps_only_watched_and_merges_multi_episode_files():
    items = [
        fake_item('/x/A - a [AAAAAAAAAAA].mp4', viewCount=1),
        fake_item('/x/A - b [BBBBBBBBBBB].mp4', viewOffset=5000),
        fake_item('/x/A - c [CCCCCCCCCCC].mp4'),
        fake_item('/x/S - S01E01-E03 [DDDDDDDDDDD].mp4', viewOffset=100),
        fake_item('/x/S - S01E01-E03 [DDDDDDDDDDD].mp4', viewCount=1),
    ]
    state = mo.watch_state_of_section(FakeSection(items, 'show'))
    assert state == {
        'AAAAAAAAAAA': {'played': True, 'offset': 0},
        'BBBBBBBBBBB': {'played': False, 'offset': 5000},
        'DDDDDDDDDDD': {'played': True, 'offset': 100},
    }
    assert set(mo.watch_state_of_section(FakeSection(items), ids={'BBBBBBBBBBB'})) == {'BBBBBBBBBBB'}


def test_export_watch_continues_when_a_user_fails():
    owner_section = FakeSection([fake_item('/x/A - a [AAAAAAAAAAA].mp4', viewCount=1)])
    friend_section = FakeSection([fake_item('/x/A - a [AAAAAAAAAAA].mp4', viewOffset=7)])

    def server(section):
        return SimpleNamespace(library=SimpleNamespace(section=lambda name: section))

    def switch_user(name):
        if name == 'broken':
            raise RuntimeError('no access')
        return server(friend_section)

    plex = server(owner_section)
    plex.switchUser = switch_user
    watch = mo.export_watch(plex, 'Manhwa', ['friend', 'broken'])
    assert watch == {
        mo.OWNER: {'AAAAAAAAAAA': {'played': True, 'offset': 0}},
        'friend': {'AAAAAAAAAAA': {'played': False, 'offset': 7}},
    }


def test_plex_to_local_uses_longest_prefix(monkeypatch):
    monkeypatch.setenv('HOME', '/Users/me')
    path_map = {'/data': '~/mnt/plex-media', '/data/Manhwa': '/fast/manhwa'}
    assert mo.plex_to_local('/data/Manhwa/a.mp4', path_map) == '/fast/manhwa/a.mp4'
    assert mo.plex_to_local('/data/Other/a.mp4', path_map) == '/Users/me/mnt/plex-media/Other/a.mp4'
    assert mo.plex_to_local('/database/a.mp4', path_map) == '/database/a.mp4'


def test_catalog_channel_from_info_json_with_filename_fallback(tmp_path):
    (tmp_path / 'Juba - Manhwa Recap - Story [abcdefghijk].info.json').write_text('{"channel": "Juba - Manhwa Recap"}')
    config = {'info_json_dir': str(tmp_path), 'path_map': {}}
    entry = mo.catalog_entry(fake_item('/data/Manhwa/Juba - Manhwa Recap - Story [abcdefghijk].mp4'), config)
    assert (entry['channel'], entry['channel_source']) == ('Juba - Manhwa Recap', 'info.json')

    entry = mo.catalog_entry(fake_item('/data/Manhwa/Juba - Manhwa Recap - Other [bbbbbbbbbbb].mp4'), config)
    assert (entry['channel'], entry['channel_source']) == ('Juba', 'filename')
    assert entry['addedAt'] == '2026-05-05T00:00:00'


def test_info_json_next_to_video_via_path_map(tmp_path):
    config = {'path_map': {'/data': str(tmp_path)}}
    assert mo.info_json_path('/data/Manhwa/X - T [abcdefghijk].mp4', config) == \
        str(tmp_path / 'Manhwa' / 'X - T [abcdefghijk].info.json')


def test_catalog_reuses_known_channels_and_reads_only_new_info_json(tmp_path):
    (tmp_path / 'Kawa - New [newnewnewne].info.json').write_text('{"channel": "Manhwa do Kawa"}')
    section = FakeSection([fake_item('/data/M/Juba - Manhwa Recap - Old [oldoldoldol].mp4'),
                           fake_item('/data/M/Kawa - New [newnewnewne].mp4')])
    previous = [{'ytid': 'oldoldoldol', 'channel': 'Juba - Manhwa Recap', 'channel_source': 'info.json'}]
    entries, _ = mo.build_catalog(section, {'info_json_dir': str(tmp_path), 'path_map': {}}, previous)
    assert [(e['channel'], e['channel_source']) for e in entries] == [
        ('Juba - Manhwa Recap', 'info.json'), ('Manhwa do Kawa', 'info.json')]
