from datetime import datetime
from types import SimpleNamespace

import manhwa_organizer as mo

CONFIG = dict(mo.DEFAULT_CONFIG, target_root='/Users/me/mnt/plex-media/Manhwa-Series',
              path_map={'/data': '/Users/me/mnt/plex-media'}, target_library='Manhwa Series',
              scan_timeout_minutes=1)
APPLIED = {
    'AAAAAAAAAA1': {'show': 'Sword', 'target': 'Sword/Season 01/Sword - S01E01 [AAAAAAAAAA1].mp4'},
    'AAAAAAAAAA2': {'show': 'Sword', 'target': 'Sword/Season 01/Sword - S01E02-E03 [AAAAAAAAAA2].mp4'},
    'BBBBBBBBBB1': {'show': 'Lone', 'target': 'Lone/Season 01/Lone - S01E01 [BBBBBBBBBB1].mp4'},
}
CATALOG = {
    'AAAAAAAAAA1': {'title': 'Chan - Sword Part 1', 'channel': 'Chan', 'summary': 'S1', 'date': '2026-01-01'},
    'AAAAAAAAAA2': {'title': 'Chan - Sword Part 2-3', 'channel': 'Chan', 'summary': 'S2', 'date': '2026-02-01'},
    'BBBBBBBBBB1': {'title': 'Chan - Lone', 'channel': 'Chan', 'summary': 'S3', 'date': '2026-03-01'},
}


class FakeEpisode:
    def __init__(self, key, path, season=1, index=1):
        self.ratingKey, self.path = key, path
        self.seasonNumber, self.index = season, index
        self.title, self.summary, self.originallyAvailableAt = 'raw', '', None
        self.isPlayed, self.viewOffset = False, 0
        self.edits = []
        self.media = [SimpleNamespace(parts=[SimpleNamespace(file=path)])]
        self.grandparentRatingKey = 100 if 'Sword' in path else 200

    def edit(self, **kwargs):
        self.edits.append(kwargs)
        self.title = kwargs['title.value']
        self.summary = kwargs['summary.value']
        self.originallyAvailableAt = datetime.strptime(kwargs['originallyAvailableAt.value'], '%Y-%m-%d')

    def markPlayed(self):
        self.isPlayed = True

    def updateProgress(self, offset, state):
        assert state == 'stopped'
        self.viewOffset = offset


class FakeSection:
    def __init__(self, episodes_per_poll):
        self.polls = list(episodes_per_poll)
        self.episodes = []
        self.title = 'Manhwa Series'
        self.updated = self.trash_emptied = False

    def update(self, path=None):
        self.updated = True
        self.scanned_paths = getattr(self, 'scanned_paths', []) + [path]

    def searchEpisodes(self):
        if self.polls:
            self.episodes = self.polls.pop(0)
        return self.episodes

    def emptyTrash(self):
        self.trash_emptied = True

    def search(self, libtype=None):
        return getattr(self, 'shows', [])


class FakeShow:
    def __init__(self, key, title, studio='', thumb='/thumb'):
        self.ratingKey, self.title, self.studio, self.thumb = key, title, studio, thumb
        self.edits, self.refreshes = [], 0

    def edit(self, **kwargs):
        self.edits.append(kwargs)
        self.title = kwargs.get('title.value', self.title)
        self.studio = kwargs.get('studio.value', self.studio)

    def refresh(self):
        self.refreshes += 1


class FakePlex:
    def __init__(self, section):
        self.section = section
        self.library = SimpleNamespace(section=lambda name: section)
        self.shows = {100: FakeShow(100, 'Sword'), 200: FakeShow(200, 'Lone Mangled')}

    def fetchItem(self, key):
        if key in self.shows:
            return self.shows[key]
        return next(e for e in self.section.episodes if e.ratingKey == key)


def plex_path(ytid):
    return '/data/Manhwa-Series/' + APPLIED[ytid]['target']


def all_episodes():
    return [FakeEpisode(1, plex_path('AAAAAAAAAA1'), 1, 1), FakeEpisode(2, plex_path('AAAAAAAAAA2'), 1, 2),
            FakeEpisode(3, plex_path('AAAAAAAAAA2'), 1, 3), FakeEpisode(4, plex_path('BBBBBBBBBB1'), 1, 1)]


def fake_clock():
    now = [0.0]

    def clock():
        return now[0]

    def sleep(seconds):
        now[0] += seconds

    return clock, sleep


def test_local_to_plex_inverts_path_map():
    assert mo.local_to_plex('/Users/me/mnt/plex-media/Manhwa-Series/x.mp4', CONFIG['path_map']) == \
        '/data/Manhwa-Series/x.mp4'
    assert mo.plex_to_local(mo.local_to_plex('/Users/me/mnt/plex-media/a', CONFIG['path_map']),
                            CONFIG['path_map']) == '/Users/me/mnt/plex-media/a'


def test_select_refresh_ids():
    last = {'new': ['BBBBBBBBBB1'], 'moved': ['AAAAAAAAAA2'], 'removed': ['GONE']}
    assert mo.select_refresh_ids(APPLIED, last, False, None) == {'BBBBBBBBBB1', 'AAAAAAAAAA2'}
    assert mo.select_refresh_ids(APPLIED, last, True, None) == set(APPLIED)
    assert mo.select_refresh_ids(APPLIED, last, False, ['AAAAAAAAAA1']) == {'AAAAAAAAAA1', 'AAAAAAAAAA2'}


def test_refresh_waits_for_scan_then_writes_metadata_and_watch_state():
    episodes = all_episodes()
    section = FakeSection([[], episodes[:1], episodes])
    plex = FakePlex(section)
    watch = {mo.OWNER: {'AAAAAAAAAA2': {'played': True, 'offset': 0},
                        'BBBBBBBBBB1': {'played': False, 'offset': 90000}}}
    clock, sleep = fake_clock()

    report = mo.refresh_library(plex, CONFIG, APPLIED, CATALOG, watch, set(APPLIED), dry_run=False,
                                sleep=sleep, clock=clock)
    assert section.updated and section.trash_emptied
    assert report['found'] == 3 and report['missing'] == []
    assert report['metadata_pending'] == 4                      # multi-episode file -> 2 episodes
    assert [e.title for e in episodes] == ['Parte 1', 'Parte 2', 'Parte 3', 'Parte 1']
    assert episodes[0].edits[0]['title.locked'] == 1
    assert episodes[0].originallyAvailableAt.strftime('%Y-%m-%d') == '2026-01-01'
    assert episodes[1].isPlayed and episodes[2].isPlayed          # both episodes of the multi-episode file
    assert episodes[3].viewOffset == 90000 and not episodes[3].isPlayed
    assert report['watch'][mo.OWNER] == {'played': 1, 'progress': 1, 'already': 0}


def test_second_refresh_changes_nothing():
    episodes = all_episodes()
    section = FakeSection([episodes])
    plex = FakePlex(section)
    watch = {mo.OWNER: {'AAAAAAAAAA1': {'played': True, 'offset': 0}}}
    clock, sleep = fake_clock()
    mo.refresh_library(plex, CONFIG, APPLIED, CATALOG, watch, set(APPLIED), dry_run=False, sleep=sleep, clock=clock)

    report = mo.refresh_library(plex, CONFIG, APPLIED, CATALOG, watch, set(APPLIED), dry_run=False,
                                sleep=sleep, clock=clock)
    assert report['metadata_pending'] == 0
    assert report['watch'][mo.OWNER] == {'played': 0, 'progress': 0, 'already': 1}
    assert all(len(e.edits) == 1 for e in episodes)


def test_dry_run_does_not_scan_edit_or_mark():
    episodes = all_episodes()
    section = FakeSection([episodes])
    plex = FakePlex(section)
    watch = {mo.OWNER: {'AAAAAAAAAA1': {'played': True, 'offset': 0}}}
    report = mo.refresh_library(plex, CONFIG, APPLIED, CATALOG, watch, set(APPLIED), dry_run=True)
    assert not section.updated and not section.trash_emptied
    assert report['metadata_pending'] == 4
    assert report['watch'][mo.OWNER]['played'] == 1
    assert not any(e.edits or e.isPlayed for e in episodes)


def test_timeout_reports_missing_and_keeps_trash():
    section = FakeSection([all_episodes()[:1]])
    clock, sleep = fake_clock()
    report = mo.refresh_library(FakePlex(section), CONFIG, APPLIED, CATALOG, {}, set(APPLIED), dry_run=False,
                                sleep=sleep, clock=clock)
    assert report['missing'] == ['AAAAAAAAAA2', 'BBBBBBBBBB1']
    assert not section.trash_emptied


def test_only_selected_ids_get_watch_state():
    episodes = all_episodes()
    section = FakeSection([episodes])
    watch = {mo.OWNER: {'AAAAAAAAAA1': {'played': True, 'offset': 0}, 'BBBBBBBBBB1': {'played': True, 'offset': 0}}}
    clock, sleep = fake_clock()
    mo.refresh_library(FakePlex(section), CONFIG, APPLIED, CATALOG, watch, {'BBBBBBBBBB1'}, dry_run=False,
                       sleep=sleep, clock=clock)
    assert episodes[3].isPlayed and not episodes[0].isPlayed
    assert not episodes[0].edits


def test_user_failure_is_reported_and_others_continue():
    episodes = all_episodes()
    section = FakeSection([episodes])
    plex = FakePlex(section)

    def switch_user(name):
        raise RuntimeError('no access to library')

    plex.switchUser = switch_user
    watch = {mo.OWNER: {'AAAAAAAAAA1': {'played': True, 'offset': 0}}, 'friend': {}}
    report = mo.refresh_library(plex, CONFIG, APPLIED, CATALOG, watch, set(APPLIED), dry_run=True)
    assert 'error' in report['watch']['friend']
    assert report['watch'][mo.OWNER]['played'] == 1


def test_scan_only_selected_show_folders():
    section = FakeSection([all_episodes()])
    clock, sleep = fake_clock()
    paths = mo.show_scan_paths(APPLIED, {'AAAAAAAAAA1'}, CONFIG)
    assert paths == ['/data/Manhwa-Series/Sword']
    mo.refresh_library(FakePlex(section), CONFIG, APPLIED, CATALOG, {}, {'AAAAAAAAAA1'}, dry_run=False,
                       sleep=sleep, clock=clock, scan_paths=paths)
    assert section.scanned_paths == ['/data/Manhwa-Series/Sword']


def test_show_title_and_studio_are_forced_and_posterless_shows_refreshed():
    section = FakeSection([all_episodes()])
    plex = FakePlex(section)
    plex.shows[100].studio = 'Chan A'
    plex.shows[200].thumb = None
    channels = {'Sword': 'Chan A', 'Lone': 'Juba - Manhwa Recap'}
    clock, sleep = fake_clock()
    plain = mo.refresh_library(plex, CONFIG, APPLIED, CATALOG, {}, set(APPLIED), dry_run=True, show_channels=channels)
    assert plain['shows_refreshed'] == 0
    dry = mo.refresh_library(plex, CONFIG, APPLIED, CATALOG, {}, set(APPLIED), dry_run=True, show_channels=channels,
                             refresh_artwork=True)
    assert dry['show_titles_pending'] == 1 and dry['shows_refreshed'] == 1
    assert plex.shows[200].title == 'Lone Mangled' and plex.shows[200].refreshes == 0

    report = mo.refresh_library(plex, CONFIG, APPLIED, CATALOG, {}, set(APPLIED), dry_run=False,
                                sleep=sleep, clock=clock, show_channels=channels, refresh_artwork=True)
    assert report['show_titles_pending'] == 1
    assert plex.shows[200].edits == [{'title.value': 'Lone', 'title.locked': 1,
                                      'studio.value': 'Juba - Manhwa Recap', 'studio.locked': 1}]
    assert plex.shows[200].refreshes == 1
    assert plex.shows[100].edits == [] and plex.shows[100].refreshes == 0


def test_episode_titles():
    assert mo.episode_title(1, 7, [7, 7]) == 'Parte 7'
    assert mo.episode_title(1, 3, [1, 3]) == 'Parte 3'          # each episode of a multi-episode file
    assert mo.episode_title(0, 1, [1, 16]) == 'Partes 1-16'
    assert mo.episode_title(0, 2, [6, 6]) == 'Parte 6'
    assert mo.episode_title(0, 3, None) == 'Especial 3'


class FakeCollectionSection:
    def __init__(self, existing):
        self.existing = [SimpleNamespace(title=t) for t in existing]
        self.created = []

    def collections(self):
        return self.existing

    def createCollection(self, **kwargs):
        if kwargs['title'] == 'Broken':
            raise RuntimeError('bad filter')
        self.created.append(kwargs)


def test_channel_collections_created_once_with_exact_studio_filter():
    section = FakeCollectionSection(['Evergarden Manhwa'])
    channels = {'Evergarden Manhwa', 'Juba - Manhwa Recap', 'Haru Senpai'}

    created, existing, errors = mo.ensure_channel_collections(section, channels, dry_run=True)
    assert created == ['Haru Senpai', 'Juba - Manhwa Recap'] and existing == 1 and section.created == []

    created, _, errors = mo.ensure_channel_collections(section, channels, dry_run=False)
    assert errors == []
    assert section.created[1] == {'title': 'Juba - Manhwa Recap', 'smart': True, 'libtype': 'show',
                                  'filters': {'studio==': 'Juba - Manhwa Recap'}, 'sort': 'titleSort:asc'}


def test_channel_collection_errors_are_reported():
    created, _, errors = mo.ensure_channel_collections(FakeCollectionSection([]), {'Broken', 'Ok'}, dry_run=False)
    assert errors == ['Broken: bad filter']
