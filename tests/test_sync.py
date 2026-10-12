import manhwa_organizer as mo


def test_diff_plan_new_moved_removed():
    plan = {'series': [{'members': [
        {'ytid': 'KEEP', 'target': 'A/Season 01/A - S01E01 [KEEP].mp4'},
        {'ytid': 'MOVE', 'target': 'A/Season 01/A - S01E03 [MOVE].mp4'},
        {'ytid': 'NEW', 'target': 'A/Season 01/A - S01E04 [NEW].mp4'},
    ]}]}
    applied = {
        'KEEP': {'target': 'A/Season 01/A - S01E01 [KEEP].mp4'},
        'MOVE': {'target': 'B/Season 01/B - S01E01 [MOVE].mp4'},
        'GONE': {'target': 'C/Season 01/C - S01E01 [GONE].mp4'},
    }
    assert mo.diff_plan(plan, applied) == (['NEW'], ['MOVE'], ['GONE'])


def test_merge_watch_combines_users_and_ids():
    old = {mo.OWNER: {'NEW': {'played': True, 'offset': 0}}, 'friend': {'NEW': {'played': False, 'offset': 5}}}
    new = {mo.OWNER: {'MOVE': {'played': False, 'offset': 9}}}
    assert mo.merge_watch(old, new, {}) == {
        mo.OWNER: {'NEW': {'played': True, 'offset': 0}, 'MOVE': {'played': False, 'offset': 9}},
        'friend': {'NEW': {'played': False, 'offset': 5}},
    }


def test_forward_watch_changes_only_moves_forward():
    old = {mo.OWNER: {
        'PLAYEDOLD': {'played': True, 'offset': 0},       # watched in old after migration
        'FURTHER': {'played': False, 'offset': 9000},     # more progress in old
        'BEHIND': {'played': False, 'offset': 1000},      # new library is further: keep
        'BOTHPLAY': {'played': True, 'offset': 0},        # already played in new
        'NEWVIDEO': {'played': True, 'offset': 0},        # not migrated yet: handled by refresh
    }}
    new = {mo.OWNER: {
        'FURTHER': {'played': False, 'offset': 5000},
        'BEHIND': {'played': False, 'offset': 7000},
        'BOTHPLAY': {'played': True, 'offset': 0},
        'NEWERPLAY': {'played': True, 'offset': 0},       # played only in new: never unmarked
    }}
    ids = {'PLAYEDOLD', 'FURTHER', 'BEHIND', 'BOTHPLAY', 'NEWERPLAY'}
    assert mo.forward_watch_changes(old, new, ids) == {mo.OWNER: {
        'PLAYEDOLD': {'played': True, 'offset': 0},
        'FURTHER': {'played': False, 'offset': 9000},
    }}


def test_filter_watch():
    watch = {mo.OWNER: {'A': {'played': True, 'offset': 0}, 'B': {'played': False, 'offset': 1}}, 'friend': {}}
    assert mo.filter_watch(watch, {'B'}) == {mo.OWNER: {'B': {'played': False, 'offset': 1}}, 'friend': {}}


def test_push_watch_forward_marks_target_episodes():
    from tests.test_refresh import APPLIED, CONFIG, FakePlex, FakeSection, all_episodes
    episodes = all_episodes()
    plex = FakePlex(FakeSection([episodes]))
    changes = {mo.OWNER: {'AAAAAAAAAA2': {'played': True, 'offset': 0},
                          'BBBBBBBBBB1': {'played': False, 'offset': 4242}}}

    assert mo.push_watch_forward(plex, CONFIG, APPLIED, changes, dry_run=True)[mo.OWNER]['played'] == 1
    assert not any(e.isPlayed for e in episodes)

    counts = mo.push_watch_forward(plex, CONFIG, APPLIED, changes, dry_run=False)
    assert counts[mo.OWNER] == {'played': 1, 'progress': 1, 'already': 0}
    assert episodes[1].isPlayed and episodes[2].isPlayed   # both episodes of the multi-episode file
    assert episodes[3].viewOffset == 4242
    assert mo.push_watch_forward(plex, CONFIG, APPLIED, {}, dry_run=False) == {}
