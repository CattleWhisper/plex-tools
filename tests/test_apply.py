import os
from io import BytesIO

import pytest
from PIL import Image

import manhwa_organizer as mo


def thumb_bytes(color=(200, 30, 30), size=(1280, 720), fmt='WEBP'):
    buffer = BytesIO()
    Image.new('RGB', size, color).save(buffer, fmt)
    return buffer.getvalue()


def fetch_image(ytid, source=''):
    return thumb_bytes()


def video(ytid, title, summary='', date='2026-01-01'):
    return {'ytid': ytid, 'channel': 'Chan', 'title': f'Chan - {title}', 'summary': summary, 'date': date,
            'file': f'/data/Manhwa/Chan - {title} [{ytid}].mp4'}


@pytest.fixture
def env(tmp_path):
    source_dir = tmp_path / 'media' / 'Manhwa'
    source_dir.mkdir(parents=True)
    catalog = [
        video('SWORDPART01', 'Sword Saint Part 1', date='2026-01-01'),
        video('SWORDPART02', 'Sword Saint Part 2', date='2026-02-01'),
        video('LONELONELON', 'Lone Story Here', date='2026-03-01'),
    ]
    for entry in catalog:
        (source_dir / os.path.basename(entry['file'])).write_bytes(b'video ' + entry['ytid'].encode())
    config = dict(mo.DEFAULT_CONFIG, target_root=str(tmp_path / 'media' / 'Manhwa-Series'),
                  path_map={'/data': str(tmp_path / 'media')}, link_mode='hardlink')
    return {'catalog': catalog, 'config': config, 'source_dir': source_dir,
            'target_root': tmp_path / 'media' / 'Manhwa-Series'}


def run(env, applied=None, overrides=None, dry_run=False, catalog=None):
    plan = mo.build_plan(catalog or env['catalog'], overrides, applied)
    return mo.apply_plan(plan, env['config'], applied or {}, fetch_image, dry_run=dry_run)


def test_apply_plan_defaults_to_dry_run(env):
    mo.apply_plan(mo.build_plan(env['catalog']), env['config'], {}, fetch_image)
    assert not env['target_root'].exists()


def snapshot(root):
    return sorted(os.path.relpath(os.path.join(d, f), root) for d, _, fs in os.walk(root) for f in fs)


def test_dry_run_changes_nothing(env):
    result = run(env, dry_run=True)
    assert result.count('link') == 3 and result.count('thumb') == 3
    assert result.count('poster') == 2 and result.count('background') == 2
    assert result.count('error') == 0
    assert not env['target_root'].exists()


def test_apply_creates_hardlinks_thumbs_poster_and_background(env):
    result = run(env)
    assert result.count('error') == 0
    root = env['target_root']
    episode = root / 'Sword Saint' / 'Season 01' / 'Sword Saint - S01E02 [SWORDPART02].mp4'
    source = env['source_dir'] / 'Chan - Sword Saint Part 2 [SWORDPART02].mp4'
    assert os.path.samefile(episode, source)
    assert os.stat(source).st_nlink == 2

    with Image.open(episode.with_suffix('.jpg')) as thumb:
        assert thumb.format == 'JPEG' and thumb.size == (1280, 720)
    with Image.open(root / 'Sword Saint' / 'poster.jpg') as poster:
        assert poster.format == 'JPEG' and poster.size == (1000, 1500)
    assert (root / 'Sword Saint' / 'background.jpg').exists()
    assert (root / 'Lone Story Here' / 'Season 01' / 'Lone Story Here - S01E01 [LONELONELON].mp4').exists()
    assert sorted(result.new) == ['LONELONELON', 'SWORDPART01', 'SWORDPART02']
    assert set(result.applied) == {'LONELONELON', 'SWORDPART01', 'SWORDPART02'}


def test_second_run_is_a_no_op(env):
    first = run(env)
    before = snapshot(env['target_root'])
    second = run(env, applied=first.applied)
    assert second.actions == []
    assert second.new == second.moved == second.removed == []
    assert snapshot(env['target_root']) == before


def test_moved_video_removes_old_link_and_empty_folder(env):
    first = run(env)
    lone_dir = env['target_root'] / 'Lone Story Here'
    assert lone_dir.exists()

    overrides = {'merge': [['SWORDPART01', 'LONELONELON']], 'parts': {'LONELONELON': 3}}
    second = run(env, applied=first.applied, overrides=overrides)
    assert second.moved == ['LONELONELON']
    assert second.count('error') == 0
    assert not lone_dir.exists()
    moved = env['target_root'] / 'Sword Saint' / 'Season 01' / 'Sword Saint - S01E03 [LONELONELON].mp4'
    assert os.path.samefile(moved, env['source_dir'] / 'Chan - Lone Story Here [LONELONELON].mp4')
    assert moved.with_suffix('.jpg').exists()
    # originals untouched
    assert len(list(env['source_dir'].iterdir())) == 3


def test_removed_video_is_cleaned_up(env):
    first = run(env)
    catalog = [e for e in env['catalog'] if e['ytid'] != 'LONELONELON']
    second = run(env, applied=first.applied, catalog=catalog)
    assert second.removed == ['LONELONELON']
    assert not (env['target_root'] / 'Lone Story Here').exists()
    assert 'LONELONELON' not in second.applied
    assert (env['source_dir'] / 'Chan - Lone Story Here [LONELONELON].mp4').exists()


def test_existing_poster_is_kept_unless_forced(env):
    first = run(env)
    poster = env['target_root'] / 'Sword Saint' / 'poster.jpg'
    Image.new('RGB', (10, 15)).save(poster, 'JPEG')
    plan = mo.build_plan(env['catalog'], applied=first.applied)

    mo.apply_plan(plan, env['config'], first.applied, fetch_image, dry_run=False)
    assert Image.open(poster).size == (10, 15)
    mo.apply_plan(plan, env['config'], first.applied, fetch_image, dry_run=False, force_posters=True)
    assert Image.open(poster).size == (1000, 1500)


def test_missing_source_and_foreign_file_are_errors(env):
    (env['source_dir'] / 'Chan - Lone Story Here [LONELONELON].mp4').unlink()
    foreign = env['target_root'] / 'Sword Saint' / 'Season 01' / 'Sword Saint - S01E01 [SWORDPART01].mp4'
    foreign.parent.mkdir(parents=True)
    foreign.write_bytes(b'something else')

    result = run(env)
    errors = {a.path: a.detail for a in result.actions if a.kind == 'error'}
    assert any('source missing' in d for d in errors.values())
    assert 'is not a hardlink' in errors[str(foreign)]
    assert foreign.read_bytes() == b'something else'
    assert 'LONELONELON' not in result.applied


def test_unknown_files_keep_orphan_folder(env):
    first = run(env)
    (env['target_root'] / 'Lone Story Here' / 'notes.txt').write_text('mine')
    catalog = [e for e in env['catalog'] if e['ytid'] != 'LONELONELON']
    second = run(env, applied=first.applied, catalog=catalog)
    assert (env['target_root'] / 'Lone Story Here' / 'notes.txt').exists()
    assert any(a.kind == 'error' and 'unknown files' in a.detail for a in second.actions)


def test_validate_rejects_target_inside_source(env):
    config = dict(env['config'], target_root=str(env['source_dir'] / 'Series'))
    assert mo.validate_apply_config(config, {str(env['source_dir'])})
    assert mo.validate_apply_config(env['config'], {str(env['source_dir'])}) == []
    assert mo.validate_apply_config(dict(env['config'], link_mode='move'), set())


def test_poster_wraps_long_names():
    from PIL import ImageDraw
    draw = ImageDraw.Draw(Image.new('RGB', (10, 10)))
    title = 'Everyone Prayed for a 1 in a Million Drop. I Just Selected It and Became the Strongest ' * 2
    font, lines, _ = mo.fit_poster_text(draw, title, 880, 500)
    assert 1 < len(lines) <= mo.POSTER_MAX_LINES
    assert all(draw.textlength(line, font=font) <= 880 for line in lines)


def test_cli_apply_dry_run(env, tmp_path):
    state = tmp_path / 'state'
    plan = mo.build_plan(env['catalog'])
    mo.write_plan(plan, str(state))
    config_file = tmp_path / 'config.yaml'
    config_file.write_text(
        f"state_dir: {state}\ntarget_root: {env['target_root']}\nlink_mode: hardlink\n"
        f"path_map:\n  /data: {tmp_path / 'media'}\n")
    assert mo.main(['--config', str(config_file), 'apply', '--dry-run']) == 0
    assert not env['target_root'].exists()
    assert not (state / 'applied.json').exists()
    assert (state / 'apply_dryrun.tsv').read_text().count('link\t') == 3


def test_image_fetcher_prefers_ytdlp_sidecar_without_connecting(tmp_path):
    source = tmp_path / 'Chan - T [abcdefghijk].mp4'
    source.write_bytes(b'v')
    (tmp_path / 'Chan - T [abcdefghijk].webp').write_bytes(b'sidecar')

    def connect():
        raise AssertionError('must not connect to Plex when a sidecar exists')

    fetch = mo.make_image_fetcher(connect, {})
    assert fetch('abcdefghijk', str(source)) == b'sidecar'


def test_write_problems_are_reported_not_raised(env, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError(30, 'Read-only file system')

    monkeypatch.setattr(mo, 'link_file', fail)
    result = run(env)
    assert result.count('error') == 3
    assert result.applied == {}
    assert mo.target_writable_problem(str(env['target_root'])) is None


def test_target_writable_problem_detects_read_only(tmp_path):
    read_only = tmp_path / 'ro'
    read_only.mkdir()
    read_only.chmod(0o555)
    try:
        assert 'cannot write' in mo.target_writable_problem(str(read_only / 'Series'))
    finally:
        read_only.chmod(0o755)


def test_macos_junk_files_are_ignored_but_removed_with_orphan_folder(env):
    first = run(env)
    season = env['target_root'] / 'Sword Saint' / 'Season 01'
    (season / '._Sword Saint - S01E01 [SWORDPART01].jpg').write_bytes(b'appledouble')
    (env['target_root'] / 'Sword Saint' / '._poster.jpg').write_bytes(b'appledouble')
    lone = env['target_root'] / 'Lone Story Here'
    (lone / '.DS_Store').write_bytes(b'x')
    (lone / 'Season 01' / '._Lone Story Here - S01E01 [LONELONELON].mp4').write_bytes(b'x')

    second = run(env, applied=first.applied)
    assert second.actions == []

    catalog = [e for e in env['catalog'] if e['ytid'] != 'LONELONELON']
    third = run(env, applied=first.applied, catalog=catalog)
    assert third.count('error') == 0
    assert not lone.exists()
    assert (season / '._Sword Saint - S01E01 [SWORDPART01].jpg').exists()


def test_applied_state_is_rebuilt_from_tree_when_missing(env, tmp_path):
    first = run(env)
    (env['target_root'] / 'Sword Saint' / 'Season 01' / '._Sword Saint - S01E01 [SWORDPART01].mp4').write_bytes(b'x')
    rebuilt = mo.rebuild_applied_from_tree(str(env['target_root']))
    assert rebuilt == {ytid: {'show': entry['show'], 'target': entry['target']} for ytid, entry in first.applied.items()}

    config = dict(env['config'], state_dir=str(tmp_path / 'fresh-state'))
    assert mo.load_applied(config) == rebuilt
    assert (tmp_path / 'fresh-state' / 'applied.json').exists()

    # with the rebuilt state nothing is new, so nothing is re-applied or re-restored
    second = run(env, applied=rebuilt)
    assert second.actions == [] and second.new == []
