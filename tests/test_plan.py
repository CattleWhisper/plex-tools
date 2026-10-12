import csv
import json
import os

import pytest

import manhwa_organizer as mo

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures', 'handoff_catalog.json')
LUMOS = "Lumos Manhwa Recap"
EVER = "Evergarden Manhwa"
EVER_SHOW = "Other Tamers Got Mighty Beasts, My Tamer Got Me as Mosquito That Evolve Without Limit"
LUMOS_B_TITLE = "He Forgot He Has 10 SAINTESSES as His Wives in Dream, Now They Come to Claim Him"


@pytest.fixture
def catalog():
    with open(FIXTURES, encoding='utf-8') as f:
        return json.load(f)


def members_by_id(plan):
    return {m['ytid']: m for s in plan['series'] for m in s['members']}


def video(ytid, title, summary='', date='2026-01-01', channel='Chan'):
    return {'ytid': ytid, 'channel': channel, 'title': title, 'summary': summary,
            'date': date, 'file': f'/media/{channel} - {title} [{ytid}].mp4'}


# --------------------------------------------------------------------------- #
# Real examples from the Lumos and Evergarden channels (tests/fixtures/handoff_catalog.json)
# --------------------------------------------------------------------------- #

def test_fixtures_expected_table(catalog):
    plan = mo.build_plan(catalog)
    by_id = members_by_id(plan)

    expected = {
        'msNnq-I5oMI': (LUMOS_B_TITLE, 'S01E02'),
        'LUMOSPART06': (LUMOS_B_TITLE, 'S01E06'),
        'EVERGPART06': (EVER_SHOW, 'S01E06'),
        '4-w0_cnxJvw': (EVER_SHOW, 'S01E13'),
        'EVERGPART20': (EVER_SHOW, 'S01E20'),
    }
    assert {k: (m['show'], m['code']) for k, m in by_id.items()} == expected
    assert len(plan['series']) == 2
    assert all(not m['flags'] for m in by_id.values())


def test_fixtures_show_name_override(catalog):
    overrides = {'show_names': {'msNnq-I5oMI': "He Forgot He Married 10 Saintesses"}}
    by_id = members_by_id(mo.build_plan(catalog, overrides))
    assert by_id['LUMOSPART06']['show'] == "He Forgot He Married 10 Saintesses"
    assert by_id['LUMOSPART06']['target'] == os.path.join(
        "He Forgot He Married 10 Saintesses", "Season 01",
        "He Forgot He Married 10 Saintesses - S01E06 [LUMOSPART06].mp4")


def test_fixtures_part_sources(catalog):
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['LUMOSPART06']['part_source'] == 'gap'            # Patreon PART 7 fills the gap list
    assert by_id['msNnq-I5oMI']['part_source'] == 'cited+gap'      # A cites "PART 2" + own gap
    assert 'self' in by_id['EVERGPART20']['part_source']
    assert 'self' in by_id['4-w0_cnxJvw']['part_source']
    assert 'cited' in by_id['4-w0_cnxJvw']['part_source']         # X cites "Part 13"
    assert by_id['EVERGPART06']['part_source'] == 'gap+self'


def test_z_joined_only_by_title(catalog):
    parsed = {e['ytid']: mo.parse_description(e['summary']) for e in catalog}
    z_cited = {t for t, _ in parsed['EVERGPART06'].links}
    xy_cited = {t for i in ('EVERGPART20', '4-w0_cnxJvw') for t, _ in parsed[i].links}
    assert not z_cited & xy_cited

    without_title = [dict(e, title=f"{e['channel']} - unique {e['ytid']}") for e in catalog]
    by_id = members_by_id(mo.build_plan(without_title))
    assert by_id['EVERGPART06']['show'] != by_id['EVERGPART20']['show']


def test_phantom_compilations_create_no_episodes(catalog):
    by_id = members_by_id(mo.build_plan(catalog))
    assert 'c3v7bdY91Qo' not in by_id and 'ZhrYqUNurgs' not in by_id
    assert len(by_id) == 5


def test_prose_numbers_are_not_parts(catalog):
    a = mo.parse_description(next(e for e in catalog if e['ytid'] == 'LUMOSPART06')['summary'])
    assert a.self_part is None
    assert a.known_numbers == {1, 2, 3, 4, 5, 7}       # "part of his memories" ignored

    x = mo.parse_description(next(e for e in catalog if e['ytid'] == 'EVERGPART20')['summary'])
    assert x.self_part == 20                            # "6 days" ignored
    assert x.known_numbers == set(range(1, 20))
    assert ('c3v7bdY91Qo', (1, 10)) in x.links


# --------------------------------------------------------------------------- #
# Acceptance 2: synthetic cases
# --------------------------------------------------------------------------- #

def test_unlabelled_link_does_not_join_series():
    catalog = [
        video('AAAAAAAAAA1', 'Sword Saint Returns Again', 'Part 1 https://youtu.be/AAAAAAAAAA0\n'
              'Check out my other story: https://youtu.be/BBBBBBBBBB1'),
        video('BBBBBBBBBB1', 'Necromancer Academy Genius', 'Veja também https://youtu.be/AAAAAAAAAA1'),
    ]
    plan = mo.build_plan(catalog)
    assert len(plan['series']) == 2


def test_navigation_label_joins_without_number():
    catalog = [
        video('AAAAAAAAAA1', 'Sword Saint Returns Again', date='2026-01-01'),
        video('AAAAAAAAAA2', 'The Saint Sword Comes Back', 'Previous: https://youtu.be/AAAAAAAAAA1',
              date='2026-02-01'),
    ]
    plan = mo.build_plan(catalog)
    assert len(plan['series']) == 1
    by_id = members_by_id(plan)
    assert (by_id['AAAAAAAAAA1']['code'], by_id['AAAAAAAAAA2']['code']) == ('S01E01', 'S01E02')


def test_description_citing_own_id_skips_gap_inference():
    info = mo.parse_description('Part 1 https://youtu.be/AAAAAAAAAA1\nPart 3 https://youtu.be/AAAAAAAAAA3')
    assert mo.infer_part_from_gap('AAAAAAAAAA1', info) is None
    assert mo.infer_part_from_gap('AAAAAAAAAA2', info) == 2


def test_two_parts_with_same_number_newer_goes_to_specials():
    catalog = [
        video('AAAAAAAAAA1', 'Sword Saint Returns', 'This is part 2', date='2026-01-01'),
        video('AAAAAAAAAA2', 'Sword Saint Returns', 'This is part 2', date='2026-03-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['AAAAAAAAAA1']['code'] == 'S01E02'
    assert by_id['AAAAAAAAAA2']['code'] == 'S00E01'
    assert 'duplicada' in by_id['AAAAAAAAAA2']['flags']


def test_compilation_covering_only_existing_parts_goes_to_specials():
    catalog = [
        video('COMPILATION', 'Sword Saint Returns Full', date='2026-05-01'),
        video('PART1PART1P', 'Sword Saint Returns Part 1', date='2026-01-01'),
        video('PART2PART2P', 'Sword Saint Returns Part 2', date='2026-02-01'),
        video('PART3PART3P', 'Sword Saint Returns Part 3', date='2026-03-01'),
        video('NEWLINKER01', 'Sword Saint Returns Part 4', 'Part 1-3 https://youtu.be/COMPILATION',
              date='2026-06-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['COMPILATION']['part'] == [1, 3]
    assert by_id['COMPILATION']['code'] == 'S00E01'
    assert 'compilacao' in by_id['COMPILATION']['flags']
    assert by_id['NEWLINKER01']['code'] == 'S01E04'


def test_compilation_takes_the_part_missing_as_individual_video():
    # juoJxqY9JLw / rxegnhZoQwo: "Part 1-4" and "Part 1-6" while parts 4 and 6 only exist inside them
    catalog = [
        video('PART1PART1P', 'Betrayed by My Friend', 'This is part 1', date='2026-07-14'),
        video('PART2PART2P', 'Betrayed by My Friend', 'This is part 2', date='2026-07-17'),
        video('PART3PART3P', 'Betrayed by My Friend', 'This is part 3', date='2026-07-23'),
        video('COMPIL1TO4P', 'Betrayed by My Friend', date='2026-07-28'),
        video('PART5PART5P', 'Betrayed by My Friend', 'Part 1-4 https://youtu.be/COMPIL1TO4P\nThis is part 5',
              date='2026-08-17'),
        video('COMPIL1TO6P', 'Betrayed by My Friend', date='2026-09-13'),
        video('PART7PART7P', 'Betrayed by My Friend', 'Part 1-6 https://youtu.be/COMPIL1TO6P\nThis is part 7',
              date='2026-09-22'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['COMPIL1TO4P']['code'] == 'S01E04'
    assert by_id['COMPIL1TO6P']['code'] == 'S01E06'
    assert 'compilacao parcial' in by_id['COMPIL1TO6P']['flags']


def test_timestamp_chapters_make_a_compilation():
    # pU3W-2exz_M: "Part 1 - 0:00 / Part 2 - 2:05:22 / Part 3 - 4:11:19" with parts 1 and 2 present
    catalog = [
        video('PART1PART1P', 'Synthesis Furnace Apocalypse', 'Part 2 https://youtu.be/PART2PART2P',
              date='2026-06-15'),
        video('PART2PART2P', 'Synthesis Furnace Apocalypse', 'Part 1 https://youtu.be/PART1PART1P',
              date='2026-07-02'),
        video('PU3W2EXZ_MX', 'Synthesis Furnace Apocalypse',
              'Part 1 - 0:00\nPart 2 - 2:05:22\nPart 3 - 4:11:19\nSurvival for 7 days', date='2026-07-24'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['PU3W2EXZ_MX']['part'] == [1, 3]
    assert by_id['PU3W2EXZ_MX']['code'] == 'S01E03'


def test_timestamp_label_after_time():
    info = mo.parse_description('0:00 - Part 1\n2:11:45 - Part 2\n10:05:03- Part 10\n16:28:26- Part 16')
    assert info.timestamp_parts == {1, 2, 10, 16}
    assert not info.known_numbers


def test_strong_citations_outrank_title_number():
    # BrLp_wdN7v8: title "Manhwa Recap 13" but the following parts cite it as 14
    catalog = [
        video('TARGETVIDEO', 'Prayed for a Drop - Manhwa Recap 13', date='2026-01-01'),
        video('CITER000001', 'Prayed for a Drop', 'Part 14 https://youtu.be/TARGETVIDEO', date='2026-02-01'),
        video('CITER000002', 'Prayed for a Drop', 'Part 14 https://youtu.be/TARGETVIDEO', date='2026-03-01'),
    ]
    member = members_by_id(mo.build_plan(catalog))['TARGETVIDEO']
    assert member['part'] == [14, 14]
    assert not member['flags']


def test_description_citing_same_video_twice_is_ignored_for_it():
    # gSGzkx5NhZg lists "PART1:<xC5>" and "PART2:<xC5>"
    catalog = [
        video('XC5PQ3NNPLU', 'Hot Women Are Useless', 'PART1: https://youtu.be/P4BDKLE5SZC', date='2026-07-07'),
        video('GSGZKX5NHZG', 'Hot Women Are Useless',
              'PART1:https://youtu.be/XC5PQ3NNPLU\nPART2:https://youtu.be/XC5PQ3NNPLU', date='2026-08-15'),
    ]
    member = members_by_id(mo.build_plan(catalog))['XC5PQ3NNPLU']
    assert member['code'] == 'S01E02'
    assert not member['flags']


def test_single_wrong_reference_against_majority_is_not_a_conflict():
    votes = [((12, 12), 2, f'cited:{i}') for i in range(5)] + [((11, 11), 2, 'cited:wrong')]
    decision = mo.decide_part(votes)
    assert decision.part == (12, 12) and not decision.conflict


def test_self_claim_contradicted_by_own_list_is_dropped():
    # kxSzPbwNMkA: lists "Part 21 <other video>" and still says "This is part 21" -> it is 22
    catalog = [
        video('NPPQ3Z9PFBI', 'Mosquito Tamer', 'Part 20 https://youtu.be/XB2T67JMRV8\nThis is part 21',
              date='2026-10-10'),
        video('KXSZPBWNMKA', 'Mosquito Tamer',
              'Part 1-20 https://youtu.be/C3V7BDY91QO\nPart 21 https://youtu.be/NPPQ3Z9PFBI\nThis is part 21',
              date='2026-10-11'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['NPPQ3Z9PFBI']['code'] == 'S01E21'
    assert by_id['KXSZPBWNMKA']['code'] == 'S01E22'
    assert not by_id['KXSZPBWNMKA']['flags']


def test_weak_duplicate_is_placed_by_date_instead_of_specials():
    # QxoIpGcnjVQ and XlLpY2zXK9c both say "PART2: <patreon>" -> both gap-inferred as part 1
    catalog = [
        video('QXOIPGCNJVQ', 'Hired To Clean Poop', 'PART2:https://www.patreon.com/x/part2', date='2026-07-15'),
        video('XLLPY2ZXK9C', 'Hired To Clean Poop', 'PART2:https://www.patreon.com/x/3-hired', date='2026-07-29'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['QXOIPGCNJVQ']['code'] == 'S01E01'
    assert by_id['XLLPY2ZXK9C']['code'] == 'S01E02'
    assert by_id['XLLPY2ZXK9C']['flags'] == ['inferido pela data']


def test_free_compilation_becomes_multi_episode():
    catalog = [
        video('COMPILATION', 'Sword Saint Returns Full', date='2026-01-01'),
        video('PART4PART4P', 'Sword Saint Returns Part 4', 'Part 1-3 https://youtu.be/COMPILATION',
              date='2026-02-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['COMPILATION']['code'] == 'S01E01-E03'
    assert by_id['COMPILATION']['target'].endswith('S01E01-E03 [COMPILATION].mp4')
    assert by_id['PART4PART4P']['code'] == 'S01E04'


def test_series_without_numbers_ordered_by_date():
    catalog = [
        video('AAAAAAAAAA3', 'Sword Saint Returns', date='2026-03-01'),
        video('AAAAAAAAAA1', 'Sword Saint Returns', date='2026-01-01'),
        video('AAAAAAAAAA2', 'Sword Saint Returns', date='2026-02-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert [by_id[f'AAAAAAAAAA{i}']['code'] for i in (1, 2, 3)] == ['S01E01', 'S01E02', 'S01E03']
    assert all(not m['flags'] for m in by_id.values())


def test_unnumbered_member_placed_after_previous_numbered_part():
    catalog = [
        video('AAAAAAAAAA1', 'Sword Saint Returns Part 1', date='2026-01-01'),
        video('AAAAAAAAAA2', 'Sword Saint Returns', date='2026-02-01'),
        video('AAAAAAAAAA5', 'Sword Saint Returns Part 5', date='2026-05-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['AAAAAAAAAA2']['code'] == 'S01E02'
    assert 'inferido pela data' in by_id['AAAAAAAAAA2']['flags']


def test_conflicting_strong_votes_are_flagged():
    catalog = [
        video('TARGETVIDEO', 'Sword Saint Returns', 'This is part 4', date='2026-01-01'),
        video('CITERVIDEO1', 'Sword Saint Returns Part 9', 'Part 5 https://youtu.be/TARGETVIDEO',
              date='2026-02-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['TARGETVIDEO']['part'] == [4, 4]
    assert 'conflito' in by_id['TARGETVIDEO']['flags']


def test_lone_video_is_its_own_series():
    by_id = members_by_id(mo.build_plan([video('LONELONELON', 'Sword Saint Returns')]))
    assert by_id['LONELONELON']['code'] == 'S01E01'
    assert by_id['LONELONELON']['show'] == 'Sword Saint Returns'


def test_short_title_keys_do_not_group():
    catalog = [video('AAAAAAAAAA1', 'Recap'), video('AAAAAAAAAA2', 'Recap')]
    assert len(mo.build_plan(catalog)['series']) == 2


def test_overrides_parts_merge_detach():
    catalog = [
        video('AAAAAAAAAA1', 'Sword Saint Returns', date='2026-01-01'),
        video('AAAAAAAAAA2', 'Sword Saint Returns', date='2026-02-01'),
        video('BBBBBBBBBB1', 'Necromancer Academy Genius', date='2026-03-01'),
    ]
    overrides = {'parts': {'AAAAAAAAAA2': 7}, 'merge': [['AAAAAAAAAA1', 'BBBBBBBBBB1']],
                 'detach': ['AAAAAAAAAA2']}
    plan = mo.build_plan(catalog, overrides)
    by_id = members_by_id(plan)
    assert by_id['AAAAAAAAAA1']['show'] == by_id['BBBBBBBBBB1']['show']
    assert by_id['AAAAAAAAAA2']['show'] != by_id['AAAAAAAAAA1']['show']
    assert by_id['AAAAAAAAAA2']['code'] == 'S01E07'


def test_pinned_name_from_applied_state():
    catalog = [video('AAAAAAAAAA1', 'Sword Saint Returns Part 2')]
    applied = {'AAAAAAAAAA1': {'show': 'Old Pinned Name'}}
    assert members_by_id(mo.build_plan(catalog, applied=applied))['AAAAAAAAAA1']['show'] == 'Old Pinned Name'


def test_name_collision_appends_channel_then_counter():
    catalog = [
        video('AAAAAAAAAA1', 'Sword Saint', channel='Chan A', date='2026-01-01'),
        video('BBBBBBBBBB1', 'Sword Saint', channel='Chan B', date='2026-02-01'),
        video('CCCCCCCCCC1', 'Sword Saint (Chan B)', channel='Chan C', date='2026-03-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    names = {by_id[i]['show'] for i in ('AAAAAAAAAA1', 'BBBBBBBBBB1', 'CCCCCCCCCC1')}
    assert by_id['AAAAAAAAAA1']['show'] == 'Sword Saint'
    assert by_id['BBBBBBBBBB1']['show'] == 'Sword Saint (Chan B)'
    assert len(names) == 3


# --------------------------------------------------------------------------- #
# Parsing helpers
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize('url, ytid', [
    ('https://youtu.be/ZhrYqUNurgs', 'ZhrYqUNurgs'),
    ('https://www.youtube.com/watch?v=p0vR0OjmvHw&ab_channel=X', 'p0vR0OjmvHw'),
    ('https://www.youtube.com/watch?feature=share&v=p0vR0OjmvHw', 'p0vR0OjmvHw'),
    ('https://youtube.com/shorts/abcdefghijk', 'abcdefghijk'),
])
def test_youtube_id_regex(url, ytid):
    assert mo.YT_ID_RE.search(url).group(1) == ytid


@pytest.mark.parametrize('title, channel, expected', [
    ("Evergarden Manhwa - Some Story - Manhwa Recap", "Evergarden Manhwa", "Some Story"),
    ("Some Story | Manhwa Recap", "X", "Some Story"),
    ("Chan: Some Story (Manhwa Recap) Part 3", "Chan", "Some Story"),
    ("Chan - Part 2", "Chan", "Chan - Part 2"),
    ("Chan - (Part 1) Elas o HUMILHARAM", "Chan", "Elas o HUMILHARAM"),
])
def test_clean_title(title, channel, expected):
    assert mo.clean_title(title, channel) == expected


def test_sanitize_folder_name():
    assert mo.sanitize_folder_name('A/B: C? "D" ...  ') == 'AB C D'
    assert len(mo.sanitize_folder_name('x' * 300)) == mo.MAX_SHOW_NAME_LEN


def test_extract_ytid_from_path():
    assert mo.extract_ytid_from_path('/m/Chan - T [abc-DEF_123].mkv') == 'abc-DEF_123'
    assert mo.extract_ytid_from_path('/m/no id.mkv') is None


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def test_plan_command_writes_json_and_csv(tmp_path):
    config = tmp_path / 'config.yaml'
    config.write_text(f"state_dir: {tmp_path / 'state'}\n")
    assert mo.main(['--config', str(config), '--overrides', str(tmp_path / 'none.yaml'),
                    'plan', '--catalog', FIXTURES]) == 0

    with open(tmp_path / 'state' / 'plan.csv', encoding='utf-8-sig') as f:
        rows = {r['ytid']: r for r in csv.DictReader(f)}
    assert rows['EVERGPART20']['code'] == 'S01E20'
    assert rows['LUMOSPART06']['part'] == '6'
    plan = json.loads((tmp_path / 'state' / 'plan.json').read_text())
    assert plan['summary']['series'] == 2


# --------------------------------------------------------------------------- #
# Real-data adaptations (approved divergences)
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize('title, channel, expected', [
    ("Nokoi Recap - Hired To Clean", "Nokoi  Recap", "Hired To Clean"),
    ("Briel VC - Manhwa Recap - Story Here", "Briel  VC - Manhwa  Recap", "Story Here"),
    ("MANGA | MANIA - Story Here", "MANGA ｜ MANIA", "Story Here"),
    ("Juba - Manhwa Recap - Ele Divide o Corpo", "Juba - Manhwa Recap", "Ele Divide o Corpo"),
    ("Jubertim - Story", "Juba", "Jubertim - Story"),
])
def test_strip_channel_prefix_tolerates_spacing_and_width(title, channel, expected):
    assert mo.strip_channel_prefix(title, channel) == expected


@pytest.mark.parametrize('title, channel, part, cleaned', [
    ("Haru Senpai - (13) Forced To Army at 13", "Haru Senpai", (13, 13), "Forced To Army at 13"),
    ("Evergarden Manhwa - I Inherited a Dead Mountain - Manhwa Recap 5", "Evergarden Manhwa", (5, 5),
     "I Inherited a Dead Mountain"),
    ("Kawa - Ele DORMIU e Conseguiu o RANK #1", "Kawa", None, "Ele DORMIU e Conseguiu o RANK #1"),
])
def test_title_part_formats(title, channel, part, cleaned):
    assert mo.parse_title_part(title, channel) == part
    assert mo.clean_title(title, channel) == cleaned


def test_numbered_titles_group_and_number_episodes():
    catalog = [
        video('AAAAAAAAAA1', 'Haru Senpai - (2) Forced To Army at 13', channel='Haru Senpai', date='2026-01-01'),
        video('AAAAAAAAAA2', 'Haru Senpai - (3) Forced To Army at 13', channel='Haru Senpai', date='2026-02-01'),
        video('BBBBBBBBBB1', 'Ever - Dead Mountain Debt - Manhwa Recap 4', channel='Ever', date='2026-01-01'),
        video('BBBBBBBBBB2', 'Ever - Dead Mountain Debt - Manhwa Recap 5', channel='Ever', date='2026-02-01'),
    ]
    plan = mo.build_plan(catalog)
    by_id = members_by_id(plan)
    assert len(plan['series']) == 2
    assert [by_id[i]['code'] for i in ('AAAAAAAAAA1', 'AAAAAAAAAA2')] == ['S01E02', 'S01E03']
    assert [by_id[i]['code'] for i in ('BBBBBBBBBB1', 'BBBBBBBBBB2')] == ['S01E04', 'S01E05']
    assert by_id['AAAAAAAAAA1']['show'] == 'Forced To Army at 13'
    assert by_id['BBBBBBBBBB1']['show'] == 'Dead Mountain Debt'


def test_duplicate_youtube_id_keeps_latest_added_file():
    old = dict(video('JSaoESzsJLg', 'Old Title Here'), addedAt='2025-12-29T10:00:00', ratingKey=22521)
    new = dict(video('JSaoESzsJLg', 'New Title Here'), addedAt='2026-01-03T10:00:00', ratingKey=22806)
    plan = mo.build_plan([new, old])
    member = members_by_id(plan)['JSaoESzsJLg']
    assert member['title'] == 'New Title Here'
    assert 'id duplicado' in member['flags']
    assert plan['ignored_duplicates'] == {'JSaoESzsJLg': [old['file']]}
    assert plan['summary']['ignored_duplicate_files'] == 1


@pytest.mark.parametrize('title, part, cleaned', [
    ("Juba - Manhwa Recap - [3] Ele possuiu o BOSS", (3, 3), "Ele possuiu o BOSS"),
    ("Juba - Manhwa Recap - [1-17] O MUNDO ACABOU - MANHWA RECAP", (1, 17), "O MUNDO ACABOU"),
    ("Juba - Manhwa Recap - (1-4) Naufraguei numa ilha", (1, 4), "Naufraguei numa ilha"),
    ("Juba - Manhwa Recap - #1+2 He Owns a System [MANHWA RECAP]", (1, 2), "He Owns a System"),
    ("Juba - Manhwa Recap - #1 He Owns a System", (1, 1), "He Owns a System"),
    ("Juba - Manhwa Recap - Ele virou o RANK #1", None, "Ele virou o RANK #1"),
    ("Juba - Manhwa Recap - [FULL] Depois de um Bug - Manhwa Recap", None, "Depois de um Bug"),
    ("Juba - Manhwa Recap - Ele volta no tempo (Completo)", None, "Ele volta no tempo"),
    ("Juba - Manhwa Recap - (Final Chapter) - My Shelter Upgrades", None, "My Shelter Upgrades"),
    ("Juba - Manhwa Recap - MILIONÁRIO volta no tempo! - Resumo Manhwa", None, "MILIONÁRIO volta no tempo!"),
    ("Juba - Manhwa Recap - O mundo CONGELOU - MANHWA CAP", None, "O mundo CONGELOU"),
    ("Juba - Manhwa Recap - Ele volta para se VINGAR! Manhwa recap", None, "Ele volta para se VINGAR!"),
    ("Juba - Manhwa Recap - Ele Volta no Tempo - Manhwa Recap!", None, "Ele Volta no Tempo"),
    ("Juba - Manhwa Recap - (FULL) They MOCKED My Skill [ze6FP0fxiYM]", None, "They MOCKED My Skill"),
    ("Juba - Manhwa Recap - [11] Ele foi eliminado, #manhwarecap", (11, 11), "Ele foi eliminado"),
])
def test_more_title_formats(title, part, cleaned):
    channel = "Juba - Manhwa Recap"
    assert mo.parse_title_part(title, channel) == part
    assert mo.clean_title(title, channel) == cleaned


def test_bracket_numbered_titles_form_one_series():
    catalog = [video(f'AAAAAAAAAA{n}', f'Juba - [{n}] Ele possuiu o BOSS Mais poderoso', channel='Juba',
                     date=f'2026-0{n}-01') for n in (2, 3, 4)]
    plan = mo.build_plan(catalog)
    assert len(plan['series']) == 1
    assert [m['code'] for m in plan['series'][0]['members']] == ['S01E02', 'S01E03', 'S01E04']


def test_full_story_joining_numbered_series_goes_to_specials():
    catalog = [
        video('AAAAAAAAAA1', 'They MOCKED My Skill Part 1', date='2026-01-01'),
        video('AAAAAAAAAA2', 'They MOCKED My Skill Part 2', date='2026-02-01'),
        video('FULLFULLFUL', '(FULL) They MOCKED My Skill', date='2026-03-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['FULLFULLFUL']['code'] == 'S00E01'
    assert 'compilacao' in by_id['FULLFULLFUL']['flags']
    assert by_id['FULLFULLFUL']['show'] == by_id['AAAAAAAAAA1']['show']


def test_lone_full_story_is_episode_one():
    by_id = members_by_id(mo.build_plan([video('FULLFULLFUL', '[FULL] Depois de um Bug')]))
    assert by_id['FULLFULLFUL']['code'] == 'S01E01'
    assert by_id['FULLFULLFUL']['show'] == 'Depois de um Bug'


def test_title_number_matching_range_end_is_not_a_conflict():
    votes = [((1, 5), 2, 'cited:X'), ((1, 5), 2, 'cited:Y'), ((5, 5), 2, 'title')]
    decision = mo.decide_part(votes)
    assert decision.part == (1, 5) and not decision.conflict
    decision = mo.decide_part([((12, 12), 2, 'cited:A'), ((12, 12), 2, 'cited:B'), ((11, 11), 2, 'cited:C')])
    assert decision.part == (12, 12) and decision.conflict


def test_timestamp_sections_of_standalone_videos_are_ignored():
    sections = '00:00 - Parte 1\n01:00:11 - Parte 2\n02:10:00 - Parte 3'
    by_id = members_by_id(mo.build_plan([video('LONELONELON', 'Arquimago Reencarnou', sections)]))
    assert by_id['LONELONELON']['code'] == 'S01E01'

    catalog = [
        video('AAAAAAAAAA1', 'Arquimago Reencarnou', sections, date='2026-01-01'),
        video('AAAAAAAAAA2', 'Arquimago Reencarnou', date='2026-02-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert [by_id[i]['code'] for i in ('AAAAAAAAAA1', 'AAAAAAAAAA2')] == ['S01E01', 'S01E02']


def test_oldest_unnumbered_video_before_all_numbered_is_part_one():
    # t96a58Pj3KE: no reference anywhere, older than the "Parte 1 / Parte 2" compilation
    sections = '00:00 - Parte 1\n01:10:00 - Parte 2'
    catalog = [
        video('T96A58PJ3KE', 'Preso em Outro Mundo', date='2025-10-26'),
        video('SIZG6IPGLOY', 'Preso em Outro Mundo', sections, date='2025-11-12'),
        video('NUMBERED003', 'Preso em Outro Mundo', 'This is part 3', date='2025-12-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['T96A58PJ3KE']['code'] == 'S01E01'
    assert by_id['T96A58PJ3KE']['flags'] == ['parte 1 inferida']
    assert by_id['SIZG6IPGLOY']['code'] == 'S01E02'
    assert by_id['NUMBERED003']['code'] == 'S01E03'


def test_unnumbered_video_is_not_part_one_when_part_one_exists_or_it_is_newer():
    catalog = [
        video('OLDUNNUMBER', 'Preso em Outro Mundo', date='2025-10-01'),
        video('PART1PART1P', 'Preso em Outro Mundo', 'This is part 1', date='2025-11-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['PART1PART1P']['code'] == 'S01E01'
    assert by_id['OLDUNNUMBER']['code'] == 'S01E02'

    catalog = [
        video('PART2PART2P', 'Preso em Outro Mundo', 'This is part 2', date='2025-10-01'),
        video('NEWUNNUMBER', 'Preso em Outro Mundo', date='2025-11-01'),
    ]
    by_id = members_by_id(mo.build_plan(catalog))
    assert by_id['NEWUNNUMBER']['code'] == 'S01E03'
