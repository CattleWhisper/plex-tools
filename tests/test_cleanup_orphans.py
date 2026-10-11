import os

import cleanup_orphans as co

STEM = "[10] Ele renasce em um NOVO corpo, e ganha um sistema de jogo ao ganhar poder infinito. [YJSfUVczqIg]"
SIDECARS = [f"{STEM}.en.vtt", f"{STEM}.info.json", f"{STEM}.jpg"]


def touch(directory, name):
    path = os.path.join(directory, name)
    with open(path, 'w') as f:
        f.write('x')
    return path


def test_extract_id_handles_brackets_in_title():
    assert co.extract_id(f"{STEM}.info.json") == "YJSfUVczqIg"
    assert co.extract_id(f"{STEM}.mp4") == "YJSfUVczqIg"
    assert co.extract_id("no id here.jpg") == ""


def test_sidecars_kept_when_video_exists():
    assert co.find_orphans_in_dir(SIDECARS + [f"{STEM}.mp4"]) == []


def test_sidecars_orphaned_when_video_deleted():
    assert co.find_orphans_in_dir(SIDECARS) == sorted(SIDECARS)


def test_other_container_counts_as_video():
    assert co.find_orphans_in_dir(SIDECARS + [f"{STEM}.mkv"]) == []


def test_in_progress_download_is_not_touched():
    names = SIDECARS + [f"{STEM}.f137.mp4.part"]
    assert co.find_orphans_in_dir(names) == []


def test_files_without_id_are_ignored():
    assert co.find_orphans_in_dir(["poster.jpg", "notes.txt"]) == []


def test_video_in_other_directory_does_not_count(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    touch(tmp_path / "a", f"{STEM}.mp4")
    orphan = touch(tmp_path / "b", f"{STEM}.jpg")
    assert co.find_orphans([str(tmp_path)]) == [orphan]


def test_delete_and_remove_empty_dirs(tmp_path):
    sub = tmp_path / "channel"
    sub.mkdir()
    paths = [touch(sub, n) for n in SIDECARS]
    keep = touch(tmp_path, "other [abcdefghijk].mp4")

    deleted, freed = co.delete_files(co.find_orphans([str(tmp_path)]))
    assert deleted == 3 and freed == 3
    assert not any(os.path.exists(p) for p in paths)
    assert os.path.exists(keep)

    assert co.remove_empty_dirs([str(tmp_path)]) == 1
    assert not sub.exists() and tmp_path.exists()
