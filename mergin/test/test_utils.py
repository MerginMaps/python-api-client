import pytest

from ..utils import path_matches_filter, filter_files
from ..common import ClientError


@pytest.mark.parametrize(
    "path, include, exclude, expected",
    [
        # no filter at all -> everything kept
        pytest.param("anything/at/all.gpkg", None, None, True, id="no-filter"),
        pytest.param("data.gpkg", [], None, True, id="empty-include-list"),
        pytest.param("data.gpkg", None, [], True, id="empty-exclude-list"),
        # basic include/exclude
        pytest.param("data.gpkg", ["*.gpkg"], None, True, id="include-match"),
        pytest.param("data.txt", ["*.gpkg"], None, False, id="include-no-match"),
        pytest.param("media/photo.jpg", None, ["media/*"], False, id="exclude-match"),
        pytest.param("data.gpkg", None, ["media/*"], True, id="exclude-no-match"),
        # subfolders: fnmatch's '*' crosses '/', so it reaches arbitrarily deep
        pytest.param("media/photo.jpg", None, ["media/*"], False, id="subfolder-direct-child"),
        pytest.param("media/sub/deep/photo.jpg", None, ["media/*"], False, id="subfolder-deeply-nested"),
        pytest.param("layer.gpkg", ["*.gpkg"], None, True, id="extension-pattern-at-root"),
        pytest.param("data/nested/deep/layer.gpkg", ["*.gpkg"], None, True, id="extension-pattern-at-any-depth"),
        # patterns still anchor to the *full* path, not just the basename
        pytest.param("nested/media/photo.jpg", None, ["media/*"], True, id="not-anchored-to-basename-kept"),
        pytest.param("nested/media/photo.jpg", None, ["*/media/*"], False, id="leading-star-catches-nested-media"),
        pytest.param("media/photo.jpg", None, ["*/media/*"], True, id="leading-star-misses-root-level-media"),
        # case sensitivity: fnmatchcase, not fnmatch - always case-sensitive, any OS
        pytest.param("data.GPKG", ["*.gpkg"], None, False, id="case-mismatch-in-path"),
        pytest.param("data.gpkg", ["*.GPKG"], None, False, id="case-mismatch-in-pattern"),
        pytest.param("Media/photo.jpg", None, ["media/*"], True, id="case-mismatch-in-directory-kept"),
        # lists of patterns: a path matches if it matches ANY pattern in the list (OR)
        pytest.param("data.gpkg", ["*.gpkg", "*.qgz", "project.qgs"], None, True, id="include-list-1st-matches"),
        pytest.param("map.qgz", ["*.gpkg", "*.qgz", "project.qgs"], None, True, id="include-list-2nd-matches"),
        pytest.param("project.qgs", ["*.gpkg", "*.qgz", "project.qgs"], None, True, id="include-list-3rd-matches"),
        pytest.param("readme.txt", ["*.gpkg", "*.qgz", "project.qgs"], None, False, id="include-list-none-match"),
        pytest.param("media/photo.jpg", None, ["media/*", "*.tmp", "*-wal"], False, id="exclude-list-1st-matches"),
        pytest.param("scratch.tmp", None, ["media/*", "*.tmp", "*-wal"], False, id="exclude-list-2nd-matches"),
        pytest.param("data.gpkg-wal", None, ["media/*", "*.tmp", "*-wal"], False, id="exclude-list-3rd-matches"),
        pytest.param("data.gpkg", None, ["media/*", "*.tmp", "*-wal"], True, id="exclude-list-none-match"),
    ],
)
def test_path_matches_filter(path, include, exclude, expected):
    assert path_matches_filter(path, include=include, exclude=exclude) is expected


@pytest.mark.parametrize(
    "include, exclude, expected_paths",
    [
        pytest.param(None, None, {"a.gpkg", "b.qgz", "c.txt", "media/d.gpkg"}, id="no-filter"),
        pytest.param(["*.gpkg", "*.qgz"], None, {"a.gpkg", "b.qgz", "media/d.gpkg"}, id="include-list"),
        pytest.param(None, ["media/*"], {"a.gpkg", "b.qgz", "c.txt"}, id="exclude-subfolder"),
    ],
)
def test_filter_files(include, exclude, expected_paths):
    files = [{"path": p} for p in ["a.gpkg", "b.qgz", "c.txt", "media/d.gpkg"]]
    result = filter_files(files, include=include, exclude=exclude)
    assert {f["path"] for f in result} == expected_paths


def test_filter_files_keeps_matching_dicts_as_is():
    """filter_files() passes matching dicts through unchanged"""
    files = [
        {"path": "project.gpkg", "size": 100},
        {"path": "media/photo.jpg", "size": 999},
    ]

    result = filter_files(files, exclude=["media/*"])

    assert result == [{"path": "project.gpkg", "size": 100}]
    assert result[0] is files[0]


def test_filter_files_raises_on_mutually_exclusive_args():
    """Unlike path_matches_filter, filter_files() is a public entry point (decorated with
    @validates_file_filter) and does enforce that include/exclude are mutually exclusive.
    """
    files = [{"path": "a.gpkg"}]
    with pytest.raises(ClientError, match="Cannot use both include and exclude"):
        filter_files(files, include=["*.gpkg"], exclude=["*.txt"])
