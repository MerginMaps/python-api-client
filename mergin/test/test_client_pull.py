import os
import tempfile
import pytest
from mergin.common import DeltaChangeType, CHUNK_SIZE
from mergin.models import ProjectDeltaItem, ProjectDeltaItemDiff
from mergin.client_pull import prepare_file_destination, get_diff_merge_files, get_download_items


def test_prepare_file_destination():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = "subdir/file.txt"
        dest = prepare_file_destination(tmp_dir, path)

        expected_dir = os.path.join(tmp_dir, "subdir")
        expected_path = os.path.join(expected_dir, "file.txt")

        assert dest == expected_path
        assert os.path.exists(expected_dir)
        assert os.path.isdir(expected_dir)
        assert not os.path.exists(expected_path)  # file should not exist yet


def test_get_diff_merge_files():
    with tempfile.TemporaryDirectory() as tmp_dir:
        item = ProjectDeltaItem(
            change=DeltaChangeType.UPDATE_DIFF,
            path="data.gpkg",
            version="v3",
            size=300,
            checksum="789",
            diffs=[
                ProjectDeltaItemDiff(id="diff2", size=20, version="v2"),
            ],
        )

        merge_files = get_diff_merge_files(item, tmp_dir)

        assert len(merge_files) == 1

        # Check diff
        f2 = merge_files[0]
        assert f2.dest_file == os.path.join(tmp_dir, "diff2")
        assert len(f2.downloaded_items) == 1
        assert f2.downloaded_items[0].file_path == "data.gpkg"
        assert f2.downloaded_items[0].size == 20
        assert f2.downloaded_items[0].version == "v2"


@pytest.fixture
def test_get_download_items():
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Case 1: Small file (one chunk)
        items = get_download_items("small.txt", 100, "v1", tmp_dir)
        assert len(items) == 1
        assert items[0].file_path == "small.txt"
        assert items[0].size == 100
        assert items[0].part_index == 0
        assert items[0].download_file_path == os.path.join(tmp_dir, "small.txt.0")

        # Case 2: Large file (multiple chunks)
        file_size = int(CHUNK_SIZE * 2.5)
        items = get_download_items("large.txt", file_size, "v1", tmp_dir)
        assert len(items) == 3

        # Chunk 0
        assert items[0].size == CHUNK_SIZE
        assert items[0].part_index == 0
        assert items[0].download_file_path == os.path.join(tmp_dir, "large.txt.0")

        # Chunk 1
        assert items[1].size == CHUNK_SIZE
        assert items[1].part_index == 1
        assert items[1].download_file_path == os.path.join(tmp_dir, "large.txt.1")

        # Chunk 2
        assert items[2].size == int(CHUNK_SIZE * 0.5)
        assert items[2].part_index == 2
        assert items[2].download_file_path == os.path.join(tmp_dir, "large.txt.2")

        # Case 3: Empty file
        items = get_download_items("empty.txt", 0, "v1", tmp_dir)
        assert len(items) == 0

        # Case 4: Diff only
        items = get_download_items("base.gpkg", 50, "v1", tmp_dir, download_path="diff_file", diff_only=True)
        assert len(items) == 1
        assert items[0].diff_only is True
        assert items[0].file_path == "base.gpkg"
        assert items[0].size == 50
        assert items[0].download_file_path == os.path.join(tmp_dir, "diff_file.0")
