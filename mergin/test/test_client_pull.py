import os
import tempfile
import pytest
from mergin.common import DeltaChangeType
from mergin.models import ProjectDeltaItem, ProjectDeltaItemDiff
from mergin.client_pull import get_delta_server_version, prepare_chunks_destination, get_delta_download_files


def test_prepare_chunks_destination():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = "subdir/file.txt"
        dest = prepare_chunks_destination(tmp_dir, path)

        expected_dir = os.path.join(tmp_dir, "subdir")
        expected_path = os.path.join(expected_dir, "file.txt")

        assert dest == expected_path
        assert os.path.exists(expected_dir)
        assert os.path.isdir(expected_dir)
        assert not os.path.exists(expected_path)  # file should not exist yet


def test_get_delta_download_files():
    with tempfile.TemporaryDirectory() as tmp_dir:
        items = [
            ProjectDeltaItem(change=DeltaChangeType.CREATE, path="file1.txt", version="v1", size=100, checksum="123"),
            ProjectDeltaItem(
                change=DeltaChangeType.UPDATE, path="subdir/file2.txt", version="v2", size=200, checksum="456"
            ),
            ProjectDeltaItem(
                change=DeltaChangeType.UPDATE_DIFF,
                path="data.gpkg",
                version="v3",
                size=300,
                checksum="789",
                diffs=[ProjectDeltaItemDiff(id="diff1"), ProjectDeltaItemDiff(id="diff2")],
            ),
        ]

        merge_files, diff_files = get_delta_download_files(items, tmp_dir)

        assert len(merge_files) + len(diff_files) == 4  # 2 files + 2 diffs

        # Check file1.txt
        f1 = merge_files[0]
        assert f1.dest_file == os.path.join(tmp_dir, "file1.txt")
        assert len(f1.downloaded_items) == 1
        assert f1.downloaded_items[0].file_path == "file1.txt"

        # Check file2.txt
        f2 = merge_files[1]
        assert f2.dest_file == os.path.join(tmp_dir, "subdir/file2.txt")
        assert len(f2.downloaded_items) == 1
        assert f2.downloaded_items[0].file_path == "subdir/file2.txt"

        # Check diffs
        d1 = diff_files[0]
        assert d1.download_file_path == os.path.join(tmp_dir, "diff1")

        d2 = diff_files[1]
        assert d2.download_file_path == os.path.join(tmp_dir, "diff2")
