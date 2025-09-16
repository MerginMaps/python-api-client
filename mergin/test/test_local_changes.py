from datetime import datetime
import pytest
from unittest.mock import patch

from ..local_changes import ChangesValidationError, LocalChange, LocalChanges, MAX_UPLOAD_CHANGES


def test_local_changes_from_dict():
    """Test generating LocalChanges from a dictionary."""
    changes_dict = {
        "added": [{"path": "file1.txt", "checksum": "abc123", "size": 1024, "mtime": datetime.now()}],
        "updated": [{"path": "file2.txt", "checksum": "xyz789", "size": 2048, "mtime": datetime.now()}],
        "removed": [
            {
                "checksum": "a4e7e35d1e8c203b5d342ecd9676adbebc924c84",
                "diff": None,
                "history": {
                    "v1": {
                        "change": "added",
                        "checksum": "a4e7e35d1e8c203b5d342ecd9676adbebc924c84",
                        "expiration": None,
                        "path": "base.gpkg",
                        "size": 98304,
                    }
                },
                "location": "v1/base.gpkg",
                "mtime": "2025-08-15T09:04:21.690590Z",
                "path": "base.gpkg",
                "size": 98304,
                "version": "v1",
            }
        ],
    }

    # Convert dictionary to LocalChanges
    added = [LocalChange(**file) for file in changes_dict["added"]]
    updated = [LocalChange(**file) for file in changes_dict["updated"]]
    removed = [LocalChange(**file) for file in changes_dict["removed"]]

    local_changes = LocalChanges(added=added, updated=updated, removed=removed)

    # Assertions
    assert len(local_changes.added) == 1
    assert len(local_changes.updated) == 1
    assert len(local_changes.removed) == 1
    assert local_changes.added[0].path == "file1.txt"
    assert local_changes.updated[0].path == "file2.txt"
    assert local_changes.removed[0].path == "base.gpkg"
    assert local_changes.removed[0].history
    assert local_changes.removed[0].location
    assert not len(local_changes.removed[0].chunks)


def test_local_change_get_diff():
    """Test the get_diff method of LocalChange."""
    local_change = LocalChange(
        path="file1.txt",
        checksum="abc123",
        size=1024,
        mtime=datetime.now(),
        diff={"path": "file1-diff", "checksum": "diff123", "size": 512, "mtime": datetime.now()},
    )

    diff = local_change.get_diff()
    assert diff is not None
    assert diff.path == "file1-diff"
    assert diff.checksum == "diff123"
    assert diff.size == 512


def test_local_changes_to_server_payload():
    """Test the to_server_payload method of LocalChanges."""
    added = [LocalChange(path="file1.txt", checksum="abc123", size=1024, mtime=datetime.now())]
    updated = [LocalChange(path="file2.txt", checksum="xyz789", size=2048, mtime=datetime.now())]
    removed = [LocalChange(path="file3.txt", checksum="lmn456", size=512, mtime=datetime.now())]

    local_changes = LocalChanges(added=added, updated=updated, removed=removed)
    server_request = local_changes.to_server_payload()

    assert "added" in server_request
    assert "updated" in server_request
    assert "removed" in server_request
    assert server_request["added"][0]["path"] == "file1.txt"
    assert server_request["updated"][0]["path"] == "file2.txt"
    assert server_request["removed"][0]["path"] == "file3.txt"


def test_local_changes_update_chunks():
    """Test the update_chunks method of LocalChanges."""
    added = [
        LocalChange(path="file1.txt", checksum="abc123", size=1024, mtime=datetime.now(), chunks=["abc123"]),
        LocalChange(path="file2.txt", checksum="abc123", size=1024, mtime=datetime.now(), chunks=["abc123"]),
    ]
    updated = [LocalChange(path="file2.txt", checksum="xyz789", size=2048, mtime=datetime.now(), chunks=["xyz789"])]

    local_changes = LocalChanges(added=added, updated=updated)
    chunks = [("abc123", "chunk1"), ("abc123", "chunk1"), ("xyz789", "chunk2")]

    local_changes.update_chunks(chunks)

    assert local_changes.added[0].chunks == ["chunk1"]
    assert local_changes.added[1].chunks == ["chunk1"]
    assert local_changes.updated[0].chunks == ["chunk2"]


def test_local_changes_get_upload_changes():
    """Test the get_upload_changes method of LocalChanges."""
    # Create sample LocalChange instances
    added = [LocalChange(path="file1.txt", checksum="abc123", size=1024, mtime=datetime.now())]
    updated = [LocalChange(path="file2.txt", checksum="xyz789", size=2048, mtime=datetime.now())]
    removed = [LocalChange(path="file3.txt", checksum="lmn456", size=512, mtime=datetime.now())]

    # Initialize LocalChanges with added, updated, and removed changes
    local_changes = LocalChanges(added=added, updated=updated, removed=removed)

    # Call get_upload_changes
    upload_changes = local_changes.get_upload_changes()

    # Assertions
    assert len(upload_changes) == 2  # Only added and updated should be included
    assert upload_changes[0].path == "file1.txt"  # First change is from added
    assert upload_changes[1].path == "file2.txt"  # Second change is from updated


def test_local_changes_post_init_validation_media():
    """Test the get_media_upload_file method of LocalChanges."""
    # Define constants
    SIZE_LIMIT_MB = 5
    SIZE_LIMIT_BYTES = SIZE_LIMIT_MB * 1024 * 1024
    SMALL_FILE_SIZE = 1024
    LARGE_FILE_SIZE = 15 * 1024 * 1024

    # Create sample LocalChange instances
    added = [
        LocalChange(path="file1.txt", checksum="abc123", size=SMALL_FILE_SIZE, mtime=datetime.now()),
        LocalChange(path="file2.jpg", checksum="xyz789", size=LARGE_FILE_SIZE, mtime=datetime.now()),  # Over limit
    ]
    updated = [
        LocalChange(path="file3.mp4", checksum="lmn456", size=5 * 1024 * 1024, mtime=datetime.now()),
        LocalChange(path="file4.gpkg", checksum="opq123", size=SMALL_FILE_SIZE, mtime=datetime.now()),
    ]

    # Initialize LocalChanges
    with patch("mergin.local_changes.MAX_UPLOAD_MEDIA_SIZE", SIZE_LIMIT_BYTES):
        with pytest.raises(ChangesValidationError, match="Some files exceed") as err:
            LocalChanges(added=added, updated=updated)
            print(err.value.invalid_changes)
    assert len(err.value.invalid_changes) == 1
    assert "file2.jpg" == err.value.invalid_changes[0].path
    assert err.value.invalid_changes[0].size == LARGE_FILE_SIZE


def test_local_changes_post_init_validation_media():
    """Test the get_gpgk_upload_file method of LocalChanges."""
    # Define constants
    SIZE_LIMIT_MB = 10
    SIZE_LIMIT_BYTES = SIZE_LIMIT_MB * 1024 * 1024
    SMALL_FILE_SIZE = 1024
    LARGE_FILE_SIZE = 15 * 1024 * 1024

    # Create sample LocalChange instances
    added = [
        LocalChange(path="file1.gpkg", checksum="abc123", size=SMALL_FILE_SIZE, mtime=datetime.now()),
        LocalChange(
            path="file2.gpkg", checksum="xyz789", size=LARGE_FILE_SIZE, mtime=datetime.now(), diff=None
        ),  # Over limit
    ]
    updated = [
        LocalChange(
            path="file3.gpkg",
            checksum="lmn456",
            size=SIZE_LIMIT_BYTES + 1,
            mtime=datetime.now(),
            diff={"path": "file3-diff.gpkg", "checksum": "diff123", "size": 1024, "mtime": datetime.now()},
        ),
        LocalChange(path="file4.txt", checksum="opq123", size=SMALL_FILE_SIZE, mtime=datetime.now()),
    ]

    # Initialize LocalChanges
    with patch("mergin.local_changes.MAX_UPLOAD_VERSIONED_SIZE", SIZE_LIMIT_BYTES):
        with pytest.raises(ChangesValidationError) as err:
            LocalChanges(added=added, updated=updated)
    assert len(err.value.invalid_changes) == 1
    assert "file2.gpkg" == err.value.invalid_changes[0].path
    assert err.value.invalid_changes[0].size == LARGE_FILE_SIZE


def test_local_changes_post_init():
    """Test the __post_init__ method of LocalChanges."""
    # Define constants
    ADDED_COUNT = 80
    UPDATED_COUNT = 21
    SMALL_FILE_SIZE = 1024
    LARGE_FILE_SIZE = 2048

    # Create more than MAX_UPLOAD_CHANGES changes
    added = [
        LocalChange(path=f"file{i}.txt", checksum="abc123", size=SMALL_FILE_SIZE, mtime=datetime.now())
        for i in range(ADDED_COUNT)
    ]
    updated = [
        LocalChange(path=f"file{i}.txt", checksum="xyz789", size=LARGE_FILE_SIZE, mtime=datetime.now())
        for i in range(UPDATED_COUNT)
    ]

    # Initialize LocalChanges
    local_changes = LocalChanges(added=added, updated=updated)

    # Assertions
    assert len(local_changes.added) == ADDED_COUNT  # All added changes are included
    assert len(local_changes.updated) == MAX_UPLOAD_CHANGES - ADDED_COUNT  # Only enough updated changes are included
    assert len(local_changes.added) + len(local_changes.updated) == MAX_UPLOAD_CHANGES  # Total is limited
