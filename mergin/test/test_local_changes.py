from datetime import datetime

from ..local_changes import FileChange, LocalProjectChanges


def test_local_changes_from_dict():
    """Test generating LocalProjectChanges from a dictionary."""
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
    added = [FileChange(**file) for file in changes_dict["added"]]
    updated = [FileChange(**file) for file in changes_dict["updated"]]
    removed = [FileChange(**file) for file in changes_dict["removed"]]

    local_changes = LocalProjectChanges(added=added, updated=updated, removed=removed)

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
    local_change = FileChange(
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
    added = [FileChange(path="file1.txt", checksum="abc123", size=1024, mtime=datetime.now())]
    updated = [FileChange(path="file2.txt", checksum="xyz789", size=2048, mtime=datetime.now())]
    removed = [FileChange(path="file3.txt", checksum="lmn456", size=512, mtime=datetime.now())]

    local_changes = LocalProjectChanges(added=added, updated=updated, removed=removed)
    server_request = local_changes.to_server_payload()

    assert "added" in server_request
    assert "updated" in server_request
    assert "removed" in server_request
    assert server_request["added"][0]["path"] == "file1.txt"
    assert server_request["updated"][0]["path"] == "file2.txt"
    assert server_request["removed"][0]["path"] == "file3.txt"


def test_local_changes_update_chunk_ids():
    """Test the update_chunks method of LocalChanges."""
    added = [
        FileChange(path="file1.txt", checksum="abc123", size=1024, mtime=datetime.now(), chunks=["abc123"]),
        FileChange(path="file2.txt", checksum="abc123", size=1024, mtime=datetime.now(), chunks=["abc123"]),
    ]
    updated = [FileChange(path="file2.txt", checksum="xyz789", size=2048, mtime=datetime.now(), chunks=["xyz789"])]

    local_changes = LocalProjectChanges(added=added, updated=updated)
    chunks = [("abc123", "chunk1"), ("abc123", "chunk1"), ("xyz789", "chunk2")]

    local_changes.update_chunk_ids(chunks)

    assert local_changes.added[0].chunks == ["chunk1"]
    assert local_changes.added[1].chunks == ["chunk1"]
    assert local_changes.updated[0].chunks == ["chunk2"]


def test_local_changes_get_upload_changes():
    """Test the get_upload_changes method of LocalProjectChanges."""
    # Create sample LocalChange instances
    added = [FileChange(path="file1.txt", checksum="abc123", size=1024, mtime=datetime.now())]
    updated = [FileChange(path="file2.txt", checksum="xyz789", size=2048, mtime=datetime.now())]
    removed = [FileChange(path="file3.txt", checksum="lmn456", size=512, mtime=datetime.now())]

    # Initialize LocalChanges with added, updated, and removed changes
    local_changes = LocalProjectChanges(added=added, updated=updated, removed=removed)

    # Call get_upload_changes
    upload_changes = local_changes.get_upload_changes()

    # Assertions
    assert len(upload_changes) == 2  # Only added and updated should be included
    assert upload_changes[0].path == "file1.txt"  # First change is from added
    assert upload_changes[1].path == "file2.txt"  # Second change is from updated
