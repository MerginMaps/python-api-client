from datetime import datetime

from ..local_changes import LocalChange, LocalChanges

def test_local_changes_from_dict():
    """Test generating LocalChanges from a dictionary."""
    changes_dict = {
        "added": [
            {"path": "file1.txt", "checksum": "abc123", "size": 1024, "mtime": datetime.now()}
        ],
        "updated": [
            {"path": "file2.txt", "checksum": "xyz789", "size": 2048, "mtime": datetime.now()}
        ],
        "removed": [
            {"path": "file3.txt", "checksum": "lmn456", "size": 512, "mtime": datetime.now()}
        ]
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
    assert local_changes.removed[0].path == "file3.txt"


def test_local_change_get_diff():
    """Test the get_diff method of LocalChange."""
    local_change = LocalChange(
        path="file1.txt",
        checksum="abc123",
        size=1024,
        mtime=datetime.now(),
        diff={"path": "file1-diff", "checksum": "diff123", "size": 512, "mtime": datetime.now()}
    )

    diff = local_change.get_diff()
    assert diff is not None
    assert diff.path == "file1-diff"
    assert diff.checksum == "diff123"
    assert diff.size == 512


def test_local_changes_get_server_request():
    """Test the get_server_request method of LocalChanges."""
    added = [
        LocalChange(path="file1.txt", checksum="abc123", size=1024, mtime=datetime.now())
    ]
    updated = [
        LocalChange(path="file2.txt", checksum="xyz789", size=2048, mtime=datetime.now())
    ]
    removed = [
        LocalChange(path="file3.txt", checksum="lmn456", size=512, mtime=datetime.now())
    ]

    local_changes = LocalChanges(added=added, updated=updated, removed=removed)
    server_request = local_changes.get_server_request()

    assert "added" in server_request
    assert "updated" in server_request
    assert "removed" in server_request
    assert server_request["added"][0]["path"] == "file1.txt"
    assert server_request["updated"][0]["path"] == "file2.txt"
    assert server_request["removed"][0]["path"] == "file3.txt"

def test_local_changes_update_chunks():
    """Test the update_chunks method of LocalChanges."""
    added = [
        LocalChange(path="file1.txt", checksum="abc123", size=1024, mtime=datetime.now())
    ]
    updated = [
        LocalChange(path="file2.txt", checksum="xyz789", size=2048, mtime=datetime.now())
    ]

    local_changes = LocalChanges(added=added, updated=updated)
    chunks = [("abc123", "chunk1"), ("xyz789", "chunk2")]

    local_changes.update_chunks(chunks)

    assert local_changes.added[0].chunks == ["chunk1"]
    assert local_changes.updated[0].chunks == ["chunk2"]

def test_local_changes_get_upload_changes():
    """Test the get_upload_changes method of LocalChanges."""
    # Create sample LocalChange instances
    added = [
        LocalChange(path="file1.txt", checksum="abc123", size=1024, mtime=datetime.now())
    ]
    updated = [
        LocalChange(path="file2.txt", checksum="xyz789", size=2048, mtime=datetime.now())
    ]
    removed = [
        LocalChange(path="file3.txt", checksum="lmn456", size=512, mtime=datetime.now())
    ]

    # Initialize LocalChanges with added, updated, and removed changes
    local_changes = LocalChanges(added=added, updated=updated, removed=removed)

    # Call get_upload_changes
    upload_changes = local_changes.get_upload_changes()

    # Assertions
    assert len(upload_changes) == 2  # Only added and updated should be included
    assert upload_changes[0].path == "file1.txt"  # First change is from added
    assert upload_changes[1].path == "file2.txt"  # Second change is from updated