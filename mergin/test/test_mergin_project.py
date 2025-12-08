import os
import shutil
import json
import tempfile
import uuid
import pytest
from mergin.merginproject import MerginProject
from mergin.common import DeltaChangeType, ProjectDeltaItem


def test_get_delta_pull_changes_create():
    """Test get_delta_pull_changes with CREATE changes."""
    mp = MerginProject.__new__(MerginProject)

    mock_response = {
        "items": [
            {
                "change": "create",
                "checksum": "22afdbbf504087ea0ff7f1a2aeca2e265cc01bb5",
                "path": "qgs_project.qgs",
                "size": 72024,
                "version": "v1",
            },
            {
                "change": "create",
                "checksum": "7f46f5d1a160a71aff5450f55853b0d552958f58",
                "path": "photo.jpg",
                "size": 5646973,
                "version": "v1",
            },
        ]
    }
    delta_changes = [ProjectDeltaItem(**item) for item in mock_response["items"]]

    result = mp.get_delta_pull_changes(delta_changes)

    assert len(result["added"]) == 2
    assert len(result["updated"]) == 0
    assert len(result["removed"]) == 0
    assert result["added"][0]["path"] == "qgs_project.qgs"
    assert result["added"][1]["path"] == "photo.jpg"


def test_get_delta_pull_changes_update():
    """Test get_delta_pull_changes with UPDATE changes."""
    mp = MerginProject.__new__(MerginProject)

    mock_response = {
        "items": [
            {
                "change": "update",
                "checksum": "3a8c54469e4fe498faffe66f4671fb9b0e6c0221",
                "path": "data.gpkg",
                "size": 126976,
                "version": "v101",
            }
        ]
    }
    delta_changes = [ProjectDeltaItem(**item) for item in mock_response["items"]]

    result = mp.get_delta_pull_changes(delta_changes)

    assert len(result["added"]) == 0
    assert len(result["updated"]) == 1
    assert len(result["removed"]) == 0
    assert result["updated"][0]["path"] == "data.gpkg"


def test_get_delta_pull_changes_delete():
    """Test get_delta_pull_changes with DELETE changes."""
    mp = MerginProject.__new__(MerginProject)

    mock_response = {
        "items": [
            {
                "change": "delete",
                "path": "old_file.txt",
                "version": "v1",
                "size": 0,
                "checksum": "",
            }
        ]
    }
    delta_changes = [ProjectDeltaItem(**item) for item in mock_response["items"]]

    result = mp.get_delta_pull_changes(delta_changes)

    assert len(result["added"]) == 0
    assert len(result["updated"]) == 0
    assert len(result["removed"]) == 1
    assert result["removed"][0]["path"] == "old_file.txt"


def test_get_delta_pull_changes_update_diff():
    """Test get_delta_pull_changes with UPDATE_DIFF changes."""
    mp = MerginProject.__new__(MerginProject)

    mock_response = {
        "items": [
            {
                "change": "update_diff",
                "path": "data.gpkg",
                "diffs": [{"path": "diff1"}, {"path": "diff2"}],
                "version": "v101",
                "size": 0,
                "checksum": "",
            }
        ]
    }
    delta_changes = [ProjectDeltaItem(**item) for item in mock_response["items"]]

    result = mp.get_delta_pull_changes(delta_changes)

    assert len(result["added"]) == 0
    assert len(result["updated"]) == 1
    assert len(result["removed"]) == 0
    assert result["updated"][0]["path"] == "data.gpkg"
    assert result["updated"][0]["diffs"][0] == "diff1"
    assert result["updated"][0]["diffs"][1] == "diff2"


def test_project_metadata():
    TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data")

    with tempfile.TemporaryDirectory() as tmp_dir:
        test_project = "test_project_metadata"
        api_user = "test_user"
        project_dir = os.path.join(tmp_dir, test_project)

        # prepare local project
        shutil.copytree(TEST_DATA_DIR, project_dir)

        # copy metadata in old format
        os.makedirs(os.path.join(project_dir, ".mergin"), exist_ok=True)
        metadata_file = os.path.join(project_dir, "old_metadata.json")

        # rewrite metadata namespace
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        project_metadata_file = os.path.join(project_dir, ".mergin", "mergin.json")
        with open(project_metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        # verify we have correct metadata
        mp = MerginProject(project_dir)
        assert mp.project_full_name() == metadata.get("name")
        project_name = metadata.get("name").split("/")[1]
        workspace_name = metadata.get("name").split("/")[0]
        assert mp.project_name() == project_name
        assert mp.workspace_name() == workspace_name
        assert mp.version() == metadata.get("version")

        # copy metadata in new format
        metadata_file = os.path.join(project_dir, "new_metadata.json")
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
        metadata["namespace"] = api_user
        with open(project_metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        # verify we have correct metadata
        mp = MerginProject(project_dir)
        project_name = metadata.get("name")
        workspace_name = metadata.get("namespace")
        assert mp.project_full_name() == f"{workspace_name}/{project_name}"
        assert mp.project_name() == project_name
        assert mp.workspace_name() == workspace_name
        assert mp.version() == metadata.get("version")

        # copy metadata in new format (v2)
        metadata_file = os.path.join(project_dir, "v2_metadata.json")
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
        with open(project_metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        # verify we have correct metadata
        mp = MerginProject(project_dir)
        project_name = metadata.get("name")
        workspace_name = metadata.get("workspace").get("name")
        assert mp.project_full_name() == f"{workspace_name}/{project_name}"
        assert mp.project_name() == project_name
        assert mp.workspace_name() == workspace_name
        assert mp.version() == metadata.get("version")
        assert mp.files() == metadata.get("files")
