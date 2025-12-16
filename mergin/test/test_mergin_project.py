import os
import shutil
import json
import tempfile
import uuid
import pytest
from mergin.merginproject import MerginProject
from mergin.common import DeltaChangeType, PullActionType
from mergin.models import ProjectDelta, ProjectDeltaItem, ProjectDeltaItemDiff


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


def test_get_pull_action():
    """Test get_pull_action with various combinations."""
    mp = MerginProject.__new__(MerginProject)

    # Test cases: (server_change, local_change, expected_action)
    test_cases = [
        (DeltaChangeType.CREATE, None, PullActionType.COPY),
        (DeltaChangeType.CREATE, DeltaChangeType.CREATE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.CREATE, DeltaChangeType.UPDATE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.CREATE, DeltaChangeType.DELETE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.UPDATE, None, PullActionType.COPY),
        (DeltaChangeType.UPDATE, DeltaChangeType.CREATE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.UPDATE, DeltaChangeType.UPDATE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.UPDATE, DeltaChangeType.DELETE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.UPDATE_DIFF, None, PullActionType.APPLY_DIFF),
        (DeltaChangeType.UPDATE_DIFF, DeltaChangeType.CREATE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.UPDATE_DIFF, DeltaChangeType.UPDATE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.UPDATE_DIFF, DeltaChangeType.DELETE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.DELETE, None, PullActionType.DELETE),
        (DeltaChangeType.DELETE, DeltaChangeType.CREATE, PullActionType.DELETE),
        (DeltaChangeType.DELETE, DeltaChangeType.UPDATE, PullActionType.DELETE),
        (DeltaChangeType.DELETE, DeltaChangeType.DELETE, PullActionType.DELETE),
    ]

    for server_change, local_change, expected_action in test_cases:
        action = mp.get_pull_action(server_change, local_change)
        assert (
            action == expected_action
        ), f"Failed for {server_change}, {local_change}. Expected {expected_action}, got {action}"
