import os
import shutil
import json
import tempfile
import pytest
from mergin.merginproject import MerginProject

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data")


def test_project_metadata():

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
