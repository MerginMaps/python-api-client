import os
import shutil
import json
import tempfile
import pytest
from mergin.merginproject import MerginProject
from mergin.common import DeltaChangeType, PullActionType, ClientError
from mergin.models import ProjectDeltaItem, ProjectDeltaItemDiff
from mergin.client_pull import PullAction
from mergin.utils import edit_conflict_file_name

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


def test_get_pull_action_valid():
    """Test get_pull_action with valid combinations."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        mp = MerginProject(tmp_dir)

    # Test cases: (server_change, local_change, expected_action)
    test_cases = [
        (DeltaChangeType.CREATE, None, PullActionType.COPY),
        (DeltaChangeType.CREATE, DeltaChangeType.CREATE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.UPDATE, None, PullActionType.COPY),
        (DeltaChangeType.UPDATE, DeltaChangeType.UPDATE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.UPDATE, DeltaChangeType.DELETE, PullActionType.COPY),
        (DeltaChangeType.UPDATE, DeltaChangeType.UPDATE_DIFF, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.UPDATE_DIFF, None, PullActionType.APPLY_DIFF),
        (DeltaChangeType.UPDATE_DIFF, DeltaChangeType.UPDATE, PullActionType.COPY_CONFLICT),
        (DeltaChangeType.UPDATE_DIFF, DeltaChangeType.DELETE, PullActionType.COPY),
        (DeltaChangeType.UPDATE_DIFF, DeltaChangeType.UPDATE_DIFF, PullActionType.APPLY_DIFF),
        (DeltaChangeType.DELETE, None, PullActionType.DELETE),
        (DeltaChangeType.DELETE, DeltaChangeType.UPDATE, None),
        (DeltaChangeType.DELETE, DeltaChangeType.DELETE, None),
        (DeltaChangeType.DELETE, DeltaChangeType.UPDATE_DIFF, None),
    ]

    for server_change, local_change, expected_action in test_cases:
        action = mp.get_pull_action(server_change, local_change)
        assert (
            action == expected_action
        ), f"Failed for {server_change}, {local_change}. Expected {expected_action}, got {action}"


def test_get_pull_action_fatal():
    """Test get_pull_action with fatal combinations."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        mp = MerginProject(tmp_dir)

    fatal_cases = [
        (DeltaChangeType.CREATE, DeltaChangeType.UPDATE),
        (DeltaChangeType.CREATE, DeltaChangeType.DELETE),
        (DeltaChangeType.CREATE, DeltaChangeType.UPDATE_DIFF),
        (DeltaChangeType.UPDATE, DeltaChangeType.CREATE),
        (DeltaChangeType.UPDATE_DIFF, DeltaChangeType.CREATE),
        (DeltaChangeType.DELETE, DeltaChangeType.CREATE),
    ]

    for server_change, local_change in fatal_cases:
        with pytest.raises(ClientError, match="Invalid combination of changes"):
            mp.get_pull_action(server_change, local_change)


def test_get_pull_delta():
    """Test get_pull_delta with mocked compare_file_sets."""
    mp = MerginProject.__new__(MerginProject)

    # Mock compare_file_sets return value
    mock_changes = {
        "added": [{"path": "new.txt", "size": 10, "checksum": "c1"}],
        "removed": [{"path": "deleted.txt", "size": 20, "checksum": "c2"}],
        "updated": [
            {"path": "updated.txt", "size": 30, "checksum": "c3", "history": {}},
            {
                "path": "data.gpkg",
                "size": 40,
                "checksum": "c4",
                "history": {
                    "v2": {"diff": {"path": "diff_v2", "size": 5}},
                    "v3": {"diff": {"path": "diff_v3", "size": 6}},
                },
            },
        ],
    }
    mp.compare_file_sets = lambda local, server: mock_changes
    mp.files = lambda: []
    mp.version = lambda: "v1"
    mp.is_versioned_file = lambda path: path.endswith(".gpkg")

    server_info = {"files": [], "version": "v3"}
    delta = mp.get_pull_delta(server_info)

    assert len(delta.items) == 4

    # Verify items
    create_item = next(i for i in delta.items if i.path == "new.txt")
    assert create_item.change == DeltaChangeType.CREATE

    delete_item = next(i for i in delta.items if i.path == "deleted.txt")
    assert delete_item.change == DeltaChangeType.DELETE

    update_item = next(i for i in delta.items if i.path == "updated.txt")
    assert update_item.change == DeltaChangeType.UPDATE

    diff_item = next(i for i in delta.items if i.path == "data.gpkg")
    assert diff_item.change == DeltaChangeType.UPDATE_DIFF
    assert len(diff_item.diffs) == 2


def test_get_local_delta():
    """Test get_local_delta with mocked compare_file_sets."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_project = "delta_test_project"
        project_dir = os.path.join(tmp_dir, test_project)
        # prepare mergin project with base geopackage
        os.makedirs(project_dir, exist_ok=True)
        mp = MerginProject(project_dir)
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "base.gpkg"), os.path.join(project_dir, ".mergin", "base.gpkg"))
        # Mock files() to return origin info for version lookup
        mp.files = lambda: []
        mp.inspect_files = lambda: []  # Dummy return

        # check if geopackage is updated (is_open) but missing - geodiff lib error, than updated file is reported
        mock_changes = {
            "updated": [
                {"path": "base.gpkg", "size": 40, "checksum": "c4"},
            ],
        }
        mp.compare_file_sets = lambda origin, server: mock_changes
        delta_items = mp.get_local_delta(project_dir)
        assert len(delta_items) == 1
        assert delta_items[0].path == "base.gpkg"
        assert delta_items[0].change == DeltaChangeType.UPDATE

        # check if geopackage is updated (is_open) but not diffable - no changes should be reported
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "base.gpkg"), os.path.join(project_dir, "base.gpkg"))
        delta_items = mp.get_local_delta(project_dir)
        assert len(delta_items) == 0

        # now test with real changes
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), os.path.join(project_dir, "base.gpkg"))
        # Mock compare_file_sets return value
        mock_changes = {
            "added": [{"path": "new.txt", "size": 10, "checksum": "c1"}],
            "removed": [{"path": "deleted.txt", "size": 20, "checksum": "c2"}],
            "updated": [
                {"path": "updated.txt", "size": 30, "checksum": "c3"},
                {"path": "base.gpkg", "size": 40, "checksum": "c4"},
            ],
        }
        mp.compare_file_sets = lambda origin, server: mock_changes

        delta_items = mp.get_local_delta(project_dir)
        assert len(delta_items) == 4

        # Verify items
        create_item = next(i for i in delta_items if i.path == "new.txt")
        assert create_item.change == DeltaChangeType.CREATE

        delete_item = next(i for i in delta_items if i.path == "deleted.txt")
        assert delete_item.change == DeltaChangeType.DELETE

        update_item = next(i for i in delta_items if i.path == "updated.txt")
        assert update_item.change == DeltaChangeType.UPDATE

        update_diff_item = next(i for i in delta_items if i.path == "base.gpkg")
        assert update_diff_item.change == DeltaChangeType.UPDATE_DIFF


def test_apply_pull_actions_apply_diff():
    """Test apply_pull_actions with APPLY_DIFF action, with and without rebase."""
    with tempfile.TemporaryDirectory() as tmp_dir, tempfile.TemporaryDirectory() as tmp_dir2:
        test_project = "apply_diff_no_rebase"
        project_dir = os.path.join(tmp_dir, test_project)
        os.makedirs(project_dir, exist_ok=True)
        live = os.path.join(project_dir, "base.gpkg")
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "base.gpkg"), live)
        mp = MerginProject(project_dir)
        base = os.path.join(project_dir, ".mergin", "base.gpkg")
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "base.gpkg"), base)

        # Check APPLY_DIFF action without rebase
        server_diff = os.path.join(tmp_dir, "server_diff_mock.diff")
        server_gpkg = os.path.join(tmp_dir, "base.gpkg")
        # mimic server changes
        mp.geodiff.create_changeset(live, os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), server_diff)
        mp.geodiff.make_copy_sqlite(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), server_gpkg)
        pull_action = PullAction(
            type=PullActionType.APPLY_DIFF,
            pull_delta_item=ProjectDeltaItem(
                change=DeltaChangeType.UPDATE_DIFF,
                path="base.gpkg",
                version="v1",
                size=40,
                checksum="c4",
                diffs=[ProjectDeltaItemDiff(id="server_diff_mock.diff")],
            ),
        )
        mp.apply_pull_actions([pull_action], tmp_dir, {}, None)
        # verify that live and base has been updated
        mp.geodiff.create_changeset(live, server_gpkg, os.path.join(tmp_dir, "live-server.diff"))
        mp.geodiff.create_changeset(live, base, os.path.join(tmp_dir, "live-base.diff"))
        assert not mp.geodiff.has_changes(os.path.join(tmp_dir, "live-server.diff"))
        assert not mp.geodiff.has_changes(os.path.join(tmp_dir, "live-base.diff"))
        # With rebase
        test_project = "apply_diff_rebase"
        project_dir = os.path.join(tmp_dir2, test_project)
        os.makedirs(project_dir, exist_ok=True)
        live = os.path.join(project_dir, "base.gpkg")
        mp = MerginProject(project_dir)
        mp.geodiff.make_copy_sqlite(os.path.join(TEST_DATA_DIR, "base.gpkg"), live)  # local change
        base = os.path.join(project_dir, ".mergin", "base.gpkg")
        mp.geodiff.make_copy_sqlite(os.path.join(TEST_DATA_DIR, "base.gpkg"), base)
        shutil.copyfile(
            os.path.join(TEST_DATA_DIR, "v2_metadata.json"), os.path.join(project_dir, ".mergin", "mergin.json")
        )

        # Check APPLY_DIFF action with rebase
        server_gpkg = os.path.join(tmp_dir2, "base.gpkg")
        assert os.path.exists(live)
        # mimic server changes + local changes
        mp.geodiff.make_copy_sqlite(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), server_gpkg)
        mp.geodiff.make_copy_sqlite(os.path.join(TEST_DATA_DIR, "inserted_1_B.gpkg"), live)  # local change
        pull_action = PullAction(
            type=PullActionType.APPLY_DIFF,
            pull_delta_item=ProjectDeltaItem(
                change=DeltaChangeType.UPDATE_DIFF,
                path="base.gpkg",
                version="v1",
                size=0,
                checksum="",
                diffs=[ProjectDeltaItemDiff(id="server_diff_mock.diff")],
            ),
            local_delta_item=ProjectDeltaItem(
                change=DeltaChangeType.UPDATE_DIFF, path="base.gpkg", version="v1", size=40, checksum="c4"
            ),
        )

        class MockMC:
            def has_editor_support(self):
                return True

            def username(self):
                return "test_user"

        mc = MockMC()
        mp.apply_pull_actions([pull_action], tmp_dir2, {}, mc)
        # here are still changes B' (fid with 4 moved to 5 after rebase)
        mp.geodiff.create_changeset(live, server_gpkg, os.path.join(tmp_dir2, "live-server.diff"))
        mp.geodiff.create_changeset(live, base, os.path.join(tmp_dir2, "live-base.diff"))
        assert mp.geodiff.has_changes(os.path.join(tmp_dir2, "live-server.diff"))
        assert mp.geodiff.has_changes(os.path.join(tmp_dir2, "live-base.diff"))
        assert not os.path.exists(edit_conflict_file_name("base.gpkg", mc.username(), "v1"))


def test_apply_pull_actions_copy():
    """Test apply_pull_actions with COPY action."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_project = "apply_copy"
        project_dir = os.path.join(tmp_dir, test_project)
        os.makedirs(project_dir, exist_ok=True)
        mp = MerginProject(project_dir)
        # mimic downloaded file to temp dir
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "test.txt"), os.path.join(tmp_dir, "test.txt"))
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "base.gpkg"), os.path.join(tmp_dir, "base.gpkg"))

        # let's prepare pull actions
        pull_actions = [
            PullAction(
                type=PullActionType.COPY,
                pull_delta_item=ProjectDeltaItem(
                    change=DeltaChangeType.CREATE,
                    path="test.txt",
                    version="v1",
                    size=10,
                    checksum="c1",
                ),
            ),
            PullAction(
                type=PullActionType.COPY,
                pull_delta_item=ProjectDeltaItem(
                    change=DeltaChangeType.CREATE,
                    path="base.gpkg",
                    version="v1",
                    size=20,
                    checksum="c2",
                ),
            ),
        ]
        mp.apply_pull_actions(pull_actions, tmp_dir, {}, None)
        assert os.path.exists(os.path.join(project_dir, "test.txt"))
        assert os.path.exists(os.path.join(project_dir, "base.gpkg"))
        assert os.path.exists(mp.fpath_meta("base.gpkg"))


def test_apply_pull_actions_delete():
    """Test apply_pull_actions with DELETE action."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_project = "apply_delete"
        project_dir = os.path.join(tmp_dir, test_project)
        os.makedirs(project_dir, exist_ok=True)
        mp = MerginProject(project_dir)
        # mimic downloaded file to temp dir
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "test.txt"), os.path.join(project_dir, "test.txt"))
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "base.gpkg"), os.path.join(project_dir, "base.gpkg"))
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "base.gpkg"), mp.fpath_meta("base.gpkg"))

        # prepare pull actions
        pull_actions = [
            PullAction(
                type=PullActionType.DELETE,
                pull_delta_item=ProjectDeltaItem(
                    change=DeltaChangeType.DELETE,
                    path="test.txt",
                    version="v1",
                    size=10,
                    checksum="c1",
                ),
            ),
            PullAction(
                type=PullActionType.DELETE,
                pull_delta_item=ProjectDeltaItem(
                    change=DeltaChangeType.DELETE,
                    path="base.gpkg",
                    version="v1",
                    size=20,
                    checksum="c2",
                ),
            ),
        ]
        mp.apply_pull_actions(pull_actions, tmp_dir, {}, None)
        assert not os.path.exists(os.path.join(project_dir, "test.txt"))
        assert not os.path.exists(os.path.join(project_dir, "base.gpkg"))
        assert not os.path.exists(mp.fpath_meta("base.gpkg"))


def test_apply_pull_actions_copy_conflict():
    """Test apply_pull_actions with COPY_CONFLICT action."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_project = "apply_copy_conflict"
        project_dir = os.path.join(tmp_dir, test_project)
        os.makedirs(project_dir, exist_ok=True)
        mp = MerginProject(project_dir)
        # mimic downloaded file to temp dir
        live = os.path.join(project_dir, "base.gpkg")
        server_gpkg = os.path.join(tmp_dir, "base.gpkg")
        base = mp.fpath_meta("base.gpkg")
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "test.txt"), os.path.join(tmp_dir, "test.txt"))
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "test.txt"), os.path.join(project_dir, "test.txt"))
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), server_gpkg)
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "base.gpkg"), live)
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "base.gpkg"), base)
        shutil.copyfile(os.path.join(TEST_DATA_DIR, "v2_metadata.json"), mp.fpath_meta("mergin.json"))
        with open(os.path.join(project_dir, "test.txt"), "w") as f:
            f.write("local change")

        # prepare pull actions
        pull_actions = [
            PullAction(
                type=PullActionType.COPY_CONFLICT,
                pull_delta_item=ProjectDeltaItem(
                    change=DeltaChangeType.UPDATE,
                    path="test.txt",
                    version="v1",
                    size=10,
                    checksum="c1",
                ),
            ),
            PullAction(
                type=PullActionType.COPY_CONFLICT,
                pull_delta_item=ProjectDeltaItem(
                    change=DeltaChangeType.UPDATE,
                    path="base.gpkg",
                    version="v1",
                    size=20,
                    checksum="c2",
                ),
            ),
        ]

        class MockMC:
            def has_editor_support(self):
                return True

            def username(self):
                return "test_user"

        mc = MockMC()
        conflicts = mp.apply_pull_actions(pull_actions, tmp_dir, {"role": "writer"}, mc)
        assert len(conflicts) == 2
        assert os.path.exists(conflicts[0])
        assert os.path.exists(os.path.join(project_dir, "test.txt"))
        with open(os.path.join(tmp_dir, "test.txt"), "r") as f, open(os.path.join(project_dir, "test.txt"), "r") as f2:
            downloaded_content = f.read()
            local_content = f2.read()
        assert downloaded_content == local_content

        with open(conflicts[0], "r") as f:
            conflict_content = f.read()
        assert conflict_content == "local change"

        mp.geodiff.create_changeset(live, server_gpkg, os.path.join(tmp_dir, "live-server.diff"))
        mp.geodiff.create_changeset(live, base, os.path.join(tmp_dir, "live-base.diff"))
        mp.geodiff.create_changeset(live, conflicts[1], os.path.join(tmp_dir, "live-conflict.diff"))
        assert not mp.geodiff.has_changes(os.path.join(tmp_dir, "live-server.diff"))
        assert not mp.geodiff.has_changes(os.path.join(tmp_dir, "live-base.diff"))
        assert mp.geodiff.has_changes(os.path.join(tmp_dir, "live-conflict.diff"))
