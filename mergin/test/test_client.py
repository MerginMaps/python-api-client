import json
import logging
import os
import tempfile
import subprocess
import shutil
from datetime import datetime, timedelta, date
import pytest
import pytz
import sqlite3
import glob

from .. import InvalidProject
from ..client import (
    MerginClient,
    ClientError,
    MerginProject,
    LoginError,
    decode_token_data,
    TokenError,
    ServerType,
)
from ..client_push import push_project_async, push_project_cancel
from ..client_pull import (
    download_project_async,
    download_project_wait,
    download_project_finalize,
    download_project_is_running,
    download_project_cancel,
)
from ..utils import (
    generate_checksum,
    get_versions_with_file_changes,
    is_version_acceptable,
    unique_path_name,
    conflicted_copy_file_name,
    edit_conflict_file_name,
)
from ..merginproject import pygeodiff
from ..report import create_report
from ..editor import EDITOR_ROLE_NAME, filter_changes, is_editor_enabled


SERVER_URL = os.environ.get("TEST_MERGIN_URL")
API_USER = os.environ.get("TEST_API_USERNAME")
USER_PWD = os.environ.get("TEST_API_PASSWORD")
API_USER2 = os.environ.get("TEST_API_USERNAME2")
USER_PWD2 = os.environ.get("TEST_API_PASSWORD2")
TMP_DIR = tempfile.gettempdir()
TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data")
CHANGED_SCHEMA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "modified_schema")
STORAGE_WORKSPACE = os.environ.get("TEST_STORAGE_WORKSPACE", "testpluginstorage")


@pytest.fixture(scope="function")
def mc():
    client = create_client(API_USER, USER_PWD)
    create_workspace_for_client(client)
    return client


@pytest.fixture(scope="function")
def mc2():
    client = create_client(API_USER2, USER_PWD2)
    create_workspace_for_client(client)
    return client


@pytest.fixture(scope="function")
def mcStorage():
    client = create_client(API_USER, USER_PWD)
    create_workspace_for_client(client, STORAGE_WORKSPACE)
    return client


def create_client(user, pwd):
    assert SERVER_URL and SERVER_URL.rstrip("/") != "https://app.merginmaps.com" and user and pwd
    return MerginClient(SERVER_URL, login=user, password=pwd)


def create_workspace_for_client(mc: MerginClient, workspace_name=None):
    try:
        mc.create_workspace(workspace_name or mc.username())
    except ClientError:
        return


def cleanup(mc, project, dirs):
    # cleanup leftovers from previous test if needed such as remote project and local directories
    try:
        mc.delete_project_now(project)
    except ClientError:
        pass
    remove_folders(dirs)


def remove_folders(dirs):
    # clean given directories
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)


def sudo_works():
    sudo_res = subprocess.run(["sudo", "echo", "test"])
    return sudo_res.returncode != 0


def server_has_editor_support(mc, access):
    """
    Checks if the server has editor support based on the provided access information.

    Returns:
        bool: True if the server has editor support, False otherwise.
    """
    return "editorsnames" in access and mc.has_editor_support()


def test_client_instance(mc, mc2):
    assert isinstance(mc, MerginClient)
    assert isinstance(mc2, MerginClient)


def test_login(mc):
    token = mc._auth_session["token"]
    assert MerginClient(mc.url, auth_token=token)

    invalid_token = "Completely invalid token...."
    with pytest.raises(TokenError, match=f"Token doesn't start with 'Bearer .': {invalid_token}"):
        decode_token_data(invalid_token)

    invalid_token = "Bearer .jas646kgfa"
    with pytest.raises(TokenError, match=f"Invalid token data: {invalid_token}"):
        decode_token_data(invalid_token)

    with pytest.raises(LoginError, match="Invalid username or password"):
        mc.login("foo", "bar")


def test_create_delete_project(mc: MerginClient):
    test_project = "test_create_delete"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    download_dir = os.path.join(TMP_DIR, "download", test_project)

    cleanup(mc, project, [project_dir, download_dir])
    # create new (empty) project on server
    mc.create_project(test_project)
    projects = mc.projects_list(flag="created")
    assert any(p for p in projects if p["name"] == test_project and p["namespace"] == API_USER)

    # try again
    with pytest.raises(ClientError, match=f"already exists"):
        mc.create_project(test_project)

    # remove project
    mc.delete_project_now(API_USER + "/" + test_project)
    projects = mc.projects_list(flag="created")
    assert not any(p for p in projects if p["name"] == test_project and p["namespace"] == API_USER)

    # try again, nothing to delete
    with pytest.raises(ClientError):
        mc.delete_project_now(API_USER + "/" + test_project)

    # test that using namespace triggers deprecate warning, but creates project correctly
    with pytest.deprecated_call(match=r"The usage of `namespace` parameter in `create_project\(\)` is deprecated."):
        mc.create_project(test_project, namespace=API_USER)
    projects = mc.projects_list(flag="created")
    assert any(p for p in projects if p["name"] == test_project and p["namespace"] == API_USER)
    mc.delete_project_now(project)

    # test that using only project name triggers deprecate warning, but creates project correctly
    with pytest.deprecated_call(match=r"The use of only project name in `create_project\(\)` is deprecated"):
        mc.create_project(test_project)
    projects = mc.projects_list(flag="created")
    assert any(p for p in projects if p["name"] == test_project and p["namespace"] == API_USER)
    mc.delete_project_now(project)

    # test that even if project is specified with full name and namespace is specified a warning is raised, but still create project correctly
    with pytest.warns(UserWarning, match="Parameter `namespace` specified with full project name"):
        mc.create_project(project, namespace=API_USER)
    projects = mc.projects_list(flag="created")
    assert any(p for p in projects if p["name"] == test_project and p["namespace"] == API_USER)
    mc.delete_project_now(project)

    # test that create project with full name works
    mc.create_project(project)
    projects = mc.projects_list(flag="created")
    assert any(p for p in projects if p["name"] == test_project and p["namespace"] == API_USER)
    mc.delete_project_now(project)


def test_create_remote_project_from_local(mc):
    test_project = "test_project"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    download_dir = os.path.join(TMP_DIR, "download", test_project)

    cleanup(mc, project, [project_dir, download_dir])
    # prepare local project
    shutil.copytree(TEST_DATA_DIR, project_dir)

    # create remote project
    mc.create_project_and_push(project, directory=project_dir)

    # verify we have correct metadata
    source_mp = MerginProject(project_dir)
    assert source_mp.project_full_name() == f"{API_USER}/{test_project}"
    assert source_mp.project_name() == test_project
    assert source_mp.workspace_name() == API_USER
    assert source_mp.version() == "v1"

    # check basic metadata about created project
    project_info = mc.project_info(project)
    assert project_info["version"] == "v1"
    assert project_info["name"] == test_project
    assert project_info["namespace"] == API_USER
    assert project_info["id"] == source_mp.project_id()

    # check project metadata retrieval by id
    project_info = mc.project_info(source_mp.project_id())
    assert project_info["version"] == "v1"
    assert project_info["name"] == test_project
    assert project_info["namespace"] == API_USER
    assert project_info["id"] == source_mp.project_id()

    versions = mc.project_versions(project)
    assert len(versions) == 1
    assert versions[0]["name"] == "v1"
    assert any(f for f in versions[0]["changes"]["added"] if f["path"] == "test.qgs")

    # check we can fully download remote project
    mc.download_project(project, download_dir)
    mp = MerginProject(download_dir)
    downloads = {"dir": os.listdir(mp.dir), "meta": os.listdir(mp.meta_dir)}
    for f in os.listdir(project_dir) + [".mergin"]:
        assert f in downloads["dir"]
        if mp.is_versioned_file(f):
            assert f in downloads["meta"]

    # unable to download to the same directory
    with pytest.raises(Exception, match="Project directory already exists"):
        mc.download_project(project, download_dir)


def test_push_pull_changes(mc):
    test_project = "test_push"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_2")  # concurrent project dir

    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    # make sure we have v1 also in concurrent project dir
    mc.download_project(project, project_dir_2)

    mp2 = MerginProject(project_dir_2)
    assert mp2.project_full_name() == f"{API_USER}/{test_project}"
    assert mp2.project_name() == test_project
    assert mp2.workspace_name() == API_USER
    assert mp2.version() == "v1"

    # test push changes (add, remove, rename, update)
    f_added = "new.txt"
    with open(os.path.join(project_dir, f_added), "w") as f:
        f.write("new file")
    f_removed = "test.txt"
    os.remove(os.path.join(project_dir, f_removed))
    f_renamed = "test_dir/test2.txt"
    shutil.move(
        os.path.normpath(os.path.join(project_dir, f_renamed)),
        os.path.join(project_dir, "renamed.txt"),
    )
    f_updated = "test3.txt"
    with open(os.path.join(project_dir, f_updated), "w") as f:
        f.write("Modified")

    # check changes before applied
    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert not sum(len(v) for v in pull_changes.values())
    assert next((f for f in push_changes["added"] if f["path"] == f_added), None)
    assert next((f for f in push_changes["removed"] if f["path"] == f_removed), None)
    assert next((f for f in push_changes["updated"] if f["path"] == f_updated), None)
    # renamed file will result in removed + added file
    assert next((f for f in push_changes["removed"] if f["path"] == f_renamed), None)
    assert next((f for f in push_changes["added"] if f["path"] == "renamed.txt"), None)
    assert not pull_changes["renamed"]  # not supported

    mc.push_project(project_dir)

    mp = MerginProject(project_dir)
    assert mp.project_full_name() == f"{API_USER}/{test_project}"
    assert mp.version() == "v2"

    project_info = mc.project_info(project)
    assert project_info["version"] == "v2"
    assert not next((f for f in project_info["files"] if f["path"] == f_removed), None)
    assert not next((f for f in project_info["files"] if f["path"] == f_renamed), None)
    assert next((f for f in project_info["files"] if f["path"] == "renamed.txt"), None)
    assert next((f for f in project_info["files"] if f["path"] == f_added), None)
    f_remote_checksum = next((f["checksum"] for f in project_info["files"] if f["path"] == f_updated), None)
    assert generate_checksum(os.path.join(project_dir, f_updated)) == f_remote_checksum
    assert project_info["id"] == mp.project_id()
    assert len(project_info["files"]) == len(mp.inspect_files())
    project_versions = mc.project_versions(project)
    assert len(project_versions) == 2
    f_change = next(
        (f for f in project_versions[-1]["changes"]["updated"] if f["path"] == f_updated),
        None,
    )
    assert "origin_checksum" not in f_change  # internal client info

    # test parallel changes
    with open(os.path.join(project_dir_2, f_updated), "w") as f:
        f.write("Make some conflict")
    f_conflict_checksum = generate_checksum(os.path.join(project_dir_2, f_updated))

    # not at latest server version
    with pytest.raises(ClientError, match="Please update your local copy"):
        mc.push_project(project_dir_2)

    # check changes in project_dir_2 before applied
    pull_changes, push_changes, _ = mc.project_status(project_dir_2)
    assert next((f for f in pull_changes["added"] if f["path"] == f_added), None)
    assert next((f for f in pull_changes["removed"] if f["path"] == f_removed), None)
    assert next((f for f in pull_changes["updated"] if f["path"] == f_updated), None)
    assert next((f for f in pull_changes["removed"] if f["path"] == f_renamed), None)
    assert next((f for f in pull_changes["added"] if f["path"] == "renamed.txt"), None)

    mc.pull_project(project_dir_2)
    assert os.path.exists(os.path.join(project_dir_2, f_added))
    assert not os.path.exists(os.path.join(project_dir_2, f_removed))
    assert not os.path.exists(os.path.join(project_dir_2, f_renamed))
    assert os.path.exists(os.path.join(project_dir_2, "renamed.txt"))
    assert os.path.exists(os.path.join(project_dir_2, conflicted_copy_file_name(f_updated, API_USER, 1)))
    assert (
        generate_checksum(os.path.join(project_dir_2, conflicted_copy_file_name(f_updated, API_USER, 1)))
        == f_conflict_checksum
    )
    assert generate_checksum(os.path.join(project_dir_2, f_updated)) == f_remote_checksum


def test_cancel_push(mc):
    """
    Start pushing and cancel the process, then try to push again and check if previous sync process was cleanly
    finished.
    """
    test_project = "test_cancel_push"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project + "_3")  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_4")
    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    # modify the project (add, update)
    f_added = "new.txt"
    with open(os.path.join(project_dir, f_added), "w") as f:
        f.write("new file")
    f_updated = "test3.txt"
    modification = "Modified"
    with open(os.path.join(project_dir, f_updated), "w") as f:
        f.write(modification)

    # check changes before applied
    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert not sum(len(v) for v in pull_changes.values())
    assert next((f for f in push_changes["added"] if f["path"] == f_added), None)
    assert next((f for f in push_changes["updated"] if f["path"] == f_updated), None)

    # start pushing and then cancel the job
    job = push_project_async(mc, project_dir)
    push_project_cancel(job)

    # if cancelled properly, we should be now able to do the push without any problem
    mc.push_project(project_dir)

    # download the project to a different directory and check the version and content
    mc.download_project(project, project_dir_2)
    mp = MerginProject(project_dir_2)
    assert mp.version() == "v2"
    assert os.path.exists(os.path.join(project_dir_2, f_added))
    with open(os.path.join(project_dir_2, f_updated), "r") as f:
        assert f.read() == modification


def test_ignore_files(mc):
    test_project = "test_blacklist"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates

    cleanup(mc, project, [project_dir])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    shutil.copy(os.path.join(project_dir, "test.qgs"), os.path.join(project_dir, "test.qgs~"))
    mc.create_project_and_push(project, project_dir)
    project_info = mc.project_info(project)
    assert not next((f for f in project_info["files"] if f["path"] == "test.qgs~"), None)

    with open(os.path.join(project_dir, ".directory"), "w") as f:
        f.write("test")
    mc.push_project(project_dir)
    assert not next((f for f in project_info["files"] if f["path"] == ".directory"), None)


def test_sync_diff(mc):
    test_project = f"test_sync_diff"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_2")  # concurrent project dir with no changes
    project_dir_3 = os.path.join(TMP_DIR, test_project + "_3")  # concurrent project dir with local changes

    cleanup(mc, project, [project_dir, project_dir_2, project_dir_3])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    # make sure we have v1 also in concurrent project dirs
    mc.download_project(project, project_dir_2)
    mc.download_project(project, project_dir_3)

    # test push changes with diffs:
    mp = MerginProject(project_dir)
    f_updated = "base.gpkg"
    # step 1) base.gpkg updated to inserted_1_A (inserted A feature)
    shutil.move(mp.fpath(f_updated), mp.fpath_meta(f_updated))  # make local copy for changeset calculation
    shutil.copy(mp.fpath("inserted_1_A.gpkg"), mp.fpath(f_updated))
    mc.push_project(project_dir)
    mp.geodiff.create_changeset(mp.fpath(f_updated), mp.fpath_meta(f_updated), mp.fpath_meta("push_diff"))
    assert not mp.geodiff.has_changes(mp.fpath_meta("push_diff"))
    # step 2) base.gpkg updated to inserted_1_A_mod (modified 2 features)
    shutil.move(mp.fpath(f_updated), mp.fpath_meta(f_updated))
    shutil.copy(mp.fpath("inserted_1_A_mod.gpkg"), mp.fpath(f_updated))
    # introduce some other changes
    f_removed = "inserted_1_B.gpkg"
    os.remove(mp.fpath(f_removed))
    f_renamed = "test_dir/modified_1_geom.gpkg"
    shutil.move(mp.fpath(f_renamed), mp.fpath("renamed.gpkg"))
    mc.push_project(project_dir)

    # check project after push
    project_info = mc.project_info(project)
    assert project_info["version"] == "v3"
    assert project_info["id"] == mp.project_id()
    f_remote = next((f for f in project_info["files"] if f["path"] == f_updated), None)
    assert next((f for f in project_info["files"] if f["path"] == "renamed.gpkg"), None)
    assert not next((f for f in project_info["files"] if f["path"] == f_removed), None)
    assert not os.path.exists(mp.fpath_meta(f_removed))
    assert "diff" in f_remote
    assert os.path.exists(mp.fpath_meta("renamed.gpkg"))

    # pull project in different directory
    mp2 = MerginProject(project_dir_2)
    mc.pull_project(project_dir_2)
    mp2.geodiff.create_changeset(mp.fpath(f_updated), mp2.fpath(f_updated), mp2.fpath_meta("diff"))
    assert not mp2.geodiff.has_changes(mp2.fpath_meta("diff"))

    # introduce conflict local change (inserted B feature to base)
    mp3 = MerginProject(project_dir_3)
    shutil.copy(mp3.fpath("inserted_1_B.gpkg"), mp3.fpath(f_updated))
    checksum = generate_checksum(mp3.fpath("inserted_1_B.gpkg"))
    mc.pull_project(project_dir_3)
    assert not os.path.exists(mp3.fpath("base.gpkg_conflict_copy"))

    # push new changes from project_3 and pull in original project
    mc.push_project(project_dir_3)
    mc.pull_project(project_dir)
    mp3.geodiff.create_changeset(mp.fpath(f_updated), mp3.fpath(f_updated), mp.fpath_meta("diff"))
    assert not mp3.geodiff.has_changes(mp.fpath_meta("diff"))


def test_list_of_push_changes(mc):
    PUSH_CHANGES_SUMMARY = {
        "base.gpkg": {"geodiff_summary": [{"table": "simple", "insert": 1, "update": 0, "delete": 0}]}
    }

    test_project = "test_list_of_push_changes"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates

    cleanup(mc, project, [project_dir])
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    f_updated = "base.gpkg"
    mp = MerginProject(project_dir)

    shutil.copy(mp.fpath("inserted_1_A.gpkg"), mp.fpath(f_updated))
    mc._auth_session["expire"] = datetime.now().replace(tzinfo=pytz.utc) - timedelta(days=1)
    pull_changes, push_changes, push_changes_summary = mc.project_status(project_dir)
    assert push_changes_summary == PUSH_CHANGES_SUMMARY


def test_token_renewal(mc):
    """Test token regeneration in case it has expired."""
    test_project = "test_token_renewal"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates

    cleanup(mc, project, [project_dir])
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    mc._auth_session["expire"] = datetime.now().replace(tzinfo=pytz.utc) - timedelta(days=1)
    pull_changes, push_changes, _ = mc.project_status(project_dir)
    to_expire = mc._auth_session["expire"] - datetime.now().replace(tzinfo=pytz.utc)
    assert to_expire.total_seconds() > (9 * 3600)


def test_force_gpkg_update(mc):
    test_project = "test_force_update"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates

    cleanup(mc, project, [project_dir])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    # test push changes with force gpkg file upload:
    mp = MerginProject(project_dir)
    f_updated = "base.gpkg"
    checksum = generate_checksum(mp.fpath(f_updated))

    # base.gpkg updated to modified_schema (inserted new column)
    shutil.move(
        mp.fpath(f_updated), mp.fpath_meta(f_updated)
    )  # make local copy for changeset calculation (which will fail)
    shutil.copy(os.path.join(CHANGED_SCHEMA_DIR, "modified_schema.gpkg"), mp.fpath(f_updated))
    shutil.copy(
        os.path.join(CHANGED_SCHEMA_DIR, "modified_schema.gpkg-wal"),
        mp.fpath(f_updated + "-wal"),
    )
    mc.push_project(project_dir)
    # by this point local file has been updated (changes committed from wal)
    updated_checksum = generate_checksum(mp.fpath(f_updated))
    assert checksum != updated_checksum

    # check project after push
    project_info = mc.project_info(project)
    assert project_info["version"] == "v2"
    f_remote = next((f for f in project_info["files"] if f["path"] == f_updated), None)
    assert "diff" not in f_remote


def test_new_project_sync(mc):
    """Create a new project, download it, add a file and then do sync - it should not fail"""

    test_project = "test_new_project_sync"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates

    cleanup(mc, project, [project_dir])
    # create remote project
    mc.create_project(test_project)

    # download the project
    mc.download_project(project, project_dir)

    # add a test file
    shutil.copy(os.path.join(TEST_DATA_DIR, "test.txt"), project_dir)

    # do a full sync - it should not fail
    mc.pull_project(project_dir)
    mc.push_project(project_dir)

    # make sure everything is up-to-date
    mp = MerginProject(project_dir)
    local_changes = mp.get_push_changes()
    assert not local_changes["added"] and not local_changes["removed"] and not local_changes["updated"]


def test_missing_basefile_pull(mc):
    """Test pull of a project where basefile of a .gpkg is missing for some reason
    (it should gracefully handle it by downloading the missing basefile)
    """

    test_project = "test_missing_basefile_pull"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_2")  # concurrent project dir
    test_data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), test_project)

    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(test_data_dir, project_dir)
    mc.create_project_and_push(project, project_dir)

    # update our gpkg in a different directory
    mc.download_project(project, project_dir_2)
    shutil.copy(
        os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"),
        os.path.join(project_dir_2, "base.gpkg"),
    )
    mc.pull_project(project_dir_2)
    mc.push_project(project_dir_2)

    # make some other local change
    shutil.copy(
        os.path.join(TEST_DATA_DIR, "inserted_1_B.gpkg"),
        os.path.join(project_dir, "base.gpkg"),
    )

    # remove the basefile to simulate the issue
    os.remove(os.path.join(project_dir, ".mergin", "base.gpkg"))

    # try to sync again  -- it should not crash
    mc.pull_project(project_dir)
    mc.push_project(project_dir)


def test_empty_file_in_subdir(mc):
    """Test pull of a project where there is an empty file in a sub-directory"""

    test_project = "test_empty_file_in_subdir"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_2")  # concurrent project dir
    test_data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), test_project)

    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(test_data_dir, project_dir)
    mc.create_project_and_push(project, project_dir)

    # try to check out the project
    mc.download_project(project, project_dir_2)
    assert os.path.exists(os.path.join(project_dir_2, "subdir", "empty.txt"))

    # add another empty file in a different subdir
    os.mkdir(os.path.join(project_dir, "subdir2"))
    shutil.copy(
        os.path.join(project_dir, "subdir", "empty.txt"),
        os.path.join(project_dir, "subdir2", "empty2.txt"),
    )
    mc.push_project(project_dir)

    # check that pull works fine
    mc.pull_project(project_dir_2)
    assert os.path.exists(os.path.join(project_dir_2, "subdir2", "empty2.txt"))


def test_clone_project(mc: MerginClient):
    test_project = "test_clone_project"
    test_project_fullname = API_USER + "/" + test_project

    # cleanups
    project_dir = os.path.join(TMP_DIR, test_project)
    cleanup(mc, test_project_fullname, [project_dir])

    # create new (empty) project on server
    mc.create_project(test_project)
    projects = mc.projects_list(flag="created")
    assert any(p for p in projects if p["name"] == test_project and p["namespace"] == API_USER)

    cloned_project_name = test_project + "_cloned"
    test_cloned_project_fullname = API_USER + "/" + cloned_project_name

    # cleanup cloned project
    cloned_project_dir = os.path.join(TMP_DIR, cloned_project_name)
    cleanup(mc, API_USER + "/" + cloned_project_name, [cloned_project_dir])

    # clone specifying cloned_project_namespace, does clone but raises deprecation warning
    with pytest.deprecated_call(match=r"The usage of `cloned_project_namespace` parameter in `clone_project\(\)`"):
        mc.clone_project(test_project_fullname, cloned_project_name, API_USER)
    projects = mc.projects_list(flag="created")
    assert any(p for p in projects if p["name"] == cloned_project_name and p["namespace"] == API_USER)
    cleanup(mc, API_USER + "/" + cloned_project_name, [cloned_project_dir])

    # clone without specifying cloned_project_namespace relies on workspace with user name, does clone but raises deprecation warning
    with pytest.deprecated_call(match=r"The use of only project name as `cloned_project_name` in `clone_project\(\)`"):
        mc.clone_project(test_project_fullname, cloned_project_name)
    projects = mc.projects_list(flag="created")
    assert any(p for p in projects if p["name"] == cloned_project_name and p["namespace"] == API_USER)
    cleanup(mc, API_USER + "/" + cloned_project_name, [cloned_project_dir])

    # clone project with full cloned project name with specification of `cloned_project_namespace` raises warning
    with pytest.warns(match=r"Parameter `cloned_project_namespace` specified with full cloned project name"):
        mc.clone_project(test_project_fullname, test_cloned_project_fullname, API_USER)
    projects = mc.projects_list(flag="created")
    assert any(p for p in projects if p["name"] == cloned_project_name and p["namespace"] == API_USER)
    cleanup(mc, API_USER + "/" + cloned_project_name, [cloned_project_dir])

    # clone project using project full name
    mc.clone_project(test_project_fullname, test_cloned_project_fullname)
    projects = mc.projects_list(flag="created")
    assert any(p for p in projects if p["name"] == cloned_project_name and p["namespace"] == API_USER)
    cleanup(mc, API_USER + "/" + cloned_project_name, [cloned_project_dir])


def test_set_read_write_access(mc):
    test_project = "test_set_read_write_access"
    test_project_fullname = API_USER + "/" + test_project

    # cleanups
    project_dir = os.path.join(TMP_DIR, test_project, API_USER)
    cleanup(mc, test_project_fullname, [project_dir])

    # create new (empty) project on server
    mc.create_project(test_project)

    # Add writer access to another client
    project_info = get_project_info(mc, API_USER, test_project)
    access = project_info["access"]
    access["writersnames"].append(API_USER2)
    access["readersnames"].append(API_USER2)
    editor_support = server_has_editor_support(mc, access)
    if editor_support:
        access["editorsnames"].append(API_USER2)
    mc.set_project_access(test_project_fullname, access)

    project_info = get_project_info(mc, API_USER, test_project)
    access = project_info["access"]
    assert API_USER2 in access["writersnames"]
    assert API_USER2 in access["readersnames"]
    if editor_support:
        assert API_USER2 in access["editorsnames"]


def test_set_editor_access(mc):
    test_project = "test_set_editor_access"
    test_project_fullname = API_USER + "/" + test_project

    # cleanups
    project_dir = os.path.join(TMP_DIR, test_project, API_USER)
    cleanup(mc, test_project_fullname, [project_dir])

    # create new (empty) project on server
    mc.create_project(test_project)

    project_info = get_project_info(mc, API_USER, test_project)
    access = project_info["access"]
    # Stop test if server does not support editor access
    if not server_has_editor_support(mc, access):
        return

    access["readersnames"].append(API_USER2)
    access["editorsnames"].append(API_USER2)
    mc.set_project_access(test_project_fullname, access)
    # check access
    project_info = get_project_info(mc, API_USER, test_project)
    access = project_info["access"]
    assert API_USER2 in access["editorsnames"]
    assert API_USER2 in access["readersnames"]
    assert API_USER2 not in access["writersnames"]


def test_available_storage_validation(mcStorage):
    """
    Testing of storage limit - applies to user pushing changes into own project (namespace matching username).
    This test also tests giving read and write access to another user. Additionally tests also uploading of big file.
    """
    test_project = "test_available_storage_validation"
    test_project_fullname = STORAGE_WORKSPACE + "/" + test_project

    # cleanups
    project_dir = os.path.join(TMP_DIR, test_project, API_USER)
    cleanup(mcStorage, test_project_fullname, [project_dir])

    # create new (empty) project on server
    # if namespace is not provided, function is creating project with username
    mcStorage.create_project(test_project_fullname)

    # download project
    mcStorage.download_project(test_project_fullname, project_dir)

    # get info about storage capacity
    storage_remaining = 0

    if mcStorage.server_type() == ServerType.OLD:
        user_info = mcStorage.user_info()
        storage_remaining = user_info["storage"] - user_info["disk_usage"]
    else:
        for workspace in mcStorage.workspaces_list():
            if workspace["name"] == STORAGE_WORKSPACE:
                storage_remaining = workspace["storage"] - workspace["disk_usage"]
                break

    # generate dummy data (remaining storage + extra 1024b)
    dummy_data_path = project_dir + "/data"
    file_size = storage_remaining + 1024
    _generate_big_file(dummy_data_path, file_size)

    # try to upload
    got_right_err = False
    try:
        mcStorage.push_project(project_dir)
    except ClientError as e:
        # Expecting "You have reached a data limit" 400 server error msg.
        assert "You have reached a data limit" in str(e)
        got_right_err = True
    assert got_right_err

    # Expecting empty project
    project_info = get_project_info(mcStorage, API_USER, test_project)
    assert project_info["version"] == "v0"
    assert project_info["disk_usage"] == 0

    # remove dummy big file from a disk
    remove_folders([project_dir])


def test_available_storage_validation2(mc, mc2):
    """
    Testing of storage limit - should not be applied for user pushing changes into project with different namespace.
    This should cover the exception of mergin-py-client that a user can push changes to someone else's project regardless
    the user's own storage limitation. Of course, other limitations are still applied (write access, owner of
    a modified project has to have enough free storage).

    Therefore NOTE that there are following assumptions:
        - API_USER2's free storage >= API_USER's free storage + 1024b (size of changes to be pushed)
        - both accounts should ideally have a free plan
    """
    test_project = "test_available_storage_validation2"
    test_project_fullname = API_USER2 + "/" + test_project

    # cleanups
    project_dir = os.path.join(TMP_DIR, test_project, API_USER)
    cleanup(mc, test_project_fullname, [project_dir])
    cleanup(mc2, test_project_fullname, [project_dir])

    # create new (empty) project on server
    mc2.create_project(test_project)

    # Add writer access to another client
    project_info = get_project_info(mc2, API_USER2, test_project)
    access = project_info["access"]
    access["writersnames"].append(API_USER)
    access["readersnames"].append(API_USER)
    mc2.set_project_access(test_project_fullname, access)

    # download project
    mc.download_project(test_project_fullname, project_dir)

    # get info about storage capacity
    storage_remaining = 0
    if mc.server_type() == ServerType.OLD:
        user_info = mc.user_info()
        storage_remaining = user_info["storage"] - user_info["disk_usage"]
    else:
        # This test does not make sense in newer servers as quotas are tied to workspaces, not users
        return

    # generate dummy data (remaining storage + extra 1024b)
    dummy_data_path = project_dir + "/data"
    file_size = storage_remaining + 1024
    _generate_big_file(dummy_data_path, file_size)

    # try to upload
    mc.push_project(project_dir)

    # Check project content
    project_info = mc.project_info(test_project_fullname)
    assert len(project_info["files"]) == 1
    assert project_info["disk_usage"] == file_size

    # remove dummy big file from a disk
    remove_folders([project_dir])


def get_project_info(mc, namespace, project_name):
    """
    Returns first (and suppose to be just one) project info dict of project matching given namespace and name.
    :param mc: MerginClient instance
    :param namespace: project's namespace
    :param project_name: project's name
    :return: dict with project info
    """
    projects = mc.projects_list(flag="created")
    test_project_list = [p for p in projects if p["name"] == project_name and p["namespace"] == namespace]
    assert len(test_project_list) == 1
    return test_project_list[0]


def _generate_big_file(filepath, size):
    """
    generate big binary file with the specified size in bytes
    :param filepath: full filepath
    :param size: the size in bytes
    """
    with open(filepath, "wb") as fout:
        fout.write(b"\0" * size)


def test_get_projects_by_name(mc):
    """Test server 'bulk' endpoint for projects' info"""
    test_projects = {
        "projectA": f"{API_USER}/projectA",
        "projectB": f"{API_USER}/projectB",
    }

    for name, full_name in test_projects.items():
        cleanup(mc, full_name, [])
        mc.create_project(name)

    resp = mc.get_projects_by_names(list(test_projects.values()))
    assert len(resp) == len(test_projects)
    for name, full_name in test_projects.items():
        assert full_name in resp
        assert resp[full_name]["name"] == name
        assert resp[full_name]["version"] == "v0"


def test_download_versions(mc):
    test_project = "test_download"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    # download dirs
    project_dir_v1 = os.path.join(TMP_DIR, test_project + "_v1")
    project_dir_v2 = os.path.join(TMP_DIR, test_project + "_v2")
    project_dir_v3 = os.path.join(TMP_DIR, test_project + "_v3")

    cleanup(mc, project, [project_dir, project_dir_v1, project_dir_v2, project_dir_v3])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    # create new version - v2
    f_added = "new.txt"
    with open(os.path.join(project_dir, f_added), "w") as f:
        f.write("new file")

    mc.push_project(project_dir)
    project_info = mc.project_info(project)
    assert project_info["version"] == "v2"

    mc.download_project(project, project_dir_v1, "v1")
    assert os.path.exists(os.path.join(project_dir_v1, "base.gpkg"))
    assert not os.path.exists(os.path.join(project_dir_v2, f_added))  # added only in v2

    mc.download_project(project, project_dir_v2, "v2")
    assert os.path.exists(os.path.join(project_dir_v2, f_added))
    assert os.path.exists(os.path.join(project_dir_v1, "base.gpkg"))  # added in v1 but still present in v2

    # try to download not-existing version
    with pytest.raises(ClientError):
        mc.download_project(project, project_dir_v3, "v3")


def test_paginated_project_list(mc):
    """Test the new endpoint for projects list with pagination, ordering etc."""
    test_projects = dict()
    for symb in "ABCDEF":
        name = f"test_paginated_{symb}"
        test_projects[name] = f"{API_USER}/{name}"

    for name, full_name in test_projects.items():
        cleanup(mc, full_name, [])
        mc.create_project(name)

    sorted_test_names = [n for n in sorted(test_projects.keys())]

    resp = mc.paginated_projects_list(
        flag="created",
        name="test_paginated",
        page=1,
        per_page=10,
        order_params="name_asc",
    )
    projects = resp["projects"]
    count = resp["count"]
    assert count == len(test_projects)
    assert len(projects) == len(test_projects)
    for i, project in enumerate(projects):
        assert project["name"] == sorted_test_names[i]

    resp = mc.paginated_projects_list(
        flag="created",
        name="test_paginated",
        page=2,
        per_page=2,
        order_params="name_asc",
    )
    projects = resp["projects"]
    assert len(projects) == 2
    for i, project in enumerate(projects):
        assert project["name"] == sorted_test_names[i + 2]


def test_missing_local_file_pull(mc):
    """Test pull of a project where a file deleted in the service is missing for some reason."""

    test_project = "test_dir"
    file_to_remove = "test2.txt"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project + "_5")  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_6")  # concurrent project dir
    test_data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data", test_project)

    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(test_data_dir, project_dir)
    mc.create_project_and_push(project, project_dir)

    # remove a file in a different directory
    mc.download_project(project, project_dir_2)
    os.remove(os.path.join(project_dir_2, file_to_remove))
    mc.push_project(project_dir_2)

    # manually remove the file also locally in the original project dir
    os.remove(os.path.join(project_dir, file_to_remove))

    # try to sync again  -- it should not crash
    mc.pull_project(project_dir)
    mc.push_project(project_dir)


def test_logging(mc):
    """Test logger creation with and w/out the env variable for log file."""
    assert isinstance(mc.log.handlers[0], logging.NullHandler)
    mc.log.info("Test info log...")
    # remove the Null handler and set the env variable with the global log file path
    mc.log.handlers = []
    os.environ["MERGIN_CLIENT_LOG"] = os.path.join(TMP_DIR, "global-mergin-log.txt")
    assert os.environ.get("MERGIN_CLIENT_LOG", None) is not None
    token = mc._auth_session["token"]
    mc1 = MerginClient(mc.url, auth_token=token)
    assert isinstance(mc1.log.handlers[0], logging.FileHandler)
    mc1.log.info("Test log info to the log file...")
    # cleanup
    mc.log.handlers = []
    del os.environ["MERGIN_CLIENT_LOG"]


def test_server_compatibility(mc):
    """Test server compatibility."""
    assert mc.is_server_compatible()


def create_versioned_project(mc, project_name, project_dir, updated_file, remove=True, overwrite=False):
    project = API_USER + "/" + project_name
    cleanup(mc, project, [project_dir])

    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    mp = MerginProject(project_dir)

    # create versions 2-4
    changes = (
        "inserted_1_A.gpkg",
        "inserted_1_A_mod.gpkg",
        "inserted_1_B.gpkg",
    )
    for change in changes:
        shutil.copy(mp.fpath(change), mp.fpath(updated_file))
        mc.push_project(project_dir)
    # create version 5 with modified file removed
    if remove:
        os.remove(os.path.join(project_dir, updated_file))
        mc.push_project(project_dir)

    # create version with forced overwrite (broken history)
    if overwrite:
        shutil.move(mp.fpath(updated_file), mp.fpath_meta(updated_file))
        shutil.copy(
            os.path.join(CHANGED_SCHEMA_DIR, "modified_schema.gpkg"),
            mp.fpath(updated_file),
        )
        shutil.copy(
            os.path.join(CHANGED_SCHEMA_DIR, "modified_schema.gpkg-wal"),
            mp.fpath(updated_file + "-wal"),
        )
        mc.push_project(project_dir)
    return mp


def test_get_versions_with_file_changes(mc):
    """Test getting versions where the file was changed."""
    test_project = "test_file_modified_versions"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    f_updated = "base.gpkg"

    mp = create_versioned_project(mc, test_project, project_dir, f_updated, remove=False)

    project_info = mc.project_info(project)
    assert project_info["version"] == "v4"
    assert project_info["id"] == mp.project_id()
    file_history = mc.project_file_history_info(project, f_updated)

    with pytest.raises(ClientError) as e:
        mod_versions = get_versions_with_file_changes(
            mc,
            project,
            f_updated,
            version_from="v1",
            version_to="v5",
            file_history=file_history,
        )
    assert "Wrong version parameters: 1-5" in str(e.value)
    assert "Available versions: [1, 2, 3, 4]" in str(e.value)

    mod_versions = get_versions_with_file_changes(
        mc,
        project,
        f_updated,
        version_from="v2",
        version_to="v4",
        file_history=file_history,
    )
    assert mod_versions == [f"v{i}" for i in range(2, 5)]


def check_gpkg_same_content(mergin_project, gpkg_path_1, gpkg_path_2):
    """Check if the two GeoPackages have equal content."""
    with tempfile.TemporaryDirectory() as temp_dir:
        diff_path = os.path.join(temp_dir, "diff_file")
        mergin_project.geodiff.create_changeset(gpkg_path_1, gpkg_path_2, diff_path)
        return not mergin_project.geodiff.has_changes(diff_path)


def test_download_file(mc):
    """Test downloading single file at specified versions."""
    test_project = "test_download_file"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    f_updated = "base.gpkg"

    mp = create_versioned_project(mc, test_project, project_dir, f_updated)

    project_info = mc.project_info(project)
    assert project_info["version"] == "v5"
    assert project_info["id"] == mp.project_id()

    # Versioned file should have the following content at versions 2-4
    expected_content = (
        "inserted_1_A.gpkg",
        "inserted_1_A_mod.gpkg",
        "inserted_1_B.gpkg",
    )

    # Download the base file at versions 2-4 and check the changes
    f_downloaded = os.path.join(project_dir, f_updated)
    for ver in range(2, 5):
        mc.download_file(project_dir, f_updated, f_downloaded, version=f"v{ver}")
        expected = os.path.join(TEST_DATA_DIR, expected_content[ver - 2])  # GeoPackage with expected content
        assert check_gpkg_same_content(mp, f_downloaded, expected)

    # make sure there will be exception raised if a file doesn't exist in the version
    with pytest.raises(ClientError, match=f"No \\[{f_updated}\\] exists at version v5"):
        mc.download_file(project_dir, f_updated, f_downloaded, version="v5")


def test_download_diffs(mc):
    """Test download diffs for a project file between specified project versions."""
    test_project = "test_download_diffs"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    download_dir = os.path.join(project_dir, "diffs")  # project for downloading files at various versions
    f_updated = "base.gpkg"
    diff_file = os.path.join(download_dir, f_updated + ".diff")

    mp = create_versioned_project(mc, test_project, project_dir, f_updated, remove=False)

    project_info = mc.project_info(project)
    assert project_info["version"] == "v4"
    assert project_info["id"] == mp.project_id()

    # Download diffs of updated file between versions 1 and 2
    mc.get_file_diff(project_dir, f_updated, diff_file, "v1", "v2")
    assert os.path.exists(diff_file)
    assert mp.geodiff.has_changes(diff_file)
    assert mp.geodiff.changes_count(diff_file) == 1
    changes_file = diff_file + ".changes1-2"
    mp.geodiff.list_changes_summary(diff_file, changes_file)
    with open(changes_file, "r") as f:
        changes = json.loads(f.read())["geodiff_summary"][0]
        assert changes["insert"] == 1
        assert changes["update"] == 0

    # Download diffs of updated file between versions 2 and 4
    mc.get_file_diff(project_dir, f_updated, diff_file, "v2", "v4")
    changes_file = diff_file + ".changes2-4"
    mp.geodiff.list_changes_summary(diff_file, changes_file)
    with open(changes_file, "r") as f:
        changes = json.loads(f.read())["geodiff_summary"][0]
        assert changes["insert"] == 0
        assert changes["update"] == 1

    with pytest.raises(ClientError) as e:
        mc.get_file_diff(project_dir, f_updated, diff_file, "v4", "v1")
    assert "Wrong version parameters" in str(e.value)
    assert "version_from needs to be smaller than version_to" in str(e.value)

    with pytest.raises(ClientError) as e:
        mc.get_file_diff(project_dir, f_updated, diff_file, "v4", "v5")
    assert "Wrong version parameters" in str(e.value)
    assert "Available versions: [1, 2, 3, 4]" in str(e.value)


def test_modify_project_permissions(mc):
    test_project = "test_project"
    test_project_fullname = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    download_dir = os.path.join(TMP_DIR, "download", test_project)

    cleanup(mc, test_project_fullname, [project_dir, download_dir])
    # prepare local project
    shutil.copytree(TEST_DATA_DIR, project_dir)

    # create remote project
    mc.create_project_and_push(test_project_fullname, directory=project_dir)

    permissions = mc.project_user_permissions(test_project_fullname)
    assert permissions["owners"] == [API_USER]
    assert permissions["writers"] == [API_USER]
    assert permissions["readers"] == [API_USER]
    editor_support = server_has_editor_support(mc, permissions)
    if editor_support:
        assert permissions["editors"] == [API_USER]

    mc.add_user_permissions_to_project(test_project_fullname, [API_USER2], "writer")
    permissions = mc.project_user_permissions(test_project_fullname)
    assert set(permissions["owners"]) == {API_USER}
    assert set(permissions["writers"]) == {API_USER, API_USER2}
    assert set(permissions["readers"]) == {API_USER, API_USER2}
    editor_support = server_has_editor_support(mc, permissions)
    if editor_support:
        assert set(permissions["editors"]) == {API_USER, API_USER2}

    mc.remove_user_permissions_from_project(test_project_fullname, [API_USER2])
    permissions = mc.project_user_permissions(test_project_fullname)
    assert permissions["owners"] == [API_USER]
    assert permissions["writers"] == [API_USER]
    assert permissions["readers"] == [API_USER]

    editor_support = server_has_editor_support(mc, permissions)
    if editor_support:
        assert permissions["editors"] == [API_USER]


def _use_wal(db_file):
    """Ensures that sqlite database is using WAL journal mode"""
    con = sqlite3.connect(db_file)
    cursor = con.cursor()
    cursor.execute("PRAGMA journal_mode=wal;")
    cursor.close()
    con.close()


def _create_test_table(db_file):
    """Creates a table called 'test' in sqlite database. Useful to simulate change of database schema."""
    con = sqlite3.connect(db_file)
    cursor = con.cursor()
    cursor.execute("CREATE TABLE test (fid SERIAL, txt TEXT);")
    cursor.execute("INSERT INTO test VALUES (123, 'hello');")
    cursor.execute("COMMIT;")


def _create_spatial_table(db_file):
    """Creates a spatial table called 'test' in sqlite database. Useful to simulate change of database schema."""
    con = sqlite3.connect(db_file)
    cursor = con.cursor()
    cursor.execute("CREATE TABLE geo_test (fid SERIAL, geometry POINT NOT NULL, txt TEXT);")
    cursor.execute(
        "INSERT INTO gpkg_contents VALUES ('geo_test', 'features','description','geo_test','2019-06-18T14:52:50.928Z',-1.08892,0.0424077,-0.363885,0.562244,4326);"
    )
    cursor.execute("INSERT INTO gpkg_geometry_columns VALUES ('geo_test','geometry','POINT',4326, 0, 0 )")
    cursor.execute("COMMIT;")


def _delete_spatial_table(db_file):
    """Drops spatial table called 'test' in sqlite database. Useful to simulate change of database schema."""
    con = sqlite3.connect(db_file)
    cursor = con.cursor()
    cursor.execute("DROP TABLE poi;")
    cursor.execute("DELETE FROM gpkg_geometry_columns WHERE table_name='poi';")
    cursor.execute("DELETE FROM gpkg_contents WHERE table_name='poi';")
    cursor.execute("COMMIT;")


def _check_test_table(db_file):
    """Checks whether the 'test' table exists and has one row - otherwise fails with an exception."""
    assert _get_table_row_count(db_file, "test") == 1


def _get_table_row_count(db_file, table):
    try:
        con_verify = sqlite3.connect(db_file)
        cursor_verify = con_verify.cursor()
        cursor_verify.execute("select count(*) from {};".format(table))
        return cursor_verify.fetchone()[0]
    finally:
        cursor_verify.close()
        con_verify.close()


def _is_file_updated(filename, changes_dict):
    """checks whether a file is listed among updated files (for pull or push changes)"""
    for f in changes_dict["updated"]:
        if f["path"] == filename:
            return True
    return False


class AnotherSqliteConn:
    """This simulates another app (e.g. QGIS) having a connection open, potentially
    with some active reader/writer.

    Note: we use a subprocess here instead of just using sqlite3 module from python
    because pygeodiff and python's sqlite3 module have their own sqlite libraries,
    and this does not work well when they are used in a single process. But if we
    use another process, things are fine. This is a limitation of how we package
    pygeodiff currently.
    """

    def __init__(self, filename):
        self.proc = subprocess.Popen(
            [
                "python3",
                os.path.join(os.path.dirname(__file__), "sqlite_con.py"),
                filename,
            ],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def run(self, cmd):
        self.proc.stdin.write(cmd.encode() + b"\n")
        self.proc.stdin.flush()

    def close(self):
        out, err = self.proc.communicate(b"stop\n")
        if self.proc.returncode != 0:
            raise ValueError("subprocess error:\n" + err.decode("utf-8"))


def test_push_gpkg_schema_change(mc):
    """Test that changes in GPKG get picked up if there were recent changes to it by another
    client and at the same time geodiff fails to find changes (a new table is added)
    """

    test_project = "test_push_gpkg_schema_change"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    test_gpkg = os.path.join(project_dir, "test.gpkg")
    test_gpkg_basefile = os.path.join(project_dir, ".mergin", "test.gpkg")
    project_dir_verify = os.path.join(TMP_DIR, test_project + "_verify")
    test_gpkg_verify = os.path.join(project_dir_verify, "test.gpkg")

    cleanup(mc, project, [project_dir, project_dir_verify])
    # create remote project
    os.makedirs(project_dir)
    shutil.copy(os.path.join(TEST_DATA_DIR, "base.gpkg"), test_gpkg)
    # shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    mp = MerginProject(project_dir)

    mp.log.info(" // create changeset")

    mp.geodiff.create_changeset(mp.fpath("test.gpkg"), mp.fpath_meta("test.gpkg"), mp.fpath_meta("diff-0"))

    mp.log.info(" // use wal")

    _use_wal(test_gpkg)

    mp.log.info(" // make changes to DB")

    # open a connection and keep it open (qgis does this with a pool of connections too)
    acon2 = AnotherSqliteConn(test_gpkg)
    acon2.run("select count(*) from simple;")

    # add a new table to ensure that geodiff will fail due to unsupported change
    # (this simulates an independent reader/writer like GDAL)
    _create_test_table(test_gpkg)

    _check_test_table(test_gpkg)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg_basefile)

    mp.log.info(" // create changeset (2)")

    # why already here there is wal recovery - it could be because of two sqlite libs linked in one executable
    # INDEED THAT WAS THE PROBLEM, now running geodiff 1.0 with shared sqlite lib seems to work fine.
    with pytest.raises(pygeodiff.geodifflib.GeoDiffLibError):
        mp.geodiff.create_changeset(mp.fpath("test.gpkg"), mp.fpath_meta("test.gpkg"), mp.fpath_meta("diff-1"))

    _check_test_table(test_gpkg)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg_basefile)

    mp.log.info(" // push project")

    # push pending changes (it should include addition of the new table)
    # at this point we still have an open sqlite connection to the GPKG, so checkpointing will not work correctly)
    mc.push_project(project_dir)

    # WITH TWO SQLITE copies: fails here  (sqlite3.OperationalError: disk I/O error)  + in geodiff log: SQLITE3: (283)recovered N frames from WAL file
    _check_test_table(test_gpkg)

    # OLD: fails here when con2 is still alive: checkpointing fails and basefile has incorrect value
    _check_test_table(test_gpkg_basefile)

    # download the project to a new directory and verify the change was pushed correctly
    mc.download_project(project, project_dir_verify)

    # OLD: fails here
    _check_test_table(test_gpkg_verify)

    acon2.close()


@pytest.mark.parametrize("extra_connection", [False, True])
def test_rebase_local_schema_change(mc, extra_connection):
    """
    Checks whether a pull with failed rebase (due to local DB schema change) is handled correctly,
    i.e. a conflict file is created with the content of the local changes.
    """

    test_project = "test_rebase_local_schema_change"
    if extra_connection:
        test_project += "_extra_conn"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_2")  # concurrent project dir
    test_gpkg = os.path.join(project_dir, "test.gpkg")
    test_gpkg_basefile = os.path.join(project_dir, ".mergin", "test.gpkg")
    test_gpkg_conflict = conflicted_copy_file_name(test_gpkg, API_USER, 1)
    cleanup(mc, project, [project_dir, project_dir_2])

    os.makedirs(project_dir)
    shutil.copy(os.path.join(TEST_DATA_DIR, "base.gpkg"), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL, that's the more common and more difficult scenario
    mc.create_project_and_push(project, project_dir)

    if extra_connection:
        # open a connection and keep it open (qgis does this with a pool of connections too)
        con_extra = sqlite3.connect(test_gpkg)
        cursor_extra = con_extra.cursor()
        cursor_extra.execute("select count(*) from simple;")

    # Download project to the concurrent dir + add a feature + push a new version
    mc.download_project(project, project_dir_2)  # download project to concurrent dir
    mp_2 = MerginProject(project_dir_2)
    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), mp_2.fpath("test.gpkg"))
    mc.push_project(project_dir_2)

    # Change schema in the primary project dir
    _create_test_table(test_gpkg)

    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert _is_file_updated("test.gpkg", pull_changes)
    assert _is_file_updated("test.gpkg", push_changes)

    assert not os.path.exists(test_gpkg_conflict)

    mc.pull_project(project_dir)

    assert os.path.exists(test_gpkg_conflict)

    # check the results after pull:
    # - conflict copy should contain the new table
    # - local file + basefile should not contain the new table

    _check_test_table(test_gpkg_conflict)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg_basefile)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg)

    # check that the local file + basefile contain the new row, and the conflict copy doesn't

    assert _get_table_row_count(test_gpkg, "simple") == 4
    assert _get_table_row_count(test_gpkg_basefile, "simple") == 4
    assert _get_table_row_count(test_gpkg_conflict, "simple") == 3


@pytest.mark.parametrize("extra_connection", [False, True])
def test_rebase_remote_schema_change(mc, extra_connection):
    """
    Checks whether a pull with failed rebase (due to remote DB schema change) is handled correctly,
    i.e. a conflict file is created with the content of the local changes.
    """

    test_project = "test_rebase_remote_schema_change"
    if extra_connection:
        test_project += "_extra_conn"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_2")  # concurrent project dir
    test_gpkg = os.path.join(project_dir, "test.gpkg")
    test_gpkg_2 = os.path.join(project_dir_2, "test.gpkg")
    test_gpkg_basefile = os.path.join(project_dir, ".mergin", "test.gpkg")
    test_gpkg_conflict = conflicted_copy_file_name(test_gpkg, API_USER, 1)
    cleanup(mc, project, [project_dir, project_dir_2])

    os.makedirs(project_dir)
    shutil.copy(os.path.join(TEST_DATA_DIR, "base.gpkg"), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL, that's the more common and more difficult scenario
    mc.create_project_and_push(project, project_dir)

    # Download project to the concurrent dir + change DB schema + push a new version
    mc.download_project(project, project_dir_2)
    _create_test_table(test_gpkg_2)
    mc.push_project(project_dir_2)

    # do changes in the local DB (added a row)
    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL

    if extra_connection:
        # open a connection and keep it open (qgis does this with a pool of connections too)
        con_extra = sqlite3.connect(test_gpkg)
        cursor_extra = con_extra.cursor()
        cursor_extra.execute("select count(*) from simple;")

    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert _is_file_updated("test.gpkg", pull_changes)
    assert _is_file_updated("test.gpkg", push_changes)

    assert not os.path.exists(test_gpkg_conflict)

    mc.pull_project(project_dir)

    assert os.path.exists(test_gpkg_conflict)

    # check the results after pull:
    # - conflict copy should not contain the new table
    # - local file + basefile should contain the new table

    _check_test_table(test_gpkg)
    _check_test_table(test_gpkg_basefile)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg_conflict)

    # check that the local file + basefile don't contain the new row, and the conflict copy does

    assert _get_table_row_count(test_gpkg, "simple") == 3
    assert _get_table_row_count(test_gpkg_basefile, "simple") == 3
    assert _get_table_row_count(test_gpkg_conflict, "simple") == 4


@pytest.mark.parametrize("extra_connection", [False, True])
def test_rebase_success(mc, extra_connection):
    """
    Checks whether a pull with successful rebase is handled correctly.
    i.e. changes are merged together and no conflict files are created.
    """

    test_project = "test_rebase_success"
    if extra_connection:
        test_project += "_extra_conn"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_2")  # concurrent project dir
    test_gpkg = os.path.join(project_dir, "test.gpkg")
    test_gpkg_2 = os.path.join(project_dir_2, "test.gpkg")
    test_gpkg_basefile = os.path.join(project_dir, ".mergin", "test.gpkg")
    test_gpkg_conflict = conflicted_copy_file_name(test_gpkg, API_USER, 1)
    cleanup(mc, project, [project_dir, project_dir_2])

    os.makedirs(project_dir)
    shutil.copy(os.path.join(TEST_DATA_DIR, "base.gpkg"), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL, that's the more common and more difficult scenario
    mc.create_project_and_push(project, project_dir)

    # Download project to the concurrent dir + add a row + push a new version
    mc.download_project(project, project_dir_2)
    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), test_gpkg_2)
    _use_wal(test_gpkg)  # make sure we use WAL
    mc.push_project(project_dir_2)

    # do changes in the local DB (added a row)
    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_B.gpkg"), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL

    if extra_connection:
        # open a connection and keep it open (qgis does this with a pool of connections too)
        con_extra = sqlite3.connect(test_gpkg)
        cursor_extra = con_extra.cursor()
        cursor_extra.execute("select count(*) from simple;")

    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert _is_file_updated("test.gpkg", pull_changes)
    assert _is_file_updated("test.gpkg", push_changes)

    assert not os.path.exists(test_gpkg_conflict)

    mc.pull_project(project_dir)

    assert not os.path.exists(test_gpkg_conflict)

    # check that the local file + basefile don't contain the new row, and the conflict copy does

    assert _get_table_row_count(test_gpkg, "simple") == 5
    assert _get_table_row_count(test_gpkg_basefile, "simple") == 4


def test_conflict_file_names():
    """
    Test generation of file names for conflicts files.
    """

    data = [
        (
            "/home/test/geo.gpkg",
            "jack",
            10,
            "/home/test/geo (conflicted copy, jack v10).gpkg",
        ),
        ("/home/test/g.pkg", "j", 0, "/home/test/g (conflicted copy, j v0).pkg"),
        (
            "home/test/geo.gpkg",
            "jack",
            10,
            "home/test/geo (conflicted copy, jack v10).gpkg",
        ),
        ("geo.gpkg", "jack", 10, "geo (conflicted copy, jack v10).gpkg"),
        ("/home/../geo.gpkg", "jack", 10, "/geo (conflicted copy, jack v10).gpkg"),
        ("/home/./geo.gpkg", "jack", 10, "/home/geo (conflicted copy, jack v10).gpkg"),
        ("/home/test/geo.gpkg", "", 10, "/home/test/geo (conflicted copy,  v10).gpkg"),
        (
            "/home/test/geo.gpkg",
            "jack",
            -1,
            "/home/test/geo (conflicted copy, jack v-1).gpkg",
        ),
        (
            "/home/test/geo.tar.gz",
            "jack",
            100,
            "/home/test/geo (conflicted copy, jack v100).tar.gz",
        ),
        ("", "jack", 1, ""),
        (
            "/home/test/survey.qgs",
            "jack",
            10,
            "/home/test/survey (conflicted copy, jack v10).qgs~",
        ),
        (
            "/home/test/survey.QGZ",
            "jack",
            10,
            "/home/test/survey (conflicted copy, jack v10).QGZ~",
        ),
    ]

    for i in data:
        file_name = conflicted_copy_file_name(i[0], i[1], i[2])
        assert file_name == i[3]

    data = [
        (
            "/home/test/geo.json",
            "jack",
            10,
            "/home/test/geo (edit conflict, jack v10).json",
        ),
        ("/home/test/g.jsn", "j", 0, "/home/test/g (edit conflict, j v0).json"),
        (
            "home/test/geo.json",
            "jack",
            10,
            "home/test/geo (edit conflict, jack v10).json",
        ),
        ("geo.json", "jack", 10, "geo (edit conflict, jack v10).json"),
        ("/home/../geo.json", "jack", 10, "/geo (edit conflict, jack v10).json"),
        ("/home/./geo.json", "jack", 10, "/home/geo (edit conflict, jack v10).json"),
        ("/home/test/geo.json", "", 10, "/home/test/geo (edit conflict,  v10).json"),
        (
            "/home/test/geo.json",
            "jack",
            -1,
            "/home/test/geo (edit conflict, jack v-1).json",
        ),
        (
            "/home/test/geo.gpkg",
            "jack",
            10,
            "/home/test/geo (edit conflict, jack v10).json",
        ),
        (
            "/home/test/geo.tar.gz",
            "jack",
            100,
            "/home/test/geo (edit conflict, jack v100).json",
        ),
        ("", "jack", 1, ""),
    ]

    for i in data:
        file_name = edit_conflict_file_name(i[0], i[1], i[2])
        assert file_name == i[3]


def test_unique_path_names():
    """
    Test generation of unique file names.
    """
    project_dir = os.path.join(TMP_DIR, "unique_file_names")

    remove_folders([project_dir])

    os.makedirs(project_dir)
    assert os.path.exists(project_dir)
    assert not any(os.scandir(project_dir))

    # Create test directory structure:
    # - folderA
    #   |- fileA.txt
    #   |- fileA (1).txt
    #   |- fileB.txt
    #   |- folderAB
    #   |- folderAB (1)
    # - file.txt
    # - another.txt
    # - another (1).txt
    # - another (2).txt
    # - arch.tar.gz
    data = {
        "folderA": {
            "files": ["fileA.txt", "fileA (1).txt", "fileB.txt"],
            "folderAB": {},
            "folderAB (1)": {},
        },
        "files": [
            "file.txt",
            "another.txt",
            "another (1).txt",
            "another (2).txt",
            "arch.tar.gz",
        ],
    }
    create_directory(project_dir, data)

    data = [
        ("file.txt", "file (1).txt"),
        ("another.txt", "another (3).txt"),
        ("folderA", "folderA (1)"),
        ("non.txt", "non.txt"),
        ("data.gpkg", "data.gpkg"),
        ("arch.tar.gz", "arch (1).tar.gz"),
        ("folderA/folder", "folderA/folder"),
        ("folderA/fileA.txt", "folderA/fileA (2).txt"),
        ("folderA/fileB.txt", "folderA/fileB (1).txt"),
        ("folderA/fileC.txt", "folderA/fileC.txt"),
        ("folderA/folderAB", "folderA/folderAB (2)"),
    ]
    for i in data:
        file_name = unique_path_name(os.path.join(project_dir, i[0]))
        assert file_name == os.path.join(project_dir, i[1])


def create_directory(root, data):
    for k, v in data.items():
        if isinstance(v, dict):
            dir_name = os.path.join(root, k)
            os.makedirs(dir_name, exist_ok=True)
            for kk in v.keys():
                create_directory(dir_name, v)
        elif isinstance(v, list):
            for file_name in v:
                open(os.path.join(root, file_name), "w").close()


@pytest.mark.skipif(sudo_works(), reason="needs working sudo")
def test_unfinished_pull(mc):
    """
    Checks whether a pull with failed rebase (due to remote DB schema change)
    and failed copy of the database file is handled correctly, i.e. an
    unfinished_pull directory is created with the content of the server changes.
    """
    test_project = "test_unfinished_pull"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_2")  # concurrent project dir
    unfinished_pull_dir = os.path.join(
        TMP_DIR, test_project, ".mergin", "unfinished_pull"
    )  # unfinished_pull dir for the primary project
    test_gpkg = os.path.join(project_dir, "test.gpkg")
    test_gpkg_2 = os.path.join(project_dir_2, "test.gpkg")
    test_gpkg_basefile = os.path.join(project_dir, ".mergin", "test.gpkg")
    test_gpkg_conflict = conflicted_copy_file_name(test_gpkg, API_USER, 2)
    test_gpkg_unfinished_pull = os.path.join(project_dir, ".mergin", "unfinished_pull", "test.gpkg")
    cleanup(mc, project, [project_dir, project_dir_2])

    os.makedirs(project_dir)
    shutil.copy(os.path.join(TEST_DATA_DIR, "base.gpkg"), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL, that's the more common and more difficult scenario
    mc.create_project_and_push(project, project_dir)

    # Download project to the concurrent dir + change DB schema + push a new version
    mc.download_project(project, project_dir_2)
    _create_test_table(test_gpkg_2)
    mc.push_project(project_dir_2)

    # do changes in the local DB (added a row)
    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL

    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert _is_file_updated("test.gpkg", pull_changes)
    assert _is_file_updated("test.gpkg", push_changes)

    assert not os.path.exists(test_gpkg_conflict)
    assert not mc.has_unfinished_pull(project_dir)

    # lock base file to emulate situation when we can't overwrite it, because
    # it is used by another process
    sudo_res = subprocess.run(["sudo", "chattr", "+i", test_gpkg])
    assert sudo_res.returncode == 0

    mc.pull_project(project_dir)

    assert not os.path.exists(test_gpkg_conflict)
    assert mc.has_unfinished_pull(project_dir)

    # check the results after pull:
    # - unfinished pull file should contain the new table
    # - local file + basefile should not contain the new table
    _check_test_table(test_gpkg_unfinished_pull)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg_basefile)

    # check that the local file contain the new row, while basefile and server version don't
    assert _get_table_row_count(test_gpkg, "simple") == 4
    assert _get_table_row_count(test_gpkg_basefile, "simple") == 3
    assert _get_table_row_count(test_gpkg_unfinished_pull, "simple") == 3

    # unlock base file, so we can apply changes from the unfinished pull
    sudo_res = subprocess.run(["sudo", "chattr", "-i", test_gpkg])
    assert sudo_res.returncode == 0

    mc.resolve_unfinished_pull(project_dir)

    assert os.path.exists(test_gpkg_conflict)
    assert not mc.has_unfinished_pull(project_dir)

    # check the results after resolving unfinished pull:
    # - conflict copy should not contain the new table
    # - local file + basefile should contain the new table
    _check_test_table(test_gpkg)
    _check_test_table(test_gpkg_basefile)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg_conflict)

    # check that the local file + basefile don't contain the new row, and the conflict copy does
    assert _get_table_row_count(test_gpkg, "simple") == 3
    assert _get_table_row_count(test_gpkg_basefile, "simple") == 3
    assert _get_table_row_count(test_gpkg_conflict, "simple") == 4


@pytest.mark.skipif(sudo_works(), reason="needs working sudo")
def test_unfinished_pull_push(mc):
    """
    Checks client behaviour when performing push and pull of the project
    in the unfinished pull state.
    """
    test_project = "test_unfinished_pull_push"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_2")  # concurrent project dir
    unfinished_pull_dir = os.path.join(
        TMP_DIR, test_project, ".mergin", "unfinished_pull"
    )  # unfinished_pull dir for the primary project
    test_gpkg = os.path.join(project_dir, "test.gpkg")
    test_gpkg_2 = os.path.join(project_dir_2, "test.gpkg")
    test_gpkg_basefile = os.path.join(project_dir, ".mergin", "test.gpkg")
    test_gpkg_conflict = conflicted_copy_file_name(test_gpkg, API_USER, 2)
    test_gpkg_unfinished_pull = os.path.join(project_dir, ".mergin", "unfinished_pull", "test.gpkg")
    cleanup(mc, project, [project_dir, project_dir_2])

    os.makedirs(project_dir)
    shutil.copy(os.path.join(TEST_DATA_DIR, "base.gpkg"), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL, that's the more common and more difficult scenario
    mc.create_project_and_push(project, project_dir)

    # Download project to the concurrent dir + change DB schema + push a new version
    mc.download_project(project, project_dir_2)
    _create_test_table(test_gpkg_2)
    mc.push_project(project_dir_2)

    # do changes in the local DB (added a row)
    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL

    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert _is_file_updated("test.gpkg", pull_changes)
    assert _is_file_updated("test.gpkg", push_changes)

    assert not os.path.exists(test_gpkg_conflict)
    assert not mc.has_unfinished_pull(project_dir)

    # lock base file to emulate situation when we can't overwrite it, because
    # it is used by another process
    sudo_res = subprocess.run(["sudo", "chattr", "+i", test_gpkg])
    assert sudo_res.returncode == 0

    mc.pull_project(project_dir)

    assert not os.path.exists(test_gpkg_conflict)
    assert mc.has_unfinished_pull(project_dir)

    # check the results after pull:
    # - unfinished pull file should contain the new table
    # - local file + basefile should not contain the new table
    _check_test_table(test_gpkg_unfinished_pull)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg_basefile)

    # check that the local file contain the new row, while basefile and server version don't
    assert _get_table_row_count(test_gpkg, "simple") == 4
    assert _get_table_row_count(test_gpkg_basefile, "simple") == 3
    assert _get_table_row_count(test_gpkg_unfinished_pull, "simple") == 3

    # attempt to push project in the unfinished pull state should
    # fail with ClientError
    with pytest.raises(ClientError):
        mc.push_project(project_dir)

    # attempt to pull project in the unfinished pull state should
    # fail with ClientError if unfinished pull can not be resolved
    with pytest.raises(ClientError):
        mc.pull_project(project_dir)

    # unlock base file, so we can apply changes from the unfinished pull
    sudo_res = subprocess.run(["sudo", "chattr", "-i", test_gpkg])
    assert sudo_res.returncode == 0

    # perform pull. This should resolve unfinished pull first and then
    # collect data from the server
    mc.pull_project(project_dir)

    assert os.path.exists(test_gpkg_conflict)
    assert not mc.has_unfinished_pull(project_dir)

    # check the results after resolving unfinished pull:
    # - conflict copy should not contain the new table
    # - local file + basefile should contain the new table
    _check_test_table(test_gpkg)
    _check_test_table(test_gpkg_basefile)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg_conflict)

    # check that the local file + basefile don't contain the new row, and the conflict copy does
    assert _get_table_row_count(test_gpkg, "simple") == 3
    assert _get_table_row_count(test_gpkg_basefile, "simple") == 3
    assert _get_table_row_count(test_gpkg_conflict, "simple") == 4


def test_project_versions_list(mc):
    """Test getting project versions in various ranges"""
    test_project = "test_project_versions"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    create_versioned_project(mc, test_project, project_dir, "base.gpkg")
    project_info = mc.project_info(project)
    assert project_info["version"] == "v5"

    # get all versions
    versions = mc.project_versions(project)
    assert len(versions) == 5
    assert versions[0]["name"] == "v1"
    assert versions[-1]["name"] == "v5"

    # get first 3 versions
    versions = mc.project_versions(project, to="v3")
    assert len(versions) == 3
    assert versions[-1]["name"] == "v3"

    # get last 2 versions
    versions = mc.project_versions(project, since="v4")
    assert len(versions) == 2
    assert versions[0]["name"] == "v4"

    # get range v2-v4
    versions = mc.project_versions(project, since="v2", to="v4")
    assert len(versions) == 3
    assert versions[0]["name"] == "v2"
    assert versions[-1]["name"] == "v4"


def test_report(mc):
    test_project = "test_report"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    f_updated = "base.gpkg"
    mp = create_versioned_project(mc, test_project, project_dir, f_updated, remove=False, overwrite=True)

    # create report for between versions 2 and 4
    directory = mp.dir
    since = "v2"
    to = "v4"
    proj_name = project.replace(os.path.sep, "-")
    report_file = os.path.join(TMP_DIR, "report", f"{proj_name}-{since}-{to}.csv")
    if os.path.exists(report_file):
        os.remove(report_file)
    warnings = create_report(mc, directory, since, to, report_file)
    assert os.path.exists(report_file)
    assert not warnings
    # assert headers and content in report file
    with open(report_file, "r") as rf:
        content = rf.read()
        headers = ",".join(
            [
                "file",
                "table",
                "author",
                "date",
                "time",
                "version",
                "operation",
                "length",
                "area",
                "count",
            ]
        )
        assert headers in content
        assert "base.gpkg,simple,test_plugin" in content
        assert "v3,update,,,2" in content
        # files not edited are not in reports
        assert "inserted_1_A.gpkg" not in content

    # create report between versions 2 and latest version (which is version 5)
    warnings = create_report(mc, directory, since, "", report_file)
    assert warnings

    # do report for v1 with added files and v5 with overwritten file
    warnings = create_report(mc, directory, "v1", "v5", report_file)
    assert warnings

    # rm local Mergin Maps project and try again
    shutil.rmtree(directory)
    with pytest.raises(InvalidProject):
        create_report(mc, directory, since, to, report_file)


def test_user_permissions(mc, mc2):
    """
    Test retrieving user permissions
    """
    test_project = "test_permissions"
    test_project_fullname = API_USER2 + "/" + test_project

    # cleanups
    project_dir = os.path.join(TMP_DIR, test_project, API_USER)
    cleanup(mc, test_project_fullname, [project_dir])
    cleanup(mc2, test_project_fullname, [project_dir])

    # create new (empty) project on server
    mc2.create_project(test_project)

    # Add reader access to another client
    project_info = get_project_info(mc2, API_USER2, test_project)
    access = project_info["access"]
    access["readersnames"].append(API_USER)
    mc2.set_project_access(test_project_fullname, access)

    # reader should not have write access
    assert not mc.has_writing_permissions(test_project_fullname)

    # Add writer access to another client
    project_info = get_project_info(mc2, API_USER2, test_project)
    access = project_info["access"]
    access["writersnames"].append(API_USER)
    mc2.set_project_access(test_project_fullname, access)

    # now user shold have write access
    assert mc.has_writing_permissions(test_project_fullname)

    # owner should have write access
    assert mc2.has_writing_permissions(test_project_fullname)


def test_report_failure(mc):
    """Check that report generated without errors when a table was added
    and then deleted.
    """
    test_project = "test_report_failure"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir
    test_gpkg = os.path.join(project_dir, "test.gpkg")
    report_file = os.path.join(TMP_DIR, "report.csv")

    cleanup(mc, project, [project_dir])

    os.makedirs(project_dir)
    shutil.copy(os.path.join(TEST_DATA_DIR, "base.gpkg"), test_gpkg)
    mc.create_project_and_push(project, project_dir)

    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), test_gpkg)
    mc.push_project(project_dir)

    # add a new table to the geopackage
    shutil.copy(os.path.join(TEST_DATA_DIR, "two_tables.gpkg"), test_gpkg)
    mc.push_project(project_dir)

    shutil.copy(os.path.join(TEST_DATA_DIR, "two_tables_1_A.gpkg"), test_gpkg)
    mc.push_project(project_dir)

    warnings = create_report(mc, project_dir, "v1", "v4", report_file)
    assert warnings

    shutil.copy(os.path.join(TEST_DATA_DIR, "two_tables_drop.gpkg"), test_gpkg)
    mc.push_project(project_dir)

    warnings = create_report(mc, project_dir, "v1", "v5", report_file)
    assert warnings


def test_changesets_download(mc):
    """Check that downloading diffs works correctly, including case when
    changesets are cached.
    """
    test_project = "test_changesets_download"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir
    test_gpkg = "test.gpkg"
    file_path = os.path.join(project_dir, "test.gpkg")
    download_dir = os.path.join(TMP_DIR, "changesets")

    cleanup(mc, project, [project_dir])

    os.makedirs(project_dir, exist_ok=True)
    shutil.copy(os.path.join(TEST_DATA_DIR, "base.gpkg"), file_path)
    mc.create_project_and_push(project, project_dir)

    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), file_path)
    mc.push_project(project_dir)

    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_A_mod.gpkg"), file_path)
    mc.push_project(project_dir)

    mp = MerginProject(project_dir)

    os.makedirs(download_dir, exist_ok=True)
    diff_file = os.path.join(download_dir, "base-v1-2.diff")
    mc.get_file_diff(project_dir, test_gpkg, diff_file, "v1", "v2")
    assert os.path.exists(diff_file)
    assert mp.geodiff.has_changes(diff_file)
    assert mp.geodiff.changes_count(diff_file) == 1

    diff_file = os.path.join(download_dir, "base-v2-3.diff")
    mc.get_file_diff(project_dir, test_gpkg, diff_file, "v2", "v3")
    assert os.path.exists(diff_file)
    assert mp.geodiff.has_changes(diff_file)
    assert mp.geodiff.changes_count(diff_file) == 2

    # even if changesets were cached and were not downloaded destination
    # file should be created and contain a valid changeset
    diff_file = os.path.join(download_dir, "base-all.diff")
    mc.get_file_diff(project_dir, test_gpkg, diff_file, "v1", "v3")
    assert os.path.exists(diff_file)
    assert mp.geodiff.has_changes(diff_file)
    assert mp.geodiff.changes_count(diff_file) == 3


def test_version_info(mc):
    """Check retrieving detailed information about single project version."""
    test_project = "test_version_info"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir
    test_gpkg = "test.gpkg"
    file_path = os.path.join(project_dir, "test.gpkg")

    cleanup(mc, project, [project_dir])

    os.makedirs(project_dir, exist_ok=True)
    shutil.copy(os.path.join(TEST_DATA_DIR, "base.gpkg"), file_path)
    mc.create_project_and_push(project, project_dir)

    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), file_path)
    mc.push_project(project_dir)

    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_A_mod.gpkg"), file_path)
    mc.push_project(project_dir)

    info = mc.project_version_info(project, 2)[0]
    assert info["namespace"] == API_USER
    assert info["project_name"] == test_project
    assert info["name"] == "v2"
    assert info["author"] == API_USER
    created = datetime.strptime(info["created"], "%Y-%m-%dT%H:%M:%SZ")
    assert created.date() == date.today()
    assert info["changes"]["updated"][0]["size"] == 98304


def test_clean_diff_files(mc):
    test_project = "test_clean"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_2")  # concurrent project dir

    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    # test push changes with diffs:
    mp = MerginProject(project_dir)
    f_updated = "base.gpkg"
    # step 1) base.gpkg updated to inserted_1_A (inserted A feature)
    shutil.move(mp.fpath(f_updated), mp.fpath_meta(f_updated))  # make local copy for changeset calculation
    shutil.copy(mp.fpath("inserted_1_A.gpkg"), mp.fpath(f_updated))
    mc.push_project(project_dir)

    diff_files = glob.glob("*-diff-*", root_dir=os.path.split(mp.fpath_meta("inserted_1_A.gpkg"))[0])

    assert diff_files == []


def test_reset_local_changes(mc: MerginClient):
    test_project = f"test_reset_local_changes"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_v2")  # primary project dir for updates

    cleanup(mc, project, [project_dir])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    # test push changes with diffs:
    mp = MerginProject(project_dir)

    # test with no changes, should pass by doing nothing
    mc.reset_local_changes(project_dir)

    f_updated = "base.gpkg"
    shutil.copy(mp.fpath("inserted_1_A.gpkg"), mp.fpath(f_updated))
    shutil.copy(mp.fpath("test.txt"), mp.fpath("new_test.txt"))
    shutil.copy(mp.fpath("test.txt"), mp.fpath("new_dir/new_test.txt"))
    os.remove(mp.fpath("test.txt"))
    os.remove(mp.fpath("test_dir/test2.txt"))
    with open(mp.fpath("test3.txt"), mode="a", encoding="utf-8") as file:
        file.write(" Add some text.")

    # push changes prior to reset
    mp = MerginProject(project_dir)
    push_changes = mp.get_push_changes()

    assert len(push_changes["added"]) == 2
    assert len(push_changes["removed"]) == 2
    assert len(push_changes["updated"]) == 2

    # reset all files back
    mc.reset_local_changes(project_dir)

    # push changes after the reset
    mp = MerginProject(project_dir)
    push_changes = mp.get_push_changes()

    assert len(push_changes["added"]) == 0
    assert len(push_changes["removed"]) == 0
    assert len(push_changes["updated"]) == 0

    cleanup(mc, project, [project_dir])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    # test push changes with diffs:
    mp = MerginProject(project_dir)

    shutil.copy(mp.fpath("inserted_1_A.gpkg"), mp.fpath(f_updated))
    shutil.copy(mp.fpath("test.txt"), mp.fpath("new_test.txt"))
    shutil.copy(mp.fpath("test.txt"), mp.fpath("new_dir/new_test.txt"))
    os.remove(mp.fpath("test.txt"))
    os.remove(mp.fpath("test_dir/test2.txt"))

    # push changes prior to reset
    mp = MerginProject(project_dir)
    push_changes = mp.get_push_changes()

    assert len(push_changes["added"]) == 2
    assert len(push_changes["removed"]) == 2
    assert len(push_changes["updated"]) == 1

    # reset local changes only to certain files, one added and one removed
    mc.reset_local_changes(project_dir, files_to_reset=["new_test.txt", "test_dir/test2.txt"])

    # push changes after the reset
    mp = MerginProject(project_dir)
    push_changes = mp.get_push_changes()

    assert len(push_changes["added"]) == 1
    assert len(push_changes["removed"]) == 1
    assert len(push_changes["updated"]) == 1

    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    # test push changes with diffs:
    mp = MerginProject(project_dir)

    # make changes creating two another versions
    shutil.copy(mp.fpath("inserted_1_A.gpkg"), mp.fpath(f_updated))
    mc.push_project(project_dir)
    shutil.copy(mp.fpath("test.txt"), mp.fpath("new_test.txt"))
    shutil.copy(mp.fpath("test.txt"), mp.fpath("new_dir/new_test.txt"))
    mc.push_project(project_dir)
    os.remove(mp.fpath("test.txt"))
    os.remove(mp.fpath("test_dir/test2.txt"))

    # download version 2 and create MerginProject for it
    mc.download_project(project, project_dir_2, version="v2")
    mp = MerginProject(project_dir_2)

    # make some changes
    shutil.copy(mp.fpath("test.txt"), mp.fpath("new_test.txt"))
    shutil.copy(mp.fpath("test.txt"), mp.fpath("new_dir/new_test.txt"))
    os.remove(mp.fpath("test.txt"))
    os.remove(mp.fpath("test_dir/test2.txt"))

    # check changes
    push_changes = mp.get_push_changes()
    assert len(push_changes["added"]) == 2
    assert len(push_changes["removed"]) == 2
    assert len(push_changes["updated"]) == 0

    # reset back to original version we had - v2
    mc.reset_local_changes(project_dir_2)

    # push changes after the reset - should be none
    push_changes = mp.get_push_changes()
    assert len(push_changes["added"]) == 0
    assert len(push_changes["removed"]) == 0
    assert len(push_changes["updated"]) == 0


def test_project_metadata(mc):
    test_project = "test_project_metadata"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)

    cleanup(mc, project, [project_dir])

    # prepare local project
    shutil.copytree(TEST_DATA_DIR, project_dir)

    # copy metadata in old format
    os.makedirs(os.path.join(project_dir, ".mergin"), exist_ok=True)
    project_metadata = os.path.join(project_dir, ".mergin", "mergin.json")
    metadata_file = os.path.join(project_dir, "old_metadata.json")
    shutil.copyfile(metadata_file, project_metadata)

    # verify we have correct metadata
    mp = MerginProject(project_dir)
    assert mp.project_full_name() == f"{API_USER}/{test_project}"
    assert mp.project_name() == test_project
    assert mp.workspace_name() == API_USER
    assert mp.version() == "v0"

    # copy metadata in new format
    metadata_file = os.path.join(project_dir, "new_metadata.json")
    shutil.copyfile(metadata_file, project_metadata)

    # verify we have correct metadata
    mp = MerginProject(project_dir)
    assert mp.project_full_name() == f"{API_USER}/{test_project}"
    assert mp.project_name() == test_project
    assert mp.workspace_name() == API_USER
    assert mp.version() == "v0"


def test_project_rename(mc: MerginClient):
    """Check project can be renamed"""

    test_project = "test_project_rename"
    test_project_renamed = "test_project_renamed"
    project = API_USER + "/" + test_project
    project_renamed = API_USER + "/" + test_project_renamed

    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir

    cleanup(mc, project, [project_dir])
    cleanup(mc, project_renamed, [])

    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    # renamed project does not exist
    with pytest.raises(ClientError, match="The requested URL was not found on the server"):
        info = mc.project_info(project_renamed)

    # rename
    mc.rename_project(project, test_project_renamed)

    # validate project info
    project_info = mc.project_info(project_renamed)
    assert project_info["version"] == "v1"
    assert project_info["name"] == test_project_renamed
    assert project_info["namespace"] == API_USER
    with pytest.raises(ClientError, match="The requested URL was not found on the server"):
        mc.project_info(project)

    # recreate project
    cleanup(mc, project, [project_dir])
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project, project_dir)

    # rename to existing name - created previously
    mc.project_info(project_renamed)
    with pytest.raises(ClientError, match="Name already exist within workspace"):
        mc.rename_project(project, test_project_renamed)

    # cannot rename project that does not exist
    with pytest.raises(ClientError, match="The requested URL was not found on the server."):
        mc.rename_project(API_USER + "/" + "non_existing_project", "new_project")

    # cannot rename with full project name
    with pytest.raises(
        ClientError,
        match="Project's new name should be without workspace specification",
    ):
        mc.rename_project(project, "workspace" + "/" + test_project_renamed)


def test_download_files(mc: MerginClient):
    """Test downloading files at specified versions."""
    test_project = "test_download_files"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    f_updated = "base.gpkg"
    download_dir = os.path.join(TMP_DIR, "test-download-files-tmp")

    cleanup(mc, project, [project_dir, download_dir])

    mp = create_versioned_project(mc, test_project, project_dir, f_updated)

    project_info = mc.project_info(project)
    assert project_info["version"] == "v5"
    assert project_info["id"] == mp.project_id()

    # Versioned file should have the following content at versions 2-4
    expected_content = (
        "inserted_1_A.gpkg",
        "inserted_1_A_mod.gpkg",
        "inserted_1_B.gpkg",
    )

    downloaded_file = os.path.join(download_dir, f_updated)

    # if output_paths is specified look at that location
    for ver in range(2, 5):
        mc.download_files(project_dir, [f_updated], [downloaded_file], version=f"v{ver}")
        expected = os.path.join(TEST_DATA_DIR, expected_content[ver - 2])  # GeoPackage with expected content
        assert check_gpkg_same_content(mp, downloaded_file, expected)

    # if output_paths is not specified look in the mergin project folder
    for ver in range(2, 5):
        mc.download_files(project_dir, [f_updated], version=f"v{ver}")
        expected = os.path.join(TEST_DATA_DIR, expected_content[ver - 2])  # GeoPackage with expected content
        assert check_gpkg_same_content(mp, mp.fpath(f_updated), expected)

    # download two files from v1 and check their content
    file_2 = "test.txt"
    downloaded_file_2 = os.path.join(download_dir, file_2)

    mc.download_files(
        project_dir,
        [f_updated, file_2],
        [downloaded_file, downloaded_file_2],
        version="v1",
    )
    assert check_gpkg_same_content(mp, downloaded_file, os.path.join(TEST_DATA_DIR, f_updated))

    with open(os.path.join(TEST_DATA_DIR, file_2), mode="r", encoding="utf-8") as file:
        content_exp = file.read()

    with open(os.path.join(download_dir, file_2), mode="r", encoding="utf-8") as file:
        content = file.read()
    assert content_exp == content

    # make sure there will be exception raised if a file doesn't exist in the version
    with pytest.raises(ClientError, match=f"No \\[{f_updated}\\] exists at version v5"):
        mc.download_files(project_dir, [f_updated], version="v5")

    with pytest.raises(ClientError, match=f"No \\[non_existing\\.file\\] exists at version v3"):
        mc.download_files(project_dir, [f_updated, "non_existing.file"], version="v3")


def test_download_failure(mc):
    test_project = "test_download_failure"
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    download_dir = os.path.join(TMP_DIR, "download", test_project)

    cleanup(mc, project, [project_dir, download_dir])
    # prepare local project
    shutil.copytree(TEST_DATA_DIR, project_dir)

    # create remote project
    mc.create_project_and_push(test_project, directory=project_dir)

    # download project async
    with pytest.raises(IsADirectoryError):
        job = download_project_async(mc, project, download_dir)
        os.makedirs(os.path.join(download_dir, "base.gpkg.0"))
        download_project_wait(job)
        download_project_finalize(job)

    assert job.failure_log_file is not None
    with open(job.failure_log_file, "r", encoding="utf-8") as f:
        content = f.read()
        assert "Traceback" in content

    # active waiting
    remove_folders([download_dir])
    job = download_project_async(mc, project, download_dir)
    os.makedirs(os.path.join(download_dir, "base.gpkg.0"))
    with pytest.raises(IsADirectoryError):
        while True:
            assert download_project_is_running(job)

    download_project_cancel(job)  # to clean up things

    assert job.failure_log_file is not None
    with open(job.failure_log_file, "r", encoding="utf-8") as f:
        content = f.read()
        assert "Traceback" in content


# TODO: consider to use separate test_editor.py file for tests that require editor
def test_editor(mc: MerginClient):
    """Test editor handler class and push with editor"""

    project_info = {"role": "editor"}
    if not mc.has_editor_support():
        assert is_editor_enabled(mc, project_info) is False
        return

    # mock that user is editor
    project_info["role"] = EDITOR_ROLE_NAME
    assert is_editor_enabled(mc, project_info) is True

    # unit test for editor methods
    qgs_changeset = {
        "added": [{"path": "/folder/project.new.Qgz"}],
        "updated": [{"path": "/folder/project.updated.Qgs"}],
        "removed": [{"path": "/folder/project.removed.qgs"}],
    }
    qgs_changeset = filter_changes(mc, project_info, qgs_changeset)
    assert sum(len(v) for v in qgs_changeset.values()) == 0

    mergin_config_changeset = {
        "added": [{"path": "/.mergin/mergin-config.json"}],
        "updated": [{"path": "/.mergin/mergin-config.json"}],
        "removed": [{"path": "/.mergin/mergin-config.json"}],
    }
    mergin_config_changeset = filter_changes(mc, project_info, mergin_config_changeset)
    assert sum(len(v) for v in mergin_config_changeset.values()) == 0

    gpkg_changeset = {
        "added": [{"path": "/.mergin/data.gpkg"}],
        "updated": [
            {"path": "/.mergin/conflict-data.gpkg"},
            {"path": "/.mergin/data.gpkg", "diff": {}},
        ],
        "removed": [{"path": "/.mergin/data.gpkg"}],
    }
    gpkg_changeset = filter_changes(mc, project_info, gpkg_changeset)
    assert sum(len(v) for v in gpkg_changeset.values()) == 2
    assert gpkg_changeset["added"][0]["path"] == "/.mergin/data.gpkg"
    assert gpkg_changeset["updated"][0]["path"] == "/.mergin/data.gpkg"


def test_editor_push(mc: MerginClient, mc2: MerginClient):
    """Test push with editor"""
    if not mc.has_editor_support():
        return
    test_project = "test_editor_push"
    test_project_fullname = API_USER + "/" + test_project
    project = API_USER + "/" + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    project_dir2 = os.path.join(TMP_DIR, test_project + "_2")
    cleanup(mc, project, [project_dir, project_dir2])

    # create new (empty) project on server
    # TODO: return project_info from create project, don't use project_full name for project info, instead returned id of project
    mc.create_project(test_project)
    mc.add_user_permissions_to_project(project, [API_USER2], "editor")
    # download empty project
    mc2.download_project(test_project_fullname, project_dir)

    # editor is starting to adding qgis files and "normal" file
    qgs_file_name = "test.qgs"
    txt_file_name = "test.txt"
    gpkg_file_name = "base.gpkg"
    files_to_push = [qgs_file_name, txt_file_name, gpkg_file_name]
    for file in files_to_push:
        shutil.copy(os.path.join(TEST_DATA_DIR, file), project_dir)
    # it's possible to push allowed files if editor
    mc2.push_project(project_dir)
    project_info = mc2.project_info(test_project_fullname)
    assert len(project_info.get("files")) == len(files_to_push) - 1  # ggs is not pushed
    # find pushed files in server
    assert any(file["path"] == qgs_file_name for file in project_info.get("files")) is False
    assert any(file["path"] == txt_file_name for file in project_info.get("files")) is True
    assert any(file["path"] == gpkg_file_name for file in project_info.get("files")) is True
    pull_changes, push_changes, push_changes_summary = mc.project_status(project_dir)
    assert not sum(len(v) for v in pull_changes.values())
    assert sum(len(v) for v in push_changes.values()) == 1
    # ggs is still waiting to push
    assert any(file["path"] == qgs_file_name for file in push_changes.get("added")) is True

    # editor is trying to psuh row to gpkg file -> it's possible
    shutil.copy(
        os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"),
        os.path.join(project_dir, gpkg_file_name),
    )
    mc2.push_project(project_dir)
    project_info = mc2.project_info(test_project_fullname)
    pull_changes, push_changes, push_changes_summary = mc.project_status(project_dir)
    assert any(file["path"] == gpkg_file_name for file in project_info.get("files")) is True
    assert any(file["path"] == gpkg_file_name for file in push_changes.get("updated")) is False

    # editor is trying to insert tables to gpkg file
    shutil.copy(
        os.path.join(TEST_DATA_DIR, "two_tables.gpkg"),
        os.path.join(project_dir, gpkg_file_name),
    )
    mc2.push_project(project_dir)
    pull_changes, push_changes, push_changes_summary = mc.project_status(project_dir)
    assert not sum(len(v) for v in pull_changes.values())
    # gpkg was filter by editor_handler in push_project, because new tables added
    assert sum(len(v) for v in push_changes.values()) == 2
    # ggs and gpkg are still waiting to push
    assert any(file["path"] == qgs_file_name for file in push_changes.get("added")) is True
    assert any(file["path"] == gpkg_file_name for file in push_changes.get("updated")) is True
    # push as owner do cleanup local changes and preparation to conflicited copy simulate
    mc.push_project(project_dir)

    # simulate conflicting copy of qgis file
    # Push from dir2 changes to qgis file
    mc.download_project(test_project_fullname, project_dir2)
    with open(os.path.join(project_dir2, qgs_file_name), "a") as f:
        f.write("C server version")
    mc.push_project(project_dir2)
    # editor is editing qgs file too in dir
    with open(os.path.join(project_dir, qgs_file_name), "a") as f:
        f.write("B local version")
    mc2.pull_project(project_dir)
    conflicted_file = None
    for project_file in os.listdir(project_dir):
        _, ext = os.path.splitext(project_file)
        if ext == ".qgs~":
            conflicted_file = project_file
    # There is no conflicted qgs file
    assert conflicted_file is None
