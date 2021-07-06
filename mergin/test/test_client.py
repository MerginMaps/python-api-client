import json
import logging
import os
import tempfile
import subprocess
import shutil
from datetime import datetime, timedelta
import pytest
import pytz
import sqlite3

from ..client import MerginClient, ClientError, MerginProject, LoginError, decode_token_data, TokenError
from ..client_push import push_project_async, push_project_cancel
from ..utils import generate_checksum, get_versions_with_file_changes
from ..merginproject import pygeodiff


SERVER_URL = os.environ.get('TEST_MERGIN_URL')
API_USER = os.environ.get('TEST_API_USERNAME')
USER_PWD = os.environ.get('TEST_API_PASSWORD')
API_USER2 = os.environ.get('TEST_API_USERNAME2')
USER_PWD2 = os.environ.get('TEST_API_PASSWORD2')
TMP_DIR = tempfile.gettempdir()
TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_data')
CHANGED_SCHEMA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'modified_schema')


@pytest.fixture(scope='function')
def mc():
    return create_client(API_USER, USER_PWD)


@pytest.fixture(scope='function')
def mc2():
    return create_client(API_USER2, USER_PWD2)


def create_client(user, pwd):
    assert SERVER_URL and SERVER_URL.rstrip('/') != 'https://public.cloudmergin.com' and user and pwd
    return MerginClient(SERVER_URL, login=user, password=pwd)


def cleanup(mc, project, dirs):
    # cleanup leftovers from previous test if needed such as remote project and local directories
    try:
        mc.delete_project(project)
    except ClientError:
        pass
    remove_folders(dirs)


def remove_folders(dirs):
    # clean given directories
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)


def test_login(mc):
    token = mc._auth_session['token']
    assert MerginClient(mc.url, auth_token=token)

    invalid_token = "Completely invalid token...."
    with pytest.raises(TokenError, match=f"Token doesn't start with 'Bearer .': {invalid_token}"):
        decode_token_data(invalid_token)

    invalid_token = "Bearer .jas646kgfa"
    with pytest.raises(TokenError, match=f"Invalid token data: {invalid_token}"):
        decode_token_data(invalid_token)

    with pytest.raises(LoginError, match='Invalid username or password'):
        mc.login('foo', 'bar')


def test_create_delete_project(mc):
    test_project = 'test_create_delete'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    download_dir = os.path.join(TMP_DIR, 'download', test_project)

    cleanup(mc, project, [project_dir, download_dir])
    # create new (empty) project on server
    mc.create_project(test_project)
    projects = mc.projects_list(flag='created')
    assert any(p for p in projects if p['name'] == test_project and p['namespace'] == API_USER)

    # try again
    with pytest.raises(ClientError, match=f'Project {test_project} already exists'):
        mc.create_project(test_project)

    # remove project
    mc.delete_project(API_USER + '/' + test_project)
    projects = mc.projects_list(flag='created')
    assert not any(p for p in projects if p['name'] == test_project and p['namespace'] == API_USER)

    # try again, nothing to delete
    with pytest.raises(ClientError):
        mc.delete_project(API_USER + '/' + test_project)


def test_create_remote_project_from_local(mc):
    test_project = 'test_project'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    download_dir = os.path.join(TMP_DIR, 'download', test_project)

    cleanup(mc, project, [project_dir, download_dir])
    # prepare local project
    shutil.copytree(TEST_DATA_DIR, project_dir)

    # create remote project
    mc.create_project_and_push(test_project, directory=project_dir)

    # check basic metadata about created project
    project_info = mc.project_info(project)
    assert project_info['version'] == 'v1'
    assert project_info['name'] == test_project
    assert project_info['namespace'] == API_USER

    versions = mc.project_versions(project)
    assert len(versions) == 1
    assert versions[0]['name'] == 'v1'
    assert any(f for f in versions[0]['changes']['added'] if f['path'] == 'test.qgs')

    # check we can fully download remote project
    mc.download_project(project, download_dir)
    mp = MerginProject(download_dir)
    downloads = {
        'dir': os.listdir(mp.dir),
        'meta': os.listdir(mp.meta_dir)
    }
    for f in os.listdir(project_dir) + ['.mergin']:
        assert f in downloads['dir']
        if mp.is_versioned_file(f):
            assert f in downloads['meta']
    
    # unable to download to the same directory
    with pytest.raises(Exception, match='Project directory already exists'):
        mc.download_project(project, download_dir)


def test_push_pull_changes(mc):
    test_project = 'test_push'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project+'_2')  # concurrent project dir

    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    # make sure we have v1 also in concurrent project dir
    mc.download_project(project, project_dir_2)

    # test push changes (add, remove, rename, update)
    f_added = 'new.txt'
    with open(os.path.join(project_dir, f_added), 'w') as f:
        f.write('new file')
    f_removed = 'test.txt'
    os.remove(os.path.join(project_dir, f_removed))
    f_renamed = 'test_dir/test2.txt'
    shutil.move(os.path.normpath(os.path.join(project_dir, f_renamed)), os.path.join(project_dir, 'renamed.txt'))
    f_updated = 'test3.txt'
    with open(os.path.join(project_dir, f_updated), 'w') as f:
        f.write('Modified')

    # check changes before applied
    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert not sum(len(v) for v in pull_changes.values())
    assert next((f for f in push_changes['added'] if f['path'] == f_added), None)
    assert next((f for f in push_changes['removed'] if f['path'] == f_removed), None)
    assert next((f for f in push_changes['updated'] if f['path'] == f_updated), None)
    # renamed file will result in removed + added file
    assert next((f for f in push_changes['removed'] if f['path'] == f_renamed), None)
    assert next((f for f in push_changes['added'] if f['path'] == 'renamed.txt'), None)
    assert not pull_changes['renamed']  # not supported

    mc.push_project(project_dir)
    project_info = mc.project_info(project)
    assert project_info['version'] == 'v2'
    assert not next((f for f in project_info['files'] if f['path'] == f_removed), None)
    assert not next((f for f in project_info['files'] if f['path'] == f_renamed), None)
    assert next((f for f in project_info['files'] if f['path'] == 'renamed.txt'), None)
    assert next((f for f in project_info['files'] if f['path'] == f_added), None)
    f_remote_checksum = next((f['checksum'] for f in project_info['files'] if f['path'] == f_updated), None)
    assert generate_checksum(os.path.join(project_dir, f_updated)) == f_remote_checksum
    mp = MerginProject(project_dir)
    assert len(project_info['files']) == len(mp.inspect_files())
    project_versions = mc.project_versions(project)
    assert len(project_versions) == 2
    f_change = next((f for f in project_versions[0]['changes']['updated'] if f['path'] == f_updated), None)
    assert 'origin_checksum' not in f_change  # internal client info

    # test parallel changes
    with open(os.path.join(project_dir_2, f_updated), 'w') as f:
        f.write('Make some conflict')
    f_conflict_checksum = generate_checksum(os.path.join(project_dir_2, f_updated))

    # not at latest server version
    with pytest.raises(ClientError, match='Please update your local copy'):
        mc.push_project(project_dir_2)

    # check changes in project_dir_2 before applied
    pull_changes, push_changes, _ = mc.project_status(project_dir_2)
    assert next((f for f in pull_changes['added'] if f['path'] == f_added), None)
    assert next((f for f in pull_changes['removed'] if f['path'] == f_removed), None)
    assert next((f for f in pull_changes['updated'] if f['path'] == f_updated), None)
    assert next((f for f in pull_changes['removed'] if f['path'] == f_renamed), None)
    assert next((f for f in pull_changes['added'] if f['path'] == 'renamed.txt'), None)

    mc.pull_project(project_dir_2)
    assert os.path.exists(os.path.join(project_dir_2, f_added))
    assert not os.path.exists(os.path.join(project_dir_2, f_removed))
    assert not os.path.exists(os.path.join(project_dir_2, f_renamed))
    assert os.path.exists(os.path.join(project_dir_2, 'renamed.txt'))
    assert os.path.exists(os.path.join(project_dir_2, f_updated+'_conflict_copy'))
    assert generate_checksum(os.path.join(project_dir_2, f_updated+'_conflict_copy')) == f_conflict_checksum
    assert generate_checksum(os.path.join(project_dir_2, f_updated)) == f_remote_checksum


def test_cancel_push(mc):
    """
    Start pushing and cancel the process, then try to push again and check if previous sync process was cleanly
    finished.
    """
    test_project = "test_cancel_push"
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project + "_3")  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_4")
    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    # modify the project (add, update)
    f_added = 'new.txt'
    with open(os.path.join(project_dir, f_added), 'w') as f:
        f.write("new file")
    f_updated = "test3.txt"
    modification = "Modified"
    with open(os.path.join(project_dir, f_updated), 'w') as f:
        f.write(modification)

    # check changes before applied
    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert not sum(len(v) for v in pull_changes.values())
    assert next((f for f in push_changes['added'] if f['path'] == f_added), None)
    assert next((f for f in push_changes['updated'] if f['path'] == f_updated), None)

    # start pushing and then cancel the job
    job = push_project_async(mc, project_dir)
    push_project_cancel(job)

    # if cancelled properly, we should be now able to do the push without any problem
    mc.push_project(project_dir)

    # download the project to a different directory and check the version and content
    mc.download_project(project, project_dir_2)
    mp = MerginProject(project_dir_2)
    assert mp.metadata["version"] == 'v2'
    assert os.path.exists(os.path.join(project_dir_2, f_added))
    with open(os.path.join(project_dir_2, f_updated), 'r') as f:
        assert f.read() == modification


def test_ignore_files(mc):
    test_project = 'test_blacklist'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates

    cleanup(mc, project, [project_dir])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    shutil.copy(os.path.join(project_dir, 'test.qgs'), os.path.join(project_dir, 'test.qgs~'))
    mc.create_project_and_push(test_project, project_dir)
    project_info = mc.project_info(project)
    assert not next((f for f in project_info['files'] if f['path'] == 'test.qgs~'), None)

    with open(os.path.join(project_dir, '.directory'), 'w') as f:
        f.write('test')
    mc.push_project(project_dir)
    assert not next((f for f in project_info['files'] if f['path'] == '.directory'), None)


def test_sync_diff(mc):

    test_project = f'test_sync_diff'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + '_2')  # concurrent project dir with no changes
    project_dir_3 = os.path.join(TMP_DIR, test_project + '_3')  # concurrent project dir with local changes

    cleanup(mc, project, [project_dir, project_dir_2, project_dir_3])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    # make sure we have v1 also in concurrent project dirs
    mc.download_project(project, project_dir_2)
    mc.download_project(project, project_dir_3)

    # test push changes with diffs:
    mp = MerginProject(project_dir)
    f_updated = 'base.gpkg'
    # step 1) base.gpkg updated to inserted_1_A (inserted A feature)
    shutil.move(mp.fpath(f_updated), mp.fpath_meta(f_updated))  # make local copy for changeset calculation
    shutil.copy(mp.fpath('inserted_1_A.gpkg'), mp.fpath(f_updated))
    mc.push_project(project_dir)
    mp.geodiff.create_changeset(mp.fpath(f_updated), mp.fpath_meta(f_updated), mp.fpath_meta('push_diff'))
    assert not mp.geodiff.has_changes(mp.fpath_meta('push_diff'))
    # step 2) base.gpkg updated to inserted_1_A_mod (modified 2 features)
    shutil.move(mp.fpath(f_updated), mp.fpath_meta(f_updated))
    shutil.copy(mp.fpath('inserted_1_A_mod.gpkg'), mp.fpath(f_updated))
    # introduce some other changes
    f_removed = 'inserted_1_B.gpkg'
    os.remove(mp.fpath(f_removed))
    f_renamed = 'test_dir/modified_1_geom.gpkg'
    shutil.move(mp.fpath(f_renamed), mp.fpath('renamed.gpkg'))
    mc.push_project(project_dir)

    # check project after push
    project_info = mc.project_info(project)
    assert project_info['version'] == 'v3'
    f_remote = next((f for f in project_info['files'] if f['path'] == f_updated), None)
    assert next((f for f in project_info['files'] if f['path'] == 'renamed.gpkg'), None)
    assert not next((f for f in project_info['files'] if f['path'] == f_removed), None)
    assert not os.path.exists(mp.fpath_meta(f_removed))
    assert 'diff' in f_remote
    assert os.path.exists(mp.fpath_meta('renamed.gpkg'))

    # pull project in different directory
    mp2 = MerginProject(project_dir_2)
    mc.pull_project(project_dir_2)
    mp2.geodiff.create_changeset(mp.fpath(f_updated), mp2.fpath(f_updated), mp2.fpath_meta('diff'))
    assert not mp2.geodiff.has_changes(mp2.fpath_meta('diff'))

    # introduce conflict local change (inserted B feature to base)
    mp3 = MerginProject(project_dir_3)
    shutil.copy(mp3.fpath('inserted_1_B.gpkg'), mp3.fpath(f_updated))
    checksum = generate_checksum(mp3.fpath('inserted_1_B.gpkg'))
    mc.pull_project(project_dir_3)
    assert not os.path.exists(mp3.fpath('base.gpkg_conflict_copy'))

    # push new changes from project_3 and pull in original project
    mc.push_project(project_dir_3)
    mc.pull_project(project_dir)
    mp3.geodiff.create_changeset(mp.fpath(f_updated), mp3.fpath(f_updated), mp.fpath_meta('diff'))
    assert not mp3.geodiff.has_changes(mp.fpath_meta('diff'))


def test_list_of_push_changes(mc):
    PUSH_CHANGES_SUMMARY = "{'base.gpkg': {'geodiff_summary': [{'table': 'simple', 'insert': 1, 'update': 0, 'delete': 0}]}}"

    test_project = 'test_list_of_push_changes'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates

    cleanup(mc, project, [project_dir])
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    f_updated = 'base.gpkg'
    mp = MerginProject(project_dir)

    shutil.copy(mp.fpath('inserted_1_A.gpkg'), mp.fpath(f_updated))
    mc._auth_session["expire"] = datetime.now().replace(tzinfo=pytz.utc) - timedelta(days=1)
    pull_changes, push_changes, push_changes_summary = mc.project_status(project_dir)
    assert str(push_changes_summary) == PUSH_CHANGES_SUMMARY


def test_token_renewal(mc):
    """Test token regeneration in case it has expired."""
    test_project = 'test_token_renewal'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates

    cleanup(mc, project, [project_dir])
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    mc._auth_session["expire"] = datetime.now().replace(tzinfo=pytz.utc) - timedelta(days=1)
    pull_changes, push_changes, _ = mc.project_status(project_dir)
    to_expire = mc._auth_session["expire"] - datetime.now().replace(tzinfo=pytz.utc)
    assert to_expire.total_seconds() > (9 * 3600)


def test_force_gpkg_update(mc):
    test_project = 'test_force_update'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates

    cleanup(mc, project, [project_dir])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    # test push changes with force gpkg file upload:
    mp = MerginProject(project_dir)
    f_updated = 'base.gpkg'
    checksum = generate_checksum(mp.fpath(f_updated))

    # base.gpkg updated to modified_schema (inserted new column)
    shutil.move(mp.fpath(f_updated), mp.fpath_meta(f_updated))  # make local copy for changeset calculation (which will fail)
    shutil.copy(os.path.join(CHANGED_SCHEMA_DIR, 'modified_schema.gpkg'), mp.fpath(f_updated))
    shutil.copy(os.path.join(CHANGED_SCHEMA_DIR, 'modified_schema.gpkg-wal'), mp.fpath(f_updated + '-wal'))
    mc.push_project(project_dir)
    # by this point local file has been updated (changes committed from wal)
    updated_checksum = generate_checksum(mp.fpath(f_updated))
    assert checksum != updated_checksum

    # check project after push
    project_info = mc.project_info(project)
    assert project_info['version'] == 'v2'
    f_remote = next((f for f in project_info['files'] if f['path'] == f_updated), None)
    assert 'diff' not in f_remote


def test_new_project_sync(mc):
    """ Create a new project, download it, add a file and then do sync - it should not fail """

    test_project = 'test_new_project_sync'
    project = API_USER + '/' + test_project
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
    """ Test pull of a project where basefile of a .gpkg is missing for some reason
    (it should gracefully handle it by downloading the missing basefile)
    """

    test_project = 'test_missing_basefile_pull'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + '_2')  # concurrent project dir
    test_data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), test_project)

    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(test_data_dir, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    # update our gpkg in a different directory
    mc.download_project(project, project_dir_2)
    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_A.gpkg"), os.path.join(project_dir_2, "base.gpkg"))
    mc.pull_project(project_dir_2)
    mc.push_project(project_dir_2)

    # make some other local change
    shutil.copy(os.path.join(TEST_DATA_DIR, "inserted_1_B.gpkg"), os.path.join(project_dir, "base.gpkg"))

    # remove the basefile to simulate the issue
    os.remove(os.path.join(project_dir, '.mergin', 'base.gpkg'))

    # try to sync again  -- it should not crash
    mc.pull_project(project_dir)
    mc.push_project(project_dir)


def test_empty_file_in_subdir(mc):
    """ Test pull of a project where there is an empty file in a sub-directory """

    test_project = 'test_empty_file_in_subdir'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + '_2')  # concurrent project dir
    test_data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), test_project)

    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(test_data_dir, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    # try to check out the project
    mc.download_project(project, project_dir_2)
    assert os.path.exists(os.path.join(project_dir_2, 'subdir', 'empty.txt'))

    # add another empty file in a different subdir
    os.mkdir(os.path.join(project_dir, 'subdir2'))
    shutil.copy(os.path.join(project_dir, 'subdir', 'empty.txt'),
                os.path.join(project_dir, 'subdir2', 'empty2.txt'))
    mc.push_project(project_dir)

    # check that pull works fine
    mc.pull_project(project_dir_2)
    assert os.path.exists(os.path.join(project_dir_2, 'subdir2', 'empty2.txt'))


def test_clone_project(mc):
    test_project = 'test_clone_project'
    test_project_fullname = API_USER + '/' + test_project

    # cleanups
    project_dir = os.path.join(TMP_DIR, test_project)
    cleanup(mc, test_project_fullname, [project_dir])

    # create new (empty) project on server
    mc.create_project(test_project)
    projects = mc.projects_list(flag='created')
    assert any(p for p in projects if p['name'] == test_project and p['namespace'] == API_USER)

    cloned_project_name = test_project + "_cloned"
    # cleanup cloned project
    cloned_project_dir = os.path.join(TMP_DIR, cloned_project_name)
    cleanup(mc, API_USER + '/' + cloned_project_name, [cloned_project_dir])

    # clone project
    mc.clone_project(test_project_fullname, cloned_project_name, API_USER)
    projects = mc.projects_list(flag='created')
    assert any(p for p in projects if p['name'] == cloned_project_name and p['namespace'] == API_USER)


def test_set_read_write_access(mc):
    test_project = 'test_set_read_write_access'
    test_project_fullname = API_USER + '/' + test_project

    # cleanups
    project_dir = os.path.join(TMP_DIR, test_project, API_USER)
    cleanup(mc, test_project_fullname, [project_dir])

    # create new (empty) project on server
    mc.create_project(test_project)

    # Add writer access to another client
    project_info = get_project_info(mc, API_USER, test_project)
    access = project_info['access']
    access['writersnames'].append(API_USER2)
    access['readersnames'].append(API_USER2)
    mc.set_project_access(test_project_fullname, access)

    # check access
    project_info = get_project_info(mc, API_USER, test_project)
    access = project_info['access']
    assert API_USER2 in access['writersnames']
    assert API_USER2 in access['readersnames']


def test_available_storage_validation(mc):
    """
    Testing of storage limit - applies to user pushing changes into own project (namespace matching username).
    This test also tests giving read and write access to another user. Additionally tests also uploading of big file.
    """
    test_project = 'test_available_storage_validation'
    test_project_fullname = API_USER + '/' + test_project

    # cleanups
    project_dir = os.path.join(TMP_DIR, test_project, API_USER)
    cleanup(mc, test_project_fullname, [project_dir])

    # create new (empty) project on server
    mc.create_project(test_project)

    # download project
    mc.download_project(test_project_fullname, project_dir)

    # get user_info about storage capacity
    user_info = mc.user_info()
    storage_remaining = user_info['storage'] - user_info['disk_usage']

    # generate dummy data (remaining storage + extra 1024b)
    dummy_data_path = project_dir + "/data"
    file_size = storage_remaining + 1024
    _generate_big_file(dummy_data_path, file_size)

    # try to upload
    got_right_err = False
    try:
        mc.push_project(project_dir)
    except ClientError as e:
        # Expecting "Storage limit has been reached" error msg.
        assert str(e).startswith("Storage limit has been reached")
        got_right_err = True    
    assert got_right_err

    # Expecting empty project
    project_info = get_project_info(mc, API_USER, test_project)
    assert project_info['version'] == 'v0'
    assert project_info['disk_usage'] == 0


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
    test_project = 'test_available_storage_validation2'
    test_project_fullname = API_USER2 + '/' + test_project

    # cleanups
    project_dir = os.path.join(TMP_DIR, test_project, API_USER)
    cleanup(mc, test_project_fullname, [project_dir])
    cleanup(mc2, test_project_fullname, [project_dir])

    # create new (empty) project on server
    mc2.create_project(test_project)

    # Add writer access to another client
    project_info = get_project_info(mc2, API_USER2, test_project)
    access = project_info['access']
    access['writersnames'].append(API_USER)
    access['readersnames'].append(API_USER)
    mc2.set_project_access(test_project_fullname, access)

    # download project
    mc.download_project(test_project_fullname, project_dir)

    # get user_info about storage capacity
    user_info = mc.user_info()
    storage_remaining = user_info['storage'] - user_info['disk_usage']

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
    projects = mc.projects_list(flag='created')
    test_project_list = [p for p in projects if p['name'] == project_name and p['namespace'] == namespace]
    assert len(test_project_list) == 1
    return test_project_list[0]


def _generate_big_file(filepath, size):
    """
    generate big binary file with the specified size in bytes
    :param filepath: full filepath
    :param size: the size in bytes
    """
    with open(filepath, 'wb') as fout:
        fout.write(b"\0" * size)


def test_get_projects_by_name(mc):
    """ Test server 'bulk' endpoint for projects' info"""
    test_projects = {
        "projectA": f"{API_USER}/projectA",
        "projectB": f"{API_USER}/projectB"
    }

    for name, full_name in test_projects.items():
        cleanup(mc, full_name, [])
        mc.create_project(name)

    resp = mc.get_projects_by_names(list(test_projects.values()))
    assert len(resp) == len(test_projects)
    for name, full_name in test_projects.items():
        assert full_name in resp
        assert resp[full_name]["name"] == name
        assert resp[full_name]["version"] == 'v0'


def test_download_versions(mc):
    test_project = 'test_download'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    # download dirs
    project_dir_v1 = os.path.join(TMP_DIR, test_project + '_v1')
    project_dir_v2 = os.path.join(TMP_DIR, test_project + '_v2')
    project_dir_v3 = os.path.join(TMP_DIR, test_project + '_v3')

    cleanup(mc, project, [project_dir, project_dir_v1, project_dir_v2, project_dir_v3])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    # create new version - v2
    f_added = 'new.txt'
    with open(os.path.join(project_dir, f_added), 'w') as f:
        f.write('new file')

    mc.push_project(project_dir)
    project_info = mc.project_info(project)
    assert project_info['version'] == 'v2'

    mc.download_project(project, project_dir_v1, 'v1')
    assert os.path.exists(os.path.join(project_dir_v1, 'base.gpkg'))
    assert not os.path.exists(os.path.join(project_dir_v2, f_added))  # added only in v2

    mc.download_project(project, project_dir_v2, 'v2')
    assert os.path.exists(os.path.join(project_dir_v2, f_added))
    assert os.path.exists(os.path.join(project_dir_v1, 'base.gpkg'))  # added in v1 but still present in v2

    # try to download not-existing version
    with pytest.raises(ClientError):
        mc.download_project(project, project_dir_v3, 'v3')


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
        flag='created', name="test_paginated", page=1, per_page=10, order_params="name_asc"
    )
    projects = resp["projects"]
    count = resp["count"]
    assert count == len(test_projects)
    assert len(projects) == len(test_projects)
    for i, project in enumerate(projects):
        assert project["name"] == sorted_test_names[i]

    resp = mc.paginated_projects_list(
        flag='created', name="test_paginated", page=2, per_page=2, order_params="name_asc"
    )
    projects = resp["projects"]
    assert len(projects) == 2
    for i, project in enumerate(projects):
        assert project["name"] == sorted_test_names[i+2]


def test_missing_local_file_pull(mc):
    """Test pull of a project where a file deleted in the service is missing for some reason."""

    test_project = "test_dir"
    file_to_remove = "test2.txt"
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project + "_5")  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + "_6")  # concurrent project dir
    test_data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data", test_project)

    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(test_data_dir, project_dir)
    mc.create_project_and_push(test_project, project_dir)

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
    os.environ['MERGIN_CLIENT_LOG'] = os.path.join(TMP_DIR, 'global-mergin-log.txt')
    assert os.environ.get('MERGIN_CLIENT_LOG', None) is not None
    token = mc._auth_session['token']
    mc1 = MerginClient(mc.url, auth_token=token)
    assert isinstance(mc1.log.handlers[0], logging.FileHandler)
    mc1.log.info("Test log info to the log file...")
    # cleanup
    mc.log.handlers = []
    del os.environ['MERGIN_CLIENT_LOG']


def test_server_compatibility(mc):
    """Test server compatibility."""
    assert mc.is_server_compatible()


def create_versioned_project(mc, project_name, project_dir, updated_file, remove=True):
    project = API_USER + '/' + project_name
    cleanup(mc, project, [project_dir])

    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(project_name, project_dir)

    mp = MerginProject(project_dir)

    # create versions 2-4
    changes = ("inserted_1_A.gpkg", "inserted_1_A_mod.gpkg", "inserted_1_B.gpkg",)
    for change in changes:
        shutil.copy(mp.fpath(change), mp.fpath(updated_file))
        mc.push_project(project_dir)
    # create version 5 with modified file removed
    if remove:
        os.remove(os.path.join(project_dir, updated_file))
        mc.push_project(project_dir)

    return mp


def test_get_versions_with_file_changes(mc):
    """Test getting versions where the file was changed."""
    test_project = 'test_file_modified_versions'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    f_updated = "base.gpkg"

    mp = create_versioned_project(mc, test_project, project_dir, f_updated, remove=False)

    project_info = mc.project_info(project)
    assert project_info["version"] == "v4"
    file_history = mc.project_file_history_info(project, f_updated)

    with pytest.raises(ClientError) as e:
        mod_versions = get_versions_with_file_changes(
            mc, project, f_updated, version_from="v1", version_to="v5", file_history=file_history
        )
    assert "Wrong version parameters: 1-5" in str(e.value)
    assert "Available versions: [1, 2, 3, 4]" in str(e.value)

    mod_versions = get_versions_with_file_changes(
        mc, project, f_updated, version_from="v2", version_to="v4", file_history=file_history
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
    test_project = 'test_download_file'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    f_updated = "base.gpkg"

    mp = create_versioned_project(mc, test_project, project_dir, f_updated)

    project_info = mc.project_info(project)
    assert project_info["version"] == "v5"

    # Versioned file should have the following content at versions 2-4
    expected_content = ("inserted_1_A.gpkg", "inserted_1_A_mod.gpkg", "inserted_1_B.gpkg")

    # Download the base file at versions 2-4 and check the changes
    f_downloaded = os.path.join(project_dir, f_updated)
    for ver in range(2, 5):
        mc.download_file(project_dir, f_updated, f_downloaded, version=f"v{ver}")
        expected = os.path.join(TEST_DATA_DIR, expected_content[ver - 2])  # GeoPackage with expected content
        assert check_gpkg_same_content(mp, f_downloaded, expected)

    # make sure there will be exception raised if a file doesn't exist in the version
    with pytest.raises(ClientError, match=f"No {f_updated} exists at version v5"):
        mc.download_file(project_dir, f_updated, f_downloaded, version=f"v5")


def test_download_diffs(mc):
    """Test download diffs for a project file between specified project versions."""
    test_project = 'test_download_diffs'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    download_dir = os.path.join(project_dir, "diffs")  # project for downloading files at various versions
    f_updated = "base.gpkg"
    diff_file = os.path.join(download_dir, f_updated + ".diff")

    mp = create_versioned_project(mc, test_project, project_dir, f_updated, remove=False)

    project_info = mc.project_info(project)
    assert project_info["version"] == "v4"

    # Download diffs of updated file between versions 1 and 2
    mc.get_file_diff(project_dir, f_updated, diff_file, "v1", "v2")
    assert os.path.exists(diff_file)
    assert mp.geodiff.has_changes(diff_file)
    assert mp.geodiff.changes_count(diff_file) == 1
    changes_file = diff_file + ".changes1-2"
    mp.geodiff.list_changes_summary(diff_file, changes_file)
    with open(changes_file, 'r') as f:
        changes = json.loads(f.read())["geodiff_summary"][0]
        assert changes["insert"] == 1
        assert changes["update"] == 0

    # Download diffs of updated file between versions 2 and 4
    mc.get_file_diff(project_dir, f_updated, diff_file, "v2", "v4")
    changes_file = diff_file + ".changes2-4"
    mp.geodiff.list_changes_summary(diff_file, changes_file)
    with open(changes_file, 'r') as f:
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


def _use_wal(db_file):
    """ Ensures that sqlite database is using WAL journal mode """
    con = sqlite3.connect(db_file)
    cursor = con.cursor()
    cursor.execute('PRAGMA journal_mode=wal;')


def _create_test_table(db_file):
    """ Creates a table called 'test' in sqlite database. Useful to simulate change of database schema. """
    con = sqlite3.connect(db_file)
    cursor = con.cursor()
    cursor.execute('CREATE TABLE test (fid SERIAL, txt TEXT);')
    cursor.execute('INSERT INTO test VALUES (123, \'hello\');')
    cursor.execute('COMMIT;')


def _check_test_table(db_file):
    """ Checks whether the 'test' table exists and has one row - otherwise fails with an exception. """
    #con_verify = sqlite3.connect(db_file)
    #cursor_verify = con_verify.cursor()
    #cursor_verify.execute('select count(*) from test;')
    #assert cursor_verify.fetchone()[0] == 1
    assert _get_table_row_count(db_file, 'test') == 1


def _get_table_row_count(db_file, table):
    con_verify = sqlite3.connect(db_file)
    cursor_verify = con_verify.cursor()
    cursor_verify.execute('select count(*) from {};'.format(table))
    return cursor_verify.fetchone()[0]


def _is_file_updated(filename, changes_dict):
    """ checks whether a file is listed among updated files (for pull or push changes) """
    for f in changes_dict['updated']:
        if f['path'] == filename:
            return True
    return False


class AnotherSqliteConn:
    """ This simulates another app (e.g. QGIS) having a connection open, potentially
    with some active reader/writer.

    Note: we use a subprocess here instead of just using sqlite3 module from python
    because pygeodiff and python's sqlite3 module have their own sqlite libraries,
    and this does not work well when they are used in a single process. But if we
    use another process, things are fine. This is a limitation of how we package
    pygeodiff currently.
    """
    def __init__(self, filename):
        self.proc = subprocess.Popen(
            ['python3', os.path.join(os.path.dirname(__file__), 'sqlite_con.py'), filename],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE)

    def run(self, cmd):
        self.proc.stdin.write(cmd.encode()+b'\n')
        self.proc.stdin.flush()

    def close(self):
        out,err = self.proc.communicate(b'stop\n')
        if self.proc.returncode != 0:
            raise ValueError("subprocess error:\n" + err.decode('utf-8'))


def test_push_gpkg_schema_change(mc):
    """ Test that changes in GPKG get picked up if there were recent changes to it by another
    client and at the same time geodiff fails to find changes (a new table is added)
    """

    test_project = 'test_push_gpkg_schema_change'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)
    test_gpkg = os.path.join(project_dir, 'test.gpkg')
    test_gpkg_basefile = os.path.join(project_dir, '.mergin', 'test.gpkg')
    project_dir_verify = os.path.join(TMP_DIR, test_project + '_verify')
    test_gpkg_verify = os.path.join(project_dir_verify, 'test.gpkg')

    cleanup(mc, project, [project_dir, project_dir_verify])
    # create remote project
    os.makedirs(project_dir)
    shutil.copy(os.path.join(TEST_DATA_DIR, 'base.gpkg'), test_gpkg)
    #shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    mp = MerginProject(project_dir)

    mp.log.info(' // create changeset')

    mp.geodiff.create_changeset(mp.fpath('test.gpkg'), mp.fpath_meta('test.gpkg'), mp.fpath_meta('diff-0'))

    mp.log.info(' // use wal')

    _use_wal(test_gpkg)

    mp.log.info(' // make changes to DB')

    # open a connection and keep it open (qgis does this with a pool of connections too)
    acon2 = AnotherSqliteConn(test_gpkg)
    acon2.run('select count(*) from simple;')

    # add a new table to ensure that geodiff will fail due to unsupported change
    # (this simulates an independent reader/writer like GDAL)
    _create_test_table(test_gpkg)

    _check_test_table(test_gpkg)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg_basefile)

    mp.log.info(' // create changeset (2)')

    # why already here there is wal recovery - it could be because of two sqlite libs linked in one executable
    # INDEED THAT WAS THE PROBLEM, now running geodiff 1.0 with shared sqlite lib seems to work fine.
    with pytest.raises(pygeodiff.geodifflib.GeoDiffLibError):
        mp.geodiff.create_changeset(mp.fpath('test.gpkg'), mp.fpath_meta('test.gpkg'), mp.fpath_meta('diff-1'))

    _check_test_table(test_gpkg)
    with pytest.raises(sqlite3.OperationalError):
        _check_test_table(test_gpkg_basefile)

    mp.log.info(' // push project')

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

    test_project = 'test_rebase_local_schema_change'
    if extra_connection:
        test_project += '_extra_conn'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir
    project_dir_2 = os.path.join(TMP_DIR, test_project+'_2')  # concurrent project dir
    test_gpkg = os.path.join(project_dir, 'test.gpkg')
    test_gpkg_basefile = os.path.join(project_dir, '.mergin', 'test.gpkg')
    test_gpkg_conflict = test_gpkg + '_conflict_copy'
    cleanup(mc, project, [project_dir, project_dir_2])

    os.makedirs(project_dir)
    shutil.copy(os.path.join(TEST_DATA_DIR, 'base.gpkg'), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL, that's the more common and more difficult scenario
    mc.create_project_and_push(test_project, project_dir)

    if extra_connection:
        # open a connection and keep it open (qgis does this with a pool of connections too)
        con_extra = sqlite3.connect(test_gpkg)
        cursor_extra = con_extra.cursor()
        cursor_extra.execute('select count(*) from simple;')

    # Download project to the concurrent dir + add a feature + push a new version
    mc.download_project(project, project_dir_2)  # download project to concurrent dir
    mp_2 = MerginProject(project_dir_2)
    shutil.copy(os.path.join(TEST_DATA_DIR, 'inserted_1_A.gpkg'), mp_2.fpath('test.gpkg'))
    mc.push_project(project_dir_2)

    # Change schema in the primary project dir
    _create_test_table(test_gpkg)

    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert _is_file_updated('test.gpkg', pull_changes)
    assert _is_file_updated('test.gpkg', push_changes)

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

    assert _get_table_row_count(test_gpkg, 'simple') == 4
    assert _get_table_row_count(test_gpkg_basefile, 'simple') == 4
    assert _get_table_row_count(test_gpkg_conflict, 'simple') == 3


@pytest.mark.parametrize("extra_connection", [False, True])
def test_rebase_remote_schema_change(mc, extra_connection):
    """
    Checks whether a pull with failed rebase (due to remote DB schema change) is handled correctly,
    i.e. a conflict file is created with the content of the local changes.
    """

    test_project = 'test_rebase_remote_schema_change'
    if extra_connection:
        test_project += '_extra_conn'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir
    project_dir_2 = os.path.join(TMP_DIR, test_project+'_2')  # concurrent project dir
    test_gpkg = os.path.join(project_dir, 'test.gpkg')
    test_gpkg_2 = os.path.join(project_dir_2, 'test.gpkg')
    test_gpkg_basefile = os.path.join(project_dir, '.mergin', 'test.gpkg')
    test_gpkg_conflict = test_gpkg + '_conflict_copy'
    cleanup(mc, project, [project_dir, project_dir_2])

    os.makedirs(project_dir)
    shutil.copy(os.path.join(TEST_DATA_DIR, 'base.gpkg'), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL, that's the more common and more difficult scenario
    mc.create_project_and_push(test_project, project_dir)

    # Download project to the concurrent dir + change DB schema + push a new version
    mc.download_project(project, project_dir_2)
    _create_test_table(test_gpkg_2)
    mc.push_project(project_dir_2)

    # do changes in the local DB (added a row)
    shutil.copy(os.path.join(TEST_DATA_DIR, 'inserted_1_A.gpkg'), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL

    if extra_connection:
        # open a connection and keep it open (qgis does this with a pool of connections too)
        con_extra = sqlite3.connect(test_gpkg)
        cursor_extra = con_extra.cursor()
        cursor_extra.execute('select count(*) from simple;')

    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert _is_file_updated('test.gpkg', pull_changes)
    assert _is_file_updated('test.gpkg', push_changes)

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

    assert _get_table_row_count(test_gpkg, 'simple') == 3
    assert _get_table_row_count(test_gpkg_basefile, 'simple') == 3
    assert _get_table_row_count(test_gpkg_conflict, 'simple') == 4


@pytest.mark.parametrize("extra_connection", [False, True])
def test_rebase_success(mc, extra_connection):
    """
    Checks whether a pull with successful rebase is handled correctly.
    i.e. changes are merged together and no conflict files are created.
    """

    test_project = 'test_rebase_success'
    if extra_connection:
        test_project += '_extra_conn'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir
    project_dir_2 = os.path.join(TMP_DIR, test_project+'_2')  # concurrent project dir
    test_gpkg = os.path.join(project_dir, 'test.gpkg')
    test_gpkg_2 = os.path.join(project_dir_2, 'test.gpkg')
    test_gpkg_basefile = os.path.join(project_dir, '.mergin', 'test.gpkg')
    test_gpkg_conflict = test_gpkg + '_conflict_copy'
    cleanup(mc, project, [project_dir, project_dir_2])

    os.makedirs(project_dir)
    shutil.copy(os.path.join(TEST_DATA_DIR, 'base.gpkg'), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL, that's the more common and more difficult scenario
    mc.create_project_and_push(test_project, project_dir)

    # Download project to the concurrent dir + add a row + push a new version
    mc.download_project(project, project_dir_2)
    shutil.copy(os.path.join(TEST_DATA_DIR, 'inserted_1_A.gpkg'), test_gpkg_2)
    _use_wal(test_gpkg)  # make sure we use WAL
    mc.push_project(project_dir_2)

    # do changes in the local DB (added a row)
    shutil.copy(os.path.join(TEST_DATA_DIR, 'inserted_1_B.gpkg'), test_gpkg)
    _use_wal(test_gpkg)  # make sure we use WAL

    if extra_connection:
        # open a connection and keep it open (qgis does this with a pool of connections too)
        con_extra = sqlite3.connect(test_gpkg)
        cursor_extra = con_extra.cursor()
        cursor_extra.execute('select count(*) from simple;')

    pull_changes, push_changes, _ = mc.project_status(project_dir)
    assert _is_file_updated('test.gpkg', pull_changes)
    assert _is_file_updated('test.gpkg', push_changes)

    assert not os.path.exists(test_gpkg_conflict)

    mc.pull_project(project_dir)

    assert not os.path.exists(test_gpkg_conflict)

    # check that the local file + basefile don't contain the new row, and the conflict copy does

    assert _get_table_row_count(test_gpkg, 'simple') == 5
    assert _get_table_row_count(test_gpkg_basefile, 'simple') == 4
