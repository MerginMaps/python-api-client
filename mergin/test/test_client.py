import os
import tempfile
import shutil
from datetime import datetime, timedelta
import pytest
import pytz

from ..client import MerginClient, ClientError, MerginProject, LoginError
from ..utils import generate_checksum

SERVER_URL = os.environ.get('TEST_MERGIN_URL')
API_USER = os.environ.get('TEST_API_USERNAME')
USER_PWD = os.environ.get('TEST_API_PASSWORD')
TMP_DIR = tempfile.gettempdir()
TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_data')
CHANGED_SCHEMA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'modified_schema')


def toggle_geodiff(enabled):
    os.environ['GEODIFF_ENABLED'] = str(enabled)


@pytest.fixture(scope='function')
def mc():
    assert SERVER_URL and SERVER_URL.rstrip('/') != 'https://public.cloudmergin.com' and API_USER and USER_PWD
    return MerginClient(SERVER_URL, login=API_USER, password=USER_PWD)


def cleanup(mc, project, dirs):
    # cleanup leftovers from previous test if needed such as remote project and local directories
    try:
        mc.delete_project(project)
    except ClientError:
        pass
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)


def test_login(mc):
    token = mc._auth_session['token']
    assert MerginClient(mc.url, auth_token=token)

    with pytest.raises(ValueError, match='Invalid token data'):
        MerginClient(mc.url, auth_token='Bearer .jas646kgfa')

    with pytest.raises(LoginError, match='Invalid username or password'):
        mc.login('foo', 'bar')


def test_create_delete_project(mc):
    # create new (empty) project on server
    test_project = 'test_create_delete'
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


# (diffs size limit, push geodiff enabled, pull geodiff enabled)
diff_test_scenarios = [
    (True, True),
    (True, False),
    (False, True),
    (False, False),
]


@pytest.mark.parametrize("push_geodiff_enabled, pull_geodiff_enabled", diff_test_scenarios)
def test_sync_diff(mc, push_geodiff_enabled, pull_geodiff_enabled):

    test_project = f'test_sync_diff_push{int(push_geodiff_enabled)}_pull{int(pull_geodiff_enabled)}'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project + '_2')  # concurrent project dir with no changes
    project_dir_3 = os.path.join(TMP_DIR, test_project + '_3')  # concurrent project dir with local changes

    cleanup(mc, project, [project_dir, project_dir_2, project_dir_3])
    # create remote project
    toggle_geodiff(push_geodiff_enabled)
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project_and_push(test_project, project_dir)

    # make sure we have v1 also in concurrent project dirs
    toggle_geodiff(pull_geodiff_enabled)
    mc.download_project(project, project_dir_2)
    mc.download_project(project, project_dir_3)

    # test push changes with diffs:
    toggle_geodiff(push_geodiff_enabled)
    mp = MerginProject(project_dir)
    f_updated = 'base.gpkg'
    # step 1) base.gpkg updated to inserted_1_A (inserted A feature)
    if push_geodiff_enabled:
        shutil.move(mp.fpath(f_updated), mp.fpath_meta(f_updated))  # make local copy for changeset calculation
    shutil.copy(mp.fpath('inserted_1_A.gpkg'), mp.fpath(f_updated))
    mc.push_project(project_dir)
    if push_geodiff_enabled:
        mp.geodiff.create_changeset(mp.fpath(f_updated), mp.fpath_meta(f_updated), mp.fpath_meta('push_diff'))
        assert not mp.geodiff.has_changes(mp.fpath_meta('push_diff'))
    else:
        assert not os.path.exists(mp.fpath_meta(f_updated))
    # step 2) base.gpkg updated to inserted_1_A_mod (modified 2 features)
    if push_geodiff_enabled:
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
    if push_geodiff_enabled:
        assert 'diff' in f_remote
        assert os.path.exists(mp.fpath_meta('renamed.gpkg'))
    else:
        assert 'diff' not in f_remote
        assert not os.path.exists(mp.fpath_meta('renamed.gpkg'))

    # pull project in different directory
    toggle_geodiff(pull_geodiff_enabled)
    mp2 = MerginProject(project_dir_2)
    mc.pull_project(project_dir_2)
    if pull_geodiff_enabled:
        mp2.geodiff.create_changeset(mp.fpath(f_updated), mp2.fpath(f_updated), mp2.fpath_meta('diff'))
        assert not mp2.geodiff.has_changes(mp2.fpath_meta('diff'))
    else:
        server_file_checksum = next((f['checksum'] for f in project_info['files'] if f['path'] == f_updated), '')
        assert server_file_checksum == generate_checksum(mp2.fpath(f_updated))

    # introduce conflict local change (inserted B feature to base)
    mp3 = MerginProject(project_dir_3)
    shutil.copy(mp3.fpath('inserted_1_B.gpkg'), mp3.fpath(f_updated))
    checksum = generate_checksum(mp3.fpath('inserted_1_B.gpkg'))
    mc.pull_project(project_dir_3)
    if pull_geodiff_enabled:
        assert not os.path.exists(mp3.fpath('base.gpkg_conflict_copy'))
    else:
        assert os.path.exists(mp3.fpath('base.gpkg_conflict_copy'))  #
        assert generate_checksum(mp3.fpath('base.gpkg_conflict_copy')) == checksum

    # push new changes from project_3 and pull in original project
    toggle_geodiff(push_geodiff_enabled)
    mc.push_project(project_dir_3)
    toggle_geodiff(pull_geodiff_enabled)
    mc.pull_project(project_dir)
    if pull_geodiff_enabled:
        mp3.geodiff.create_changeset(mp.fpath(f_updated), mp3.fpath(f_updated), mp.fpath_meta('diff'))
        assert not mp3.geodiff.has_changes(mp.fpath_meta('diff'))
    else:
        assert os.path.exists(mp.fpath('base.gpkg_conflict_copy'))

    # make sure that we leave geodiff enabled for further tests
    toggle_geodiff(True)


@pytest.mark.skip(reason="currently fails on test.dev instance due to server bug")
def test_list_of_push_changes(mc):
    PUSH_CHANGES_SUMMARY = "{'base.gpkg': {'geodiff_summary': [{'table': 'gpkg_contents', 'insert': 0, 'update': 1, 'delete': 0}, {'table': 'simple', 'insert': 1, 'update': 0, 'delete': 0}]}}"

    test_project = 'test_list_of_push_changes'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates

    cleanup(mc, project, [project_dir])
    shutil.copytree(TEST_DATA_DIR, project_dir)
    toggle_geodiff(True)
    mc.create_project_and_push(test_project, project_dir)

    f_updated = 'base.gpkg'
    mp = MerginProject(project_dir)

    shutil.copy(mp.fpath('inserted_1_A.gpkg'), mp.fpath(f_updated))
    mc._auth_session["expire"] = datetime.now().replace(tzinfo=pytz.utc) - timedelta(days=1)
    pull_changes, push_changes, push_changes_summary = mc.project_status(project_dir)
    assert str(push_changes_summary) == PUSH_CHANGES_SUMMARY

    mc._auth_session["expire"] = datetime.now().replace(tzinfo=pytz.utc) - timedelta(days=1)
    mc._auth_params = None
    with pytest.raises(ClientError, match="You don't have the permission to access the requested resource. It is either read-protected or not readable by the server."):
        mc.project_status(project_dir)


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
    assert f_remote['checksum'] == updated_checksum
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
