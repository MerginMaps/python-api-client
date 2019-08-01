import os
import tempfile
import shutil
import pytest
from ..client import MerginClient, ClientError, list_project_directory
from ..utils import generate_checksum

SERVER_URL = os.environ.get('TEST_MERGIN_URL')
API_USER = os.environ.get('TEST_API_USERNAME')
USER_PWD = os.environ.get('TEST_API_PASSWORD')
TMP_DIR = tempfile.gettempdir()
TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_data')


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

    with pytest.raises(ClientError, match='Invalid username or password'):
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
    mc.create_project(test_project, directory=project_dir)

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
    assert all(f in os.listdir(download_dir) for f in os.listdir(project_dir) + ['.mergin'])
    
    # unable to download to the same directory
    with pytest.raises(Exception, match='Project directory already exists'):
        mc.download_project(project, download_dir)


@pytest.mark.parametrize("parallel", [True, False])
def test_push_pull_changes(mc, parallel):
    test_project = 'test_push'
    project = API_USER + '/' + test_project
    project_dir = os.path.join(TMP_DIR, test_project)  # primary project dir for updates
    project_dir_2 = os.path.join(TMP_DIR, test_project+'_2')  # concurrent project dir

    cleanup(mc, project, [project_dir, project_dir_2])
    # create remote project
    shutil.copytree(TEST_DATA_DIR, project_dir)
    mc.create_project(test_project, project_dir)

    # make sure we have v1 also in concurrent project dir
    mc.download_project(project, project_dir_2)

    # test push changes (add, remove, rename, update)
    f_added = 'new.txt'
    with open(os.path.join(project_dir, f_added), 'w') as f:
        f.write('new file')
    f_removed = 'test.txt'
    os.remove(os.path.join(project_dir, f_removed))
    f_renamed = 'test_dir/test2.txt'
    shutil.move(os.path.join(project_dir, f_renamed), os.path.join(project_dir, 'renamed.txt'))
    f_updated = 'test3.txt'
    with open(os.path.join(project_dir, f_updated), 'w') as f:
        f.write('Modified')

    mc.push_project(project_dir, parallel=parallel)
    project_info = mc.project_info(project)
    assert project_info['version'] == 'v2'
    assert not next((f for f in project_info['files'] if f['path'] == f_removed), None)
    assert not next((f for f in project_info['files'] if f['path'] == f_renamed), None)
    assert next((f for f in project_info['files'] if f['path'] == 'renamed.txt'), None)
    assert next((f for f in project_info['files'] if f['path'] == f_added), None)
    f_remote_checksum = next((f['checksum'] for f in project_info['files'] if f['path'] == f_updated), None)
    assert generate_checksum(os.path.join(project_dir, f_updated)) == f_remote_checksum
    assert len(project_info['files']) == len(list_project_directory(project_dir))

    # test parallel changes
    with open(os.path.join(project_dir_2, f_updated), 'w') as f:
        f.write('Make some conflict')
    f_conflict_checksum = generate_checksum(os.path.join(project_dir_2, f_updated))

    # not at latest server version
    with pytest.raises(ClientError, match='Update your local repository'):
        mc.push_project(project_dir_2)

    mc.pull_project(project_dir_2, parallel=parallel)
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
    mc.create_project(test_project, project_dir)
    project_info = mc.project_info(project)
    assert not next((f for f in project_info['files'] if f['path'] == 'test.qgs~'), None)

    with open(os.path.join(project_dir, '.directory'), 'w') as f:
        f.write('test')
    mc.push_project(project_dir)
    assert not next((f for f in project_info['files'] if f['path'] == '.directory'), None)
