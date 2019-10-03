import os
import re
import json
import zlib
import base64
import shutil
import urllib.parse
import urllib.request
import uuid
import math
import hashlib
import copy
from datetime import datetime, timezone
import concurrent.futures

from .utils import save_to_file, generate_checksum, move_file, DateTimeEncoder, int_version, find

this_dir = os.path.dirname(os.path.realpath(__file__))

try:
    import dateutil.parser
    from dateutil.tz import tzlocal
    import pygeodiff
except ImportError:
    # this is to import all dependencies shipped with package (e.g. to use in qgis-plugin)
    deps_dir = os.path.join(this_dir, 'deps')
    if os.path.exists(deps_dir):
        import sys
        for f in os.listdir(os.path.join(deps_dir)):
            sys.path.append(os.path.join(deps_dir, f))

        import dateutil.parser
        from dateutil.tz import tzlocal
        import pygeodiff

CHUNK_SIZE = 100 * 1024 * 1024
# there is an upper limit for chunk size on server, ideally should be requested from there once implemented
UPLOAD_CHUNK_SIZE = 10 * 1024 * 1024


class InvalidProject(Exception):
    pass


class ClientError(Exception):
    pass


class SyncError(Exception):
    pass


class MerginProject:
    """ Base class for Mergin local projects.

    Linked to existing local directory, with project metadata (mergin.json) and backups located in .mergin directory.
    """
    def __init__(self, directory):
        self.dir = os.path.abspath(directory)
        if not os.path.exists(self.dir):
            raise InvalidProject('Project directory does not exist')

        self.geodiff = pygeodiff.GeoDiff() if os.environ.get('GEODIFF_ENABLED', 'True').lower() == 'true' else None
        self.meta_dir = os.path.join(self.dir, '.mergin')
        if not os.path.exists(self.meta_dir):
            os.mkdir(self.meta_dir)

    def fpath(self, file, other_dir=None):
        """
        Helper function to get absolute path of project file. Defaults to project dir but
        alternative dir get be provided (mostly meta or temp). Also making sure that parent dirs to file exist.

        :param file: relative file path in project (posix)
        :type file: str
        :param other_dir: alternative base directory for file, defaults to None
        :type other_dir: str
        :returns: file's absolute path
        :rtype: str
        """
        root = other_dir or self.dir
        abs_path = os.path.abspath(os.path.join(root, file))
        f_dir = os.path.dirname(abs_path)
        os.makedirs(f_dir, exist_ok=True)
        return abs_path

    def fpath_meta(self, file):
        """ Helper function to get absolute path of file in meta dir. """
        return self.fpath(file, self.meta_dir)

    @property
    def metadata(self):
        if not os.path.exists(self.fpath_meta('mergin.json')):
            raise InvalidProject('Project metadata has not been created yet')
        with open(self.fpath_meta('mergin.json'), 'r') as file:
            return json.load(file)

    @metadata.setter
    def metadata(self, data):
        with open(self.fpath_meta('mergin.json'), 'w') as file:
            json.dump(data, file, indent=2)

    def is_versioned_file(self, file):
        """ Check if file is compatible with geodiff lib and hence suitable for versioning.

        :param file: file path
        :type file: str
        :returns: if file is compatible with geodiff lib
        :rtype: bool
        """
        if not self.geodiff:
            return False
        diff_extensions = ['.gpkg', '.sqlite']
        f_extension = os.path.splitext(file)[1]
        return f_extension in diff_extensions

    def ignore_file(self, file):
        """
        Helper function for blacklisting certain types of files.

        :param file: file path in project
        :type file: str
        :returns: whether file should be ignored
        :rtype: bool
        """
        ignore_ext = re.compile(r'({})$'.format('|'.join(re.escape(x) for x in ['-shm', '-wal', '~', 'pyc', 'swap'])))
        ignore_files = ['.DS_Store', '.directory']
        name, ext = os.path.splitext(file)
        if ext and ignore_ext.search(ext):
            return True
        if file in ignore_files:
            return True
        return False

    def inspect_files(self):
        """
        Inspect files in project directory and return metadata.

        :returns: metadata for files in project directory in server required format
        :rtype: list[dict]
        """
        files_meta = []
        for root, dirs, files in os.walk(self.dir, topdown=True):
            dirs[:] = [d for d in dirs if d not in ['.mergin']]
            for file in files:
                if self.ignore_file(file):
                    continue
                abs_path = os.path.abspath(os.path.join(root, file))
                rel_path = os.path.relpath(abs_path, start=self.dir)
                proj_path = '/'.join(rel_path.split(os.path.sep))  # we need posix path
                files_meta.append({
                    "path": proj_path,
                    "checksum": generate_checksum(abs_path),
                    "size": os.path.getsize(abs_path),
                    "mtime": datetime.fromtimestamp(os.path.getmtime(abs_path), tzlocal())
                })
        return files_meta

    def compare_file_sets(self, origin, current):
        """
        Helper function to calculate difference between two sets of files metadata using file names and checksums.

        :Example:

        >>> origin = [{'checksum': '08b0e8caddafe74bf5c11a45f65cedf974210fed', 'path': 'base.gpkg', 'size': 2793, 'mtime': '2019-08-26T11:08:34.051221+02:00'}]
        >>> current = [{'checksum': 'c9a4fd2afd513a97aba19d450396a4c9df8b2ba4', 'path': 'test.qgs', 'size': 31980, 'mtime': '2019-08-26T11:09:30.051221+02:00'}]
        >>> self.compare_file_sets(origin, current)
        {"added": [{'checksum': 'c9a4fd2afd513a97aba19d450396a4c9df8b2ba4', 'path': 'test.qgs', 'size': 31980, 'mtime': '2019-08-26T11:09:30.051221+02:00'}], "removed": [[{'checksum': '08b0e8caddafe74bf5c11a45f65cedf974210fed', 'path': 'base.gpkg', 'size': 2793, 'mtime': '2019-08-26T11:08:34.051221+02:00'}]], "renamed": [], "updated": []}

        :param origin: origin set of files metadata
        :type origin: list[dict]
        :param current: current set of files metadata to be compared against origin
        :type current: list[dict]
        :returns: changes between two sets with change type
        :rtype: dict[str, list[dict]]'
        """
        origin_map = {f["path"]: f for f in origin}
        current_map = {f["path"]: f for f in current}
        removed = [f for f in origin if f["path"] not in current_map]

        added = []
        for f in current:
            if f["path"] in origin_map:
                continue
            added.append(f)

        moved = []
        for rf in removed:
            match = find(
                current,
                lambda f: f["checksum"] == rf["checksum"] and f["size"] == rf["size"] and all(
                    f["path"] != mf["path"] for mf in moved)
            )
            if match:
                moved.append({**rf, "new_path": match["path"]})

        added = [f for f in added if all(f["path"] != mf["new_path"] for mf in moved)]
        removed = [f for f in removed if all(f["path"] != mf["path"] for mf in moved)]

        updated = []
        for f in current:
            path = f["path"]
            if path not in origin_map:
                continue
            if f["checksum"] == origin_map[path]["checksum"]:
                continue
            f["origin_checksum"] = origin_map[path]["checksum"]
            updated.append(f)

        return {
            "renamed": moved,
            "added": added,
            "removed": removed,
            "updated": updated
        }

    def get_pull_changes(self, server_files):
        """
        Calculate changes needed to be pulled from server.

        Calculate diffs between local files metadata and server's ones. Because simple metadata like file size or
        checksum are not enough to determine geodiff files changes, evaluate also their history (provided by server).
        For small files ask for full versions of geodiff files, otherwise determine list of diffs needed to update file.

        .. seealso:: self.compare_file_sets

        :param server_files: list of server files' metadata (see also self.inspect_files())
        :type server_files: list[dict]
        :returns: changes metadata for files to be pulled from server
        :rtype: dict
        """
        changes = self.compare_file_sets(self.metadata['files'], server_files)
        if not self.geodiff:
            return changes

        size_limit = int(os.environ.get('DIFFS_LIMIT_SIZE', 1024 * 1024)) # with smaller values than limit download full file instead of diffs
        not_updated = []
        for file in changes['updated']:
            # for small geodiff files it does not make sense to download diff and then apply it (slow)
            if not self.is_versioned_file(file["path"]):
                continue

            diffs = []
            diffs_size = 0
            is_updated = False
            # need to track geodiff file history to see if there were any changes
            for k, v in file['history'].items():
                if int_version(k) <= int_version(self.metadata['version']):
                    continue  # ignore history of no interest
                is_updated = True
                if 'diff' in v:
                    diffs.append(v['diff']['path'])
                    diffs_size += v['diff']['size']
                else:
                    diffs = []
                    break  # we found force update in history, does not make sense to download diffs

            if is_updated:
                if diffs and file['size'] > size_limit and diffs_size < file['size']/2:
                    file['diffs'] = diffs
            else:
                not_updated.append(file)

        changes['updated'] = [f for f in changes['updated'] if f not in not_updated]
        return changes

    def get_push_changes(self):
        """
        Calculate changes needed to be pushed to server.

        Calculate diffs between local files metadata and actual files in project directory. Because simple metadata like
        file size or checksum are not enough to determine geodiff files changes, geodiff tool is used to determine change
        of file content and update corresponding metadata.

        .. seealso:: self.compare_file_sets

        :returns: changes metadata for files to be pushed to server
        :rtype: dict
        """
        changes = self.compare_file_sets(self.metadata['files'], self.inspect_files())
        for file in changes['added'] + changes['updated']:
            file['chunks'] = [str(uuid.uuid4()) for i in range(math.ceil(file["size"] / UPLOAD_CHUNK_SIZE))]

        if not self.geodiff:
            return changes

        # need to check for for real changes in geodiff files using geodiff tool (comparing checksum is not enough)
        not_updated = []
        for file in changes['updated']:
            path = file["path"]
            if not self.is_versioned_file(path):
                continue

            current_file = self.fpath(path)
            origin_file = self.fpath(path, self.meta_dir)
            diff_id = str(uuid.uuid4())
            diff_name = path + '-diff-' + diff_id
            diff_file = self.fpath_meta(diff_name)
            try:
                self.geodiff.create_changeset(origin_file, current_file, diff_file)
                if self.geodiff.has_changes(diff_file):
                    diff_size = os.path.getsize(diff_file)
                    file['checksum'] = file['origin_checksum']  # need to match basefile on server
                    file['chunks'] = [str(uuid.uuid4()) for i in range(math.ceil(diff_size / UPLOAD_CHUNK_SIZE))]
                    file['mtime'] = datetime.fromtimestamp(os.path.getmtime(current_file), tzlocal())
                    file['diff'] = {
                        "path": diff_name,
                        "checksum": generate_checksum(diff_file),
                        "size": diff_size,
                        'mtime': datetime.fromtimestamp(os.path.getmtime(diff_file), tzlocal())
                    }
                else:
                    not_updated.append(file)
            except (pygeodiff.GeoDiffLibError, pygeodiff.GeoDiffLibConflictError):
                pass  # we do force update

        changes['updated'] = [f for f in changes['updated'] if f not in not_updated]
        return changes

    def apply_pull_changes(self, changes, temp_dir):
        """
        Apply changes pulled from server.

        Update project files according to file changes. Apply changes to geodiff basefiles as well
        so they are up to date with server. In case of conflicts create backups from locally modified versions.

        .. seealso:: self.pull_changes

        :param changes: metadata for pulled files
        :type changes: dict[str, list[dict]]
        :param temp_dir: directory with downloaded files from server
        :type temp_dir: str
        :returns: files where conflicts were found
        :rtype: list[str]
        """
        conflicts = []
        local_changes = self.get_push_changes()
        modified = {}
        for f in local_changes["added"] + local_changes["updated"]:
            modified.update({f['path']: f})
        for f in local_changes["renamed"]:
            modified.update({f['new_path']: f})

        local_files_map = {}
        for f in self.inspect_files():
            local_files_map.update({f['path']: f})

        for k, v in changes.items():
            for item in v:
                path = item['path'] if k != 'renamed' else item['new_path']
                src = self.fpath(path, temp_dir) if k != 'renamed' else self.fpath(item["path"])
                dest = self.fpath(path)
                basefile = self.fpath_meta(path)

                # special care is needed for geodiff files
                if self.is_versioned_file(path) and k == 'updated':
                    if path in modified:
                        server_diff = self.fpath(f'{path}-server_diff', temp_dir)  # single origin diff from 'diffs' for use in rebase
                        rebased_diff = self.fpath(f'{path}-rebased', temp_dir)
                        patchedfile = self.fpath(f'{path}-patched', temp_dir)  # patched server version with local changes
                        changeset = self.fpath(f'{path}-local_diff', temp_dir)  # final changeset to be potentially committed
                        try:
                            self.geodiff.create_changeset(basefile, src, server_diff)
                            self.geodiff.create_rebased_changeset(basefile, dest, server_diff, rebased_diff)
                            self.geodiff.apply_changeset(src, patchedfile, rebased_diff)
                            self.geodiff.create_changeset(src, patchedfile, changeset)
                            shutil.copy(src, basefile)
                            shutil.copy(patchedfile, dest)
                        except (pygeodiff.GeoDiffLibError, pygeodiff.GeoDiffLibConflictError):
                            # it would not be possible to commit local changes, create new conflict file instead
                            conflict = self.backup_file(path)
                            conflicts.append(conflict)
                            shutil.copy(src, dest)
                            shutil.copy(src, basefile)
                    else:
                        # just use already updated tmp_basefile to update project file and its basefile
                        shutil.copy(src, dest)
                        shutil.copy(src, basefile)
                else:
                    # backup if needed
                    if path in modified and item['checksum'] != local_files_map[path]['checksum']:
                        conflict = self.backup_file(path)
                        conflicts.append(conflict)

                    if k == 'removed':
                        os.remove(dest)
                        if self.is_versioned_file(path):
                            os.remove(basefile)
                    elif k == 'renamed':
                        move_file(src, dest)
                        if self.is_versioned_file(path):
                            move_file(self.fpath_meta(item["path"]), basefile)
                    else:
                        shutil.copy(src, dest)
                        if self.is_versioned_file(path):
                            shutil.copy(src, basefile)

        return conflicts

    def apply_push_changes(self, changes):
        """
        For geodiff files update basefiles according to changes pushed to server.

        :param changes: metadata for pulled files
        :type changes: dict[str, list[dict]]
        """
        if not self.geodiff:
            return
        for k, v in changes.items():
            for item in v:
                path = item['path'] if k != 'renamed' else item['new_path']
                if not self.is_versioned_file(path):
                    continue

                basefile = self.fpath_meta(path)
                if k == 'renamed':
                    move_file(self.fpath_meta(item["path"]), basefile)
                elif k == 'removed':
                    os.remove(basefile)
                elif k == 'added':
                    shutil.copy(self.fpath(path), basefile)
                elif k == 'updated':
                    # better to apply diff to previous basefile to avoid issues with geodiff tmp files
                    changeset = self.fpath_meta(item['diff']['path'])
                    patchedfile = self.apply_diffs(basefile, [changeset])
                    if patchedfile:
                        move_file(patchedfile, basefile)
                    else:
                        # in case of local sync issues it is safier to remove basefile, next time it will be downloaded from server
                        os.remove(basefile)
                else:
                    pass

    def backup_file(self, file):
        """
        Create backup file next to its origin.

        :param file: path of file in project
        :type file: str
        :returns: path to backupfile
        :rtype: str
        """
        src = self.fpath(file)
        if not os.path.exists(src):
            return
        backup_path = self.fpath(f'{file}_conflict_copy')
        index = 2
        while os.path.exists(backup_path):
            backup_path = self.fpath(f'{file}_conflict_copy{index}')
            index += 1
        shutil.copy(src, backup_path)
        return backup_path

    def apply_diffs(self, basefile, diffs):
        """
        Helper function to update content of geodiff file using list of diffs.

        :param basefile: abs path to file to be updated
        :type basefile: str
        :param diffs: list of abs paths to geodiff changeset files
        :type diffs: list[str]
        :returns:  abs path of created patched file
        :rtype: str
        """
        if not self.is_versioned_file(basefile):
            return

        patchedfile = None
        for index, diff in enumerate(diffs):
            patchedfile = f'{basefile}-patched-{index}'
            try:
                self.geodiff.apply_changeset(basefile, patchedfile, diff)
            except (pygeodiff.GeoDiffLibError, pygeodiff.GeoDiffLibConflictError):
                return
            previous = f'{basefile}-patched-{index-1}'
            if os.path.exists(previous):
                os.remove(previous)
            basefile = patchedfile
        return patchedfile


def decode_token_data(token):
    token_prefix = "Bearer ."
    if not token.startswith(token_prefix):
        raise ValueError("Invalid token type")

    try:
        data = token[len(token_prefix):].split('.')[0]
        # add proper base64 padding"
        data += "=" * (-len(data) % 4)
        decoded = zlib.decompress(base64.urlsafe_b64decode(data))
        return json.loads(decoded)
    except (IndexError, TypeError, ValueError, zlib.error):
        raise ValueError("Invalid token data")


class MerginClient:
    def __init__(self, url, auth_token=None, login=None, password=None):
        self.url = url

        self._auth_params = None
        self._auth_session = None
        self._user_info = None
        if auth_token:
            token_data = decode_token_data(auth_token)
            self._auth_session = {
                "token": auth_token,
                "expire": dateutil.parser.parse(token_data["expire"])
            }
            self._user_info = {
                "username": token_data["username"]
            }

        self.opener = urllib.request.build_opener()
        urllib.request.install_opener(self.opener)

        if login and password:
            self.login(login, password)

    def _do_request(self, request):
        if self._auth_session:
            delta = self._auth_session["expire"] - datetime.now(timezone.utc)
            if delta.total_seconds() < 1:
                self._auth_session = None
                # Refresh auth token when login credentials are available
                if self._auth_params:
                    self.login(self._auth_params["login"], self._auth_params["password"])

            if self._auth_session:
                request.add_header("Authorization", self._auth_session["token"])
        try:
            return self.opener.open(request)
        except urllib.error.HTTPError as e:
            if e.headers.get("Content-Type", "") == "application/problem+json":
                info = json.load(e)
                raise ClientError(info.get("detail"))
            raise ClientError(e.read().decode("utf-8"))

    def get(self, path, data=None, headers={}):
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        if data:
            url += "?" + urllib.parse.urlencode(data)
        request = urllib.request.Request(url, headers=headers)
        return self._do_request(request)

    def post(self, path, data=None, headers={}):
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        if headers.get("Content-Type", None) == "application/json":
            data = json.dumps(data, cls=DateTimeEncoder).encode("utf-8")
        request = urllib.request.Request(url, data, headers, method="POST")
        return self._do_request(request)

    def is_server_compatible(self):
        """
        Test whether version of the server meets the required set of endpoints.

        :returns: client compatible with server
        rtype: Boolean
        """
        resp = self.get("/ping")
        data = json.load(resp)
        if 'endpoints' not in data:
            return False
        endpoints = data['endpoints']

        client_endpoints = {
            "data_sync": {
                "GET": ["/project/raw/{namespace}/{project_name}"],
                "POST": [
                    "/project/push/cancel/{transaction_id}",
                    "/project/push/finish/{transaction_id}",
                    "/project/push/{namespace}/{project_name}",
                   # "/project/push/chunk/{transaction_id}/{chunk_id}" # issue in server
                ]
            },
            "project": {
                "DELETE": ["/project/{namespace}/{project_name}"],
                "GET": [
                    "/project",
                    "/project/{namespace}/{project_name}",
                    "/project/version/{namespace}/{project_name}"
                ],
                "POST": ["/project/{namespace}"]
            },
            "user": {
                "POST": ["/auth/login"]
            }
        }

        for k, v in client_endpoints.items():
            if k not in endpoints:
                return False
            for method, url_list in v.items():
                if method not in endpoints[k]:
                    return False
                for url in url_list:
                    if url not in endpoints[k][method]:
                        return False
        return True

    def login(self, login, password):
        """
        Authenticate login credentials and store session token

        :param login: User's username of email address
        :type login: String

        :param password: User's password
        :type password: String
        """
        params = {
            "login": login,
            "password": password
        }
        self._auth_params = params
        resp = self.post("/v1/auth/login", params, {"Content-Type": "application/json"})
        data = json.load(resp)
        session = data["session"]
        self._auth_session = {
            "token": "Bearer %s" % session["token"],
            "expire": dateutil.parser.parse(session["expire"])
        }
        self._user_info = {
            "username": data["username"]
        }
        return session

    def create_project(self, project_name, directory=None, is_public=False):
        """
        Create new project repository in user namespace on Mergin server.
        Optionally initialized from given local directory.

        :param project_name: Project name
        :type project_name: String

        :param directory: Local project directory, defaults to None
        :type directory: String

        :param is_public: Flag for public/private project, defaults to False
        :type directory: Boolean
        """
        if not self._user_info:
            raise Exception("Authentication required")
        if directory and not os.path.exists(directory):
            raise Exception("Project directory does not exists")

        params = {
            "name": project_name,
            "public": is_public
        }
        namespace = self._user_info["username"]
        self.post("/v1/project/%s" % namespace, params, {"Content-Type": "application/json"})
        data = {
            "name": "%s/%s" % (namespace, project_name),
            "version": "v0",
            "files": []
        }
        if directory:
            mp = MerginProject(directory)
            mp.metadata = data
            if mp.inspect_files():
                self.push_project(directory)

    def projects_list(self, tags=None, user=None, flag=None, q=None):
        """
        Find all available mergin projects.

        :param tags: Filter projects by tags ('valid_qgis', 'mappin_use', input_use')
        :type tags: List

        :param user: Username for 'flag' filter. If not provided, it means user executing request.
        :type user: String

        :param flag: Predefined filter flag ('created', 'shared')
        :type flag: String

        :param q: Search query string
        :type q: String

        :rtype: List[Dict]
        """
        params = {}
        if tags:
            params["tags"] = ",".join(tags)
        if user:
            params["user"] = user
        if flag:
            params["flag"] = flag
        if q:
            params["q"] = q
        resp = self.get("/v1/project", params)
        projects = json.load(resp)
        return projects

    def project_info(self, project_path, since=None):
        """
        Fetch info about project.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String
        :param since: Version to track history of geodiff files from
        :type since: String
        :rtype: Dict
        """
        params = {'since': since} if since else {}
        resp = self.get("/v1/project/{}".format(project_path), params)
        return json.load(resp)

    def project_versions(self, project_path):
        """
        Get records of all project's versions (history).

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String

        :rtype: List[Dict]
        """
        resp = self.get("/v1/project/version/{}".format(project_path))
        return json.load(resp)

    def download_project(self, project_path, directory, parallel=True):
        """
        Download latest version of project into given directory.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String

        :param directory: Target directory
        :type directory: String

        :param parallel: Use multi-thread approach to download files in parallel requests, default True
        :type parallel: Boolean
        """
        if os.path.exists(directory):
            raise Exception("Project directory already exists")
        os.makedirs(directory)
        mp = MerginProject(directory)

        project_info = self.project_info(project_path)
        version = project_info['version'] if project_info['version'] else 'v0'

        # sending parallel requests is good for projects with a lot of small files
        if parallel:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures_map = {}
                for file in project_info['files']:
                    file['version'] = version
                    future = executor.submit(self._download_file, project_path, file, directory, parallel)
                    futures_map[future] = file

                for future in concurrent.futures.as_completed(futures_map):
                    file = futures_map[future]
                    try:
                        f_downloaded = future.result(600)
                        # create local backups, so called basefiles
                        if mp.is_versioned_file(file['path']):
                            shutil.copy(mp.fpath(f_downloaded), mp.fpath_meta(f_downloaded))
                    except concurrent.futures.TimeoutError:
                        raise ClientError("Timeout error: failed to download {}".format(file))
        else:
            for file in project_info['files']:
                file['version'] = version
                f_downloaded = self._download_file(project_path, file, directory, parallel)
                if mp.is_versioned_file(file['path']):
                    shutil.copy(mp.fpath(f_downloaded), mp.fpath_meta(f_downloaded))

        mp.metadata = {
            "name": project_path,
            "version": version,
            "files": project_info["files"]
        }

    def push_project(self, directory, parallel=True):
        """
        Upload local changes to the repository.

        :param directory: Project's directory
        :type directory: String
        :param parallel: Use multi-thread approach to upload files in parallel requests, defaults to True
        :type parallel: Boolean
        """
        mp = MerginProject(directory)
        project_path = mp.metadata["name"]
        local_version = mp.metadata["version"]
        server_info = self.project_info(project_path)
        server_version = server_info["version"] if server_info["version"] else "v0"
        if local_version != server_version:
            raise ClientError("Update your local repository")

        changes = mp.get_push_changes()
        if not sum(len(v) for v in changes.values()):
            return
        # drop internal info from being sent to server
        for item in changes['updated']:
            item.pop('origin_checksum', None)
        data = {
            "version": local_version,
            "changes": changes
        }
        server_resp = self._push_changes(mp, data, parallel)
        if 'error' in server_resp:
            #TODO would be good to get some detailed info from server so user could decide what to do with it
            # e.g. diff conflicts, basefiles issues, or any other failure
            raise ClientError(server_resp['error'])

        mp.metadata = {
            'name': project_path,
            'version': server_resp['version'],
            'files': server_resp["files"]
        }
        mp.apply_push_changes(changes)

    def _push_changes(self, mp, data, parallel):
        project = mp.metadata['name']
        resp = self.post(f'/v1/project/push/{project}', data, {"Content-Type": "application/json"})
        info = json.load(resp)

        upload_files = data['changes']["added"] + data['changes']["updated"]
        # upload files' chunks and close transaction
        if upload_files:
            if parallel:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures_map = {}
                    for file in upload_files:
                        file['location'] = mp.fpath_meta(file['diff']['path']) if 'diff' in file else mp.fpath(file['path'])
                        future = executor.submit(self._upload_file, info["transaction"], file, parallel)
                        futures_map[future] = file

                    for future in concurrent.futures.as_completed(futures_map):
                        file = futures_map[future]
                        try:
                            future.result(600)
                        except concurrent.futures.TimeoutError:
                            raise ClientError("Timeout error: failed to upload {}".format(file))
            else:
                for file in upload_files:
                    file['location'] = mp.fpath_meta(file['diff']['path']) if 'diff' in file else mp.fpath(file['path'])
                    self._upload_file(info["transaction"], file, parallel)

            try:
                resp = self.post("/v1/project/push/finish/%s" % info["transaction"])
                info = json.load(resp)
            except ClientError as err:
                self.post("/v1/project/push/cancel/%s" % info["transaction"])
                # server returns various error messages with filename or something generic
                # it would be better if it returned list of failed files (and reasons) whenever possible
                return {'error': str(err)}
        return info

    def pull_project(self, directory, parallel=True):
        """
        Fetch and apply changes from repository.

        :param directory: Project's directory
        :type directory: String
        :param parallel: Use multi-thread approach to fetch files in parallel requests, defaults to True
        :type parallel: Boolean
        """
        mp = MerginProject(directory)
        project_path = mp.metadata["name"]
        local_version = mp.metadata["version"]
        server_info = self.project_info(project_path, since=local_version)
        if local_version == server_info["version"]:
            return  # Project is up to date

        temp_dir = mp.fpath_meta(f'fetch_{local_version}-{server_info["version"]}')
        os.makedirs(temp_dir, exist_ok=True)
        pull_changes = mp.get_pull_changes(server_info["files"])
        fetch_files = []
        for f in pull_changes["added"]:
            f['version'] = server_info['version']
            fetch_files.append(f)
        # extend fetch files download list with various version of diff files (if needed)
        for f in pull_changes["updated"]:
            if 'diffs' in f:
                for diff in f['diffs']:
                    diff_file = copy.deepcopy(f)
                    for k, v in f['history'].items():
                        if 'diff' not in v:
                            continue
                        if diff == v['diff']['path']:
                            diff_file['version'] = k
                            diff_file['diff'] = v['diff']
                    fetch_files.append(diff_file)
            else:
                f['version'] = server_info['version']
                fetch_files.append(f)

        # sending parallel requests is good for projects with a lot of small files
        if parallel:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures_map = {}
                for file in fetch_files:
                    diff_only = 'diffs' in file
                    future = executor.submit(self._download_file, project_path, file, temp_dir, parallel, diff_only)
                    futures_map[future] = file

                for future in concurrent.futures.as_completed(futures_map):
                    file = futures_map[future]
                    try:
                        future.result(600)
                    except concurrent.futures.TimeoutError:
                        raise ClientError("Timeout error: failed to download {}".format(file))
        else:
            for file in fetch_files:
                # TODO check it does not fail, do some retry on ClientError
                diff_only = 'diffs' in file
                self._download_file(project_path, file, temp_dir, parallel, diff_only)

        # make sure we can update geodiff reference files (aka. basefiles) with diffs or
        # download their full versions so we have them up-to-date for applying changes
        for file in pull_changes['updated']:
            if 'diffs' not in file:
                continue
            file['version'] = server_info['version']
            basefile = mp.fpath_meta(file['path'])
            if not os.path.exists(basefile):
                self._download_file(project_path, file, temp_dir, parallel, diff_only=False)

            diffs = [mp.fpath(f, temp_dir) for f in file['diffs']]
            patched_reference = mp.apply_diffs(basefile, diffs)
            if not patched_reference:
                self._download_file(project_path, file, temp_dir, parallel, diff_only=False)

            move_file(patched_reference, mp.fpath(file["path"], temp_dir))

        conflicts = mp.apply_pull_changes(pull_changes, temp_dir)
        mp.metadata = {
            'name': project_path,
            'version': server_info['version'] if server_info['version'] else 'v0',
            'files': server_info['files']
        }
        
        shutil.rmtree(temp_dir)
        return conflicts

    def _download_file(self, project_path, file, directory, parallel=True, diff_only=False):
        """
        Helper to download single project file from server in chunks.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String
        :param file: File metadata item from Project['files']
        :type file: dict
        :param directory: Project's directory
        :type directory: String
        :param parallel: Use multi-thread approach to download parts in parallel requests, default True
        :type parallel: Boolean
        """
        query_params = {
            "file": file['path'],
            "version": file['version'],
            "diff": diff_only
        }
        file_dir = os.path.dirname(os.path.normpath(os.path.join(directory, file['path'])))
        basename = os.path.basename(file['diff']['path']) if diff_only else os.path.basename(file['path'])

        def download_file_part(part):
            """Callback to get a part of file using request to server with Range header."""
            start = part * (1 + CHUNK_SIZE)
            range_header = {"Range": "bytes={}-{}".format(start, start + CHUNK_SIZE)}
            resp = self.get("/v1/project/raw/{}".format(project_path), data=query_params, headers=range_header)
            if resp.status in [200, 206]:
                save_to_file(resp, os.path.join(file_dir, basename + ".{}".format(part)))
            else:
                raise ClientError('Failed to download part {} of file {}'.format(part, basename))

        # download large files in chunks is beneficial mostly for retry on failure
        chunks = math.ceil(file['size'] / CHUNK_SIZE)
        if parallel:
            # create separate n threads, default as cores * 5
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures_map = {executor.submit(download_file_part, i): i for i in range(chunks)}
                for future in concurrent.futures.as_completed(futures_map):
                    i = futures_map[future]
                    try:
                        future.result(300)
                    except concurrent.futures.TimeoutError:
                        raise ClientError('Timeout error: failed to download part {} of file {}'.format(i, basename))
        else:
            for i in range(chunks):
                download_file_part(i)

        # merge chunks together
        with open(os.path.join(file_dir, basename), 'wb') as final:
            for i in range(chunks):
                file_part = os.path.join(file_dir, basename + ".{}".format(i))
                with open(file_part, 'rb') as chunk:
                    shutil.copyfileobj(chunk, final)
                os.remove(file_part)

        return file['path']

    def delete_project(self, project_path):
        """
        Delete project repository on server.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String

        """
        path = "/v1/project/%s" % project_path
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        request = urllib.request.Request(url, method="DELETE")
        self._do_request(request)

    def _upload_file(self, transaction, file_meta, parallel=True):
        """
        Upload file in open upload transaction.

        :param transaction: transaction uuid
        :type transaction: String
        :param project_dir: local project directory
        :type project_dir: String
        :param file_meta: metadata for file to upload
        :type file_meta: Dict
        :param parallel: Use multi-thread approach to upload file chunks in parallel requests, defaults to True
        :type parallel: Boolean
        :raises ClientError: raise on data integrity check failure
        """
        headers = {"Content-Type": "application/octet-stream"}

        def upload_chunk(chunk_id, data):
            checksum = hashlib.sha1()
            checksum.update(data)
            size = len(data)
            resp = self.post("/v1/project/push/chunk/{}/{}".format(transaction, chunk_id), data, headers)
            data = json.load(resp)
            if not (data['size'] == size and data['checksum'] == checksum.hexdigest()):
                self.post("/v1/project/push/cancel/{}".format(transaction))
                raise ClientError("Mismatch between uploaded file chunk {} and local one".format(chunk))

        with open(file_meta['location'], 'rb') as file:
            if parallel:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures_map = {executor.submit(upload_chunk, chunk, file.read(UPLOAD_CHUNK_SIZE)): chunk for chunk in file_meta["chunks"]}
                    for future in concurrent.futures.as_completed(futures_map):
                        chunk = futures_map[future]
                        try:
                            future.result(300)
                        except concurrent.futures.TimeoutError:
                            raise ClientError('Timeout error: failed to upload chunk {}'.format(chunk))
            else:
                for chunk in file_meta["chunks"]:
                    data = file.read(UPLOAD_CHUNK_SIZE)
                    upload_chunk(chunk, data)
