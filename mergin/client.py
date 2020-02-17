import os
import json
import zlib
import base64
import shutil
import urllib.parse
import urllib.request
import math
import hashlib
import copy
import platform
from datetime import datetime, timezone
import dateutil.parser
import concurrent.futures
import ssl

from pip._vendor import distro

from .common import CHUNK_SIZE, UPLOAD_CHUNK_SIZE, ClientError
from .merginproject import MerginProject
from .client_pull import download_project_async, download_project_wait, download_project_finalize
from .client_pull import pull_project_async, pull_project_wait, pull_project_finalize
from .client_push import push_project_async, push_project_wait, push_project_finalize
from .utils import save_to_file, generate_checksum, DateTimeEncoder, do_sqlite_checkpoint


class LoginError(Exception):
    pass


class SyncError(Exception):
    def __init__(self, msg, detail=""):
        super().__init__(msg)
        self.detail = detail


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
    def __init__(self, url, auth_token=None, login=None, password=None, plugin_version=None):
        self.url = url

        self._auth_params = None
        self._auth_session = None
        self._user_info = None
        self.client_version = plugin_version if plugin_version is not None else "Python-client/--"
        if auth_token:
            token_data = decode_token_data(auth_token)
            self._auth_session = {
                "token": auth_token,
                "expire": dateutil.parser.parse(token_data["expire"])
            }
            self._user_info = {
                "username": token_data["username"]
            }

        # fix for wrong macos installation of python certificates,
        # see https://github.com/lutraconsulting/qgis-mergin-plugin/issues/70
        # remove when https://github.com/qgis/QGIS-Mac-Packager/issues/32
        # is fixed.
        default_capath = ssl.get_default_verify_paths().openssl_capath
        if os.path.exists(default_capath):
            self.opener = urllib.request.build_opener()
        else:
            cafile = os.path.join(this_dir, 'cert.pem')
            if not os.path.exists(cafile):
                raise Exception("missing " + cafile)
            ctx = ssl.SSLContext()
            ctx.load_verify_locations(cafile)
            https_handler = urllib.request.HTTPSHandler(context=ctx)
            self.opener = urllib.request.build_opener(https_handler)
        urllib.request.install_opener(self.opener)

        if login and password:
            self._auth_params = {
                "login": login,
                "password": password
            }
            if not self._auth_session:
                self.login(login, password)

    def _check_token(f):
        def wrapper(self, *args):
            if (not self._auth_session or self._auth_session['expire'] < datetime.now(timezone.utc)) and self._auth_params:
                self.login(self._auth_params['login'], self._auth_params['password'])
            return f(self, *args)
        return wrapper

    @_check_token
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
                system_version = "Unknown"
                if platform.system() == "Linux":
                    system_version = distro.linux_distribution()[0]
                elif platform.system() == "Windows":
                    system_version =  platform.win32_ver()[0]
                elif platform.system() == "Mac":
                    system_version = platform.mac_ver()[0]
                request.add_header("User-Agent", f"{self.client_version} ({platform.system()}/{system_version})")
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
        try:
            self._auth_params = params
            url = urllib.parse.urljoin(self.url, urllib.parse.quote("/v1/auth/login"))
            data = json.dumps(self._auth_params, cls=DateTimeEncoder).encode("utf-8")
            request = urllib.request.Request(url, data, {"Content-Type": "application/json"}, method="POST")
            resp = self.opener.open(request)
            data = json.load(resp)
            session = data["session"]
        except urllib.error.HTTPError as e:
            if e.headers.get("Content-Type", "") == "application/problem+json":
                info = json.load(e)
                raise LoginError(info.get("detail"))
            raise LoginError(e.read().decode("utf-8"))
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
        try:
            self.post("/v1/project/%s" % namespace, params, {"Content-Type": "application/json"})
            data = {
                "name": "%s/%s" % (namespace, project_name),
                "version": "v0",
                "files": []
            }
        except Exception as e:
            detail = f"Namespace: {namespace}, project name: {project_name}"
            raise SyncError(str(e), detail)
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
        job = download_project_async(self, project_path, directory)
        download_project_wait(job)
        download_project_finalize(job)

    def download_project_old(self, project_path, directory, parallel=True):
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

    def enough_storage_available(self, data):
        user_name = self._user_info["username"]
        resp = self.get('/v1/user/' + user_name)
        info = json.load(resp)
        free_space = int(info["storage_limit"]) - int(info["disk_usage"])
        upload_files_sizes =[f["size"] for f in data["updated"] + data["added"]]
        size_to_upload = sum(upload_files_sizes)

        if size_to_upload > free_space:
            return False, free_space

        return True, free_space

    def push_project(self, directory, parallel=True):
        job = push_project_async(self, directory)
        if job is None:
            return   # there is nothing to push (or we only deleted some files)
        push_project_wait(job)
        push_project_finalize(job)

    def push_project_old(self, directory, parallel=True):
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
        enough_free_space, freespace = self.enough_storage_available(changes)
        if not enough_free_space:
            freespace = int(freespace/(1024*1024))
            raise SyncError("Storage limit has been reached. Only " + str(freespace) + "MB left")

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
        job = pull_project_async(self, directory)
        if job is None:
            return   # project is up to date
        pull_project_wait(job)
        return pull_project_finalize(job)

    def pull_project_old(self, directory, parallel=True):
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
            server_file = mp.fpath(file["path"], temp_dir)
            if os.path.exists(basefile):
                shutil.copy(basefile, server_file)
                diffs = [mp.fpath(f, temp_dir) for f in file['diffs']]
                patch_error = mp.apply_diffs(server_file, diffs)
                if patch_error:
                    # we can't use diffs, overwrite with full version of file fetched from server
                    self._download_file(project_path, file, temp_dir, parallel, diff_only=False)
            else:
                self._download_file(project_path, file, temp_dir, parallel, diff_only=False)

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
        expected_size = file['diff']['size'] if diff_only else file['size']

        if file['size'] == 0:
            os.makedirs(file_dir, exist_ok=True)
            open(os.path.join(file_dir, basename), 'w').close()
            return file["path"]

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

        if os.path.getsize(os.path.join(file_dir, basename)) != expected_size:
            os.remove(os.path.join(file_dir, basename))
            raise ClientError(f'Download of file {basename} failed. Please try it again.')

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
    def project_status(self, directory):
        """
        Get project status, e.g. server and local changes.

        :param directory: Project's directory
        :type directory: String
        :returns: changes metadata for files modified on server, and for those modified locally
        :rtype: dict, dict
        """
        mp = MerginProject(directory)
        project_path = mp.metadata["name"]
        local_version = mp.metadata["version"]
        server_info = self.project_info(project_path, since=local_version)
        pull_changes = mp.get_pull_changes(server_info["files"])

        push_changes = mp.get_push_changes()
        push_changes_summary = mp.get_list_of_push_changes(push_changes)

        return pull_changes, push_changes, push_changes_summary
