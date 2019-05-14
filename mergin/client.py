import os
import re
import json
import zlib
import base64
import shutil
import urllib.parse
import urllib.request
from datetime import datetime

this_dir = os.path.dirname(os.path.realpath(__file__))

try:
    from requests_toolbelt import MultipartEncoder
    import pytz
    import dateutil.parser
except ImportError:
    # this is to import all dependencies shipped with package (e.g. to use in qgis-plugin)
    deps_dir = os.path.join(this_dir, 'deps')
    if os.path.exists(deps_dir):
        import sys
        for f in os.listdir(os.path.join(deps_dir)):
            sys.path.append(os.path.join(deps_dir, f))

        from requests_toolbelt import MultipartEncoder
        import pytz
        import dateutil.parser

from .utils import save_to_file, generate_checksum, move_file
from .multipart import MultipartReader, parse_boundary


class InvalidProject(Exception):
    pass

class ClientError(Exception):
    pass


def find(items, fn):
    for item in items:
        if fn(item):
            return item


# TODO: maybe following functions (or some of them) could be
# encapsulated in the new MerginProject class

def list_project_directory(directory):
    prefix = os.path.abspath(directory) # .rstrip(os.path.sep)
    proj_files = []
    excluded_dirs = ['.mergin']
    for root, dirs, files in os.walk(directory, topdown=True):
        dirs[:] = [d for d in dirs if d not in excluded_dirs]
        for file in files:
            abs_path = os.path.abspath(os.path.join(root, file))
            proj_path = abs_path[len(prefix) + 1:]
            proj_files.append({
                "path": proj_path,
                "checksum": generate_checksum(abs_path),
                "size": os.path.getsize(abs_path)
            })
    return proj_files


def project_changes(origin, current):
    origin_map = {f["path"]: f for f in origin}
    current_map = {f["path"]: f for f in current}
    removed = [f for f in origin if f["path"] not in current_map]
    added = [f for f in current if f["path"] not in origin_map]

    # updated = list(filter(
    #     lambda f: f["path"] in origin_map and f["checksum"] != origin_map[f["path"]]["checksum"],
    #     current
    # ))
    updated = []
    for f in current:
        path = f["path"]
        if path in origin_map and f["checksum"] != origin_map[path]["checksum"]:
            updated.append(f)

    moved = []
    for rf in removed:
        match = find(
            current,
            lambda f: f["checksum"] == rf["checksum"] and f["size"] == rf["size"] and all(f["path"] != mf["path"] for mf in moved)
        )
        if match:
            moved.append({**rf, "new_path": match["path"]})

    added = [f for f in added if all(f["path"] != mf["new_path"] for mf in moved)]
    removed = [f for f in removed if all(f["path"] != mf["path"] for mf in moved)]

    return {
        "renamed": moved,
        "added": added,
        "removed": removed,
        "updated": updated
    }

def inspect_project(directory):
    meta_dir = os.path.join(directory, '.mergin')
    info_file = os.path.join(meta_dir, 'mergin.json')
    if not os.path.exists(meta_dir) or not os.path.exists(info_file):
        raise InvalidProject(directory)

    with open(info_file, 'r') as f:
        return json.load(f)

def save_project_file(project_directory, data):
    meta_dir = os.path.join(project_directory, '.mergin')
    if not os.path.exists(meta_dir):
        os.makedirs(meta_dir)

    project_file = os.path.join(meta_dir, 'mergin.json')
    with open(project_file, 'w') as f:
        json.dump(data, f, indent=2)

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
    except (IndexError, TypeError, ValueError):
        raise ValueError("Invalid token data")


class MerginClient:
    min_server_version = '2019.3-22'

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
            delta = self._auth_session["expire"] - datetime.now(pytz.utc)
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

    def get(self, path, data=None):
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        if data:
            url += "?" + urllib.parse.urlencode(data)
        request = urllib.request.Request(url)
        return self._do_request(request)

    def post(self, path, data, headers={}):
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        if headers.get("Content-Type", None) == "application/json":
            data = json.dumps(data).encode("utf-8")
        request = urllib.request.Request(url, data, headers)
        return self._do_request(request)

    def server_version(self):
        """
        Return version of the server

        rtype: String
        """
        resp = self.get("/ping")
        try:
            data = json.load(resp)
            return data["version"]
        except:
            raise ClientError("Unknown server")

    def check_version(self):
        """
        Test whether version of the server meets the minimum required value

        rtype: Boolean
        """
        server_version = self.server_version()

        if server_version == "dev":
            return True

        def parse_version(string):
            m = re.match("^(\d+).(\d+)-(\d+)", string)
            if m:
                return list(map(int, m.groups()))

        try:
            s1, s2, s3 = parse_version(server_version)
            m1, m2, m3 = parse_version(self.min_server_version)
            return s1 > m1 or (s1 == m1 and (s2 > m2 or (s2 == m2 and s3 >= m3)))
        except (TypeError, ValueError):
            raise ClientError("Unknown server version: %s" % server_version)

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
            "token" : session["token"],
            "expire": dateutil.parser.parse(session["expire"])
        }
        self._user_info = {
            "username": data["username"]
        }
        return session

    def create_project(self, project_name, directory, is_public=False):
        """
        Create new project repository on Mergin server from given directory.

        :param project_name: Project's name
        :type project_name: String

        :param directory: Project's directory
        :type directory: String

        :param is_public: Flag for public/private project
        :type directory: Boolean
        """
        if not self._user_info:
            raise Exception("Authentication required")
        if not os.path.exists(directory):
            raise Exception("Project directory does not exists")

        params = {
            "name": project_name,
            "public": is_public
        }
        namespace = self._user_info["username"]
        self.post("/v1/project/%s" % namespace, params, {"Content-Type": "application/json"})
        data = {
            "name": "%s/%s" % (namespace, project_name),
            "version": "",
            "files": None
        }
        save_project_file(directory, data)
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

    def project_info(self, project_path):
        """
        Fetch info about project.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String

        :rtype: Dict
        """
        resp = self.get("/v1/project/{}".format(project_path))
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

    def download_project(self, project_path, directory):
        """
        Download last version of project into given directory.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String

        :param directory: Target directory
        :type directory: String
        """
        if os.path.exists(directory):
            raise Exception("Project directory already exists")
        os.makedirs(directory)

        # TODO: this shouldn't be two independent operations, or we should use version tag in download requests.
        # But current server API doesn't allow something better.
        project_info = self.project_info(project_path)
        resp = self.get("/v1/project/download/{}".format(project_path))
        reader = MultipartReader(resp, parse_boundary(resp.headers["Content-Type"]))
        part = reader.next_part()
        while part:
            dest = os.path.join(directory, part.filename)
            save_to_file(part, dest)
            part = reader.next_part()

        data = {
            "name": project_path,
            "version": project_info["version"],
            "files": project_info["files"]
        }
        save_project_file(directory, data)

    def push_project(self, directory):
        """
        Upload local changes to the repository.

        :param directory: Project's directory
        :type directory: String
        """
        local_info = inspect_project(directory)
        project_path = local_info["name"]
        server_info = self.project_info(project_path)
        if local_info.get("version", "") != server_info.get("version", ""):
            raise Exception("Update your local repository")

        files = list_project_directory(directory)
        changes = project_changes(server_info["files"], files)
        count = sum(len(items) for items in changes.values())
        if count:
            # Custom MultipartEncoder doesn't compute Content-Length,
            # which is currently required by gunicorn.
            # def fields():
            #     yield Field("changes", json.dumps(changes).encode("utf-8"))
            #     for file in (changes["added"] + changes["updated"]):
            #         path = file["path"]
            #         with open(os.path.join(directory, path), "rb") as f:
            #             yield Field(path, f, filename=path, content_type="application/octet-stream")
            # encoder = MultipartEncoder(fields())

            fields = {"changes": json.dumps(changes).encode("utf-8")}
            for file in (changes["added"] + changes["updated"]):
                path = file["path"]
                fields[path] = (path, open(os.path.join(directory, path), 'rb'), "application/octet-stream")
            encoder = MultipartEncoder(fields=fields)
            headers = {
                "Content-Type": encoder.content_type,
                "Content-Length": encoder.len
            }
            resp = self.post("/v1/project/data_sync/{}".format(project_path), encoder, headers=headers)
            new_project_info = json.load(resp)
            local_info["files"] = new_project_info["files"]
            local_info["version"] = new_project_info["version"]
            save_project_file(directory, local_info)

    def pull_project(self, directory):
        """
        Fetch and apply changes from repository.

        :param directory: Project's directory
        :type directory: String
        """

        local_info = inspect_project(directory)
        project_path = local_info["name"]
        server_info = self.project_info(project_path)

        if local_info["version"] == server_info["version"]:
            return # Project is up to date

        files = list_project_directory(directory)
        local_changes = project_changes(files, local_info["files"])
        pull_changes = project_changes(local_info["files"], server_info["files"])

        def local_path(path):
            return os.path.join(directory, path)

        locally_modified = list(
            [f["path"] for f in local_changes["added"] + local_changes["updated"]] + \
            [f["new_path"] for f in local_changes["renamed"]]
        )

        def backup_if_conflict(path, checksum):
            if path in locally_modified:
                current_file = find(files, lambda f: f["path"] == path)
                if current_file["checksum"] != checksum:
                    backup_path = local_path("{}_conflict_copy".format(path))
                    index = 2
                    while os.path.exists(backup_path):
                        backup_path = local_path("{}_conflict_copy{}".format(path, index))
                        index += 1
                    shutil.copy(local_path(path), backup_path)

        fetch_files = pull_changes["added"] + pull_changes["updated"]
        if fetch_files:
            resp = self.post(
                "/v1/project/fetch/{}".format(project_path),
                fetch_files,
                {"Content-Type": "application/json"}
            )
            reader = MultipartReader(resp, parse_boundary(resp.headers["Content-Type"]))
            part = reader.next_part()
            temp_dir = os.path.join(directory, '.mergin', 'fetch_{}-{}'.format(local_info["version"], server_info["version"]))
            while part:
                dest = os.path.join(temp_dir, part.filename)
                save_to_file(part, dest)
                part = reader.next_part()

            for file in fetch_files:
                src = os.path.join(temp_dir, file["path"])
                dest = local_path(file["path"])
                backup_if_conflict(file["path"], file["checksum"])
                move_file(src, dest)
            shutil.rmtree(temp_dir)

        for file in pull_changes["removed"]:
            backup_if_conflict(file["path"], file["checksum"])
            os.remove(local_path(file["path"]))

        for file in pull_changes["renamed"]:
            backup_if_conflict(file["new_path"], file["checksum"])
            move_file(local_path(file["path"]), local_path(file["new_path"]))

        local_info["files"] = server_info["files"]
        local_info["version"] = server_info["version"]
        save_project_file(directory, local_info)
