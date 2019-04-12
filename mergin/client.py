import os
import json
import base64
import shutil
import urllib.parse
import urllib.request

from .utils import save_to_file, generate_checksum, move_file
from .multipart import MultipartReader, MultipartEncoder, Field, parse_boundary


class InvalidProject(Exception):
    pass

class ClientError(Exception):
    pass


def find(items, fn):
    for item in items:
        if fn(item):
            return item

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


class MerginClient:

    def __init__(self, url, username=None, password=None):
        self.url = url

        # auth_handler = urllib.request.HTTPBasicAuthHandler()
        # auth_handler.add_password("", url, username, password)
        # opener = urllib.request.build_opener(auth_handler)

        self.opener = urllib.request.build_opener()
        urllib.request.install_opener(self.opener)
        if username and password:
            auth_string = "{}:{}".format(username, password)
            self._auth_header = base64.standard_b64encode(auth_string.encode("utf-8")).decode("utf-8")
        else:
            self._auth_header = None

    def _do_request(self, request):
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
        if self._auth_header:
            request.add_header("Authorization", "Basic {}".format(self._auth_header))
        return self._do_request(request)

    def post(self, path, data, headers={}):
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        if headers.get("Content-Type", None) == "application/json":
            data = json.dumps(data).encode("utf-8")
        request = urllib.request.Request(url, data, headers)
        if self._auth_header:
            request.add_header("Authorization", "Basic {}".format(self._auth_header))
        return self._do_request(request)

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
        if not os.path.exists(directory):
            raise Exception("Project directory does not exists")

        params = {
            "name": project_name,
            "public": is_public
        }
        self.post("/v1/project", params, {"Content-Type": "application/json"})
        data = {
            "name": project_name,
            "version": "",
            "files": None
        }
        save_project_file(directory, data)
        self.push_project(directory)

    def projects_list(self, tags=None):
        """
        Find all available mergin projects.

        :param tags: Filter projects by tags ('valid_qgis', 'mappin_use', input_use')
        :type tags: List

        :rtype: List[Dict]
        """
        params = {"tags": ",".join(tags)} if tags else None
        resp = self.get("/v1/project", params)
        projects = json.load(resp)
        return projects

    def project_info(self, project_name):
        """
        Fetch info about project.

        :param project_name: Project's name
        :type project_name: String

        :rtype: Dict
        """
        resp = self.get("/v1/project/{}".format(project_name))
        return json.load(resp)

    def project_versions(self, project_name):
        """
        Get records of all project's versions (history).

        :param project_name: Project's name
        :type project_name: String

        :rtype: List[Dict]
        """
        resp = self.get("/v1/project/version/{}".format(project_name))
        return json.load(resp)

    def download_project(self, project_name, directory):
        """
        Download last version of project into given directory.

        :param project_name: Project's name
        :type project_name: String

        :param directory: Target directory
        :type directory: String
        """
        if os.path.exists(directory):
            raise Exception("Project directory already exists")
        os.makedirs(directory)

        # TODO: this shouldn't be two independent operations, or we should use version tag in download requests.
        # But current server API doesn't allow something better.
        project_info = self.project_info(project_name)
        resp = self.get("/v1/project/download/{}".format(project_name))
        reader = MultipartReader(resp, parse_boundary(resp.headers["Content-Type"]))
        part = reader.next_part()
        while part:
            dest = os.path.join(directory, part.filename)
            save_to_file(part, dest)
            part = reader.next_part()

        data = {
            "name": project_name,
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
        project_name = local_info["name"]
        server_info = self.project_info(project_name)
        if local_info.get("version", "") != server_info.get("version", ""):
            raise Exception("Update your local repository")

        files = list_project_directory(directory)
        changes = project_changes(server_info["files"], files)
        count = sum(len(items) for items in changes.values())
        if count:
            def fields():
                yield Field("changes", json.dumps(changes).encode("utf-8"))
                for file in (changes["added"] + changes["updated"]):
                    path = file["path"]
                    with open(os.path.join(directory, path), "rb") as f:
                        yield Field(path, f, filename=path, content_type="application/octet-stream")

            encoder = MultipartEncoder(fields())
            resp = self.post("/v1/project/data_sync/{}".format(project_name), encoder, encoder.get_headers())
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
        project_name = local_info["name"]

        server_info = self.project_info(project_name)
        files = list_project_directory(directory)
        changes = project_changes(files, server_info["files"])

        def local_path(path):
            return os.path.join(directory, path)

        def can_overwrite(file):
            return False

        fetch_files = changes["added"] + changes["updated"]
        if fetch_files:
            resp = self.post(
                "/v1/project/fetch/{}".format(project_name),
                fetch_files,
                {"Content-Type": "application/json"}
            )
            reader = MultipartReader(resp, parse_boundary(resp.headers["Content-Type"]))
            part = reader.next_part()
            temp_dir = os.path.join(directory, '.mergin', 'fetch_{}'.format(server_info["version"]))
            while part:
                print("Fetching", part.filename)
                dest = os.path.join(temp_dir, part.filename)
                save_to_file(part, dest)
                part = reader.next_part()

            for file in fetch_files:
                src = os.path.join(temp_dir, file["path"])
                move_file(src, local_path(file["path"]))
            shutil.rmtree(temp_dir)

        for file in changes["removed"]:
            print("Deleting", file["path"])
            os.remove(local_path(file["path"]))

        for file in changes["renamed"]:
            print("Renaming", file["path"], "->", file["new_path"])
            move_file(local_path(file["path"]), local_path(file["new_path"]))

        local_info["files"] = server_info["files"]
        local_info["version"] = server_info["version"]
        save_project_file(directory, local_info)
