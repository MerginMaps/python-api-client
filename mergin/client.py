import os
import json
import zlib
import base64
import urllib.parse
import urllib.request
import urllib.error
import platform
from datetime import datetime, timezone
import dateutil.parser
import ssl

from .common import ClientError, LoginError
from .merginproject import MerginProject
from .client_pull import download_project_async, download_project_wait, download_project_finalize
from .client_pull import pull_project_async, pull_project_wait, pull_project_finalize
from .client_push import push_project_async, push_project_wait, push_project_finalize
from .utils import DateTimeEncoder
from .version import __version__


this_dir = os.path.dirname(os.path.realpath(__file__))


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
    def __init__(self, url=None, auth_token=None, login=None, password=None, plugin_version=None):
        self.url = url if url is not None else MerginClient.default_url()

        self._auth_params = None
        self._auth_session = None
        self._user_info = None
        self.client_version = "Python-client/" + __version__
        if plugin_version is not None:   # this could be e.g. "Plugin/2020.1 QGIS/3.14"
            self.client_version += " " + plugin_version
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

        if login and not password:
            raise ClientError("Unable to log in: no password provided for '{}'".format(login))
        if password and not login:
            raise ClientError("Unable to log in: password provided but no username/email")

        if login and password:
            self._auth_params = {
                "login": login,
                "password": password
            }
            if not self._auth_session:
                self.login(login, password)

    @staticmethod
    def default_url():
        """ Returns URL of the public instance of Mergin """
        return 'https://public.cloudmergin.com'

    def user_agent_info(self):
        """ Returns string as it is sent as User-Agent http header to the server """
        system_version = "Unknown"
        if platform.system() == "Linux":
            try:
                from pip._vendor import distro
                system_version = distro.linux_distribution()[0]
            except ModuleNotFoundError:  # pip may not be installed...
                system_version = "Linux"
        elif platform.system() == "Windows":
            system_version = platform.win32_ver()[0]
        elif platform.system() in ["Mac", "Darwin"]:
            system_version = platform.mac_ver()[0]
        return f"{self.client_version} ({platform.system()}/{system_version})"

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
        request.add_header("User-Agent", self.user_agent_info())
        try:
            return self.opener.open(request)
        except urllib.error.HTTPError as e:
            if e.headers.get("Content-Type", "") == "application/problem+json":
                info = json.load(e)
                raise ClientError(info.get("detail"))
            raise ClientError(e.read().decode("utf-8"))
        except urllib.error.URLError as e:
            # e.g. when DNS resolution fails (no internet connection?)
            raise ClientError("Error requesting " + request.full_url + ": " + str(e))

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
            request.add_header("User-Agent", self.user_agent_info())
            resp = self.opener.open(request)
            data = json.load(resp)
            session = data["session"]
        except urllib.error.HTTPError as e:
            if e.headers.get("Content-Type", "") == "application/problem+json":
                info = json.load(e)
                raise LoginError(info.get("detail"))
            raise LoginError(e.read().decode("utf-8"))
        except urllib.error.URLError as e:
            # e.g. when DNS resolution fails (no internet connection?)
            raise ClientError("Error trying to log in: " + str(e))
        self._auth_session = {
            "token": "Bearer %s" % session["token"],
            "expire": dateutil.parser.parse(session["expire"])
        }
        self._user_info = {
            "username": data["username"]
        }
        return session

    def username(self):
        """ Returns user name used in this session or None if not authenticated """
        if not self._user_info:
            return None  # not authenticated
        return self._user_info["username"]

    def create_project(self, project_name, is_public=False, namespace=None):
        """
        Create new project repository in user namespace on Mergin server.
        Optionally initialized from given local directory.

        :param project_name: Project name
        :type project_name: String

        :param is_public: Flag for public/private project, defaults to False
        :type is_public: Boolean

        :param namespace: Optional namespace for a new project. If empty username is used.
        :type namespace: String
        """
        if not self._user_info:
            raise Exception("Authentication required")

        params = {
            "name": project_name,
            "public": is_public
        }
        if namespace is None:
            namespace = self.username()
        try:
            self.post("/v1/project/%s" % namespace, params, {"Content-Type": "application/json"})
        except Exception as e:
            detail = f"Namespace: {namespace}, project name: {project_name}"
            raise ClientError(str(e), detail)

    def create_project_and_push(self, project_name, directory, is_public=False):
        """
        Convenience method to create project and push the initial version right after that.
        """
        self.create_project(project_name, is_public)
        if directory:
            mp = MerginProject(directory)
            full_project_name = "{}/{}".format(self.username(), project_name)
            mp.metadata = {"name": full_project_name, "version": "v0", "files": []}
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

    def download_project(self, project_path, directory):
        """
        Download latest version of project into given directory.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String

        :param directory: Target directory
        :type directory: String
        """
        job = download_project_async(self, project_path, directory)
        download_project_wait(job)
        download_project_finalize(job)

    def enough_storage_available(self, data):
        info = self.user_info()
        free_space = int(info["storage_limit"]) - int(info["disk_usage"])
        upload_files_sizes = [f["size"] for f in data["updated"] + data["added"]]
        size_to_upload = sum(upload_files_sizes)

        if size_to_upload > free_space:
            return False, free_space

        return True, free_space

    def user_info(self):
        resp = self.get('/v1/user/' + self.username())
        return json.load(resp)

    def set_project_access(self, project_path, access):
        """
        Updates access for given project.
        :param project_path: project full name (<namespace>/<name>)
        :param access: dict <readersnames, writersnames, ownersnames> -> list of str username we want to give access to
        """
        if not self._user_info:
            raise Exception("Authentication required")

        params = {"access": access}
        path = "/v1/project/%s" % project_path
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        json_headers = {'Content-Type': 'application/json'}
        try:
            request = urllib.request.Request(url, data=json.dumps(params).encode(), headers=json_headers, method="PUT")
            self._do_request(request)
        except Exception as e:
            detail = f"Project path: {project_path}"
            raise ClientError(str(e), detail)


    def push_project(self, directory):
        """
        Upload local changes to the repository.

        :param directory: Project's directory
        :type directory: String
        """
        job = push_project_async(self, directory)
        if job is None:
            return   # there is nothing to push (or we only deleted some files)
        push_project_wait(job)
        push_project_finalize(job)

    def pull_project(self, directory):
        """
        Fetch and apply changes from repository.

        :param directory: Project's directory
        :type directory: String
        """
        job = pull_project_async(self, directory)
        if job is None:
            return   # project is up to date
        pull_project_wait(job)
        return pull_project_finalize(job)

    def clone_project(self, source_project_path, cloned_project_name, cloned_project_namespace=None):
        """
        Clone project on server.
        :param source_project_path: Project's full name (<namespace>/<name>)
        :type source_project_path: String
        :param cloned_project_name: Cloned project's name
        :type cloned_project_name: String
        :param cloned_project_namespace: Cloned project's namespace, username is used if not defined
        :type cloned_project_namespace: String

        """
        path = "/v1/project/clone/%s" % source_project_path
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        json_headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        data = {
            'namespace': cloned_project_namespace if cloned_project_namespace else self.username(),
            'project': cloned_project_name
        }

        request = urllib.request.Request(url, data=json.dumps(data).encode(), headers=json_headers, method="POST")
        self._do_request(request)

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

    def project_version_info(self, project_path, version):
        """ Returns JSON with detailed information about a single project version"""
        params = {'version_id': version}
        resp = self.get("/v1/project/version/{}".format(project_path), params)
        return json.load(resp)

    def project_file_history_info(self, project_path, file_path):
        """ Returns JSON with full history of a single file within a project """
        params = {'path': file_path}
        resp = self.get("/v1/resource/history/{}".format(project_path), params)
        return json.load(resp)

    def project_file_changeset_info(self, project_path, file_path, version):
        """ Returns JSON with changeset details of a particular version of a file within a project """
        params = {'path': file_path}
        resp = self.get("/v1/resource/changesets/{}/{}".format(project_path, version), params)
        return json.load(resp)

    def get_projects_by_names(self, projects):
        """ Returns JSON with projects' info for list of required projects.
        The schema of the returned information is the same as the response from projects_list().

        This is useful when we have a couple of Mergin projects available locally and we want to
        find out their status at once (e.g. whether there is a new version on the server).

        :param projects: list of projects in the form 'namespace/project_name'
        :type projects: List[String]
        """

        resp = self.post("/v1/project/by_names", {"projects": projects}, {"Content-Type": "application/json"})
        return json.load(resp)
