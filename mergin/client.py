import logging
import math
import os
import json
import shutil
import zlib
import base64
import urllib.parse
import urllib.request
import urllib.error
import platform
from datetime import datetime, timezone
import dateutil.parser
import ssl
import re

from .common import ClientError, LoginError, InvalidProject
from .merginproject import MerginProject
from .client_pull import (
    download_file_finalize,
    download_project_async,
    download_file_async,
    download_diffs_async,
    download_project_finalize,
    download_project_wait,
    download_diffs_finalize,
)
from .client_pull import pull_project_async, pull_project_wait, pull_project_finalize
from .client_push import push_project_async, push_project_wait, push_project_finalize
from .utils import DateTimeEncoder, get_versions_with_file_changes, int_version
from .version import __version__

this_dir = os.path.dirname(os.path.realpath(__file__))


class TokenError(Exception):
    pass


def decode_token_data(token):
    token_prefix = "Bearer ."
    if not token.startswith(token_prefix):
        raise TokenError(f"Token doesn't start with 'Bearer .': {token}")
    try:
        data = token[len(token_prefix) :].split(".")[0]
        # add proper base64 padding"
        data += "=" * (-len(data) % 4)
        decoded = zlib.decompress(base64.urlsafe_b64decode(data))
        return json.loads(decoded)
    except (IndexError, TypeError, ValueError, zlib.error):
        raise TokenError(f"Invalid token data: {token}")


class MerginClient:
    """
    Client for Mergin Maps service.

    :param url: String, Mergin Maps service URL.
    :param auth_token: String, Mergin Maps authorization token.
    :param login: String, login for Mergin Maps service.
    :param password: String, password for Mergin Maps service.
    :param plugin_version: String, info about plugin and QGIS version.
    :param proxy_config: Dictionary, proxy settings to use when connecting to Mergin Maps service. At least url and port
        of the server should be provided. Expected keys: "url", "port", "user", "password".
        Currently, only HTTP proxies are supported.
    """

    def __init__(self, url=None, auth_token=None, login=None, password=None, plugin_version=None, proxy_config=None):
        self.url = url if url is not None else MerginClient.default_url()
        self._auth_params = None
        self._auth_session = None
        self._user_info = None
        self.client_version = "Python-client/" + __version__
        if plugin_version is not None:  # this could be e.g. "Plugin/2020.1 QGIS/3.14"
            self.client_version += " " + plugin_version
        self.setup_logging()
        if auth_token:
            try:
                token_data = decode_token_data(auth_token)
                self._auth_session = {"token": auth_token, "expire": dateutil.parser.parse(token_data["expire"])}
                self._user_info = {"username": token_data["username"]}
            except TokenError as e:
                self.log.error(e)
                raise ClientError("Auth token error: " + str(e))
        handlers = []

        # Create handlers for proxy, if needed
        if proxy_config is not None:
            proxy_url_parsed = urllib.parse.urlparse(proxy_config["url"])
            proxy_url = proxy_url_parsed.path
            if proxy_config["user"] and proxy_config["password"]:
                handlers.append(
                    urllib.request.ProxyHandler(
                        {
                            "https": f"{proxy_config['user']}:{proxy_config['password']}@{proxy_url}:{proxy_config['port']}"
                        }
                    )
                )
                handlers.append(urllib.request.HTTPBasicAuthHandler())
            else:
                handlers.append(urllib.request.ProxyHandler({"https": f"{proxy_url}:{proxy_config['port']}"}))

        # fix for wrong macos installation of python certificates,
        # see https://github.com/lutraconsulting/qgis-mergin-plugin/issues/70
        # remove when https://github.com/qgis/QGIS-Mac-Packager/issues/32
        # is fixed.
        default_capath = ssl.get_default_verify_paths().openssl_capath
        if os.path.exists(default_capath):
            self.opener = urllib.request.build_opener(*handlers, urllib.request.HTTPSHandler())
        else:
            cafile = os.path.join(this_dir, "cert.pem")
            if not os.path.exists(cafile):
                raise Exception("missing " + cafile)
            ctx = ssl.SSLContext()
            ctx.load_verify_locations(cafile)
            https_handler = urllib.request.HTTPSHandler(context=ctx)
            self.opener = urllib.request.build_opener(*handlers, https_handler)
        urllib.request.install_opener(self.opener)

        if login and not password:
            raise ClientError("Unable to log in: no password provided for '{}'".format(login))
        if password and not login:
            raise ClientError("Unable to log in: password provided but no username/email")

        if login and password:
            self._auth_params = {"login": login, "password": password}
            if not self._auth_session:
                self.login(login, password)

    def setup_logging(self):
        """Setup Mergin Maps client logging."""
        client_log_file = os.environ.get("MERGIN_CLIENT_LOG", None)
        self.log = logging.getLogger("mergin.client")
        self.log.setLevel(logging.DEBUG)  # log everything (it would otherwise log just warnings+errors)
        if not self.log.handlers:
            if client_log_file:
                log_handler = logging.FileHandler(client_log_file)
                log_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
                self.log.addHandler(log_handler)
            else:
                # no Mergin Maps log path in the environment - create a null handler that does nothing
                null_logger = logging.NullHandler()
                self.log.addHandler(null_logger)

    @staticmethod
    def default_url():
        """Returns URL of the public instance of Mergin Maps"""
        return "https://app.merginmaps.com"

    def user_agent_info(self):
        """Returns string as it is sent as User-Agent http header to the server"""
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
        """Wrapper for creating/renewing authorization token."""

        def wrapper(self, *args):
            if self._auth_params:
                if self._auth_session:
                    # Refresh auth token if it expired or will expire very soon
                    delta = self._auth_session["expire"] - datetime.now(timezone.utc)
                    if delta.total_seconds() < 5:
                        self.log.info("Token has expired - refreshing...")
                        self.login(self._auth_params["login"], self._auth_params["password"])
                else:
                    # Create a new authorization token
                    self.log.info(f"No token - login user: {self._auth_params['login']}")
                    self.login(self._auth_params["login"], self._auth_params["password"])
            return f(self, *args)

        return wrapper

    @_check_token
    def _do_request(self, request):
        """General server request method."""
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
        if "endpoints" not in data:
            return False
        endpoints = data["endpoints"]

        client_endpoints = {
            "data_sync": {
                "GET": ["/project/raw/{namespace}/{project_name}"],
                "POST": [
                    "/project/push/cancel/{transaction_id}",
                    "/project/push/finish/{transaction_id}",
                    "/project/push/{namespace}/{project_name}",
                    # "/project/push/chunk/{transaction_id}/{chunk_id}" # issue in server
                ],
            },
            "project": {
                "DELETE": ["/project/{namespace}/{project_name}"],
                "GET": [
                    "/project",
                    "/project/{namespace}/{project_name}",
                    "/project/version/{namespace}/{project_name}",
                ],
                "POST": ["/project/{namespace}"],
            },
            "user": {"POST": ["/auth/login"]},
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
        params = {"login": login, "password": password}
        self._auth_session = None
        self.log.info(f"Going to log in user {login}")
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
                self.log.info(f"Login problem: {info.get('detail')}")
                raise LoginError(info.get("detail"))
            self.log.info(f"Login problem: {e.read().decode('utf-8')}")
            raise LoginError(e.read().decode("utf-8"))
        except urllib.error.URLError as e:
            # e.g. when DNS resolution fails (no internet connection?)
            raise ClientError("failure reason: " + str(e.reason))
        self._auth_session = {
            "token": "Bearer %s" % session["token"],
            "expire": dateutil.parser.parse(session["expire"]),
        }
        self._user_info = {"username": data["username"]}
        self.log.info(f"User {data['username']} successfully logged in.")
        return session

    def username(self):
        """Returns user name used in this session or None if not authenticated"""
        if not self._user_info:
            return None  # not authenticated
        return self._user_info["username"]

    def user_service(self):
        """
        Requests information about user from /user/service endpoint if such exists in self.url server.

        Returns response from server as JSON dict or None if endpoint is not found
        """

        try:
            response = self.get("/v1/user/service")
        except ClientError as e:
            self.log.debug("Unable to query for /user/service endpoint")
            return

        response = json.loads(response.read())

        return response

    def create_project(self, project_name, is_public=False, namespace=None):
        """
        Create new project repository in user namespace on Mergin Maps server.
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

        params = {"name": project_name, "public": is_public}
        if namespace is None:
            namespace = self.username()
        try:
            self.post("/v1/project/%s" % namespace, params, {"Content-Type": "application/json"})
        except Exception as e:
            detail = f"Namespace: {namespace}, project name: {project_name}"
            raise ClientError(str(e), detail)

    def create_project_and_push(self, project_name, directory, is_public=False, namespace=None):
        """
        Convenience method to create project and push the initial version right after that.
        """
        if os.path.exists(os.path.join(directory, ".mergin")):
            raise ClientError("Directory is already assigned to a Mergin Maps project (contains .mergin sub-dir)")

        if namespace is None:
            namespace = self.username()
        self.create_project(project_name, is_public, namespace)
        if directory:
            mp = MerginProject(directory)
            full_project_name = "{}/{}".format(namespace, project_name)
            mp.metadata = {"name": full_project_name, "version": "v0", "files": []}
            if mp.inspect_files():
                self.push_project(directory)

    def paginated_projects_list(
        self, page=1, per_page=50, tags=None, user=None, flag=None, name=None, namespace=None, order_params=None
    ):
        """
        Find all available Mergin Maps projects.

        :param tags: Filter projects by tags ('valid_qgis', 'mappin_use', input_use')
        :type tags: List

        :param user: Username for 'flag' filter. If not provided, it means user executing request.
        :type user: String

        :param flag: Predefined filter flag ('created', 'shared')
        :type flag: String

        :param name: Filter projects with name like name
        :type name: String

        :param namespace: Filter projects with namespace like namespace
        :type namespace: String

        :param page: Page number for paginated projects list
        :type page: Integer

        :param per_page: Number of projects fetched per page, max 100 (restriction set by server)
        :type per_page: Integer

        :param order_params: optional attributes for sorting the list. It should be a comma separated attribute names
            with _asc or _desc appended for sorting direction. For example: "namespace_asc,disk_usage_desc".
            Available attrs: namespace, name, created, updated, disk_usage, creator
        :type order_params: String

        :rtype: List[Dict]
        """
        params = {}
        if tags:
            params["tags"] = ",".join(tags)
        if user:
            params["user"] = user
        if flag:
            params["flag"] = flag
        if name:
            params["name"] = name
        if namespace:
            params["namespace"] = namespace
        params["page"] = page
        params["per_page"] = per_page
        if order_params is not None:
            params["order_params"] = order_params
        resp = self.get("/v1/project/paginated", params)
        projects = json.load(resp)
        return projects

    def projects_list(self, tags=None, user=None, flag=None, name=None, namespace=None, order_params=None):
        """
        Find all available Mergin Maps projects.

        Calls paginated_projects_list for all pages. Can take significant time to fetch all pages.

        :param tags: Filter projects by tags ('valid_qgis', 'mappin_use', input_use')
        :type tags: List

        :param user: Username for 'flag' filter. If not provided, it means user executing request.
        :type user: String

        :param flag: Predefined filter flag ('created', 'shared')
        :type flag: String

        :param name: Filter projects with name like name
        :type name: String

        :param namespace: Filter projects with namespace like namespace
        :type namespace: String

        :param order_params: optional attributes for sorting the list. It should be a comma separated attribute names
            with _asc or _desc appended for sorting direction. For example: "namespace_asc,disk_usage_desc".
            Available attrs: namespace, name, created, updated, disk_usage, creator
        :type order_params: String

        :rtype: List[Dict]
        """
        projects = []
        page_i = 1
        fetched_projects = 0
        while True:
            resp = self.paginated_projects_list(
                page=page_i,
                per_page=50,
                tags=tags,
                user=user,
                flag=flag,
                name=name,
                namespace=namespace,
                order_params=order_params,
            )
            fetched_projects += len(resp["projects"])
            count = resp["count"]
            projects += resp["projects"]
            if fetched_projects < count:
                page_i += 1
            else:
                break
        return projects

    def project_info(self, project_path, since=None, version=None):
        """
        Fetch info about project.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String
        :param since: Version to track history of geodiff files from
        :type since: String
        :param version: Project version to get details for (particularly list of files)
        :type version: String
        :rtype: Dict
        """
        params = {"since": since} if since else {}
        # since and version are mutually exclusive
        if version:
            params = {"version": version}
        resp = self.get("/v1/project/{}".format(project_path), params)
        return json.load(resp)

    def project_versions(self, project_path, since=None, to=None):
        """
        Get records of project's versions (history) in ascending order.
        If neither 'since' nor 'to' is specified it will return all versions.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String
        :param since: Version to track project history from
        :type since: String
        :param to: Version to track project history to
        :type to: String

        :rtype: List[Dict]
        """
        versions = []
        per_page = 100  # server limit
        num_since = int_version(since) if since else 1
        num_to = int_version(to) if to else None  # we may not know yet
        start_page = math.ceil(num_since / per_page)
        if not num_to:
            # let's get first page and count
            params = {"page": start_page, "per_page": per_page, "descending": False}
            resp = self.get("/v1/project/versions/paginated/{}".format(project_path), params)
            resp_json = json.load(resp)
            versions = resp_json["versions"]
            num_to = resp_json["count"]
            latest_version = int_version(versions[-1]["name"])
            if latest_version < num_to:
                versions += self.project_versions(project_path, f"v{latest_version+1}", f"v{num_to}")
        else:
            end_page = math.ceil(num_to / per_page)
            for page in range(start_page, end_page + 1):
                params = {"page": page, "per_page": per_page, "descending": False}
                resp = self.get("/v1/project/versions/paginated/{}".format(project_path), params)
                versions += json.load(resp)["versions"]

        # filter out versions not within range
        filtered_versions = list(filter(lambda v: (num_since <= int_version(v["name"]) <= num_to), versions))
        return filtered_versions

    def download_project(self, project_path, directory, version=None):
        """
        Download project into given directory. If version is not specified, latest version is downloaded

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String

        :param version: Project version to download, e.g. v42
        :type version: String

        :param directory: Target directory
        :type directory: String
        """
        job = download_project_async(self, project_path, directory, version)
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
        resp = self.get("/v1/user/" + self.username())
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
        json_headers = {"Content-Type": "application/json"}
        try:
            request = urllib.request.Request(url, data=json.dumps(params).encode(), headers=json_headers, method="PUT")
            self._do_request(request)
        except Exception as e:
            detail = f"Project path: {project_path}"
            raise ClientError(str(e), detail)

    def add_user_permissions_to_project(self, project_path, usernames, permission_level):
        """
        Add specified permissions to specified users to project
        :param project_path: project full name (<namespace>/<name>)
        :param usernames: list of usernames to be granted specified permission level
        :param permission_level: string (reader, writer, owner)
        """
        if permission_level not in ["owner", "reader", "writer"]:
            raise ClientError("Unsupported permission level")

        project_info = self.project_info(project_path)
        access = project_info.get("access")
        for name in usernames:
            if permission_level == "owner":
                access.get("ownersnames").append(name)
            if permission_level == "writer" or permission_level == "owner":
                access.get("writersnames").append(name)
            if permission_level == "writer" or permission_level == "owner" or permission_level == "reader":
                access.get("readersnames").append(name)
        self.set_project_access(project_path, access)

    def remove_user_permissions_from_project(self, project_path, usernames):
        """
        Removes specified users from project
        :param project_path: project full name (<namespace>/<name>)
        :param usernames: list of usernames to be granted specified permission level
        """
        project_info = self.project_info(project_path)
        access = project_info.get("access")
        for name in usernames:
            if name in access.get("ownersnames"):
                access.get("ownersnames").remove(name)
            if name in access.get("writersnames"):
                access.get("writersnames").remove(name)
            if name in access.get("readersnames"):
                access.get("readersnames").remove(name)
        self.set_project_access(project_path, access)

    def project_user_permissions(self, project_path):
        """
        Returns permissions for project
        :param project_path: project full name (<namespace>/<name>)
        :return dict("owners": list(usernames),
                     "writers": list(usernames),
                     "readers": list(usernames))
        """
        project_info = self.project_info(project_path)
        access = project_info.get("access")
        result = {}
        result["owners"] = access.get("ownersnames")
        result["writers"] = access.get("writersnames")
        result["readers"] = access.get("readersnames")
        return result

    def push_project(self, directory):
        """
        Upload local changes to the repository.

        :param directory: Project's directory
        :type directory: String
        """
        job = push_project_async(self, directory)
        if job is None:
            return  # there is nothing to push (or we only deleted some files)
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
            return  # project is up to date
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
        json_headers = {"Content-Type": "application/json", "Accept": "application/json"}
        data = {
            "namespace": cloned_project_namespace if cloned_project_namespace else self.username(),
            "project": cloned_project_name,
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
        """Returns JSON with detailed information about a single project version"""
        params = {"version_id": version}
        resp = self.get("/v1/project/version/{}".format(project_path), params)
        return json.load(resp)

    def project_file_history_info(self, project_path, file_path):
        """Returns JSON with full history of a single file within a project"""
        params = {"path": file_path}
        resp = self.get("/v1/resource/history/{}".format(project_path), params)
        return json.load(resp)

    def project_file_changeset_info(self, project_path, file_path, version):
        """Returns JSON with changeset details of a particular version of a file within a project"""
        params = {"path": file_path}
        resp = self.get("/v1/resource/changesets/{}/{}".format(project_path, version), params)
        return json.load(resp)

    def get_projects_by_names(self, projects):
        """Returns JSON with projects' info for list of required projects.
        The schema of the returned information is the same as the response from projects_list().

        This is useful when we have a couple of Mergin Maps projects available locally and we want to
        find out their status at once (e.g. whether there is a new version on the server).

        :param projects: list of projects in the form 'namespace/project_name'
        :type projects: List[String]
        """

        resp = self.post("/v1/project/by_names", {"projects": projects}, {"Content-Type": "application/json"})
        return json.load(resp)

    def download_file(self, project_dir, file_path, output_filename, version=None):
        """
        Download project file at specified version. Get the latest if no version specified.

        :param project_dir: project local directory
        :type project_dir: String
        :param file_path: relative path of file to download in the project directory
        :type file_path: String
        :param output_filename: full destination path for saving the downloaded file
        :type output_filename: String
        :param version: optional version tag for downloaded file
        :type version: String
        """
        job = download_file_async(self, project_dir, file_path, output_filename, version=version)
        pull_project_wait(job)
        download_file_finalize(job)

    def get_file_diff(self, project_dir, file_path, output_diff, version_from, version_to):
        """Create concatenated diff for project file diffs between versions version_from and version_to.

        :param project_dir: project local directory
        :type project_dir: String
        :param file_path: relative path of file to download in the project directory
        :type file_path: String
        :param output_diff: full destination path for concatenated diff file
        :type output_diff: String
        :param version_from: starting project version tag for getting diff, for example 'v3'
        :type version_from: String
        :param version_to: ending project version tag for getting diff
        :type version_to: String
        """
        mp = MerginProject(project_dir)
        project_path = mp.metadata["name"]
        file_history = self.project_file_history_info(project_path, file_path)
        versions_to_fetch = get_versions_with_file_changes(
            self, project_path, file_path, version_from=version_from, version_to=version_to, file_history=file_history
        )
        self.download_file_diffs(project_dir, file_path, versions_to_fetch[1:])

        # collect required versions from the cache
        diffs = []
        for v in versions_to_fetch[1:]:
            diffs.append(mp.fpath_cache(file_history["history"][v]["diff"]["path"], v))

        # concatenate diffs, if needed
        output_dir = os.path.dirname(output_diff)
        if len(diffs) >= 1:
            os.makedirs(output_dir, exist_ok=True)
            if len(diffs) > 1:
                mp.geodiff.concat_changes(diffs, output_diff)
            elif len(diffs) == 1:
                shutil.copy(diffs[0], output_diff)

    def download_file_diffs(self, project_dir, file_path, versions):
        """Download file diffs for specified versions if they are not present
        in the cache.

        :param project_dir: project local directory
        :type project_dir: String
        :param file_path: relative path of file to download in the project directory
        :type file_path: String
        :param versions: list of versions to download diffs for, for example ['v3', 'v5']
        :type versions: List(String)
        :returns: list of downloaded diffs (their actual locations on disk)
        :rtype: List(String)
        """
        job = download_diffs_async(self, project_dir, file_path, versions)
        pull_project_wait(job)
        diffs = download_diffs_finalize(job)
        return diffs

    def has_unfinished_pull(self, directory):
        """
        Test whether project has an unfinished pull.

        Unfinished pull means that a previous pull_project() call has
        failed in the final stage due to some files being in read-only
        mode. When a project has unfinished pull, it has to be resolved
        before allowing further pulls or pushes.

        .. seealso:: self.resolve_unfinished_pull

        :param directory: project's directory
        :type directory: String
        :returns: project has unfinished pull
        rtype: Boolean
        """
        mp = MerginProject(directory)

        return mp.has_unfinished_pull()

    def resolve_unfinished_pull(self, directory):
        """
        Try to resolve unfinished pull.

        Unfinished pull means that a previous pull_project() call has
        failed in the final stage due to some files being in read-only
        mode. When a project has unfinished pull, it has to be resolved
        before allowing further pulls or pushes.

        To resolving unfinihed pull includes creation of conflicted copy
        and replacement of the original file by the new version from the
        server.

        .. seealso:: self.has_unfinished_pull

        :param directory: project's directory
        :type directory: str
        :returns: files where conflicts were found
        :rtype: list[str]
        """
        mp = MerginProject(directory)
        conflicts = mp.resolve_unfinished_pull(self.username())
        return conflicts

    def has_writing_permissions(self, project_path):
        """
        Test whether user has writing permissions on the given project.
        We rely on the "permissions" field here, as it provides more accurate
        information and take into account namespace permissions too.

        :param project_path: project's path on server in the form or namespace/project
        :type directory: str
        :returns: whether user has writing (upload) permission on the project
        :rtype: bool
        """
        info = self.project_info(project_path)
        return info["permissions"]["upload"]
