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
from enum import Enum, auto
import re
import typing
import warnings

from typing import List

from .common import ClientError, LoginError, WorkspaceRole, ProjectRole, LOG_FILE_SIZE_TO_SEND, MERGIN_DEFAULT_LOGS_URL
from .merginproject import MerginProject
from .client_pull import (
    download_file_finalize,
    download_project_async,
    download_file_async,
    download_files_async,
    download_files_finalize,
    download_diffs_async,
    download_project_finalize,
    download_project_wait,
    download_diffs_finalize,
)
from .client_pull import pull_project_async, pull_project_wait, pull_project_finalize
from .client_push import push_project_async, push_project_wait, push_project_finalize
from .utils import DateTimeEncoder, get_versions_with_file_changes, int_version, is_version_acceptable
from .version import __version__

this_dir = os.path.dirname(os.path.realpath(__file__))
json_headers = {"Content-Type": "application/json"}


class TokenError(Exception):
    pass


class AuthTokenExpiredError(Exception):
    pass


class ServerType(Enum):
    OLD = auto()  # Server is old and does not support workspaces
    CE = auto()  # Server is Community Edition
    EE = auto()  # Server is Enterprise Edition
    SAAS = auto()  # Server is SaaS


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

    def __init__(
        self,
        url=None,
        auth_token=None,
        login=None,
        password=None,
        plugin_version=None,
        proxy_config=None,
    ):
        self.url = url if url is not None else MerginClient.default_url()
        self._auth_params = None
        self._auth_session = None
        self._user_info = None
        self._server_type = None
        self._server_version = None
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

                system_version = distro.id().capitalize()
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
                        if self._auth_params.get("login", None) and self._auth_params.get("password", None):
                            self.log.info("Token has expired - refreshing...")
                            self.login(self._auth_params["login"], self._auth_params["password"])
                        else:
                            raise AuthTokenExpiredError("Token has expired - please re-login")
                else:
                    # Create a new authorization token
                    self.log.info(f"No token - login user: {self._auth_params['login']}")
                    if self._auth_params.get("login", None) and self._auth_params.get("password", None):
                        self.login(self._auth_params["login"], self._auth_params["password"])
                    else:
                        raise ClientError("Missing login or password")

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
            server_response = json.load(e)

            err_detail = None
            server_code = None
            # Try to get error detail
            if isinstance(server_response, dict):
                server_code = server_response.get("code")
                err_detail = server_response.get("detail")
                if not err_detail:
                    # Extract all field-specific errors and format them
                    err_detail = "\n".join(
                        f"{key}: {', '.join(map(str, value))}"
                        for key, value in server_response.items()
                        if isinstance(value, list)
                    ) or str(
                        server_response
                    )  # Fallback to raw response if structure is unexpected
            else:
                err_detail = str(server_response)

            raise ClientError(
                detail=err_detail,
                url=request.get_full_url(),
                server_code=server_code,
                server_response=server_response,
                http_error=e.code,
                http_method=request.get_method(),
            )
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

    def patch(self, path, data=None, headers={}):
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        if headers.get("Content-Type", None) == "application/json":
            data = json.dumps(data, cls=DateTimeEncoder).encode("utf-8")
        request = urllib.request.Request(url, data, headers, method="PATCH")
        return self._do_request(request)

    def delete(self, path):
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        request = urllib.request.Request(url, method="DELETE")
        return self._do_request(request)

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

    def workspace_service(self, workspace_id):
        """
        This Requests information about a workspace service from /workspace/{id}/service endpoint,
        if such exists in self.url server.

        Returns response from server as JSON dict or None if endpoint is not found
        """
        resp = self.get(f"/v1/workspace/{workspace_id}/service")
        return json.load(resp)

    def workspace_usage(self, workspace_id):
        """
        This Requests information about a workspace usage from /workspace/{id}/usage endpoint,
        if such exists in self.url server.

        Returns response from server as JSON dict or None if endpoint is not found
        """

        resp = self.get(f"/v1/workspace/{workspace_id}/usage")
        return json.load(resp)

    def server_type(self):
        """
        Returns the deployment type of the server

        The value is cached for self's lifetime

        :returns: ServerType of server deployment
        :rtype: ServerType
        """
        if not self._server_type:
            try:
                resp = self.get("/config")
                config = json.load(resp)
                if config["server_type"] == "ce":
                    self._server_type = ServerType.CE
                elif config["server_type"] == "ee":
                    self._server_type = ServerType.EE
                elif config["server_type"] == "saas":
                    self._server_type = ServerType.SAAS
            except (ClientError, KeyError):
                self._server_type = ServerType.OLD

        return self._server_type

    def server_version(self):
        """
        Returns version of the server

        :returns: Version string, e.g. "2023.5.0". For old servers (pre-2023) this may be empty string.
        :rtype: str
        """
        if self._server_version is None:
            try:
                resp = self.get("/config")
                config = json.load(resp)
                self._server_version = config["version"]
            except (ClientError, KeyError):
                self._server_version = ""

        return self._server_version

    def workspaces_list(self):
        """
        Find all available workspaces

        :rtype: List[Dict]
        """
        resp = self.get("/v1/workspaces")
        workspaces = json.load(resp)
        return workspaces

    def create_workspace(self, workspace_name):
        """
        Create new workspace for currently active user.

        :param workspace_name: Workspace name to create
        :type workspace_name: String
        """
        if not self._user_info:
            raise Exception("Authentication required")

        params = {"name": workspace_name}

        try:
            self.post("/v1/workspace", params, {"Content-Type": "application/json"})
        except ClientError as e:
            e.extra = f"Workspace name: {workspace_name}"
            raise e

    def create_project(self, project_name, is_public=False, namespace=None):
        """
        Create new project repository in user namespace on Mergin Maps server.
        Optionally initialized from given local directory.

        :param project_name: Project's full name (<namespace>/<name>)
        :type project_name: String

        :param is_public: Flag for public/private project, defaults to False
        :type is_public: Boolean

        :param namespace: Deprecated. project_name should be full project name. Optional namespace for a new project. If empty username is used.
        :type namespace: String
        """
        if not self._user_info:
            raise Exception("Authentication required")

        if namespace and "/" not in project_name:
            warnings.warn(
                "The usage of `namespace` parameter in `create_project()` is deprecated."
                "Specify `project_name` as full name (<namespace>/<name>) instead.",
                category=DeprecationWarning,
            )

        if "/" in project_name:
            if namespace:
                warnings.warn(
                    "Parameter `namespace` specified with full project name (<namespace>/<name>)."
                    "The parameter will be ignored."
                )

            namespace, project_name = project_name.split("/")

        elif namespace is None:
            warnings.warn(
                "The use of only project name in `create_project()` is deprecated."
                "The `project_name` should be full name (<namespace>/<name>).",
                category=DeprecationWarning,
            )

        params = {"name": project_name, "public": is_public}
        if namespace is None:
            namespace = self.username()
        try:
            self.post(f"/v1/project/{namespace}", params, {"Content-Type": "application/json"})
        except ClientError as e:
            e.extra = f"Namespace: {namespace}, project name: {project_name}"
            raise e

    def create_project_and_push(self, project_name, directory, is_public=False, namespace=None):
        """
        Convenience method to create project and push the initial version right after that.

        :param project_name: Project's full name (<namespace>/<name>)
        :type project_name: String

        :param namespace: Deprecated. project_name should be full project name. Optional namespace for a new project. If empty username is used.
        :type namespace: String
        """
        if os.path.exists(os.path.join(directory, ".mergin")):
            raise ClientError("Directory is already assigned to a Mergin Maps project (contains .mergin sub-dir)")

        if namespace and "/" not in project_name:
            warnings.warn(
                "The usage of `namespace` parameter in `create_project_and_push()` is deprecated."
                "Specify `project_name` as full name (<namespace>/<name>) instead.",
                category=DeprecationWarning,
            )
            project_name = f"{namespace}/{project_name}"

        if "/" in project_name:
            if namespace:
                warnings.warn(
                    "Parameter `namespace` specified with full project name (<namespace>/<name>)."
                    "The parameter will be ignored."
                )

        elif namespace is None:
            warnings.warn(
                "The use of only project name in `create_project()` is deprecated."
                "The `project_name` should be full name (<namespace>/<name>).",
                category=DeprecationWarning,
            )
            namespace = self.username()
            project_name = f"{namespace}/{project_name}"

        self.create_project(project_name, is_public)
        if directory:
            project_info = self.project_info(project_name)
            MerginProject.write_metadata(directory, project_info)
            mp = MerginProject(directory)
            if mp.inspect_files():
                self.push_project(directory)

    def paginated_projects_list(
        self,
        page=1,
        per_page=50,
        tags=None,
        user=None,
        flag=None,
        name=None,
        only_namespace=None,
        namespace=None,
        order_params=None,
        only_public=None,
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

        :param only_namespace: Filter projects with namespace exactly equal to namespace
        :type namespace: String

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

        :param only_public: Only fetch public projects
        :type only_public: Bool

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
        if only_namespace:
            params["only_namespace"] = only_namespace
        elif namespace:
            params["namespace"] = namespace
        if only_public:
            params["only_public"] = only_public
        params["page"] = page
        params["per_page"] = per_page
        if order_params is not None:
            params["order_params"] = order_params
        resp = self.get("/v1/project/paginated", params)
        projects = json.load(resp)
        return projects

    def projects_list(
        self,
        tags=None,
        user=None,
        flag=None,
        name=None,
        only_namespace=None,
        namespace=None,
        order_params=None,
        only_public=None,
    ):
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

        :param only_namespace: Filter projects with namespace exactly equal to namespace
        :type namespace: String

        :param namespace: Filter projects with namespace like namespace
        :type namespace: String

        :param order_params: optional attributes for sorting the list. It should be a comma separated attribute names
            with _asc or _desc appended for sorting direction. For example: "namespace_asc,disk_usage_desc".
            Available attrs: namespace, name, created, updated, disk_usage, creator
        :type order_params: String

        :param only_public: Only fetch public projects
        :type only_public: Bool

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
                only_namespace=only_namespace,
                namespace=namespace,
                order_params=order_params,
                only_public=only_public,
            )
            fetched_projects += len(resp["projects"])
            count = resp["count"]
            projects += resp["projects"]
            if fetched_projects < count:
                page_i += 1
            else:
                break
        return projects

    def project_info(self, project_path_or_id, since=None, version=None):
        """
        Fetch info about project.

        :param project_path_or_id: Project's full name (<namespace>/<name>) or id
        :type project_path_or_id: String
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

        if re.match("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", project_path_or_id):
            resp = self.get("/v1/project/by_uuid/{}".format(project_path_or_id), params)
        else:
            resp = self.get("/v1/project/{}".format(project_path_or_id), params)
        return json.load(resp)

    def paginated_project_versions(self, project_path, page, per_page=100, descending=False):
        """
        Get records of project's versions (history) using calculated pagination.
        wrapper around the /v1/project/versions/paginated/{} API end point

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String | Int
        :param page: page number
        :type page: Int
        :param per_page: number of results per page default 100
        :type per_page: Int
        :param descending: order of sorting
        :type descending: Bool

        :rtype: List[Dict], Int
        """
        params = {"page": page, "per_page": per_page, "descending": descending}
        resp = self.get("/v1/project/versions/paginated/{}".format(project_path), params)
        resp_json = json.load(resp)
        return resp_json["versions"], resp_json["count"]

    def project_versions_count(self, project_path):
        """
        return the total count of versions
        To note we fetch only one page and one item as we only need the "count" response

        :param project_path_or_id: Project's full name (<namespace>/<name>) or id
        :type project_path_or_id: String

        :rtype: Int
        """
        params = {"page": 1, "per_page": 1, "descending": False}
        resp = self.get("/v1/project/versions/paginated/{}".format(project_path), params)
        resp_json = json.load(resp)
        return resp_json["count"]

    def project_versions(self, project_path, since=1, to=None):
        """
        Get records of project's versions (history) in ascending order.
        If neither 'since' nor 'to' is specified it will return all versions.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String
        :param since: Version to track project history from
        :type since: String | Int
        :param to: Version to track project history to
        :type to: String | Int

        :rtype: List[Dict]
        """
        versions = []
        per_page = 100  # server limit

        if type(since) == str:
            num_since = int_version(since)
        else:
            # keep the since parameter as is
            num_since = since

        if type(to) == str:
            num_to = int_version(to)
        else:
            # keep the to parameter as is
            num_to = to

        start_page = math.ceil(num_since / per_page)
        if not num_to:
            # let's get first page and count
            versions, num_to = self.paginated_project_versions(project_path, start_page, per_page)

            latest_version = int_version(versions[-1]["name"])
            if latest_version < num_to:
                versions += self.project_versions(project_path, f"v{latest_version+1}", f"v{num_to}")
        else:
            end_page = math.ceil(num_to / per_page)
            for page in range(start_page, end_page + 1):
                page_versions, _ = self.paginated_project_versions(project_path, page, per_page)
                versions += page_versions

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

    def user_info(self):
        server_type = self.server_type()
        if server_type == ServerType.OLD:
            resp = self.get("/v1/user/" + self.username())
        else:
            resp = self.get("/v1/user/profile")
        return json.load(resp)

    def set_project_access(self, project_path, access):
        """
        Updates access for given project.
        :param project_path: project full name (<namespace>/<name>)
        :param access: dict <readersnames, editorsnames, writersnames, ownersnames> -> list of str username we want to give access to (editorsnames are only supported on servers at version 2024.4.0 or later)
        """
        if "editorsnames" in access and not self.has_editor_support():
            raise NotImplementedError("Editors are only supported on servers at version 2024.4.0 or later")

        if not self._user_info:
            raise Exception("Authentication required")

        params = {"access": access}
        path = "/v1/project/%s" % project_path
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        json_headers = {"Content-Type": "application/json"}
        try:
            request = urllib.request.Request(url, data=json.dumps(params).encode(), headers=json_headers, method="PUT")
            self._do_request(request)
        except ClientError as e:
            e.extra = f"Project path: {project_path}"
            raise e

    def add_user_permissions_to_project(self, project_path, usernames, permission_level):
        """
        Add specified permissions to specified users to project
        :param project_path: project full name (<namespace>/<name>)
        :param usernames: list of usernames to be granted specified permission level
        :param permission_level: string (reader, editor, writer, owner)

        Editor permission_level is only supported on servers at version 2024.4.0 or later.
        """
        if permission_level not in ["owner", "reader", "writer", "editor"] or (
            permission_level == "editor" and not self.has_editor_support()
        ):
            raise ClientError("Unsupported permission level")

        project_info = self.project_info(project_path)
        access = project_info.get("access")
        for name in usernames:
            if permission_level == "owner":
                access.get("ownersnames").append(name)
            if permission_level in ("writer", "owner"):
                access.get("writersnames").append(name)
            if permission_level in ("writer", "owner", "editor") and "editorsnames" in access:
                access.get("editorsnames").append(name)
            if permission_level in ("writer", "owner", "editor", "reader"):
                access.get("readersnames").append(name)
        self.set_project_access(project_path, access)
        warnings.warn(
            "This method will be deprecated in the next major release (1.0.0)"
            "Use `add_project_collaborator` to create a project permission and "
            "`update_project_collaborator` to change it instead.",
            category=DeprecationWarning,
        )

    def remove_user_permissions_from_project(self, project_path, usernames):
        """
        Removes specified users from project
        :param project_path: project full name (<namespace>/<name>)
        :param usernames: list of usernames to be granted specified permission level
        """
        project_info = self.project_info(project_path)
        access = project_info.get("access")
        for name in usernames:
            if name in access.get("ownersnames", []):
                access.get("ownersnames").remove(name)
            if name in access.get("writersnames", []):
                access.get("writersnames").remove(name)
            if name in access.get("editorsnames", []):
                access.get("editorsnames").remove(name)
            if name in access.get("readersnames", []):
                access.get("readersnames").remove(name)
        self.set_project_access(project_path, access)
        warnings.warn(
            "This method will be deprecated in the next major release (1.0.0)"
            "Use `remove_project_collaborator` instead.",
            category=DeprecationWarning,
        )

    def project_user_permissions(self, project_path):
        """
        Returns permissions for project
        :param project_path: project full name (<namespace>/<name>)
        :return dict("owners": list(usernames),
                     "writers": list(usernames),
                     "editors": list(usernames) - only on servers at version 2024.4.0 or later,
                     "readers": list(usernames))
        """
        project_info = self.project_info(project_path)
        access = project_info.get("access")
        result = {}
        if "editorsnames" in access:
            result["editors"] = access.get("editorsnames", [])

        result["owners"] = access.get("ownersnames", [])
        result["writers"] = access.get("writersnames", [])
        result["readers"] = access.get("readersnames", [])
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
        :param cloned_project_name: Cloned project's full name (<namespace>/<name>)
        :type cloned_project_name: String
        :param cloned_project_namespace: Deprecated. cloned_project_name should be full project name. Cloned project's namespace, username is used if not defined
        :type cloned_project_namespace: String

        """

        if cloned_project_namespace and "/" not in cloned_project_name:
            warnings.warn(
                "The usage of `cloned_project_namespace` parameter in `clone_project()` is deprecated."
                "Specify `cloned_project_name` as full name (<namespace>/<name>) instead.",
                category=DeprecationWarning,
            )

        if "/" in cloned_project_name:
            if cloned_project_namespace:
                warnings.warn(
                    "Parameter `cloned_project_namespace` specified with full cloned project name (<namespace>/<name>)."
                    "The parameter will be ignored."
                )

            cloned_project_namespace, cloned_project_name = cloned_project_name.split("/")

        elif cloned_project_namespace is None:
            warnings.warn(
                "The use of only project name as `cloned_project_name` in `clone_project()` is deprecated."
                "The `cloned_project_name` should be full name (<namespace>/<name>).",
                category=DeprecationWarning,
            )

        path = f"/v1/project/clone/{source_project_path}"
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        json_headers = {"Content-Type": "application/json", "Accept": "application/json"}
        data = {
            "namespace": cloned_project_namespace if cloned_project_namespace else self.username(),
            "project": cloned_project_name,
        }
        request = urllib.request.Request(url, data=json.dumps(data).encode(), headers=json_headers, method="POST")
        self._do_request(request)

    def delete_project_now(self, project_path):
        """
        Delete project repository on server immediately.

        This should be typically in development, e.g. auto tests, where we
        do not want scheduled project deletes as done by delete_project().

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String

        """
        # TODO: this version check should be replaced by checking against the list
        # of endpoints that server publishes in /config (once implemented)
        if not is_version_acceptable(self.server_version(), "2023.5"):
            raise NotImplementedError("This needs server at version 2023.5 or later")

        project_info = self.project_info(project_path)
        project_id = project_info["id"]
        path = "/v2/projects/" + project_id
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        request = urllib.request.Request(url, method="DELETE")
        self._do_request(request)

    def delete_project(self, project_path):
        """
        Delete project repository on server. Newer servers since 2023
        will schedule deletion of the project, but not fully remove it immediately.
        This is the preferred way when the removal is user initiated, so that
        it is not possible to replace the project with another one with the same
        name, which would confuse clients that do not use project IDs yet.

        There is also delete_project_now() on newer servers which deletes projects
        immediately.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String

        """
        # TODO: newer server version (since 2023.5.0) have a new endpoint
        # (POST /v2/projects/<id>/scheduleDelete) that does the same thing,
        # but using project ID instead of the name. At some point we may
        # want to migrate to it.
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
        server_info = self.project_info(mp.project_full_name(), since=mp.version())

        pull_changes = mp.get_pull_changes(server_info["files"])

        push_changes = mp.get_push_changes()
        push_changes_summary = mp.get_list_of_push_changes(push_changes)

        return pull_changes, push_changes, push_changes_summary

    def project_version_info(self, project_id, version):
        """Returns JSON with detailed information about a single project version"""
        resp = self.get(f"/v1/project/version/{project_id}/{version}")
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
        project_path = mp.project_full_name()
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

    def rename_project(self, project_path: str, new_project_name: str) -> None:
        """
        Rename project on server.

        :param project_path: Project's full name (<namespace>/<name>)
        :type project_path: String
        :param new_project_name: Project's new name (<name>)
        :type new_project_name: String
        """
        # TODO: this version check should be replaced by checking against the list
        # of endpoints that server publishes in /config (once implemented)
        if not is_version_acceptable(self.server_version(), "2023.5.4"):
            raise NotImplementedError("This needs server at version 2023.5.4 or later")

        if "/" in new_project_name:
            raise ClientError(
                "Project's new name should be without workspace specification (<name>). Project can only be renamed within its current workspace."
            )

        project_info = self.project_info(project_path)
        project_id = project_info["id"]
        path = "/v2/projects/" + project_id
        url = urllib.parse.urljoin(self.url, urllib.parse.quote(path))
        json_headers = {"Content-Type": "application/json"}
        data = {"name": new_project_name}
        request = urllib.request.Request(
            url, data=json.dumps(data).encode("utf-8"), headers=json_headers, method="PATCH"
        )
        self._do_request(request)

    def reset_local_changes(self, directory: str, files_to_reset: typing.List[str] = None) -> None:
        """
        Reset local changes to either all files or only listed files.
        Added files are removed, removed files are brought back and updates are discarded.

        :param directory: Project's directory
        :type directory: String
        :param files_to_reset List of files to reset, relative paths of file
        :type files_to_reset: List of strings, default None
        """
        all_files = files_to_reset is None

        mp = MerginProject(directory)

        current_version = mp.version()

        push_changes = mp.get_push_changes()

        files_download = []

        # remove all added files
        for file in push_changes["added"]:
            if all_files or file["path"] in files_to_reset:
                os.remove(mp.fpath(file["path"]))

        # update files get override with previous version
        for file in push_changes["updated"]:
            if all_files or file["path"] in files_to_reset:
                if mp.is_versioned_file(file["path"]):
                    mp.geodiff.make_copy_sqlite(mp.fpath_meta(file["path"]), mp.fpath(file["path"]))
                else:
                    files_download.append(file["path"])

        # removed files are redownloaded
        for file in push_changes["removed"]:
            if all_files or file["path"] in files_to_reset:
                files_download.append(file["path"])

        if files_download:
            self.download_files(directory, files_download, version=current_version)

    def download_files(
        self, project_dir: str, file_paths: typing.List[str], output_paths: typing.List[str] = None, version: str = None
    ):
        """
        Download project files at specified version. Get the latest if no version specified.

        :param project_dir: project local directory
        :type project_dir: String
        :param file_path: List of relative paths of files to download in the project directory
        :type file_path: List[String]
        :param output_paths: List of paths for files to download to. Should be same length of as file_path. Default is `None` which means that files are downloaded into MerginProject at project_dir.
        :type output_paths: List[String]
        :param version: optional version tag for downloaded file
        :type version: String
        """
        job = download_files_async(self, project_dir, file_paths, output_paths, version=version)
        pull_project_wait(job)
        download_files_finalize(job)

    def has_editor_support(self):
        """
        Returns whether the server version is acceptable for editor support.
        """
        return is_version_acceptable(self.server_version(), "2024.4.0")

    def create_user(
        self,
        email: str,
        password: str,
        workspace_id: int,
        workspace_role: WorkspaceRole,
        username: str = None,
        notify_user: bool = False,
    ) -> dict:
        """
        Create a new user in a workspace. The username is generated from the email address.

        param email: email of the new user - must be unique
        param password: password - must meet the requirements
        param workspace_id: id of the workspace user is created in
        param workspace_role: workspace role of the user
        param username: username - will be autogenerated from the email if not provided
        param notify_user: flag for email notifications - confirmation email will be sent
        """
        params = {
            "email": email,
            "password": password,
            "workspace_id": workspace_id,
            "role": workspace_role.value,
            "notify_user": notify_user,
        }
        if username:
            params["username"] = username
        user_info = self.post("v2/users", params, json_headers)
        return json.load(user_info)

    def get_workspace_member(self, workspace_id: int, user_id: int) -> dict:
        """
        Get a workspace member detail
        """
        resp = self.get(f"v2/workspaces/{workspace_id}/members/{user_id}")
        return json.load(resp)

    def list_workspace_members(self, workspace_id: int) -> List[dict]:
        """
        Get a list of workspace members
        """
        resp = self.get(f"v2/workspaces/{workspace_id}/members")
        return json.load(resp)

    def update_workspace_member(
        self, workspace_id: int, user_id: int, workspace_role: WorkspaceRole, reset_projects_roles: bool = False
    ) -> dict:
        """
        Update workspace role of a workspace member, optionally resets the projects role

        param reset_projects_roles: all project specific roles will be removed
        """
        params = {
            "reset_projects_roles": reset_projects_roles,
            "workspace_role": workspace_role.value,
        }
        workspace_member = self.patch(f"v2/workspaces/{workspace_id}/members/{user_id}", params, json_headers)
        return json.load(workspace_member)

    def remove_workspace_member(self, workspace_id: int, user_id: int):
        """
        Remove a user from workspace members
        """
        self.delete(f"v2/workspaces/{workspace_id}/members/{user_id}")

    def list_project_collaborators(self, project_id: str) -> List[dict]:
        """
        Get a list of project collaborators
        """
        project_collaborators = self.get(f"v2/projects/{project_id}/collaborators")
        return json.load(project_collaborators)

    def add_project_collaborator(self, project_id: str, user: str, project_role: ProjectRole) -> dict:
        """
        Add a user to project collaborators and grant them a project role.
        Fails if user is already a member of the project.

        param user: login (username or email) of the user
        """
        params = {"role": project_role.value, "user": user}
        project_collaborator = self.post(f"v2/projects/{project_id}/collaborators", params, json_headers)
        return json.load(project_collaborator)

    def update_project_collaborator(self, project_id: str, user_id: int, project_role: ProjectRole) -> dict:
        """
        Update project role of the existing project collaborator.
        Fails if user is not a member of the project yet.
        """
        params = {"role": project_role.value}
        project_collaborator = self.patch(f"v2/projects/{project_id}/collaborators/{user_id}", params, json_headers)
        return json.load(project_collaborator)

    def remove_project_collaborator(self, project_id: str, user_id: int):
        """
        Remove a user from project collaborators
        """
        self.delete(f"v2/projects/{project_id}/collaborators/{user_id}")

    def server_config(self) -> dict:
        """Get server configuration as dictionary."""
        response = self.get("/config")
        return json.load(response)

    def send_logs(
        self,
        logfile: str,
        global_log_file: typing.Optional[str] = None,
        application: typing.Optional[str] = None,
        meta: typing.Optional[str] = None,
    ):
        """Send logs to configured server or the default Mergin server."""

        if application is None:
            application = "mergin-client-{}".format(__version__)

        params = {"app": application, "username": self.username()}

        config = self.server_config()
        diagnostic_logs_url = config.get("diagnostic_logs_url", None)

        use_server_api = False
        if is_version_acceptable(self.server_version(), "2025.4.1") and (
            diagnostic_logs_url is None or diagnostic_logs_url == ""
        ):
            url = "v2/diagnostic-logs" + "?" + urllib.parse.urlencode(params)
            use_server_api = True
        else:
            if diagnostic_logs_url:
                url = diagnostic_logs_url + "?" + urllib.parse.urlencode(params)
            else:
                # fallback to default logs URL
                url = MERGIN_DEFAULT_LOGS_URL + "?" + urllib.parse.urlencode(params)

        if meta is None:
            meta = "Python API Client\nSystem: {} \nMergin Maps URL: {} \nMergin Maps user: {} \n--------------------------------\n\n".format(
                platform.system(), self.url, self.username()
            )

        global_logs = b""
        if global_log_file and os.path.exists(global_log_file):
            with open(global_log_file, "rb") as f:
                if os.path.getsize(global_log_file) > LOG_FILE_SIZE_TO_SEND:
                    f.seek(-LOG_FILE_SIZE_TO_SEND, os.SEEK_END)
                global_logs = f.read() + b"\n--------------------------------\n\n"

        with open(logfile, "rb") as f:
            if os.path.getsize(logfile) > 512 * 1024:
                f.seek(-512 * 1024, os.SEEK_END)
            logs = f.read()

        payload = meta.encode() + global_logs + logs
        header = {"content-type": "text/plain"}

        if use_server_api:
            return self.post(url, data=payload, headers=header)
        else:
            request = urllib.request.Request(url, data=payload, headers=header)
            return self._do_request(request)
