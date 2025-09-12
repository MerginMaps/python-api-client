import os
from enum import Enum

CHUNK_SIZE = 100 * 1024 * 1024

# there is an upper limit for chunk size on server, ideally should be requested from there once implemented
UPLOAD_CHUNK_SIZE = 10 * 1024 * 1024

# number of attempts to upload a chunk
UPLOAD_CHUNK_ATTEMPTS = 2

# seconds to wait between attempts of uploading a chunk
UPLOAD_CHUNK_ATTEMPT_WAIT = 5

# size of the log file part to send (if file is larger only this size from end will be sent)
MAX_LOG_FILE_SIZE_TO_SEND = 5 * 1024 * 1024

# number of attempts to push changes (in case of network issues etc)
PUSH_ATTEMPTS = 10

# seconds to wait between attempts to push changes
PUSH_ATTEMPT_WAIT = 5

# seconds to wait between sync callback calls
SYNC_CALLBACK_WAIT = 0.01

# default URL for submitting logs
MERGIN_DEFAULT_LOGS_URL = "https://g4pfq226j0.execute-api.eu-west-1.amazonaws.com/mergin_client_log_submit"

this_dir = os.path.dirname(os.path.realpath(__file__))


# Error code from the public API, add to the end of enum as we handle more eror
class ErrorCode(Enum):
    ProjectsLimitHit = "ProjectsLimitHit"
    StorageLimitHit = "StorageLimitHit"
    ProjectVersionExists = "ProjectVersionExists"
    AnotherUploadRunning = "AnotherUploadRunning"


class ClientError(Exception):
    def __init__(self, detail: str, url=None, server_code=None, server_response=None, http_error=None, http_method=None):
        self.detail = detail
        self.url = url
        self.http_error = http_error
        self.http_method = http_method

        self.server_code = server_code
        self.server_response = server_response

        self.extra = None

    def __str__(self):
        string_res = f"Detail: {self.detail}\n"
        if self.http_error:
            string_res += f"HTTP Error: {self.http_error}\n"
        if self.url:
            string_res += f"URL: {self.url}\n"
        if self.http_method:
            string_res += f"Method: {self.http_method}\n"
        if self.server_code:
            string_res += f"Error code: {self.server_code}\n"
        if self.extra:
            string_res += f"{self.extra}\n"
        return string_res

    def is_rate_limit(self) -> bool:
        """Check if error is rate limit error based on server code"""
        return self.http_error == 429

    def is_blocking_sync(self) -> bool:
        """
        Check if error is blocking sync based on server code.
        Blocking sync means, that the sync is blocked by another user in server.
        """
        return self.server_code in [
            ErrorCode.AnotherUploadRunning.value,
            ErrorCode.ProjectVersionExists.value,
        ]

    def is_retryable_sync(self) -> bool:
        # Check if error is retryable based on server code
        if self.is_blocking_sync() or self.is_rate_limit():
            return True

        # Check retryable based on http error and message from v1 sync API endpoint
        if (
            self.http_error
            and self.detail
            and self.http_error == 400
            and ("Another process" in self.detail or "Version mismatch" in self.detail)
        ):
            return True

        return False


class LoginError(Exception):
    pass


class InvalidProject(Exception):
    pass


try:
    import dateutil.parser
    from dateutil.tz import tzlocal
except ImportError:
    # this is to import all dependencies shipped with package (e.g. to use in qgis-plugin)
    deps_dir = os.path.join(this_dir, "deps")
    if os.path.exists(deps_dir):
        import sys

        for f in os.listdir(os.path.join(deps_dir)):
            sys.path.append(os.path.join(deps_dir, f))

        import dateutil.parser
        from dateutil.tz import tzlocal


class WorkspaceRole(Enum):
    """
    Workspace roles
    """

    GUEST = "guest"
    READER = "reader"
    EDITOR = "editor"
    WRITER = "writer"
    ADMIN = "admin"
    OWNER = "owner"


class ProjectRole(Enum):
    """
    Project roles
    """

    READER = "reader"
    EDITOR = "editor"
    WRITER = "writer"
    OWNER = "owner"
