import os


CHUNK_SIZE = 100 * 1024 * 1024

# there is an upper limit for chunk size on server, ideally should be requested from there once implemented
UPLOAD_CHUNK_SIZE = 10 * 1024 * 1024


this_dir = os.path.dirname(os.path.realpath(__file__))


class ClientError(Exception):
    def __init__(self, detail, url=None, server_code=None, server_response=None, http_error=None, http_method=None):
        self.detail = detail
        self.url = url
        self.http_error = http_error
        self.http_method = http_method

        self.server_code = server_code
        self.server_response = server_response

        self.extra = None

    def __str__(self):
        return (
            f"Detail: {self.detail}\n" + f"HTTP Error: {self.http_error}\n"
            if self.http_error
            else (
                None + f"URL: {self.url}\n"
                if self.url
                else (
                    None + f"Method: {self.http_method}\n"
                    if self.http_method
                    else None + f"{self.extra}\n" if self.extra else None
                )
            )
        )


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
