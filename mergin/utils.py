import os
import io
import json
import hashlib
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from .common import ClientError


def generate_checksum(file, chunk_size=4096):
    """
    Generate checksum for file from chunks.

    :param file: file to calculate checksum
    :param chunk_size: size of chunk
    :return: sha1 checksum
    """
    checksum = hashlib.sha1()
    with open(file, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                return checksum.hexdigest()
            checksum.update(chunk)


def save_to_file(stream, path):
    """
    Save readable object in file.
    :param stream: object implementing readable interface
    :param path: destination file path
    """
    directory = os.path.abspath(os.path.dirname(path))

    os.makedirs(directory, exist_ok=True)

    with open(path, "wb") as output:
        writer = io.BufferedWriter(output, buffer_size=32768)
        while True:
            part = stream.read(4096)
            if part:
                writer.write(part)
            else:
                writer.flush()
                break


def move_file(src, dest):
    dest_dir = os.path.dirname(dest)
    os.makedirs(dest_dir, exist_ok=True)
    os.rename(src, dest)


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()

        return super().default(obj)


def find(items, fn):
    for item in items:
        if fn(item):
            return item


def int_version(version):
    """Convert v<n> format of version to integer representation."""
    return int(version.lstrip("v")) if re.match(r"v\d", version) else None


def do_sqlite_checkpoint(path, log=None):
    """
    Function to do checkpoint over the geopackage file which was not able to do diff file.

    :param path: file's absolute path on disk
    :type path: str
    :param log: optional reference to a logger
    :type log: logging.Logger
    :returns: new size and checksum of file after checkpoint
    :rtype: int, str
    """
    new_size = None
    new_checksum = None
    if ".gpkg" in path and os.path.exists(f"{path}-wal"):
        if log:
            log.info("checkpoint - going to add it in " + path)
        conn = sqlite3.connect(path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA wal_checkpoint=FULL")
        if log:
            log.info("checkpoint - return value: " + str(cursor.fetchone()))
        cursor.execute("VACUUM")
        conn.commit()
        conn.close()
        new_size = os.path.getsize(path)
        new_checksum = generate_checksum(path)
        if log:
            log.info("checkpoint - new size {} checksum {}".format(new_size, new_checksum))

    return new_size, new_checksum


def get_versions_with_file_changes(mc, project_path, file_path, version_from=None, version_to=None, file_history=None):
    """
    Get the project version tags where the file was added, modified or deleted.

    Args:
        mc (MerginClient): MerginClient instance
        project_path (str): project full name (<namespace>/<name>)
        file_path (str): relative path of file to download in the project directory
        version_from (str): optional minimum version to fetch, for example "v3"
        version_to (str): optional maximum version to fetch
        file_history (dict): optional file history info, result of project_file_history_info().

    Returns:
        list of version tags, for example ["v4", "v7", "v8"]
    """
    if file_history is None:
        file_history = mc.project_file_history_info(project_path, file_path)
    all_version_numbers = sorted([int(k[1:]) for k in file_history["history"].keys()])
    version_from = all_version_numbers[0] if version_from is None else int_version(version_from)
    version_to = all_version_numbers[-1] if version_to is None else int_version(version_to)
    if version_from is None or version_to is None:
        err = f"Wrong version parameters: {version_from}-{version_to} while getting diffs for {file_path}. "
        err += f"Version tags required in the form: 'v2', 'v11', etc."
        raise ClientError(err)
    if version_from >= version_to:
        err = f"Wrong version parameters: {version_from}-{version_to} while getting diffs for {file_path}. "
        err += f"version_from needs to be smaller than version_to."
        raise ClientError(err)
    if version_from not in all_version_numbers or version_to not in all_version_numbers:
        err = f"Wrong version parameters: {version_from}-{version_to} while getting diffs for {file_path}. "
        err += f"Available versions: {all_version_numbers}"
        raise ClientError(err)

    # Find versions to fetch between the 'from' and 'to' versions
    idx_from = idx_to = None
    for idx, version in enumerate(all_version_numbers):
        if version == version_from:
            idx_from = idx
        elif version == version_to:
            idx_to = idx
            break
    return [f"v{ver_nr}" for ver_nr in all_version_numbers[idx_from : idx_to + 1]]


def unique_path_name(path):
    """
    Generates an unique name for the given path. If the given path does
    not exist yet it will be returned unchanged, otherwise a sequntial
    number will be added to the path in format:
      - if path is a directory: "folder" -> "folder (1)"
      - if path is a file: "filename.txt" -> "filename (1).txt"

    :param path: path to file or directory
    :type path: str
    :returns: unique path
    :rtype: str
    """
    unique_path = str(path)

    is_dir = os.path.isdir(path)
    head, tail = os.path.split(os.path.normpath(path))
    ext = "".join(Path(tail).suffixes)
    file_name = tail.replace(ext, "")

    i = 0
    while os.path.exists(unique_path):
        i += 1

        if is_dir:
            unique_path = f"{path} ({i})"
        else:
            unique_path = os.path.join(head, file_name) + f" ({i}){ext}"

    return unique_path


def conflicted_copy_file_name(path, user, version):
    """
    Generates a file name for the conflict copy file in the following form
    <filename> (conflicted copy, <username> v<version>).gpkg. Example:
    * data (conflicted copy, martin v5).gpkg

    :param path: name of the file
    :type path: str
    :param user: name of the user
    :type user: str
    :param version: version of the Mergin Maps project
    :type version: str
    :returns: new file name
    :rtype: str
    """
    if not path:
        return ""

    head, tail = os.path.split(os.path.normpath(path))
    ext = "".join(Path(tail).suffixes)
    file_name = tail.replace(ext, "")
    # in case of QGIS project files we have to add "~" (tilde) to suffix
    # to avoid having several QGIS project files inside Mergin Maps project.
    # See https://github.com/lutraconsulting/qgis-mergin-plugin/issues/382
    # for more details
    if is_qgis_file(path):
        ext += "~"
    return os.path.join(head, file_name) + f" (conflicted copy, {user} v{version}){ext}"


def edit_conflict_file_name(path, user, version):
    """
    Generates a file name for the edit conflict file in the following form
    <filename> (edit conflict, <username> v<version>).gpkg. Example:
    * data (edit conflict, martin v5).gpkg

    :param path: name of the file
    :type path: str
    :param user: name of the user
    :type user: str
    :param version: version of the Mergin Maps project
    :type version: str
    :returns: new file name
    :rtype: str
    """
    if not path:
        return ""

    head, tail = os.path.split(os.path.normpath(path))
    ext = "".join(Path(tail).suffixes)
    file_name = tail.replace(ext, "")
    return os.path.join(head, file_name) + f" (edit conflict, {user} v{version}).json"


def is_version_acceptable(version, min_version):
    """
    Checks whether given version is at least min_version or later (both given as strings).

    Versions are expected to be using syntax X.Y.Z

    Returns true if version >= min_version
    """
    m = re.search("(\\d+)[.](\\d+)", min_version)
    min_major, min_minor = m.group(1), m.group(2)

    if len(version) == 0:
        return False  # unknown version is assumed to be old

    m = re.search("(\\d+)[.](\\d+)", version)
    if m is None:
        return False

    major, minor = m.group(1), m.group(2)

    return major > min_major or (major == min_major and minor >= min_minor)


def is_versioned_file(path: str) -> bool:
    diff_extensions = [".gpkg", ".sqlite"]
    f_extension = os.path.splitext(path)[1]
    return f_extension.lower() in diff_extensions


def is_qgis_file(path: str) -> bool:
    """
    Check if file is a QGIS project file.
    """
    f_extension = os.path.splitext(path)[1]
    return f_extension.lower() in [".qgs", ".qgz"]


def is_mergin_config(path: str) -> bool:
    """Check if the given path is for file mergin-config.json"""
    filename = os.path.basename(path).lower()
    return filename == "mergin-config.json"
