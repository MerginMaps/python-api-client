import os
import io
import json
import hashlib
import re
import sqlite3
from datetime import datetime


def generate_checksum(file, chunk_size=4096):
    """
    Generate checksum for file from chunks.

    :param file: file to calculate checksum
    :param chunk_size: size of chunk
    :return: sha1 checksum
    """
    checksum = hashlib.sha1()
    with open(file, 'rb') as f:
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

    with open(path, 'wb') as output:
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
    """ Convert v<n> format of version to integer representation. """
    return int(version.lstrip('v')) if re.match(r'v\d', version) else None


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
    if ".gpkg" in path and os.path.exists(f'{path}-wal'):
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
