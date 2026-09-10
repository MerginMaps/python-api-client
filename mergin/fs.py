"""
Thin wrappers around the standard-library filesystem calls used by the sync code.

Every wrapper applies utils.long_path() to its path argument(s) so that Windows paths
longer than MAX_PATH are handled transparently.

Use these instead of calling os.* / shutil.* / open() / sqlite3.connect() directly on
project file paths.
"""

import os
import shutil
import sqlite3

from .utils import long_path


def remove(path):
    os.remove(long_path(path))


def exists(path) -> bool:
    return os.path.exists(long_path(path))


def getsize(path) -> int:
    return os.path.getsize(long_path(path))


def getmtime(path) -> float:
    return os.path.getmtime(long_path(path))


def copy(src, dst):
    return shutil.copy(long_path(src), long_path(dst))


def walk(path, **kwargs):
    return os.walk(long_path(path), **kwargs)


def makedirs(path, exist_ok=False):
    os.makedirs(long_path(path), exist_ok=exist_ok)


def mkdir(path):
    os.mkdir(long_path(path))


def rmtree(path):
    shutil.rmtree(long_path(path))


def connect(path):
    return sqlite3.connect(long_path(path))


def open_file(path, *args, **kwargs):
    return open(long_path(path), *args, **kwargs)
