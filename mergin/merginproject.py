
import json
import logging
import math
import os
import re
import shutil
import uuid
from datetime import datetime
from dateutil.tz import tzlocal

from .common import UPLOAD_CHUNK_SIZE, InvalidProject, ClientError
from .utils import generate_checksum, move_file, int_version, find, do_sqlite_checkpoint


this_dir = os.path.dirname(os.path.realpath(__file__))


# Try to import pygeodiff from "deps" sub-directory (which should be present e.g. when
# used within QGIS plugin), if that's not available then try to import it from standard
# python paths.
try:
    from .deps import pygeodiff
except ImportError:
    import pygeodiff


class MerginProject:
    """ Base class for Mergin local projects.

    Linked to existing local directory, with project metadata (mergin.json) and backups located in .mergin directory.
    """
    def __init__(self, directory):
        self.dir = os.path.abspath(directory)
        if not os.path.exists(self.dir):
            raise InvalidProject('Project directory does not exist')

        self.meta_dir = os.path.join(self.dir, '.mergin')
        if not os.path.exists(self.meta_dir):
            os.mkdir(self.meta_dir)

        self.setup_logging(directory)

        # make sure we can load correct pygeodiff
        try:
            self.geodiff = pygeodiff.GeoDiff()
        except pygeodiff.geodifflib.GeoDiffLibVersionError:
            # this is a fatal error, we can't live without geodiff
            self.log.error("Unable to load geodiff! (lib version error)")
            raise ClientError("Unable to load geodiff library!")

        # redirect any geodiff output to our log file
        def _logger_callback(level, text_bytes):
            text = text_bytes.decode()  # convert bytes to str
            if level == pygeodiff.GeoDiff.LevelError:
                self.log.error("GEODIFF: " + text)
            elif level == pygeodiff.GeoDiff.LevelWarning:
                self.log.warning("GEODIFF: " + text)
            else:
                self.log.info("GEODIFF: " + text)
        self.geodiff.set_logger_callback(_logger_callback)
        self.geodiff.set_maximum_logger_level(pygeodiff.GeoDiff.LevelDebug)

    def setup_logging(self, logger_name):
        """Setup logging into project directory's .mergin/client-log.txt file."""
        self.log = logging.getLogger('mergin.project.' + logger_name)
        self.log.setLevel(logging.DEBUG)  # log everything (it would otherwise log just warnings+errors)
        if not self.log.handlers:
            # we only need to set the handler once
            # (otherwise we would get things logged multiple times as loggers are cached)
            log_handler = logging.FileHandler(os.path.join(self.meta_dir, "client-log.txt"))
            log_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
            self.log.addHandler(log_handler)

    def remove_logging_handler(self):
        """Check if there is a logger handler defined for the project and remove it. There should be only one."""
        if len(self.log.handlers) > 0:
            handler = self.log.handlers[0]
            self.log.removeHandler(handler)
            handler.flush()
            handler.close()

    def fpath(self, file, other_dir=None):
        """
        Helper function to get absolute path of project file. Defaults to project dir but
        alternative dir get be provided (mostly meta or temp). Also making sure that parent dirs to file exist.

        :param file: relative file path in project (posix)
        :type file: str
        :param other_dir: alternative base directory for file, defaults to None
        :type other_dir: str
        :returns: file's absolute path
        :rtype: str
        """
        root = other_dir or self.dir
        abs_path = os.path.abspath(os.path.join(root, file))
        f_dir = os.path.dirname(abs_path)
        os.makedirs(f_dir, exist_ok=True)
        return abs_path

    def fpath_meta(self, file):
        """ Helper function to get absolute path of file in meta dir. """
        return self.fpath(file, self.meta_dir)

    @property
    def metadata(self):
        if not os.path.exists(self.fpath_meta('mergin.json')):
            raise InvalidProject('Project metadata has not been created yet')
        with open(self.fpath_meta('mergin.json'), 'r') as file:
            return json.load(file)

    @metadata.setter
    def metadata(self, data):
        with open(self.fpath_meta('mergin.json'), 'w') as file:
            json.dump(data, file, indent=2)

    def is_versioned_file(self, file):
        """ Check if file is compatible with geodiff lib and hence suitable for versioning.

        :param file: file path
        :type file: str
        :returns: if file is compatible with geodiff lib
        :rtype: bool
        """
        diff_extensions = ['.gpkg', '.sqlite']
        f_extension = os.path.splitext(file)[1]
        return f_extension in diff_extensions

    def is_gpkg_open(self, path):
        """
        Check whether geopackage file is open (and wal file exists)

        :param path: absolute path of file on disk
        :type path: str
        :returns: whether file is open
        :rtype: bool
        """
        f_extension = os.path.splitext(path)[1]
        if f_extension != '.gpkg':
            return False
        if os.path.exists(f'{path}-wal'):
            return True
        return False

    def ignore_file(self, file):
        """
        Helper function for blacklisting certain types of files.

        :param file: file path in project
        :type file: str
        :returns: whether file should be ignored
        :rtype: bool
        """
        ignore_ext = re.compile(r'({})$'.format('|'.join(re.escape(x) for x in ['-shm', '-wal', '~', 'pyc', 'swap'])))
        ignore_files = ['.DS_Store', '.directory']
        name, ext = os.path.splitext(file)
        if ext and ignore_ext.search(ext):
            return True
        if file in ignore_files:
            return True
        return False

    def inspect_files(self):
        """
        Inspect files in project directory and return metadata.

        :returns: metadata for files in project directory in server required format
        :rtype: list[dict]
        """
        files_meta = []
        for root, dirs, files in os.walk(self.dir, topdown=True):
            dirs[:] = [d for d in dirs if d not in ['.mergin']]
            for file in files:
                if self.ignore_file(file):
                    continue

                abs_path = os.path.abspath(os.path.join(root, file))
                rel_path = os.path.relpath(abs_path, start=self.dir)
                proj_path = '/'.join(rel_path.split(os.path.sep))  # we need posix path
                files_meta.append({
                    "path": proj_path,
                    "checksum": generate_checksum(abs_path),
                    "size": os.path.getsize(abs_path),
                    "mtime": datetime.fromtimestamp(os.path.getmtime(abs_path), tzlocal())
                })
        return files_meta

    def compare_file_sets(self, origin, current):
        """
        Helper function to calculate difference between two sets of files metadata using file names and checksums.

        :Example:

        >>> origin = [{'checksum': '08b0e8caddafe74bf5c11a45f65cedf974210fed', 'path': 'base.gpkg', 'size': 2793, 'mtime': '2019-08-26T11:08:34.051221+02:00'}]
        >>> current = [{'checksum': 'c9a4fd2afd513a97aba19d450396a4c9df8b2ba4', 'path': 'test.qgs', 'size': 31980, 'mtime': '2019-08-26T11:09:30.051221+02:00'}]
        >>> self.compare_file_sets(origin, current)
        {"added": [{'checksum': 'c9a4fd2afd513a97aba19d450396a4c9df8b2ba4', 'path': 'test.qgs', 'size': 31980, 'mtime': '2019-08-26T11:09:30.051221+02:00'}], "removed": [[{'checksum': '08b0e8caddafe74bf5c11a45f65cedf974210fed', 'path': 'base.gpkg', 'size': 2793, 'mtime': '2019-08-26T11:08:34.051221+02:00'}]], "renamed": [], "updated": []}

        :param origin: origin set of files metadata
        :type origin: list[dict]
        :param current: current set of files metadata to be compared against origin
        :type current: list[dict]
        :returns: changes between two sets with change type
        :rtype: dict[str, list[dict]]'
        """
        origin_map = {f["path"]: f for f in origin}
        current_map = {f["path"]: f for f in current}
        removed = [f for f in origin if f["path"] not in current_map]
        added = [f for f in current if f["path"] not in origin_map]
        updated = []
        for f in current:
            path = f["path"]
            if path not in origin_map:
                continue
            # with open WAL files we don't know yet, better to mark file as updated
            if not self.is_gpkg_open(self.fpath(path)) and f["checksum"] == origin_map[path]["checksum"]:
                continue
            f["origin_checksum"] = origin_map[path]["checksum"]
            updated.append(f)

        return {
            "renamed": [],
            "added": added,
            "removed": removed,
            "updated": updated
        }

    def get_pull_changes(self, server_files):
        """
        Calculate changes needed to be pulled from server.

        Calculate diffs between local files metadata and server's ones. Because simple metadata like file size or
        checksum are not enough to determine geodiff files changes, evaluate also their history (provided by server).
        For small files ask for full versions of geodiff files, otherwise determine list of diffs needed to update file.

        .. seealso:: self.compare_file_sets

        :param server_files: list of server files' metadata (see also self.inspect_files())
        :type server_files: list[dict]
        :returns: changes metadata for files to be pulled from server
        :rtype: dict
        """

        # first let's have a look at the added/updated/removed files
        changes = self.compare_file_sets(self.metadata['files'], server_files)

        # then let's inspect our versioned files (geopackages) if there are any relevant changes
        not_updated = []
        for file in changes['updated']:
            if not self.is_versioned_file(file["path"]):
                continue

            diffs = []
            diffs_size = 0
            is_updated = False

            # get sorted list of the history (they may not be sorted or using lexical sorting - "v10", "v11", "v5", "v6", ...)
            history_list = []
            for version_str, version_info in file['history'].items():
                history_list.append( (int_version(version_str), version_info) )
            history_list = sorted(history_list, key=lambda item: item[0])  # sort tuples based on version numbers

            # need to track geodiff file history to see if there were any changes
            for version, version_info in history_list:
                if version <= int_version(self.metadata['version']):
                    continue  # ignore history of no interest
                is_updated = True
                if 'diff' in version_info:
                    diffs.append(version_info['diff']['path'])
                    diffs_size += version_info['diff']['size']
                else:
                    diffs = []
                    break  # we found force update in history, does not make sense to download diffs

            if is_updated:
                file['diffs'] = diffs
            else:
                not_updated.append(file)

        changes['updated'] = [f for f in changes['updated'] if f not in not_updated]
        return changes

    def get_push_changes(self):
        """
        Calculate changes needed to be pushed to server.

        Calculate diffs between local files metadata and actual files in project directory. Because simple metadata like
        file size or checksum are not enough to determine geodiff files changes, geodiff tool is used to determine change
        of file content and update corresponding metadata.

        .. seealso:: self.compare_file_sets

        :returns: changes metadata for files to be pushed to server
        :rtype: dict
        """
        changes = self.compare_file_sets(self.metadata['files'], self.inspect_files())
        # do checkpoint to push changes from wal file to gpkg
        for file in changes['added'] + changes['updated']:
            size, checksum = do_sqlite_checkpoint(self.fpath(file["path"]), self.log)
            if size and checksum:
                file["size"] = size
                file["checksum"] = checksum
            file['chunks'] = [str(uuid.uuid4()) for i in range(math.ceil(file["size"] / UPLOAD_CHUNK_SIZE))]

        # need to check for for real changes in geodiff files using geodiff tool (comparing checksum is not enough)
        not_updated = []
        for file in changes['updated']:
            path = file["path"]
            if not self.is_versioned_file(path):
                continue

            # we use geodiff to check if we can push only diff files
            current_file = self.fpath(path)
            origin_file = self.fpath_meta(path)
            diff_id = str(uuid.uuid4())
            diff_name = path + '-diff-' + diff_id
            diff_file = self.fpath_meta(diff_name)
            try:
                self.geodiff.create_changeset(origin_file, current_file, diff_file)
                if self.geodiff.has_changes(diff_file):
                    diff_size = os.path.getsize(diff_file)
                    file['checksum'] = file['origin_checksum']  # need to match basefile on server
                    file['chunks'] = [str(uuid.uuid4()) for i in range(math.ceil(diff_size / UPLOAD_CHUNK_SIZE))]
                    file['mtime'] = datetime.fromtimestamp(os.path.getmtime(current_file), tzlocal())
                    file['diff'] = {
                        "path": diff_name,
                        "checksum": generate_checksum(diff_file),
                        "size": diff_size,
                        'mtime': datetime.fromtimestamp(os.path.getmtime(diff_file), tzlocal())
                    }
                else:
                    not_updated.append(file)
            except (pygeodiff.GeoDiffLibError, pygeodiff.GeoDiffLibConflictError) as e:
                self.log.warning("failed to create changeset for " + path)
                # probably the database schema has been modified if geodiff cannot create changeset.
                # we will need to do full upload of the file
                pass

        changes['updated'] = [f for f in changes['updated'] if f not in not_updated]
        return changes

    def copy_versioned_file_for_upload(self, f, tmp_dir):
        """
        Make a temporary copy of the versioned file using geodiff, to make sure that we have full
        content in a single file (nothing left in WAL journal)
        """
        path = f["path"]
        self.log.info("Making a temporary copy (full upload): " + path)
        tmp_file = os.path.join(tmp_dir, path)
        os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
        self.geodiff.make_copy_sqlite(self.fpath(path), tmp_file)
        f["size"] = os.path.getsize(tmp_file)
        f["checksum"] = generate_checksum(tmp_file)
        f["chunks"] = [str(uuid.uuid4()) for i in range(math.ceil(f["size"] / UPLOAD_CHUNK_SIZE))]
        f["upload_file"] = tmp_file
        return tmp_file

    def get_list_of_push_changes(self, push_changes):
        changes = {}
        for idx, file in enumerate(push_changes["updated"]):
            if "diff" in file:
                changeset_path = file["diff"]["path"]
                changeset = self.fpath_meta(changeset_path)
                result_file = self.fpath("change_list" + str(idx), self.meta_dir)
                try:
                    self.geodiff.list_changes_summary(changeset, result_file)
                    with open(result_file, 'r') as f:
                        change = f.read()
                        changes[file["path"]] = json.loads(change)
                    os.remove(result_file)
                except (pygeodiff.GeoDiffLibError, pygeodiff.GeoDiffLibConflictError):
                    pass
        return changes

    def apply_pull_changes(self, changes, temp_dir):
        """
        Apply changes pulled from server.

        Update project files according to file changes. Apply changes to geodiff basefiles as well
        so they are up to date with server. In case of conflicts create backups from locally modified versions.

        .. seealso:: self.get_pull_changes

        :param changes: metadata for pulled files
        :type changes: dict[str, list[dict]]
        :param temp_dir: directory with downloaded files from server
        :type temp_dir: str
        :returns: files where conflicts were found
        :rtype: list[str]
        """
        conflicts = []
        local_changes = self.get_push_changes()
        modified = {}
        for f in local_changes["added"] + local_changes["updated"]:
            modified.update({f['path']: f})

        local_files_map = {}
        for f in self.inspect_files():
            local_files_map.update({f['path']: f})

        for k, v in changes.items():
            for item in v:
                path = item['path']
                src = self.fpath(path, temp_dir)
                dest = self.fpath(path)
                basefile = self.fpath_meta(path)

                # special care is needed for geodiff files
                # 'src' here is server version of file and 'dest' is locally modified
                if self.is_versioned_file(path) and k == 'updated':
                    if path in modified:
                        self.log.info("updating file with rebase: " + path)
                        server_diff = self.fpath(f'{path}-server_diff', temp_dir)  # diff between server file and local basefile
                        local_diff = self.fpath(f'{path}-local_diff', temp_dir)

                        # temporary backup of file pulled from server for recovery
                        f_server_backup = self.fpath(f'{path}-server_backup', temp_dir)
                        self.geodiff.make_copy_sqlite(src, f_server_backup)

                        # create temp backup (ideally with geodiff) of locally modified file if needed later
                        f_conflict_file = self.fpath(f'{path}-local_backup', temp_dir)
                        try:
                            self.geodiff.create_changeset(basefile, dest, local_diff)
                            self.geodiff.make_copy_sqlite(basefile, f_conflict_file)
                            self.geodiff.apply_changeset(f_conflict_file, local_diff)
                        except (pygeodiff.GeoDiffLibError, pygeodiff.GeoDiffLibConflictError):
                            self.log.info("backup of local file with geodiff failed - need to do hard copy")
                            self.geodiff.make_copy_sqlite(dest, f_conflict_file)

                        # in case there will be any conflicting operations found during rebase,
                        # they will be stored in a JSON file - if there are no conflicts, the file
                        # won't even be created
                        rebase_conflicts = self.fpath(f'{path}_rebase_conflicts')

                        # try to do rebase magic
                        try:
                            self.geodiff.create_changeset(basefile, src, server_diff)
                            self.geodiff.rebase(basefile, src, dest, rebase_conflicts)
                            # make sure basefile is in the same state as remote server file (for calc of push changes)
                            self.geodiff.apply_changeset(basefile, server_diff)
                            self.log.info("rebase successful!")
                        except (pygeodiff.GeoDiffLibError, pygeodiff.GeoDiffLibConflictError) as err:
                            self.log.warning("rebase failed! going to create conflict file")
                            # it would not be possible to commit local changes, they need to end up in new conflict file
                            self.geodiff.make_copy_sqlite(f_conflict_file, dest)
                            conflict = self.backup_file(path)
                            conflicts.append(conflict)
                            # original file synced with server
                            self.geodiff.make_copy_sqlite(f_server_backup, basefile)
                            self.geodiff.make_copy_sqlite(f_server_backup, dest)
                    else:
                        # The local file is not modified -> no rebase needed.
                        # We just apply the diff between our copy and server to both the local copy and its basefile
                        self.log.info("updating file without rebase: " + path)
                        try:
                            server_diff = self.fpath(f'{path}-server_diff', temp_dir)  # diff between server file and local basefile
                            # TODO: it could happen that basefile does not exist.
                            # It was either never created (e.g. when pushing without geodiff)
                            # or it was deleted by mistake(?) by the user. We should detect that
                            # when starting pull and download it as well
                            self.geodiff.create_changeset(basefile, src, server_diff)
                            self.geodiff.apply_changeset(dest, server_diff)
                            self.geodiff.apply_changeset(basefile, server_diff)
                            self.log.info("update successful")
                        except (pygeodiff.GeoDiffLibError, pygeodiff.GeoDiffLibConflictError):
                            self.log.warning("update failed! going to copy file")
                            # something bad happened and we have failed to patch our local files - this should not happen if there
                            # wasn't a schema change or something similar that geodiff can't handle.
                            self.geodiff.make_copy_sqlite(src, dest)
                            self.geodiff.make_copy_sqlite(src, basefile)
                else:
                    # backup if needed
                    if path in modified and item['checksum'] != local_files_map[path]['checksum']:
                        conflict = self.backup_file(path)
                        conflicts.append(conflict)

                    if k == 'removed':
                        if os.path.exists(dest):
                            os.remove(dest)
                        else:
                            # the file could be deleted via web interface AND also manually locally -> just log it
                            self.log.warning(f"File to be removed locally doesn't exist: {dest}")
                        if self.is_versioned_file(path):
                            os.remove(basefile)
                    else:
                        if self.is_versioned_file(path):
                            self.geodiff.make_copy_sqlite(src, dest)
                            self.geodiff.make_copy_sqlite(src, basefile)
                        else:
                            shutil.copy(src, dest)

        return conflicts

    def apply_push_changes(self, changes):
        """
        For geodiff files update basefiles according to changes pushed to server.

        :param changes: metadata for pulled files
        :type changes: dict[str, list[dict]]
        """
        for k, v in changes.items():
            for item in v:
                path = item['path']
                if not self.is_versioned_file(path):
                    continue

                basefile = self.fpath_meta(path)
                if k == 'removed':
                    os.remove(basefile)
                elif k == 'added':
                    self.geodiff.make_copy_sqlite(self.fpath(path), basefile)
                elif k == 'updated':
                    # in case for geopackage cannot be created diff (e.g. forced update with committed changes from wal file)
                    if "diff" not in item:
                        self.log.info("updating basefile (copy) for: " + path)
                        self.geodiff.make_copy_sqlite(self.fpath(path), basefile)
                    else:
                        self.log.info("updating basefile (diff) for: " + path)
                        # better to apply diff to previous basefile to avoid issues with geodiff tmp files
                        changeset = self.fpath_meta(item['diff']['path'])
                        patch_error = self.apply_diffs(basefile, [changeset])
                        if patch_error:
                            # in case of local sync issues it is safier to remove basefile, next time it will be downloaded from server
                            self.log.warning("removing basefile (because of apply diff error) for: " + path)
                            os.remove(basefile)
                else:
                    pass

    def backup_file(self, file):
        """
        Create backup file next to its origin.

        :param file: path of file in project
        :type file: str
        :returns: path to backupfile
        :rtype: str
        """
        src = self.fpath(file)
        if not os.path.exists(src):
            return
        backup_path = self.fpath(f'{file}_conflict_copy')
        index = 2
        while os.path.exists(backup_path):
            backup_path = self.fpath(f'{file}_conflict_copy{index}')
            index += 1
        if self.is_versioned_file(file):
            self.geodiff.make_copy_sqlite(src, backup_path)
        else:
            shutil.copy(src, backup_path)
        return backup_path

    def apply_diffs(self, basefile, diffs):
        """
        Helper function to update content of geodiff file using list of diffs.
        Input file will be overwritten (make sure to create backup if needed).

        :param basefile: abs path to file to be updated
        :type basefile: str
        :param diffs: list of abs paths to geodiff changeset files
        :type diffs: list[str]
        :returns: error message if diffs were not successfully applied or None
        :rtype: str
        """
        error = None
        if not self.is_versioned_file(basefile):
            return error

        for index, diff in enumerate(diffs):
            try:
                self.geodiff.apply_changeset(basefile, diff)
            except (pygeodiff.GeoDiffLibError, pygeodiff.GeoDiffLibConflictError) as e:
                self.log.warning("failed to apply changeset " + diff + " to " + basefile)
                error = str(e)
                break
        return error
