"""
To download projects asynchronously. Start download: (does not block)

job = download_project_async(mergin_client, 'user/project', '/tmp/my_project)

Then we need to wait until we are finished downloading - either by periodically
calling download_project_is_running(job) that will just return True/False or by calling
download_project_wait(job) that will block the current thread (not good for GUI).
To finish the download job, we have to call download_project_finalize(job).
"""

import copy
import math
import os
import pprint
import shutil
import tempfile

import concurrent.futures

from .common import CHUNK_SIZE, ClientError
from .merginproject import MerginProject
from .utils import save_to_file, get_versions_with_file_changes


# status = download_project_async(...)
#
# for completely async approach:
# - a method called (in worker thread(!)) when new data are received -- to update progress bar
# - a method called (in worker thread(!)) when download is complete -- and we just need to do the final steps (in main thread)
# - the methods in worker threads could send queued signals to some QObject instances owned by main thread to do updating/finalization
#
# polling approach:
#  - caller will caller a method every X ms to check the status
#  - once status says download is finished, the caller would call a function to do finalization


class DownloadJob:
    """
    Keeps all the important data about a pending download job.
    Used for downloading whole projects but also single files.
    """
    
    def __init__(self, project_path, total_size, version, update_tasks, download_queue_items, directory, mp, project_info):
        self.project_path = project_path
        self.total_size = total_size      # size of data to download (in bytes)
        self.transferred_size = 0
        self.version = version
        self.update_tasks = update_tasks
        self.download_queue_items = download_queue_items
        self.directory = directory    # project's directory
        self.mp = mp   # MerginProject instance
        self.is_cancelled = False
        self.project_info = project_info   # parsed JSON with project info returned from the server

    def dump(self):
        print("--- JOB ---", self.total_size, "bytes")
        for task in self.update_tasks:
            print("- {} ... {}".format(task.file_path, len(task.download_queue_items)))
        print ("--")
        for item in self.download_queue_items:
            print("- {} {} {} {}".format(item.file_path, item.version, item.part_index, item.size))
        print("--- END ---")


def _download_items(file, directory, diff_only=False):
    """ Returns an array of download queue items """

    file_dir = os.path.dirname(os.path.normpath(os.path.join(directory, file['path'])))
    basename = os.path.basename(file['diff']['path']) if diff_only else os.path.basename(file['path'])
    file_size = file['diff']['size'] if diff_only else file['size']
    chunks = math.ceil(file_size / CHUNK_SIZE)
    
    items = []
    for part_index in range(chunks):
        download_file_path = os.path.join(file_dir, basename + ".{}".format(part_index))
        size = min(CHUNK_SIZE, file_size - part_index * CHUNK_SIZE)
        items.append(DownloadQueueItem(file['path'], size, file['version'], diff_only, part_index, download_file_path))
        
    return items


def _do_download(item, mc, mp, project_path, job):
    """ runs in worker thread """
    if job.is_cancelled:
        return
    
    # TODO: make download_blocking / save_to_file cancellable so that we can cancel as soon as possible

    item.download_blocking(mc, mp, project_path)
    job.transferred_size += item.size


def _cleanup_failed_download(directory, mergin_project=None):
    """
    If a download job fails, there will be the newly created directory left behind with some
    temporary files in it. We want to remove it because a new download would fail because
    the directory already exists.
    """
    # First try to get the Mergin project logger and remove its handlers to allow the log file deletion
    if mergin_project is not None:
        mergin_project.remove_logging_handler()

    shutil.rmtree(directory)


def download_project_async(mc, project_path, directory, project_version=None):
    """
    Starts project download in background and returns handle to the pending project download.
    Using that object it is possible to watch progress or cancel the ongoing work.
    """

    if '/' not in project_path:
        raise ClientError("Project name needs to be fully qualified, e.g. <username>/<projectname>")
    if os.path.exists(directory):
        raise ClientError("Project directory already exists")
    os.makedirs(directory)
    mp = MerginProject(directory)

    mp.log.info("--- version: " + mc.user_agent_info())
    mp.log.info(f"--- start download {project_path}")

    try:
        project_info = mc.project_info(project_path, version=project_version)
    except ClientError:
        _cleanup_failed_download(directory, mp)
        raise

    version = project_info['version'] if project_info['version'] else 'v0'

    mp.log.info(f"got project info. version {version}")

    # prepare download
    update_tasks = []  # stuff to do at the end of download
    for file in project_info['files']:
        file['version'] = version
        items = _download_items(file, directory)
        update_tasks.append(UpdateTask(file['path'], items))

    # make a single list of items to download
    total_size = 0
    download_list = []
    for task in update_tasks:
        download_list.extend(task.download_queue_items)
        for item in task.download_queue_items:
            total_size += item.size

    mp.log.info(f"will download {len(update_tasks)} files in {len(download_list)} chunks, total size {total_size}")
    
    job = DownloadJob(project_path, total_size, version, update_tasks, download_list, directory, mp, project_info)
    
    # start download
    job.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    job.futures = []
    for item in download_list:
        future = job.executor.submit(_do_download, item, mc, mp, project_path, job)
        job.futures.append(future)

    return job


def download_project_wait(job):
    """ blocks until all download tasks are finished """
    
    concurrent.futures.wait(job.futures)


def download_project_is_running(job):
    """
    Returns true/false depending on whether we have some pending downloads.

    It also forwards any exceptions from workers (e.g. some network errors). If an exception
    is raised, it is advised to call download_project_cancel() to abort the job.
    """
    for future in job.futures:
        if future.done() and future.exception() is not None:
            _cleanup_failed_download(job.directory, job.mp)
            raise future.exception()
        if future.running():
            return True
    return False


def download_project_finalize(job):
    """
    To be called when download in the background is finished and we need to do the finalization (merge chunks etc.)

    This should not be called from a worker thread (e.g. directly from a handler when download is complete).

    If any of the workers has thrown any exception, it will be re-raised (e.g. some network errors).
    That also means that the whole job has been aborted.
    """

    job.executor.shutdown(wait=True)

    # make sure any exceptions from threads are not lost
    for future in job.futures:
        if future.exception() is not None:
            _cleanup_failed_download(job.directory, job.mp)
            raise future.exception()

    job.mp.log.info("--- download finished")

    for task in job.update_tasks:
        
        # right now only copy tasks...
        task.apply(job.directory, job.mp)
    
    # final update of project metadata
    # TODO: why not exact copy of project info JSON ?
    job.mp.metadata = {
        "name": job.project_path,
        "version": job.version,
        "files": job.project_info["files"]
    }


def download_project_cancel(job):
    """
    To be called (from main thread) to cancel a job that has downloads in progress.
    Returns once all background tasks have exited (may block for a bit of time).
    """
    job.mp.log.info("user cancelled downloading...")
    # set job as cancelled
    job.is_cancelled = True
    job.executor.shutdown(wait=True)
    job.mp.log.info("--- download cancelled")


class UpdateTask:
    """
    Entry for each file that will be updated.
    At the end of a successful download of new data, all the tasks are executed.
    """
    
    # TODO: methods other than COPY
    def __init__(self, file_path, download_queue_items, destination_file=None):
        self.file_path = file_path
        self.destination_file = destination_file
        self.download_queue_items = download_queue_items

    def apply(self, directory, mp):
        """ assemble downloaded chunks into a single file """
        
        if self.destination_file is None:
            basename = os.path.basename(self.file_path)
            file_dir = os.path.dirname(os.path.normpath(os.path.join(directory, self.file_path)))
            dest_file_path = os.path.join(file_dir, basename)
        else:
            file_dir = os.path.dirname(os.path.normpath(self.destination_file))
            dest_file_path = self.destination_file
        os.makedirs(file_dir, exist_ok=True)

        # merge chunks together (and delete them afterwards)
        file_to_merge = FileToMerge(dest_file_path, self.download_queue_items)
        file_to_merge.merge()

        # Make a copy of the file to meta dir only if there is no user-specified path for the file.
        # destination_file is None for full project download and takes a meaningful value for a single file download.
        if mp.is_versioned_file(self.file_path) and self.destination_file is None:
            mp.geodiff.make_copy_sqlite(mp.fpath(self.file_path), mp.fpath_meta(self.file_path))


class DownloadQueueItem:
    """ a piece of data from a project that should be downloaded - it can be either a chunk or it can be a diff """
    
    def __init__(self, file_path, size, version, diff_only, part_index, download_file_path):
        self.file_path = file_path     # relative path to the file within project
        self.size = size               # size of the item in bytes
        self.version = version         # version of the file ("v123")
        self.diff_only = diff_only     # whether downloading diff or full version
        self.part_index = part_index    # index of the chunk
        self.download_file_path = download_file_path   # full path to a temporary file which will receive the content
        
    def __repr__(self):
        return "<DownloadQueueItem path={} version={} diff_only={} part_index={} size={} dest={}>".format(
            self.file_path, self.version, self.diff_only, self.part_index, self.size, self.download_file_path)

    def download_blocking(self, mc, mp, project_path):
        """ Starts download and only returns once the file has been fully downloaded and saved """

        mp.log.debug(f"Downloading {self.file_path} version={self.version} diff={self.diff_only} part={self.part_index}")
        start = self.part_index * (1 + CHUNK_SIZE)
        resp = mc.get("/v1/project/raw/{}".format(project_path), data={
                "file": self.file_path,
                "version": self.version,
                "diff": self.diff_only
            }, headers={
                "Range": "bytes={}-{}".format(start, start + CHUNK_SIZE)
            }
        )
        if resp.status in [200, 206]:
            mp.log.debug(f"Download finished: {self.file_path}")
            save_to_file(resp, self.download_file_path)
        else:
            mp.log.error(f"Download failed: {self.file_path}")
            raise ClientError('Failed to download part {} of file {}'.format(self.part_index, self.file_path))


class PullJob:
    def __init__(self, project_path, pull_changes, total_size, version, files_to_merge, download_queue_items,
                 temp_dir, mp, project_info, basefiles_to_patch):
        self.project_path = project_path
        self.pull_changes = pull_changes    # dictionary with changes (dict[str, list[dict]] - keys: "added", "updated", ...)
        self.total_size = total_size      # size of data to download (in bytes)
        self.transferred_size = 0
        self.version = version
        self.files_to_merge = files_to_merge   # list of FileToMerge instances
        self.download_queue_items = download_queue_items
        self.temp_dir = temp_dir            # full path to temporary directory where we store downloaded files
        self.mp = mp   # MerginProject instance
        self.is_cancelled = False
        self.project_info = project_info   # parsed JSON with project info returned from the server
        self.basefiles_to_patch = basefiles_to_patch  # list of tuples (relative path within project, list of diff files in temp dir to apply)

    def dump(self):
        print("--- JOB ---", self.total_size, "bytes")
        for file_to_merge in self.files_to_merge:
            print("- {}  ... download items={}".format(file_to_merge.dest_file, len(file_to_merge.downloaded_items)))
        print("--")
        for basefile, diffs in self.basefiles_to_patch:
            print("patch basefile {}  with {} diffs".format(basefile, len(diffs)))
        print("--")
        for item in self.download_queue_items:
            print("- {} {} {} {}".format(item.file_path, item.version, item.part_index, item.size))
        print("--- END ---")


def pull_project_async(mc, directory):
    """
    Starts project pull in background and returns handle to the pending job.
    Using that object it is possible to watch progress or cancel the ongoing work.
    """

    mp = MerginProject(directory)
    project_path = mp.metadata["name"]
    local_version = mp.metadata["version"]

    mp.log.info("--- version: " + mc.user_agent_info())
    mp.log.info(f"--- start pull {project_path}")

    try:
        server_info = mc.project_info(project_path, since=local_version)
    except ClientError as err:
        mp.log.error("Error getting project info: " + str(err))
        mp.log.info("--- pull aborted")
        raise
    server_version = server_info["version"]

    mp.log.info(f"got project info: local version {local_version} / server version {server_version}")

    if local_version == server_version:
        mp.log.info("--- pull - nothing to do (already at server version)")
        return  # Project is up to date

    # we either download a versioned file using diffs (strongly preferred),
    # but if we don't have history with diffs (e.g. uploaded without diffs)
    # then we just download the whole file
    _pulling_file_with_diffs = lambda f: 'diffs' in f and len(f['diffs']) != 0

    temp_dir = mp.fpath_meta(f'fetch_{local_version}-{server_version}')
    os.makedirs(temp_dir, exist_ok=True)
    pull_changes = mp.get_pull_changes(server_info["files"])
    mp.log.debug("pull changes:\n" + pprint.pformat(pull_changes))
    fetch_files = []
    for f in pull_changes["added"]:
        f['version'] = server_version
        fetch_files.append(f)
    # extend fetch files download list with various version of diff files (if needed)
    for f in pull_changes["updated"]:
        if _pulling_file_with_diffs(f):
            for diff in f['diffs']:
                diff_file = copy.deepcopy(f)
                for k, v in f['history'].items():
                    if 'diff' not in v:
                        continue
                    if diff == v['diff']['path']:
                        diff_file['version'] = k
                        diff_file['diff'] = v['diff']
                fetch_files.append(diff_file)
        else:
            f['version'] = server_version
            fetch_files.append(f)

    files_to_merge = []   # list of FileToMerge instances

    for file in fetch_files:
        diff_only = _pulling_file_with_diffs(file)
        items = _download_items(file, temp_dir, diff_only)
        
        # figure out destination path for the file
        file_dir = os.path.dirname(os.path.normpath(os.path.join(temp_dir, file['path'])))
        basename = os.path.basename(file['diff']['path']) if diff_only else os.path.basename(file['path'])
        dest_file_path = os.path.join(file_dir, basename)
        os.makedirs(file_dir, exist_ok=True)
        files_to_merge.append( FileToMerge(dest_file_path, items) )


    # make sure we can update geodiff reference files (aka. basefiles) with diffs or
    # download their full versions so we have them up-to-date for applying changes
    basefiles_to_patch = []  # list of tuples (relative path within project, list of diff files in temp dir to apply)
    for file in pull_changes['updated']:
        if not _pulling_file_with_diffs(file):
            continue  # this is only for diffable files (e.g. geopackages)

        basefile = mp.fpath_meta(file['path'])
        if not os.path.exists(basefile):
            # The basefile does not exist for some reason. This should not happen normally (maybe user removed the file
            # or we removed it within previous pull because we failed to apply patch the older version for some reason).
            # But it's not a problem - we will download the newest version and we're sorted.
            file_path = file['path']
            mp.log.info(f"missing base file for {file_path} -> going to download it (version {server_version})")
            file['version'] = server_version
            items = _download_items(file, temp_dir, diff_only=False)
            dest_file_path = mp.fpath(file["path"], temp_dir)
            #dest_file_path = os.path.join(os.path.dirname(os.path.normpath(os.path.join(temp_dir, file['path']))), os.path.basename(file['path']))
            files_to_merge.append( FileToMerge(dest_file_path, items) )
            continue

        basefiles_to_patch.append( (file['path'], file['diffs']) )

    # make a single list of items to download
    total_size = 0
    download_list = []
    for file_to_merge in files_to_merge:
        download_list.extend(file_to_merge.downloaded_items)
        for item in file_to_merge.downloaded_items:
            total_size += item.size

    mp.log.info(f"will download {len(download_list)} chunks, total size {total_size}")

    job = PullJob(project_path, pull_changes, total_size, server_version, files_to_merge, download_list, temp_dir, mp, server_info, basefiles_to_patch)

    # start download
    job.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    job.futures = []
    for item in download_list:
        future = job.executor.submit(_do_download, item, mc, mp, project_path, job)
        job.futures.append(future)
    
    return job


def pull_project_wait(job):
    """ blocks until all download tasks are finished """
    
    concurrent.futures.wait(job.futures)


def pull_project_is_running(job):
    """
    Returns true/false depending on whether we have some pending downloads

    It also forwards any exceptions from workers (e.g. some network errors). If an exception
    is raised, it is advised to call pull_project_cancel() to abort the job.
    """
    for future in job.futures:
        if future.done() and future.exception() is not None:
            job.mp.log.error("Error while pulling data: " + str(future.exception()))
            job.mp.log.info("--- pull aborted")
            raise future.exception()
        if future.running():
            return True
    return False


def pull_project_cancel(job):
    """
    To be called (from main thread) to cancel a job that has downloads in progress.
    Returns once all background tasks have exited (may block for a bit of time).
    """
    job.mp.log.info("user cancelled the pull...")
    # set job as cancelled
    job.is_cancelled = True
    job.executor.shutdown(wait=True)
    job.mp.log.info("--- pull cancelled")


class FileToMerge:
    """
    Keeps information about how to create a file (path specified by dest_file) from a couple
    of downloaded items (chunks) - each item is DownloadQueueItem object which has path
    to the temporary file containing its data. Calling merge() will create the destination file
    and remove the temporary files of the chunks
    """
    def __init__(self, dest_file, downloaded_items):
        self.dest_file = dest_file   # full path to the destination file to be created
        self.downloaded_items = downloaded_items   # list of pieces of the destination file to be merged

    def merge(self):
        with open(self.dest_file, 'wb') as final:
            for item in self.downloaded_items:
                with open(item.download_file_path, 'rb') as chunk:
                    shutil.copyfileobj(chunk, final)
                os.remove(item.download_file_path)

        expected_size = sum(item.size for item in self.downloaded_items)
        if os.path.getsize(self.dest_file) != expected_size:
            os.remove(self.dest_file)
            raise ClientError('Download of file {} failed. Please try it again.'.format(self.dest_file))


def pull_project_finalize(job):
    """
    To be called when pull in the background is finished and we need to do the finalization (merge chunks etc.)
    
    This should not be called from a worker thread (e.g. directly from a handler when download is complete)

    If any of the workers has thrown any exception, it will be re-raised (e.g. some network errors).
    That also means that the whole job has been aborted.
    """
    
    job.executor.shutdown(wait=True)

    # make sure any exceptions from threads are not lost
    for future in job.futures:
        if future.exception() is not None:
            job.mp.log.error("Error while pulling data: " + str(future.exception()))
            job.mp.log.info("--- pull aborted")
            raise future.exception()

    job.mp.log.info("finalizing pull")

    # merge downloaded chunks
    try:
        for file_to_merge in job.files_to_merge:
            file_to_merge.merge()
    except ClientError as err:
        job.mp.log.error("Error merging chunks of downloaded file: " + str(err))
        job.mp.log.info("--- pull aborted")
        raise

    # make sure we can update geodiff reference files (aka. basefiles) with diffs or
    # download their full versions so we have them up-to-date for applying changes
    for file_path, file_diffs in job.basefiles_to_patch:
        basefile = job.mp.fpath_meta(file_path)
        server_file = job.mp.fpath(file_path, job.temp_dir)

        shutil.copy(basefile, server_file)
        diffs = [job.mp.fpath(f, job.temp_dir) for f in file_diffs]
        patch_error = job.mp.apply_diffs(server_file, diffs)
        if patch_error:
            # that's weird that we are unable to apply diffs to the basefile!
            # because it should be possible to apply them cleanly since the server
            # was also able to apply those diffs. It could be that someone modified
            # the basefile and we ended up in this inconsistent state.
            # let's remove the basefile and let the user retry - we should download clean version again
            job.mp.log.error(f"Error patching basefile {basefile}")
            job.mp.log.error("Diffs we were applying: " + str(diffs))
            job.mp.log.error("Removing basefile because it would be corrupted anyway...")
            job.mp.log.info("--- pull aborted")
            os.remove(basefile)
            raise ClientError("Cannot patch basefile {}! Please try syncing again.".format(basefile))

    conflicts = job.mp.apply_pull_changes(job.pull_changes, job.temp_dir)
    job.mp.metadata = {
        'name': job.project_path,
        'version': job.version if job.version else "v0",  # for new projects server version is ""
        'files': job.project_info['files']
    }

    job.mp.log.info("--- pull finished -- at version " + job.mp.metadata['version'])

    shutil.rmtree(job.temp_dir)
    return conflicts


def download_file_async(mc, project_dir, file_path, output_file, version):
    """
    Starts background download project file at specified version.
    Returns handle to the pending download.
    """
    mp = MerginProject(project_dir)
    project_path = mp.metadata["name"]
    ver_info = f"at version {version}" if version is not None else "at latest version"
    mp.log.info(f"Getting {file_path} {ver_info}")
    project_info = mc.project_info(project_path, version=version)
    mp.log.info(f"Got project info. version {project_info['version']}")

    # set temporary directory for download
    temp_dir = tempfile.mkdtemp(prefix="mergin-py-client-")

    download_list = []
    update_tasks = []
    total_size = 0
    for file in project_info['files']:
        if file["path"] == file_path:
            file['version'] = version
            items = _download_items(file, temp_dir)
            task = UpdateTask(file['path'], items, output_file)
            download_list.extend(task.download_queue_items)
            for item in task.download_queue_items:
                total_size += item.size
            update_tasks.append(task)
            break
    if not download_list:
        warn = f"No {file_path} exists at version {version}"
        mp.log.warning(warn)
        shutil.rmtree(temp_dir)
        raise ClientError(warn)

    mp.log.info(f"will download file {file_path} in {len(download_list)} chunks, total size {total_size}")
    job = DownloadJob(
        project_path, total_size, version, update_tasks, download_list, temp_dir, mp, project_info
    )
    job.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    job.futures = []
    for item in download_list:
        future = job.executor.submit(_do_download, item, mc, mp, project_path, job)
        job.futures.append(future)

    return job


def download_file_finalize(job):
    """
    To be called when download_file_async is finished
    """
    job.executor.shutdown(wait=True)

    # make sure any exceptions from threads are not lost
    for future in job.futures:
        if future.exception() is not None:
            raise future.exception()

    job.mp.log.info("--- download finished")

    temp_dir = None
    for task in job.update_tasks:
        task.apply(job.directory, job.mp)
        if task.download_queue_items:
            temp_dir = os.path.dirname(task.download_queue_items[0].download_file_path)

    # Remove temporary download directory
    if temp_dir is not None:
        shutil.rmtree(temp_dir)


def download_diffs_async(mc, project_directory, file_path, version_from, version_to):
    """
    Starts background download project file diffs for specified versions.
    Returns handle to the pending download.

    Args:
        mc (MerginClient): MerginClient instance.
        project_directory (str): local project directory.
        file_path (str): file path relative to Mergin project root.
        version_from (str): starting project version tag for getting diff, for example 'v3'.
        version_to (str): ending project version tag for getting diff.

    Returns:
        PullJob/None: a handle for the pending download.
    """
    mp = MerginProject(project_directory)
    project_path = mp.metadata["name"]
    file_history = mc.project_file_history_info(project_path, file_path)
    versions_to_fetch = get_versions_with_file_changes(
        mc, project_path, file_path, version_from=version_from, version_to=version_to, file_history=file_history
    )
    mp.log.info(f"--- version: {mc.user_agent_info()}")
    mp.log.info(f"--- start download diffs for {file_path} of {project_path}, versions: {[v for v in versions_to_fetch]}")

    try:
        server_info = mc.project_info(project_path)
        if file_history is None:
            file_history = mc.project_file_history_info(project_path, file_path)
    except ClientError as err:
        mp.log.error("Error getting project info: " + str(err))
        mp.log.info("--- downloading diffs aborted")
        raise

    temp_dir = tempfile.mkdtemp(prefix="mergin-py-client-")
    fetch_files = []

    for version in versions_to_fetch[1:]:
        version_data = file_history["history"][version]
        diff_data = copy.deepcopy(version_data)
        diff_data['version'] = version
        diff_data['diff'] = version_data['diff']
        fetch_files.append(diff_data)

    files_to_merge = []  # list of FileToMerge instances
    download_list = []  # list of all items to be downloaded
    total_size = 0
    for file in fetch_files:
        items = _download_items(file, temp_dir, diff_only=True)
        dest_file_path = os.path.normpath(os.path.join(temp_dir, os.path.basename(file['diff']['path'])))
        files_to_merge.append(FileToMerge(dest_file_path, items))
        download_list.extend(items)
        for item in items:
            total_size += item.size

    mp.log.info(f"will download {len(download_list)} chunks, total size {total_size}")

    job = PullJob(project_path, None, total_size, None, files_to_merge, download_list, temp_dir, mp,
                  server_info, {})

    # start download
    job.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    job.futures = []
    for item in download_list:
        future = job.executor.submit(_do_download, item, mc, mp, project_path, job)
        job.futures.append(future)

    return job


def download_diffs_finalize(job, output_diff):
    """ To be called after download_diffs_async """

    job.executor.shutdown(wait=True)

    # make sure any exceptions from threads are not lost
    for future in job.futures:
        if future.exception() is not None:
            job.mp.log.error("Error while pulling data: " + str(future.exception()))
            job.mp.log.info("--- diffs download aborted")
            raise future.exception()

    job.mp.log.info("finalizing diffs pull")

    # merge downloaded chunks
    try:
        for file_to_merge in job.files_to_merge:
            file_to_merge.merge()
    except ClientError as err:
        job.mp.log.error("Error merging chunks of downloaded file: " + str(err))
        job.mp.log.info("--- diffs pull aborted")
        raise

    job.mp.log.info("--- diffs pull finished")

    # Collect and finally concatenate diffs, if needed
    diffs = []
    for file_to_merge in job.files_to_merge:
        diffs.append(file_to_merge.dest_file)

    output_dir = os.path.dirname(output_diff)
    temp_dir = None
    if len(diffs) >= 1:
        os.makedirs(output_dir, exist_ok=True)
        temp_dir = os.path.dirname(diffs[0])
        if len(diffs) > 1:
            job.mp.geodiff.concat_changes(diffs, output_diff)
        elif len(diffs) == 1:
            shutil.copy(diffs[0], output_diff)
    for diff in diffs:
        os.remove(diff)

    # remove the diffs download temporary directory
    if temp_dir is not None:
        shutil.rmtree(temp_dir)
