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
import typing
import traceback
from dataclasses import asdict

import concurrent.futures


from .common import CHUNK_SIZE, ClientError, DeltaChangeType, PullActionType
from .models import ProjectDelta, ProjectDeltaItem, PullAction
from .merginproject import MerginProject
from .utils import cleanup_tmp_dir, int_version, save_to_file
from typing import List, Optional, Tuple


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

    def __init__(
        self,
        project_path,
        total_size,
        version,
        update_tasks,
        download_queue_items,
        tmp_dir: tempfile.TemporaryDirectory,
        mp,
        project_info,
    ):
        self.project_path = project_path
        self.total_size = total_size  # size of data to download (in bytes)
        self.transferred_size = 0
        self.version = version
        self.update_tasks = update_tasks
        self.download_queue_items = download_queue_items
        self.tmp_dir = tmp_dir
        self.mp = mp  # MerginProject instance
        self.is_cancelled = False
        self.project_info = project_info  # parsed JSON with project info returned from the server
        self.failure_log_file = None  # log file, copied from the project directory if download fails

    def dump(self):
        print("--- JOB ---", self.total_size, "bytes")
        for task in self.update_tasks:
            print("- {} ... {}".format(task.file_path, len(task.download_queue_items)))
        print("--")
        for item in self.download_queue_items:
            print("- {} {} {} {}".format(item.file_path, item.version, item.part_index, item.size))
        print("--- END ---")


class DownloadQueueItem:
    """a piece of data from a project that should be downloaded - it can be either a chunk or it can be a diff"""

    def __init__(self, file_path, size, version, diff_only, part_index, download_file_path):
        self.file_path = file_path  # relative path to the file within project
        self.size = size  # size of the item in bytes
        self.version = version  # version of the file ("v123")
        self.diff_only = diff_only  # whether downloading diff or full version
        self.part_index = part_index  # index of the chunk
        self.download_file_path = download_file_path  # full path to a temporary file which will receive the content

    def __repr__(self):
        return "<DownloadQueueItem path={} version={} diff_only={} part_index={} size={} dest={}>".format(
            self.file_path, self.version, self.diff_only, self.part_index, self.size, self.download_file_path
        )

    def download_blocking(self, mc, mp, project_path):
        """Starts download and only returns once the file has been fully downloaded and saved"""

        mp.log.debug(
            f"Downloading {self.file_path} version={self.version} diff={self.diff_only} part={self.part_index}"
        )
        start = self.part_index * (1 + CHUNK_SIZE)
        resp = mc.get(
            "/v1/project/raw/{}".format(project_path),
            data={"file": self.file_path, "version": self.version, "diff": self.diff_only},
            headers={"Range": "bytes={}-{}".format(start, start + CHUNK_SIZE)},
        )
        if resp.status in [200, 206]:
            mp.log.debug(f"Download finished: {self.file_path}")
            save_to_file(resp, self.download_file_path)
        else:
            mp.log.error(f"Download failed: {self.file_path}")
            raise ClientError("Failed to download part {} of file {}".format(self.part_index, self.file_path))


class DownloadDiffQueueItem:
    """Download item representing a diff file to be downloaded using v2 diff/raw endpoint"""

    def __init__(self, file_path, download_file_path):
        self.file_path = file_path  # relative path to the file within project
        self.download_file_path = download_file_path  # full path to a temporary file which will receive the content
        self.size = 0  # size of the item in bytes

    def __repr__(self):
        return f"<DownloadDiffQueueItem path={self.file_path} size={self.size} dest={self.download_file_path}>"

    def download_blocking(self, mc, mp):
        """Starts download and only returns once the file has been fully downloaded and saved"""

        mp.log.debug(f"Downloading diff {self.file_path}")
        resp = mc.get(
            f"/v2/projects/{mp.project_id()}/raw/diff/{self.file_path}",
        )
        if resp.status in [200, 206]:
            mp.log.debug(f"Download finished: {self.file_path}")
            save_to_file(resp, self.download_file_path)
            self.size = os.path.getsize(self.download_file_path)
        else:
            mp.log.error(f"Download failed: {self.file_path}")
            raise ClientError(f"Failed to download of diff file {self.file_path} to {self.download_file_path}")


class DownloadFile:
    """
    Keeps information about how to create a file (path specified by dest_file) from a couple
    of downloaded items (chunks) - each item is DownloadQueueItem object which has path
    to the temporary file containing its data. Calling from_chunks() will create the destination file
    and remove the temporary files of the chunks
    """

    def __init__(self, dest_file, downloaded_items: typing.List[DownloadQueueItem], size_check=True):
        self.dest_file = dest_file  # full path to the destination file to be created
        self.downloaded_items = downloaded_items  # list of pieces of the destination file to be merged
        self.size_check = size_check  # whether we want to do merged file size check

    def from_chunks(self):
        """Merges downloaded chunks into a single file at dest_file path"""
        file_dir = os.path.dirname(self.dest_file)
        os.makedirs(file_dir, exist_ok=True)

        with open(self.dest_file, "wb") as final:
            for item in self.downloaded_items:
                with open(item.download_file_path, "rb") as chunk:
                    shutil.copyfileobj(chunk, final)
                os.remove(item.download_file_path)

        if not self.size_check:
            return
        expected_size = sum(item.size for item in self.downloaded_items)
        if os.path.getsize(self.dest_file) != expected_size:
            os.remove(self.dest_file)
            raise ClientError("Download of file {} failed. Please try it again.".format(self.dest_file))


def get_download_items(
    file_path: str,
    file_size: int,
    file_version: str,
    download_directory: str,
    download_path: Optional[str] = None,
    diff_only=False,
) -> List[DownloadQueueItem]:
    """Returns an array of download queue items (chunks) for the given file"""

    file_dir = os.path.dirname(os.path.normpath(os.path.join(download_directory, file_path)))
    basename = os.path.basename(download_path) if download_path else os.path.basename(file_path)
    chunks = math.ceil(file_size / CHUNK_SIZE)

    items = []
    for part_index in range(chunks):
        download_file_path = os.path.join(file_dir, basename + ".{}".format(part_index))
        size = min(CHUNK_SIZE, file_size - part_index * CHUNK_SIZE)
        items.append(DownloadQueueItem(file_path, size, file_version, diff_only, part_index, download_file_path))

    return items


def _do_download(item: typing.Union[DownloadQueueItem, DownloadDiffQueueItem], mc, mp, project_path, job):
    """runs in worker thread"""
    if job.is_cancelled:
        return

    # TODO: make download_blocking / save_to_file cancellable so that we can cancel as soon as possible

    if isinstance(item, DownloadDiffQueueItem):
        item.download_blocking(mc, mp)
    else:
        item.download_blocking(mc, mp, project_path)
    job.transferred_size += item.size


def _cleanup_failed_download(mergin_project: MerginProject = None):
    """
    If a download job fails, there will be the newly created directory left behind with some
    temporary files in it. We want to remove it because a new download would fail because
    the directory already exists.

    Returns path to the client log file or None if log file does not exist.
    """
    # First try to get the Mergin Maps project logger and remove its handlers to allow the log file deletion
    if mergin_project is not None:
        mergin_project.remove_logging_handler()

    # keep log file as it might contain useful debug info
    log_file = os.path.join(mergin_project.dir, ".mergin", "client-log.txt")
    dest_path = None

    if os.path.exists(log_file):
        tmp_file = tempfile.NamedTemporaryFile(prefix="mergin-", suffix=".txt", delete=False)
        tmp_file.close()
        dest_path = tmp_file.name
        shutil.copyfile(log_file, dest_path)

    return dest_path


def download_project_async(mc, project_path, directory, project_version=None):
    """
    Starts project download in background and returns handle to the pending project download.
    Using that object it is possible to watch progress or cancel the ongoing work.
    """

    if "/" not in project_path:
        raise ClientError("Project name needs to be fully qualified, e.g. <username>/<projectname>")
    if os.path.exists(directory):
        raise ClientError("Project directory already exists")
    os.makedirs(directory)
    mp = MerginProject(directory)

    mp.log.info("--- version: " + mc.user_agent_info())
    mp.log.info(f"--- start download {project_path}")

    tmp_dir = tempfile.TemporaryDirectory(prefix="python-api-client-")

    try:
        # check whether we download the latest version or not
        latest_proj_info = mc.project_info(project_path)
        if project_version:
            project_info = mc.project_info(project_path, version=project_version)
        else:
            project_info = latest_proj_info

    except ClientError:
        _cleanup_failed_download(mp)
        raise

    version = project_info["version"] if project_info["version"] else "v0"

    mp.log.info(f"got project info. version {version}")

    # prepare download
    update_tasks = []  # stuff to do at the end of download
    for file in project_info["files"]:
        file["version"] = version
        items = get_download_items(file.get("path"), file.get("size"), file.get("version"), tmp_dir.name)
        is_latest_version = project_version == latest_proj_info["version"]
        update_tasks.append(UpdateTask(file["path"], items, latest_version=is_latest_version))

    # make a single list of items to download
    total_size = 0
    download_list = []
    for task in update_tasks:
        download_list.extend(task.download_queue_items)
        for item in task.download_queue_items:
            total_size += item.size

    mp.log.info(f"will download {len(update_tasks)} files in {len(download_list)} chunks, total size {total_size}")

    job = DownloadJob(project_path, total_size, version, update_tasks, download_list, tmp_dir, mp, project_info)

    # start download
    job.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    job.futures = []
    for item in download_list:
        future = job.executor.submit(_do_download, item, mc, mp, project_path, job)
        job.futures.append(future)

    return job


def download_project_wait(job):
    """blocks until all download tasks are finished"""

    concurrent.futures.wait(job.futures)


def download_project_is_running(job):
    """
    Returns true/false depending on whether we have some pending downloads.

    It also forwards any exceptions from workers (e.g. some network errors). If an exception
    is raised, it is advised to call download_project_cancel() to abort the job.
    """
    for future in job.futures:
        if future.done() and future.exception() is not None:
            exc = future.exception()
            traceback_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
            job.mp.log.error("Error while downloading project: " + "".join(traceback_lines))
            job.mp.log.info("--- download aborted")
            job.failure_log_file = _cleanup_failed_download(job.mp)
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
            exc = future.exception()
            traceback_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
            job.mp.log.error("Error while downloading project: " + "".join(traceback_lines))
            job.mp.log.info("--- download aborted")
            job.failure_log_file = _cleanup_failed_download(job.mp)
            raise future.exception()

    job.mp.log.info("--- download finished")

    for task in job.update_tasks:
        # right now only copy tasks...
        task.apply(job.mp.dir, job.mp)

    # final update of project metadata
    job.mp.update_metadata(job.project_info)

    cleanup_tmp_dir(job.mp, job.tmp_dir)


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
    cleanup_tmp_dir(job.mp, job.tmp_dir)


class UpdateTask:
    """
    Entry for each file that will be updated.
    At the end of a successful download of new data, all the tasks are executed.
    """

    # TODO: methods other than COPY
    def __init__(
        self,
        file_path,
        download_queue_items: typing.List[DownloadQueueItem],
        destination_file=None,
        latest_version=True,
    ):
        self.file_path = file_path
        self.destination_file = destination_file
        self.download_queue_items = download_queue_items
        self.latest_version = latest_version

    def apply(self, directory, mp):
        """assemble downloaded chunks into a single file"""

        if self.destination_file is None:
            basename = os.path.basename(self.file_path)
            file_dir = os.path.dirname(os.path.normpath(os.path.join(directory, self.file_path)))
            dest_file_path = os.path.join(file_dir, basename)
        else:
            file_dir = os.path.dirname(os.path.normpath(self.destination_file))
            dest_file_path = self.destination_file
        os.makedirs(file_dir, exist_ok=True)

        # ignore check if we download not-latest version of gpkg file (possibly reconstructed on server on demand)
        check_size = self.latest_version or not mp.is_versioned_file(self.file_path)
        # merge chunks together (and delete them afterwards)
        file_to_merge = DownloadFile(dest_file_path, self.download_queue_items, check_size)
        file_to_merge.from_chunks()

        # Make a copy of the file to meta dir only if there is no user-specified path for the file.
        # destination_file is None for full project download and takes a meaningful value for a single file download.
        if mp.is_versioned_file(self.file_path) and self.destination_file is None:
            mp.geodiff.make_copy_sqlite(mp.fpath(self.file_path), mp.fpath_meta(self.file_path))


class PullJob:
    def __init__(
        self,
        project_path,
        pull_actions,
        total_size,
        version,
        download_files: List[DownloadFile],
        download_queue_items,
        tmp_dir,
        mp,
        project_info,
        basefiles_to_patch,
        mc,
        v2_pull_enabled=False,
    ):
        self.project_path = project_path
        self.pull_actions: Optional[List[PullAction]] = pull_actions
        self.total_size = total_size  # size of data to download (in bytes)
        self.transferred_size = 0
        self.version = version
        self.download_files = download_files  # list of DownloadFile instances
        self.download_queue_items = download_queue_items
        self.tmp_dir = tmp_dir  # TemporaryDirectory instance where we store downloaded files
        self.mp: MerginProject = mp
        self.is_cancelled = False
        self.project_info = project_info  # parsed JSON with project info returned from the server
        self.basefiles_to_patch = (
            basefiles_to_patch  # list of tuples (relative path within project, list of diff files in temp dir to apply)
        )
        self.mc = mc
        self.futures = []  # list of concurrent.futures.Future instances
        self.v2_pull_enabled = v2_pull_enabled  # whether v2 pull mechanism is used

    def dump(self):
        print("--- JOB ---", self.total_size, "bytes")
        for download_file in self.download_files:
            print("- {}  ... download items={}".format(download_file.dest_file, len(download_file.downloaded_items)))
        print("--")
        for basefile, diffs in self.basefiles_to_patch:
            print("patch basefile {}  with {} diffs".format(basefile, len(diffs)))
        print("--")
        for item in self.download_queue_items:
            print("- {} {} {} {}".format(item.file_path, item.version, item.part_index, item.size))
        print("--- END ---")


def get_download_diff_files(delta_item: ProjectDeltaItem, target_dir: str) -> List[DownloadFile]:
    """
    Extracts list of diff files to be downloaded from delta item using v1 endpoint per chunk.
    """
    result = []

    for diff in delta_item.diffs:
        dest_file_path = os.path.normpath(os.path.join(target_dir, diff.id))
        download_items = get_download_items(delta_item.path, diff.size, diff.version, target_dir, diff.id, True)
        result.append(DownloadFile(dest_file_path, download_items))
    return result


def pull_project_async(mc, directory) -> Optional[PullJob]:
    """
    Starts project pull in background and returns handle to the pending job.
    Using that object it is possible to watch progress or cancel the ongoing work.
    """

    mp = MerginProject(directory)
    if mp.has_unfinished_pull():
        try:
            mp.resolve_unfinished_pull(mc.username())
        except ClientError as err:
            mp.log.error("Error resolving unfinished pull: " + str(err))
            mp.log.info("--- pull aborted")
            raise

    project_path = mp.project_full_name()
    project_id = mp.project_id()
    local_version = mp.version()

    mp.log.info("--- version: " + mc.user_agent_info())
    mp.log.info(f"--- start pull {project_path}")

    delta = None
    server_info = None
    server_version = None
    v2_pull_enabled = mc.server_features().get("v2_pull_enabled", False)
    try:
        if v2_pull_enabled:
            mp.log.info("Using v2 pull delta endpoint")
            delta: ProjectDelta = mc.get_project_delta(project_id, since=local_version)
            server_version = delta.to_version
        else:
            server_info = mc.project_info(project_path, since=local_version)
            server_version = server_info.get("version")
            delta = mp.get_pull_delta(server_info)
    except ClientError as err:
        mp.log.error("Error getting project info: " + str(err))
        mp.log.info("--- pull aborted")
        raise

    if not delta.items:
        mp.log.info("--- pull - nothing to do (no delta changes detected)")
        return  # Project is up to date

    if local_version == server_version:
        mp.log.info("--- pull - nothing to do (already at server version)")
        return  # Project is up to date
    mp.log.info(f"got project versions: local version {local_version} / server version {server_version}")

    tmp_dir = tempfile.TemporaryDirectory(prefix="mm-pull-")
    # list of DownloadFile instances, which consists of destination path and list of download items
    download_files = []
    diff_files = []
    basefiles_to_patch = []  # list of tuples (relative path within project, list of diff files in temp dir to apply)
    pull_actions = []
    local_delta = mp.get_local_delta(tmp_dir.name)
    # Converting local to PullActions
    for item in delta.items:
        # find corresponding local delta item
        local_item = next((i for i in local_delta if i.path == item.path), None)
        local_item_change = local_item.change if local_item else None

        # compare server and local changes to decide what to do in pull
        pull_action_type = mp.get_pull_action(item.change, local_item_change)
        if not pull_action_type:
            continue  # no action needed

        pull_action = PullAction(pull_action_type, item, local_item)
        if pull_action_type == PullActionType.APPLY_DIFF or (
            pull_action_type == PullActionType.COPY_CONFLICT and item.change == DeltaChangeType.UPDATE_DIFF
        ):
            basefile = mp.fpath_meta(item.path)
            if not os.path.exists(basefile):
                # The basefile does not exist for some reason. This should not happen normally (maybe user removed the file
                # or we removed it within previous pull because we failed to apply patch the older version for some reason).
                # But it's not a problem - we will download the newest version and we're sorted.
                mp.log.info(f"missing base file for {item.path} -> going to download it (version {server_version})")
                items = get_download_items(item.path, item.size, server_version, tmp_dir.name)
                dest_file_path = mp.fpath(item.path, tmp_dir.name)
                download_files.append(DownloadFile(dest_file_path, items))

                # Force use COPY_CONFLICT action to apply the new version instead of trying to apply diffs
                # We are not able to get local changes anyway as base file is missing
                pull_action.type = PullActionType.COPY_CONFLICT
                pull_actions.append(pull_action)
                continue

            # if we have diff to apply, let's download the diff files
            # if we have conflict and diff update, download the diff files
            if v2_pull_enabled:
                # using v2 endpoint to download diff files, without chunks. Then we are creating DownloadDiffQueueItem instances for each diff file.
                diff_files.extend(
                    [
                        DownloadDiffQueueItem(diff_item.id, os.path.join(tmp_dir.name, diff_item.id))
                        for diff_item in item.diffs
                    ]
                )
                basefiles_to_patch.append((item.path, [diff.id for diff in item.diffs]))

            else:
                # fallback for diff files using v1 endpoint /raw
                # download chunks and create DownloadFile instances for each diff file
                diff_download_files = get_download_diff_files(item, tmp_dir.name)
                download_files.extend(diff_download_files)
                basefiles_to_patch.append((item.path, [diff.id for diff in item.diffs]))

        elif pull_action_type == PullActionType.COPY or pull_action_type == PullActionType.COPY_CONFLICT:
            # simply download the server version of the files
            dest_file_path = os.path.normpath(os.path.join(tmp_dir.name, item.path))
            download_items = get_download_items(item.path, item.size, server_version, tmp_dir.name)
            download_files.append(DownloadFile(dest_file_path, download_items))

        pull_actions.append(pull_action)
        # Do nothing for DELETE actions

    # make a single list of items to download
    total_size = 0
    download_queue_items = []
    # Diff files downloaded without chunks (downloaded as a whole, do need to be merged)
    for diff_file in diff_files:
        download_queue_items.append(diff_file)
        total_size += diff_file.size
    for file_to_merge in download_files:
        download_queue_items.extend(file_to_merge.downloaded_items)
        for item in file_to_merge.downloaded_items:
            total_size += item.size

    mp.log.info(f"will download {len(download_queue_items)} chunks, total size {total_size}")

    job = PullJob(
        project_path,
        pull_actions,
        total_size,
        server_version,
        download_files,
        download_queue_items,
        tmp_dir,
        mp,
        server_info,
        basefiles_to_patch,
        mc,
        v2_pull_enabled,
    )

    # start download
    job.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    job.futures = []
    for item in download_queue_items:
        future = job.executor.submit(_do_download, item, mc, mp, project_path, job)
        job.futures.append(future)

    return job


def pull_project_wait(job):
    """blocks until all download tasks are finished"""

    concurrent.futures.wait(job.futures)


def pull_project_is_running(job: PullJob):
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


def pull_project_cancel(job: PullJob):
    """
    To be called (from main thread) to cancel a job that has downloads in progress.
    Returns once all background tasks have exited (may block for a bit of time).
    """
    job.mp.log.info("user cancelled the pull...")
    # set job as cancelled
    job.is_cancelled = True
    job.executor.shutdown(wait=True)
    job.mp.log.info("--- pull cancelled")
    cleanup_tmp_dir(job.mp, job.tmp_dir)  # delete our temporary dir and all its content


def pull_project_finalize(job: PullJob):
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
    try:
        if not job.project_info and job.v2_pull_enabled:
            project_info_response = job.mc.project_info_v2(job.mp.project_id(), files_at_version=job.version)
            job.project_info = asdict(project_info_response)
    except NotImplementedError as e:
        job.mp.log.error("Failed to get project info v2 in this server version: " + str(e))
        job.mp.log.info("--- pull aborted")
        raise ClientError("Failed to get project info v2 as server not support it: " + str(e))
    except ClientError as e:
        job.mp.log.error("Failed to get project info v2: " + str(e))
        job.mp.log.info("--- pull aborted")
        raise

    if not job.project_info:
        job.mp.log.error("No project info available to finalize pull")
        job.mp.log.info("--- pull aborted")
        raise ClientError("No project info available to finalize pull")

    # create files from downloaded chunks
    try:
        for download_file in job.download_files:
            download_file.from_chunks()
    except ClientError as err:
        job.mp.log.error("Error merging chunks of downloaded file: " + str(err))
        job.mp.log.info("--- pull aborted")
        raise

    # make sure we can update geodiff reference files (aka. basefiles) with diffs or
    # download their full versions so we have them up-to-date for applying changes
    for file_path, file_diffs in job.basefiles_to_patch:
        basefile = job.mp.fpath_meta(file_path)
        server_file = job.mp.fpath(file_path, job.tmp_dir.name)

        job.mp.geodiff.make_copy_sqlite(basefile, server_file)
        diffs = [job.mp.fpath(f, job.tmp_dir.name) for f in file_diffs]
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
    conflicts = []
    job.mp.log.info(f"--- applying pull actions {job.pull_actions}")
    try:
        if job.pull_actions:
            conflicts = job.mp.apply_pull_actions(job.pull_actions, job.tmp_dir.name, job.project_info, job.mc)
    except Exception as e:
        job.mp.log.error("Failed to apply pull actions: " + str(e))
        job.mp.log.info("--- pull aborted")
        cleanup_tmp_dir(job.mp, job.tmp_dir)  # delete our temporary dir and all its content
        raise ClientError("Failed to apply pull actions: " + str(e))

    job.mp.update_metadata(job.project_info)

    if job.mp.has_unfinished_pull():
        job.mp.log.info("--- failed to complete pull -- project left in the unfinished pull state")
    else:
        job.mp.log.info("--- pull finished -- at version " + job.mp.version())

    cleanup_tmp_dir(job.mp, job.tmp_dir)  # delete our temporary dir and all its content
    return conflicts


def download_file_async(mc, project_dir, file_path, output_file, version):
    """
    Starts background download project file at specified version.
    Returns handle to the pending download.
    """
    return download_files_async(mc, project_dir, [file_path], [output_file], version)


def download_file_finalize(job):
    """
    To be called when download_file_async is finished
    """
    download_files_finalize(job)


def download_diffs_async(mc, project_directory, file_path, versions):
    """
    Starts background download project file diffs for specified versions.
    Returns handle to the pending download.

    Args:
        mc (MerginClient): MerginClient instance.
        project_directory (str): local project directory.
        file_path (str): file path relative to Mergin Maps project root.
        versions (list): list of versions to download diffs for, e.g. ['v1', 'v2'].

    Returns:
        PullJob/None: a handle for the pending download.
    """
    mp = MerginProject(project_directory)
    project_path = mp.project_full_name()
    file_history = mc.project_file_history_info(project_path, file_path)
    mp.log.info(f"--- version: {mc.user_agent_info()}")
    mp.log.info(f"--- start download diffs for {file_path} of {project_path}, versions: {[v for v in versions]}")

    try:
        server_info = mc.project_info(project_path)
        if file_history is None:
            file_history = mc.project_file_history_info(project_path, file_path)
    except ClientError as err:
        mp.log.error("Error getting project info: " + str(err))
        mp.log.info("--- downloading diffs aborted")
        raise

    fetch_files = []

    for version in versions:
        if version not in file_history["history"]:
            continue  # skip if this file was not modified at this version
        version_data = file_history["history"][version]
        if "diff" not in version_data:
            continue  # skip if there is no diff in history
        diff_data = copy.deepcopy(version_data)
        diff_data["version"] = version
        diff_data["diff"] = version_data["diff"]
        fetch_files.append(diff_data)

    download_files = []  # list of DownloadFile instances
    download_list = []  # list of all items to be downloaded
    total_size = 0
    for file in fetch_files:
        diff = file.get("diff")
        items = get_download_items(
            file.get("path"),
            diff["size"],
            file["version"],
            mp.cache_dir,
            download_path=diff.get("path"),
            diff_only=True,
        )
        dest_file_path = mp.fpath_cache(diff["path"], version=file["version"])
        if os.path.exists(dest_file_path):
            continue
        download_files.append(DownloadFile(dest_file_path, items))
        download_list.extend(items)
        for item in items:
            total_size += item.size

    mp.log.info(f"will download {len(download_list)} chunks, total size {total_size}")

    job = PullJob(
        project_path,
        None,
        total_size,
        None,
        download_files,
        download_list,
        mp.cache_dir,
        mp,
        server_info,
        {},
        mc,
    )

    # start download
    job.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    job.futures = []
    for item in download_list:
        future = job.executor.submit(_do_download, item, mc, mp, project_path, job)
        job.futures.append(future)

    return job


def download_diffs_finalize(job: PullJob) -> List[str]:
    """To be called after download_diffs_async

    Returns:
        diffs: list of downloaded diffs (their actual locations on disk)
    """

    job.executor.shutdown(wait=True)

    # make sure any exceptions from threads are not lost
    for future in job.futures:
        if future.exception() is not None:
            job.mp.log.error("Error while pulling data: " + str(future.exception()))
            job.mp.log.info("--- diffs download aborted")
            raise future.exception()

    job.mp.log.info("finalizing diffs pull")
    diffs = []

    # merge downloaded chunks
    try:
        for download_file in job.download_files:
            download_file.from_chunks()
            diffs.append(download_file.dest_file)
    except ClientError as err:
        job.mp.log.error("Error merging chunks of downloaded file: " + str(err))
        job.mp.log.info("--- diffs pull aborted")
        raise

    job.mp.log.info("--- diffs pull finished")
    return diffs


def download_files_async(
    mc, project_dir: str, file_paths: typing.List[str], output_paths: typing.List[str], version: str
):
    """
    Starts background download project files at specified version.
    Returns handle to the pending download.
    """
    mp = MerginProject(project_dir)
    project_path = mp.project_full_name()
    ver_info = f"at version {version}" if version is not None else "at latest version"
    mp.log.info(f"Getting [{', '.join(file_paths)}] {ver_info}")
    latest_proj_info = mc.project_info(project_path)
    if version:
        project_info = mc.project_info(project_path, version=version)
    else:
        project_info = latest_proj_info
    mp.log.info(f"Got project info. version {project_info['version']}")

    # set temporary directory for download
    tmp_dir = tempfile.mkdtemp(prefix="python-api-client-")

    if output_paths is None:
        output_paths = []
        for file in file_paths:
            output_paths.append(mp.fpath(file))

    if len(output_paths) != len(file_paths):
        warn = "Output file paths are not of the same length as file paths. Cannot store required files."
        mp.log.warning(warn)
        shutil.rmtree(tmp_dir)
        raise ClientError(warn)

    download_list = []
    update_tasks = []
    total_size = 0
    # None can not be used to indicate latest version of the file, so
    # it is necessary to pass actual version.
    if version is None:
        version = latest_proj_info["version"]
    for file in project_info["files"]:
        if file["path"] in file_paths:
            index = file_paths.index(file["path"])
            file["version"] = version
            items = get_download_items(file["path"], file["size"], version, mp.cache_dir)
            is_latest_version = version == latest_proj_info["version"]
            task = UpdateTask(file["path"], items, output_paths[index], latest_version=is_latest_version)
            download_list.extend(task.download_queue_items)
            for item in task.download_queue_items:
                total_size += item.size
            update_tasks.append(task)

    missing_files = []
    files_to_download = []
    project_file_paths = [file["path"] for file in project_info["files"]]
    for file in file_paths:
        if file not in project_file_paths:
            missing_files.append(file)
        else:
            files_to_download.append(file)

    if not download_list or missing_files:
        warn = f"No [{', '.join(missing_files)}] exists at version {version}"
        mp.log.warning(warn)
        shutil.rmtree(tmp_dir)
        raise ClientError(warn)

    mp.log.info(
        f"will download files [{', '.join(files_to_download)}] in {len(download_list)} chunks, total size {total_size}"
    )
    job = DownloadJob(project_path, total_size, version, update_tasks, download_list, tmp_dir, mp, project_info)
    job.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    job.futures = []
    for item in download_list:
        future = job.executor.submit(_do_download, item, mc, mp, project_path, job)
        job.futures.append(future)

    return job


def download_files_finalize(job):
    """
    To be called when download_file_async is finished
    """
    job.executor.shutdown(wait=True)

    # make sure any exceptions from threads are not lost
    for future in job.futures:
        if future.exception() is not None:
            raise future.exception()

    job.mp.log.info("--- download finished")

    for task in job.update_tasks:
        task.apply(job.tmp_dir, job.mp)

    # Remove temporary download directory
    if job.tmp_dir is not None and os.path.exists(job.tmp_dir):
        shutil.rmtree(job.tmp_dir)
