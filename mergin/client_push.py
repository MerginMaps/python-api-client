"""
To push projects asynchronously. Start push: (does not block)

job = push_project_async(mergin_client, '/tmp/my_project')

Then we need to wait until we are finished uploading - either by periodically
calling push_project_is_running(job) that will just return True/False or by calling
push_project_wait(job) that will block the current thread (not good for GUI).
To finish the upload job, we have to call push_project_finalize(job).
"""

import json
import hashlib
import pprint
import tempfile
import concurrent.futures
import os
from typing import Dict, List, Optional, Tuple

from .common import UPLOAD_CHUNK_SIZE, ClientError
from .merginproject import MerginProject
from .editor import is_editor_enabled, _apply_editor_filters
from .utils import is_qgis_file, is_versioned_file


class UploadJob:
    """Keeps all the important data about a pending upload job"""

    def __init__(self, project_path, changes, transaction_id, mp, mc, tmp_dir, exclusive: bool):
        self.project_path = project_path  # full project name ("username/projectname")
        self.changes = changes  # dictionary of local changes to the project
        self.transaction_id = transaction_id  # ID of the transaction assigned by the server
        self.total_size = 0  # size of data to upload (in bytes)
        self.transferred_size = 0  # size of data already uploaded (in bytes)
        self.upload_queue_items = []  # list of items to upload in the background
        self.mp = mp  # MerginProject instance
        self.mc = mc  # MerginClient instance
        self.tmp_dir = tmp_dir  # TemporaryDirectory instance for any temp file we need
        self.exclusive = exclusive  # flag whether this upload blocks other uploads
        self.is_cancelled = False  # whether upload has been cancelled
        self.executor = None  # ThreadPoolExecutor that manages background upload tasks
        self.futures = []  # list of futures submitted to the executor
        self.server_resp = None  # server response when transaction is finished

    def dump(self):
        print("--- JOB ---", self.total_size, "bytes")
        for item in self.upload_queue_items:
            print("- {} {} {}".format(item.file_path, item.chunk_index, item.size))
        print("--- END ---")


class UploadQueueItem:
    """A single chunk of data that needs to be uploaded"""

    def __init__(self, file_path, size, transaction_id, chunk_id, chunk_index):
        self.file_path = file_path  # full path to the file
        self.size = size  # size of the chunk in bytes
        self.chunk_id = chunk_id  # ID of the chunk within transaction
        self.chunk_index = chunk_index  # index (starting from zero) of the chunk within the file
        self.transaction_id = transaction_id  # ID of the transaction

    def upload_blocking(self, mc, mp):
        with open(self.file_path, "rb") as file_handle:
            file_handle.seek(self.chunk_index * UPLOAD_CHUNK_SIZE)
            data = file_handle.read(UPLOAD_CHUNK_SIZE)

            checksum = hashlib.sha1()
            checksum.update(data)

            mp.log.debug(f"Uploading {self.file_path} part={self.chunk_index}")

            headers = {"Content-Type": "application/octet-stream"}
            resp = mc.post(
                "/v1/project/push/chunk/{}/{}".format(self.transaction_id, self.chunk_id),
                data,
                headers,
            )
            resp_dict = json.load(resp)
            mp.log.debug(f"Upload finished: {self.file_path}")
            if not (resp_dict["size"] == len(data) and resp_dict["checksum"] == checksum.hexdigest()):
                try:
                    mc.post("/v1/project/push/cancel/{}".format(self.transaction_id))
                except ClientError:
                    pass
                raise ClientError("Mismatch between uploaded file chunk {} and local one".format(self.chunk_id))


class ChangesHandler:
    """
    Handles preparation of file changes to be uploaded to the server.

    This class is responsible for:
    - Filtering project file changes.
    - Splitting changes into blocking and non-blocking groups.
    - TODO: Applying limits such as max file count or size to break large uploads into smaller batches.
    - Generating upload-ready change groups for asynchronous job creation.
    """

    def __init__(self, client, project_dir):
        self.client = client
        self.mp = MerginProject(project_dir)
        self._raw_changes = self.mp.get_push_changes()

    @staticmethod
    def is_blocking_file(file):
        return is_qgis_file(file["path"]) or is_versioned_file(file["path"])

    def _split_by_type(self, changes: Dict[str, List[dict]]) -> List[Dict[str, List[dict]]]:
        """
        Split raw filtered changes into two batches:
        1. Blocking: updated/removed and added files that are blocking
        2. Non-blocking: added files that are not blocking
        """
        blocking_changes = {"added": [], "updated": [], "removed": []}
        non_blocking_changes = {"added": [], "updated": [], "removed": []}

        for f in changes.get("added", []):
            if self.is_blocking_file(f):
                blocking_changes["added"].append(f)
            else:
                non_blocking_changes["added"].append(f)

        for f in changes.get("updated", []):
            blocking_changes["updated"].append(f)

        for f in changes.get("removed", []):
            blocking_changes["removed"].append(f)

        result = []
        if any(len(v) for v in blocking_changes.values()):
            result.append(blocking_changes)
        if any(len(v) for v in non_blocking_changes.values()):
            result.append(non_blocking_changes)

        return result

    def split(self) -> List[Dict[str, List[dict]]]:
        """
        Applies all configured internal filters and returns a list of change ready to be uploaded.
        """
        project_name = self.mp.project_full_name()
        try:
            project_info = self.client.project_info(project_name)
        except ClientError as e:
            self.mp.log.error(f"Failed to get project info for project {project_name}: {e}")
            raise
        changes = filter_changes(self.client, project_info, self._raw_changes)
        changes_list = self._split_by_type(changes)
        # TODO: apply limits; changes = self._limit_by_file_count(changes)
        return changes_list


def filter_changes(mc, project_info, changes: Dict[str, List[dict]]) -> Dict[str, List[dict]]:
    """
    Filters the given changes dictionary based on the editor's enabled state.

    If the editor is not enabled, the changes dictionary is returned as-is. Otherwise, the changes are passed through the `_apply_editor_filters` method to apply any configured filters.

    Args:
        changes (dict[str, list[dict]]): A dictionary mapping file paths to lists of change dictionaries.

    Returns:
        dict[str, list[dict]]: The filtered changes dictionary.
    """
    if is_editor_enabled(mc, project_info):
        changes = _apply_editor_filters(changes)
    return changes


def get_change_batch(mc, project_dir) -> Tuple[Optional[Dict[str, List[dict]]], bool]:
    """
    Return the next changes dictionary and flag if there are more changes (to be uploaded in the next upload job)
    """
    changes_list = ChangesHandler(mc, project_dir).split()
    if not changes_list:
        return None, False
    non_empty_length = sum(any(v for v in d.values()) for d in changes_list)
    return changes_list[0], non_empty_length > 1


def push_project_async(mc, directory, changes=None) -> Optional[UploadJob]:
    """
    Starts push in background and returns pending upload job.
    Pushes all project changes unless change_batch is provided.
    When specific change is provided, initial version check is skipped (the pull has just been done).

    :param changes: The changes to upload are either (1) provided (and already split to blocking and bob-blocking batches)
        or (2) all local changes are retrieved to upload
    Pushing only non-blocking changes results in non-exclusive upload which server allows to be concurrent.
    """

    mp = MerginProject(directory)
    if mp.has_unfinished_pull():
        raise ClientError("Project is in unfinished pull state. Please resolve unfinished pull and try again.")

    project_path = mp.project_full_name()
    local_version = mp.version()

    mp.log.info("--- version: " + mc.user_agent_info())
    mp.log.info(f"--- start push {project_path}")

    # if we have specific change to push we don't need version check
    if not changes:
        try:
            project_info = mc.project_info(project_path)
        except ClientError as err:
            mp.log.error("Error getting project info: " + str(err))
            mp.log.info("--- push aborted")
            raise
        server_version = project_info["version"] if project_info["version"] else "v0"

        mp.log.info(f"got project info: local version {local_version} / server version {server_version}")

        username = mc.username()
        # permissions field contains information about update, delete and upload privileges of the user
        # on a specific project. This is more accurate information than "writernames" field, as it takes
        # into account namespace privileges. So we have to check only "permissions", namely "upload" once
        if not mc.has_writing_permissions(project_path):
            mp.log.error(f"--- push {project_path} - username {username} does not have write access")
            raise ClientError(f"You do not seem to have write access to the project (username '{username}')")

        if local_version != server_version:
            mp.log.error(f"--- push {project_path} - not up to date (local {local_version} vs server {server_version})")
            raise ClientError(
                "There is a new version of the project on the server. Please update your local copy."
                + f"\n\nLocal version: {local_version}\nServer version: {server_version}"
            )
        changes = filter_changes(mc, project_info, mp.get_push_changes())

    mp.log.debug("push change:\n" + pprint.pformat(changes))

    tmp_dir = tempfile.TemporaryDirectory(prefix="python-api-client-")

    # If there are any versioned files (aka .gpkg) that are not updated through a diff,
    # we need to make a temporary copy somewhere to be sure that we are uploading full content.
    # That's because if there are pending transactions, checkpointing or switching from WAL mode
    # won't work, and we would end up with some changes left in -wal file which do not get
    # uploaded. The temporary copy using geodiff uses sqlite backup API and should copy everything.
    for f in changes["updated"]:
        if mp.is_versioned_file(f["path"]) and "diff" not in f:
            mp.copy_versioned_file_for_upload(f, tmp_dir.name)

    for f in changes["added"]:
        if mp.is_versioned_file(f["path"]):
            mp.copy_versioned_file_for_upload(f, tmp_dir.name)

    if not any(len(v) for v in changes.values()):
        mp.log.info(f"--- push {project_path} - nothing to do")
        return

    # drop internal info from being sent to server
    for item in changes["updated"]:
        item.pop("origin_checksum", None)
    data = {"version": local_version, "changes": changes}

    try:
        resp = mc.post(
            f"/v1/project/push/{project_path}",
            data,
            {"Content-Type": "application/json"},
        )
    except ClientError as err:
        mp.log.error("Error starting transaction: " + str(err))
        mp.log.info("--- push aborted")
        raise
    server_resp = json.load(resp)

    upload_files = data["changes"]["added"] + data["changes"]["updated"]

    transaction_id = server_resp["transaction"] if upload_files else None
    exclusive = server_resp.get("blocking", True)
    job = UploadJob(project_path, changes, transaction_id, mp, mc, tmp_dir, exclusive)

    if not upload_files:
        mp.log.info("not uploading any files")
        job.server_resp = server_resp
        push_project_finalize(job)
        return None  # all done - no pending job

    mp.log.info(f"got transaction ID {transaction_id}, {'exclusive' if exclusive else 'non-exclusive'} upload")

    upload_queue_items = []
    total_size = 0
    # prepare file chunks for upload
    for file in upload_files:
        if "diff" in file:
            # versioned file - uploading diff
            file_location = mp.fpath_meta(file["diff"]["path"])
            file_size = file["diff"]["size"]
        elif "upload_file" in file:
            # versioned file - uploading full (a temporary copy)
            file_location = file["upload_file"]
            file_size = file["size"]
        else:
            # non-versioned file
            file_location = mp.fpath(file["path"])
            file_size = file["size"]

        for chunk_index, chunk_id in enumerate(file["chunks"]):
            size = min(UPLOAD_CHUNK_SIZE, file_size - chunk_index * UPLOAD_CHUNK_SIZE)
            upload_queue_items.append(UploadQueueItem(file_location, size, transaction_id, chunk_id, chunk_index))

        total_size += file_size

    job.total_size = total_size
    job.upload_queue_items = upload_queue_items

    mp.log.info(f"will upload {len(upload_queue_items)} items with total size {total_size}")

    # start uploads in background
    job.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    for item in upload_queue_items:
        future = job.executor.submit(_do_upload, item, job)
        job.futures.append(future)

    return job


def push_project_wait(job):
    """blocks until all upload tasks are finished"""

    concurrent.futures.wait(job.futures)


def push_project_is_running(job):
    """
    Returns true/false depending on whether we have some pending uploads

    It also forwards any exceptions from workers (e.g. some network errors). If an exception
    is raised, it is advised to call push_project_cancel() to abort the job.
    """
    for future in job.futures:
        if future.done() and future.exception() is not None:
            job.mp.log.error("Error while pushing data: " + str(future.exception()))
            job.mp.log.info("--- push aborted")
            raise future.exception()
        if future.running():
            return True
    return False


def push_project_finalize(job):
    """
    To be called when push in the background is finished and we need to do the finalization

    This should not be called from a worker thread (e.g. directly from a handler when push is complete).

    If any of the workers has thrown any exception, it will be re-raised (e.g. some network errors).
    That also means that the whole job has been aborted.
    """

    with_upload_of_files = job.executor is not None

    if with_upload_of_files:
        job.executor.shutdown(wait=True)

        # make sure any exceptions from threads are not lost
        for future in job.futures:
            if future.exception() is not None:
                job.mp.log.error("Error while pushing data: " + str(future.exception()))
                job.mp.log.info("--- push aborted")
                raise future.exception()

    if job.transferred_size != job.total_size:
        error_msg = "Transferred size ({}) and expected total size ({}) do not match!".format(
            job.transferred_size, job.total_size
        )
        job.mp.log.error("--- push finish failed! " + error_msg)
        raise ClientError("Upload error: " + error_msg)

    if with_upload_of_files:
        try:
            job.mp.log.info(
                f"Finishing {'exclusive' if job.exclusive else 'non-exclusive'} transaction {job.transaction_id}"
            )
            resp = job.mc.post("/v1/project/push/finish/%s" % job.transaction_id)
            job.server_resp = json.load(resp)
        except ClientError as err:
            # server returns various error messages with filename or something generic
            # it would be better if it returned list of failed files (and reasons) whenever possible
            job.mp.log.error("--- push finish failed! " + str(err))

            # if push finish fails, the transaction is not killed, so we
            # need to cancel it so it does not block further uploads
            job.mp.log.info("canceling the pending transaction...")
            try:
                resp_cancel = job.mc.post("/v1/project/push/cancel/%s" % job.transaction_id)
                job.mp.log.info("cancel response: " + resp_cancel.msg)
            except ClientError as err2:
                job.mp.log.info("cancel response: " + str(err2))
            raise err

    job.mp.update_metadata(job.server_resp)
    try:
        job.mp.apply_push_changes(job.changes)
    except Exception as e:
        job.mp.log.error("Failed to apply push changes: " + str(e))
        job.mp.log.info("--- push aborted")
        raise ClientError("Failed to apply push changes: " + str(e))

    job.tmp_dir.cleanup()  # delete our temporary dir and all its content

    remove_diff_files(job)

    job.mp.log.info("--- push finished - new project version " + job.server_resp["version"])


def push_project_cancel(job):
    """
    To be called (from main thread) to cancel a job that has uploads in progress.
    Returns once all background tasks have exited (may block for a bit of time).
    """

    job.mp.log.info("user cancelled the push...")
    # set job as cancelled
    job.is_cancelled = True

    job.executor.shutdown(wait=True)
    try:
        resp_cancel = job.mc.post("/v1/project/push/cancel/%s" % job.transaction_id)
        job.server_resp = resp_cancel.msg
    except ClientError as err:
        job.mp.log.error("--- push cancelling failed! " + str(err))
        raise err
    job.mp.log.info("--- push cancel response: " + str(job.server_resp))


def _do_upload(item, job):
    """runs in worker thread"""
    if job.is_cancelled:
        return

    item.upload_blocking(job.mc, job.mp)
    job.transferred_size += item.size


def remove_diff_files(job) -> None:
    """Looks for diff files in the job and removes them."""

    for change in job.changes["updated"]:
        if "diff" in change.keys():
            diff_file = job.mp.fpath_meta(change["diff"]["path"])
            if os.path.exists(diff_file):
                os.remove(diff_file)


def total_upload_size(directory) -> int:
    """
    Total up the number of bytes that need to be uploaded.
    """
    mp = MerginProject(directory)
    changes = mp.get_push_changes()
    files = changes.get("added", []) + changes.get("updated", [])
    size = sum(f.get("diff", {}).get("size", f.get("size", 0)) for f in files)
    mp.log.info(f"Upload size of all files is {size}")
    return size
