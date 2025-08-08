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
import math
from typing import List, Tuple, Optional
import uuid

from .common import UPLOAD_CHUNK_SIZE, ClientError
from .merginproject import MerginProject
from .editor import filter_changes

def _do_upload(item, job):
    """runs in worker thread, have to be defined here to avoid worker threads repeating this function"""
    if job.is_cancelled:
        return

    item.upload_blocking()
    job.transferred_size += item.size


class UploadChunksCache:
    """A cache for uploaded chunks to avoid re-uploading them, using checksum as key."""

    def __init__(self):
        self.cache = {}

    def add(self, checksum, chunk_id):
        self.cache[checksum] = chunk_id

    def get_chunk_id(self, checksum):
        """Get chunk_id by checksum, or None if not present."""
        return self.cache.get(checksum)


class UploadQueueItem:
    """A single chunk of data that needs to be uploaded"""

    def __init__(self, mc, mp, file_path, size, file_checksum, chunk_id, chunk_index):
        self.mc = mc  # MerginClient instance, set when uploading
        self.mp: MerginProject = mp  # MerginProject instance, set when uploading
        self.file_path = file_path  # full path to the file
        self.size = size  # size of the chunk in bytes
        self.file_checksum = file_checksum  # checksum of the file, set when uploading
        self.chunk_id = chunk_id  # ID of the chunk within transaction
        self.chunk_index = chunk_index  # index (starting from zero) of the chunk within the file
        self.transaction_id = ""  # ID of the transaction, assigned by the server when starting the upload

        self._request_headers = {"Content-Type": "application/octet-stream"}

    def upload_chunk(self, data: bytes, checksum: str):
        """
        Uploads the chunk to the server.
        """
        resp = self.mc.post(
            f"/v1/project/push/chunk/{self.transaction_id}/{self.chunk_id}",
            data,
            self._request_headers,
        )
        resp_dict = json.load(resp)

        if not (resp_dict["size"] == len(data) and resp_dict["checksum"] == checksum):
            try:
                self.mc.post("/v1/project/push/cancel/{}".format(self.transaction_id))
            except ClientError:
                pass
            raise ClientError("Mismatch between uploaded file chunk {} and local one".format(self.chunk_id))

    def upload_chunk_v2_api(self, data: bytes, checksum: str):
        """
        Uploads the chunk to the server.
        :param mc: MerginClient instance
        """
        # try to lookup the chunk in the cache, if yes use it.
        cached_chunk_id = self.mc.upload_chunks_cache.get_chunk_id(checksum)
        if cached_chunk_id:
            self.chunk_id = cached_chunk_id
            self.mp.log.debug(f"Chunk {self.chunk_id} already uploaded, skipping upload")
            return
        project_id = self.mp.project_id()
        resp = self.mc.post(
            f"/v2/projects/{project_id}/chunks",
            data,
            self._request_headers,
        )
        resp_dict = json.load(resp)
        self.chunk_id = resp_dict.get("id")
        self.mp.log.debug(f"Upload chunk finished: {self.file_path}")

    def upload_blocking(self):
        with open(self.file_path, "rb") as file_handle:
            file_handle.seek(self.chunk_index * UPLOAD_CHUNK_SIZE)
            data = file_handle.read(UPLOAD_CHUNK_SIZE)
            checksum = hashlib.sha1()
            checksum.update(data)
            checksum_str = checksum.hexdigest()

            self.mp.log.debug(f"Uploading {self.file_path} part={self.chunk_index}")
            if self.mc.server_features().get("v2_push_enabled"):
                # use v2 API for uploading chunks
                self.upload_chunk_v2_api(data, checksum_str)
            else:
                # use v1 API for uploading chunks
                self.upload_chunk(data, checksum_str)

            self.mp.log.debug(f"Upload chunk finished: {self.file_path}")


class UploadJob:
    """Keeps all the important data about a pending upload job"""

    def __init__(self, version: str, changes: dict, transaction_id: Optional[str], mp: MerginProject, mc, tmp_dir):
        # self.project_path = project_path  # full project name ("username/projectname")
        self.version = version
        self.changes = changes  # dictionary of local changes to the project
        self.transaction_id = transaction_id  # ID of the transaction assigned by the server
        self.total_size = 0  # size of data to upload (in bytes)
        self.transferred_size = 0  # size of data already uploaded (in bytes)
        self.upload_queue_items = []  # list of items to upload in the background
        self.mp = mp  # MerginProject instance
        self.mc = mc  # MerginClient instance
        self.tmp_dir = tmp_dir  # TemporaryDirectory instance for any temp file we need
        self.is_cancelled = False  # whether upload has been cancelled
        self.executor = None  # ThreadPoolExecutor that manages background upload tasks
        self.futures = []  # list of futures submitted to the executor
        self.server_resp = None  # server response when transaction is finished

    def dump(self):
        print("--- JOB ---", self.total_size, "bytes")
        for item in self.upload_queue_items:
            print("- {} {} {}".format(item.file_path, item.chunk_index, item.size))
        print("--- END ---")


    def submit_item_to_thread(self, item: UploadQueueItem):
        """Upload a single item in the background"""
        feature = self.executor.submit(_do_upload, item, self)
        self.futures.append(feature)

    def add_items(self, items: List[UploadQueueItem], total_size: int):
        """Add multiple chunks to the upload queue"""
        self.total_size = total_size
        self.upload_queue_items = items

        self.mp.log.info(f"will upload {len(items)} items with total size {total_size}")

        # start uploads in background
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        for item in items:
            item.transaction_id = self.transaction_id  # ensure transaction ID is set
            self.submit_item_to_thread(item)


def create_upload_chunks(mc, mp: MerginProject, files: dict) -> Tuple[List[UploadQueueItem], int]:
    """
    Create a list of UploadQueueItem objects from the changes dictionary and calculate total size of files.
    This is used to prepare the upload queue for the upload job.
    """
    upload_queue_items = []
    total_size = 0
    # prepare file chunks for upload
    for file in files:
        file_checksum = file.get("checksum")
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
            upload_queue_items.append(
                UploadQueueItem(mc, mp, file_location, size, file_checksum, chunk_id, chunk_index)
            )

        total_size += file_size

    return upload_queue_items, total_size


def create_upload_job(mc, mp: MerginProject, changes: dict, tmp_dir: tempfile.TemporaryDirectory):
    project_path = mp.project_full_name()
    local_version = mp.version()

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
    job = UploadJob(local_version, changes, transaction_id, mp, mc, tmp_dir)

    if not upload_files:
        mp.log.info("not uploading any files")
        job.server_resp = server_resp
        push_project_finalize(job)
        return None  # all done - no pending job

    mp.log.info(f"got transaction ID {transaction_id}")

    # prepare file chunks for upload
    upload_queue_items, total_size = create_upload_chunks(mc, mp, upload_files)
    job.add_items(upload_queue_items, total_size)
    return job


def create_upload_job_v2_api(mc, mp: MerginProject, changes: dict, tmp_dir: tempfile.TemporaryDirectory):
    project_id = mp.project_id()
    local_version = mp.version()

    data = {"version": local_version, "changes": changes}
    try:
        resp = mc.post(
            f"/v2/projects/{project_id}/versions",
            {**data, "check_only": True},
            {"Content-Type": "application/json"},
        )
    except ClientError as err:
        mp.log.error("Error starting transaction: " + str(err))
        mp.log.info("--- push aborted")
        raise

    upload_files = data["changes"]["added"] + data["changes"]["updated"]

    job = UploadJob(local_version, changes, None, mp, mc, tmp_dir)

    if not upload_files:
        mp.log.info("not uploading any files")
        job.server_resp = resp
        push_project_finalize(job)
        return None  # all done - no pending job

    mp.log.info(f"Starting upload chunks for project {project_id}")

    # prepare file chunks for upload
    upload_queue_items, total_size = create_upload_chunks(mc, mp, upload_files)
    job.add_items(upload_queue_items, total_size)
    return job


def push_project_async(mc, directory) -> Optional[UploadJob]:
    """Starts push of a project and returns pending upload job"""

    mp = MerginProject(directory)
    if mp.has_unfinished_pull():
        raise ClientError("Project is in unfinished pull state. Please resolve unfinished pull and try again.")

    project_path = mp.project_full_name()
    local_version = mp.version()

    mp.log.info("--- version: " + mc.user_agent_info())
    mp.log.info(f"--- start push {project_path}")

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
    # on a specific project. This is more accurate information then "writernames" field, as it takes
    # into account namespace privileges. So we have to check only "permissions", namely "upload" one
    if not mc.has_writing_permissions(project_path):
        mp.log.error(f"--- push {project_path} - username {username} does not have write access")
        raise ClientError(f"You do not seem to have write access to the project (username '{username}')")

    if local_version != server_version:
        mp.log.error(f"--- push {project_path} - not up to date (local {local_version} vs server {server_version})")
        raise ClientError(
            "There is a new version of the project on the server. Please update your local copy."
            + f"\n\nLocal version: {local_version}\nServer version: {server_version}"
        )

    changes = mp.get_push_changes()
    changes = filter_changes(mc, project_info, changes)
    mp.log.debug("push changes:\n" + pprint.pformat(changes))

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

    if not sum(len(v) for v in changes.values()):
        mp.log.info(f"--- push {project_path} - nothing to do")
        return

    # drop internal info from being sent to server
    for item in changes["updated"]:
        item.pop("origin_checksum", None)
    server_feaure_flags = mc.server_features()
    job = None
    if server_feaure_flags.get("v2_push_enabled"):
        # v2 push uses a different endpoint
        job = create_upload_job_v2_api(mc, mp, changes, tmp_dir)
    else:
        # v1 push uses the old endpoint
        job = create_upload_job(mc, mp, changes, tmp_dir)
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


def push_project_finalize(job: UploadJob):
    """
    To be called when push in the background is finished and we need to do the finalization

    This should not be called from a worker thread (e.g. directly from a handler when push is complete).

    If any of the workers has thrown any exception, it will be re-raised (e.g. some network errors).
    That also means that the whole job has been aborted.
    """

    with_upload_of_files = job.executor is not None
    server_features = job.mc.server_features()

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

    # map chunk ids to file checksums
    change_keys = ["added", "updated"]
    for key in change_keys:
        if (key not in job.changes):
            continue
        uploaded_chunks = []
        for change in job.changes.get(key, []):
            change["chunks"] = [
                upload.chunk_id for upload in job.upload_queue_items if upload.file_checksum == change.get("checksum")
            ]
            uploaded_chunks.append(change)
            print(change)
        job.changes[key] = uploaded_chunks

    if with_upload_of_files:
        if server_features.get("v2_push_enabled"):
            # v2 push uses a different endpoint
            try:
                job.mp.log.info(f"Finishing transaction for project {job.mp.project_full_name()}")
                job.mc.post(
                    f"/v2/projects/{job.mp.project_id()}/versions",
                    data={
                        "version": job.version,
                        "changes": job.changes,
                    },
                    headers={"Content-Type": "application/json"},
                )
                project_info = job.mc.project_info(job.mp.project_id())
                job.server_resp = project_info
            except ClientError as err:
                job.mp.log.error("--- push finish failed! " + str(err))
                raise err
        else:
            try:
                job.mp.log.info(f"Finishing transaction {job.transaction_id}")
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


def remove_diff_files(job) -> None:
    """Looks for diff files in the job and removes them."""

    for change in job.changes["updated"]:
        if "diff" in change.keys():
            diff_file = job.mp.fpath_meta(change["diff"]["path"])
            if os.path.exists(diff_file):
                os.remove(diff_file)
