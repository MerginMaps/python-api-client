"""
To push projects asynchronously. Start push: (does not block)

job = push_project_async(mergin_client, '/tmp/my_project')

Then we need to wait until we are finished uploading - either by periodically
calling push_project_is_running(job) that will just return True/False or by calling
push_project_wait(job) that will block the current thread (not good for GUI).
To finish the upload job, we have to call push_project_finalize(job).

We introduced here compability with:
v1 Push API using transaction approach
v2 Push API using raw chunks upload approach
"""

from dataclasses import asdict
import json
import pprint
import tempfile
import concurrent.futures
import os
import time
from typing import List, Tuple, Optional, ByteString

from .local_changes import ChangesValidationError, FileChange, LocalProjectChanges

from .common import (
    MAX_UPLOAD_VERSIONED_SIZE,
    UPLOAD_CHUNK_ATTEMPT_WAIT,
    UPLOAD_CHUNK_ATTEMPTS,
    UPLOAD_CHUNK_SIZE,
    MAX_UPLOAD_MEDIA_SIZE,
    ClientError,
)
from .merginproject import MerginProject
from .editor import filter_changes
from .utils import get_data_checksum

POST_JSON_HEADERS = {"Content-Type": "application/json"}


class UploadChunksCache:
    """A cache for uploaded chunks to avoid re-uploading them, using checksum as key."""

    def __init__(self):
        self.cache = {}

    def add(self, checksum, chunk_id):
        self.cache[checksum] = chunk_id

    def get_chunk_id(self, checksum):
        """Get chunk_id by checksum, or None if not present."""
        return self.cache.get(checksum)

    def clear(self):
        """Clear the cache."""
        self.cache.clear()


class UploadQueueItem:
    """A single chunk of data that needs to be uploaded"""

    def __init__(
        self, mc, mp: MerginProject, file_path: str, size: int, file_checksum: str, chunk_id: str, chunk_index: int
    ):
        self.mc = mc  # MerginClient instance, set when uploading
        self.mp: MerginProject = mp  # MerginProject instance, set when uploading
        self.file_path = file_path  # full path to the file
        self.size = size  # size of the chunk in bytes
        self.file_checksum = file_checksum  # checksum of the file uploaded file
        self.chunk_id = chunk_id  # ID of the chunk within transaction
        self.server_chunk_id = None  # ID of the chunk on the server, set when uploading
        self.chunk_index = chunk_index  # index (starting from zero) of the chunk within the file
        self.transaction_id = ""  # ID of the transaction, assigned by the server when starting the upload

        self._request_headers = {"Content-Type": "application/octet-stream"}

    def upload_chunk_v1_api(self, data: ByteString, checksum: str):
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
                self.mc.post(f"/v1/project/push/cancel/{self.transaction_id}")
            except ClientError:
                pass
            raise ClientError(f"Mismatch between uploaded file chunk {self.chunk_id} and local one")

    def upload_chunk_v2_api(self, data: ByteString, checksum: str):
        """
        Uploads the chunk to the server.
        """
        # try to lookup the chunk in the cache, if yes use it.
        cached_chunk_id = self.mc.upload_chunks_cache.get_chunk_id(checksum)
        if cached_chunk_id:
            self.server_chunk_id = cached_chunk_id
            self.mp.log.debug(f"Chunk {self.server_chunk_id} already uploaded, skipping upload")
            return
        project_id = self.mp.project_id()
        resp = self.mc.post(
            f"/v2/projects/{project_id}/chunks",
            data,
            self._request_headers,
        )
        resp_dict = json.load(resp)
        self.server_chunk_id = resp_dict.get("id")
        self.mc.upload_chunks_cache.add(checksum, self.server_chunk_id)

    def upload_blocking(self):
        with open(self.file_path, "rb") as file_handle:
            file_handle.seek(self.chunk_index * UPLOAD_CHUNK_SIZE)
            data = file_handle.read(UPLOAD_CHUNK_SIZE)
            checksum_str = get_data_checksum(data)

            self.mp.log.debug(f"Uploading {self.file_path} part={self.chunk_index}")
            for attempt in range(UPLOAD_CHUNK_ATTEMPTS):
                try:
                    if self.mc.server_features().get("v2_push_enabled"):
                        # use v2 API for uploading chunks
                        self.upload_chunk_v2_api(data, checksum_str)
                    else:
                        # use v1 API for uploading chunks
                        self.upload_chunk_v1_api(data, checksum_str)
                    break  # exit loop if upload was successful
                except ClientError as e:
                    if attempt < UPLOAD_CHUNK_ATTEMPTS - 1:
                        time.sleep(UPLOAD_CHUNK_ATTEMPT_WAIT)
                        continue
                    raise

            self.mp.log.debug(f"Upload chunk {self.server_chunk_id or self.chunk_id} finished: {self.file_path}")


class UploadJob:
    """Keeps all the important data about a pending upload job"""

    def __init__(
        self, version: str, changes: LocalProjectChanges, transaction_id: Optional[str], mp: MerginProject, mc, tmp_dir
    ):
        self.version = version
        self.changes: LocalProjectChanges = changes  # dictionary of local changes to the project
        self.transaction_id = transaction_id  # ID of the transaction assigned by the server
        self.total_size = 0  # size of data to upload (in bytes)
        self.transferred_size = 0  # size of data already uploaded (in bytes)
        self.upload_queue_items: List[UploadQueueItem] = []  # list of items to upload in the background
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
            print(f"- {item.file_path} {item.chunk_index} {item.size}")
        print("--- END ---")

    def _submit_item_to_thread(self, item: UploadQueueItem):
        """Upload a single item in the background"""
        future = self.executor.submit(_do_upload, item, self)
        self.futures.append(future)

    def start(self, items: List[UploadQueueItem]):
        """Starting upload in background with multiple upload items (UploadQueueItem)"""
        self.total_size = sum(item.size for item in items)
        self.upload_queue_items = items

        self.mp.log.info(f"will upload {len(self.upload_queue_items)} items with total size {self.total_size}")

        # start uploads in background
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        for item in items:
            if self.transaction_id:
                item.transaction_id = self.transaction_id
            self._submit_item_to_thread(item)

    def update_chunks_from_items(self):
        """
        Update chunks in LocalProjectChanges from the upload queue items.
        Used just before finalizing the transaction to set the server_chunk_id in v2 API.
        """
        self.changes.update_chunk_ids([(item.chunk_id, item.server_chunk_id) for item in self.upload_queue_items])


def _do_upload(item: UploadQueueItem, job: UploadJob):
    """runs in worker thread, have to be defined here to avoid worker threads repeating this function"""
    if job.is_cancelled:
        return

    item.upload_blocking()
    job.transferred_size += item.size


def create_upload_chunks(mc, mp: MerginProject, local_changes: List[FileChange]) -> List[UploadQueueItem]:
    """
    Create a list of UploadQueueItem objects from the changes dictionary and calculate total size of files.
    This is used to prepare the upload queue for the upload job.
    """
    upload_queue_items = []
    # prepare file chunks for upload
    for change in local_changes:
        file_checksum = change.checksum
        diff = change.get_diff()
        if diff:
            # versioned file - uploading diff
            file_location = mp.fpath_meta(diff.path)
            file_size = diff.size
        elif change.upload_file:
            # versioned file - uploading full (a temporary copy)
            file_location = change.upload_file
            file_size = change.size
        else:
            # non-versioned file
            file_location = mp.fpath(change.path)
            file_size = change.size

        for chunk_index, chunk_id in enumerate(change.chunks):
            size = min(UPLOAD_CHUNK_SIZE, file_size - chunk_index * UPLOAD_CHUNK_SIZE)
            upload_queue_items.append(
                UploadQueueItem(mc, mp, file_location, size, file_checksum, chunk_id, chunk_index)
            )

    return upload_queue_items


def create_upload_job(
    mc, mp: MerginProject, changes: LocalProjectChanges, tmp_dir: tempfile.TemporaryDirectory
) -> Optional[UploadJob]:
    """
    Prepare transaction and create an upload job for the project using the v1 API.
    """
    project_path = mp.project_full_name()
    project_id = mp.project_id()
    local_version = mp.version()
    server_feaures = mc.server_features()

    data = {"version": local_version, "changes": changes.to_server_payload()}
    push_start_resp = {}
    try:
        if server_feaures.get("v2_push_enabled"):
            mc.post(
                f"/v2/projects/{project_id}/versions",
                {**data, "check_only": True},
                POST_JSON_HEADERS,
            )
        else:
            resp = mc.post(
                f"/v1/project/push/{project_path}",
                data,
                POST_JSON_HEADERS,
            )
            push_start_resp = json.load(resp)
    except ClientError as err:
        if not err.is_blocking_sync():
            mp.log.error("Error starting transaction: " + str(err))
            mp.log.info("--- push aborted")
            raise
        else:
            mp.log.info("Project version exists or another upload is running, continuing with upload.")

    upload_changes = changes.get_upload_changes()
    transaction_id = push_start_resp.get("transaction") if upload_changes else None
    job = UploadJob(local_version, changes, transaction_id, mp, mc, tmp_dir)
    if not upload_changes:
        mp.log.info("not uploading any files")
        if push_start_resp:
            # This is related just to v1 API
            job.server_resp = push_start_resp
        push_project_finalize(job)
        return  # all done - no pending job

    if transaction_id:
        # This is related just to v1 API
        mp.log.info(f"got transaction ID {transaction_id}")

    # prepare file chunks for upload
    upload_queue_items = create_upload_chunks(mc, mp, upload_changes)

    mp.log.info(f"Starting upload chunks for project {project_id}")
    job.start(upload_queue_items)
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

    mp.log.info(f"got project info: local version {local_version}")

    changes, changes_len = get_push_changes_batch(mc, directory)
    if not changes_len:
        mp.log.info(f"--- push {project_path} - nothing to do")
        return

    mp.log.debug("push changes:\n" + pprint.pformat(asdict(changes)))
    tmp_dir = tempfile.TemporaryDirectory(prefix="python-api-client-")

    # If there are any versioned files (aka .gpkg) that are not updated through a diff,
    # we need to make a temporary copy somewhere to be sure that we are uploading full content.
    # That's because if there are pending transactions, checkpointing or switching from WAL mode
    # won't work, and we would end up with some changes left in -wal file which do not get
    # uploaded. The temporary copy using geodiff uses sqlite backup API and should copy everything.
    for f in changes.updated:
        if mp.is_versioned_file(f.path) and not f.diff:
            mp.copy_versioned_file_for_upload(f, tmp_dir.name)

    for f in changes.added:
        if mp.is_versioned_file(f.path):
            mp.copy_versioned_file_for_upload(f, tmp_dir.name)

    job = create_upload_job(mc, mp, changes, tmp_dir)
    return job


def push_project_wait(job):
    """blocks until all upload tasks are finished"""

    concurrent.futures.wait(job.futures)


def push_project_is_running(job: UploadJob):
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
        error_msg = (
            f"Transferred size ({job.transferred_size}) and expected total size ({job.total_size}) do not match!"
        )
        job.mp.log.error("--- push finish failed! " + error_msg)
        raise ClientError("Upload error: " + error_msg)

    if server_features.get("v2_push_enabled"):
        # v2 push uses a different endpoint
        try:
            job.update_chunks_from_items()
            job.mp.log.info(f"Finishing transaction for project {job.mp.project_full_name()}")
            resp = job.mc.post(
                f"/v2/projects/{job.mp.project_id()}/versions",
                data={
                    "version": job.version,
                    "changes": job.changes.to_server_payload(),
                },
                headers={"Content-Type": "application/json"},
            )
            project_info = json.load(resp)
            job.server_resp = project_info
        except ClientError as err:
            if not err.is_retryable_sync():
                # clear the upload chunks cache, as we are getting fatal from server
                job.mc.upload_chunks_cache.clear()
            job.mp.log.error("--- push finish failed! " + str(err))
            raise err
    elif with_upload_of_files:
        try:
            job.mp.log.info(f"Finishing transaction {job.transaction_id}")
            resp = job.mc.post("/v1/project/push/finish/%s" % job.transaction_id)
            job.server_resp = json.load(resp)
        except ClientError as err:
            # Log additional metadata on server error 502 or 504
            if hasattr(err, "http_error") and err.http_error in (502, 504):
                job.mp.log.error(
                    f"Push failed with HTTP error {err.http_error}. "
                    f"Upload details: {len(job.upload_queue_items)} file chunks, total size {job.total_size} bytes."
                )
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
        job.mp.apply_push_changes(asdict(job.changes))
    except Exception as e:
        job.mp.log.error("Failed to apply push changes: " + str(e))
        job.mp.log.info("--- push aborted")
        raise ClientError("Failed to apply push changes: " + str(e))

    job.tmp_dir.cleanup()  # delete our temporary dir and all its content
    job.mc.upload_chunks_cache.clear()  # clear the upload chunks cache

    remove_diff_files(job)

    job.mp.log.info("--- push finished - new project version " + job.server_resp["version"])


def push_project_cancel(job: UploadJob):
    """
    To be called (from main thread) to cancel a job that has uploads in progress.
    Returns once all background tasks have exited (may block for a bit of time).
    """

    job.mp.log.info("user cancelled the push...")
    # set job as cancelled
    job.is_cancelled = True

    job.executor.shutdown(wait=True)
    if not job.transaction_id:
        # If not v2 api endpoint with transaction, nothing to cancel on server
        job.mp.log.info("--- push cancelled")
        return
    try:
        resp_cancel = job.mc.post("/v1/project/push/cancel/%s" % job.transaction_id)
        job.server_resp = resp_cancel.msg
    except ClientError as err:
        job.mp.log.error("--- push cancelling failed! " + str(err))
        raise err
    job.mp.log.info("--- push cancel response: " + str(job.server_resp))


def remove_diff_files(job: UploadJob) -> None:
    """Looks for diff files in the job and removes them."""

    for change in job.changes.updated:
        diff = change.get_diff()
        if diff:
            diff_file = job.mp.fpath_meta(diff.path)
            if os.path.exists(diff_file):
                os.remove(diff_file)


def get_push_changes_batch(mc, directory: str) -> Tuple[LocalProjectChanges, int]:
    """
    Get changes that need to be pushed to the server.
    """
    mp = MerginProject(directory)
    changes = mp.get_push_changes()
    project_role = mp.project_role()
    changes = filter_changes(mc, project_role, changes)

    try:
        local_changes = LocalProjectChanges(
            added=[FileChange(**change) for change in changes["added"]],
            updated=[FileChange(**change) for change in changes["updated"]],
            removed=[FileChange(**change) for change in changes["removed"]],
        )
    except ChangesValidationError as e:
        raise ClientError(
            f"Some files exceeded maximum upload size. Files: {', '.join([c.path for c in e.invalid_changes])}. Maximum size for media files is {MAX_UPLOAD_MEDIA_SIZE / (1024**3)} GB and for geopackage files {MAX_UPLOAD_VERSIONED_SIZE / (1024**3)} GB."
        )
    mp.log.info("-- Push changes --")
    mp.log.info(pprint.pformat(changes))
    mp.log.info(sum(len(v) for v in changes.values()))
    return local_changes, sum(len(v) for v in changes.values())
