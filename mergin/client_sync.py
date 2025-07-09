import time
from typing import Dict, Any

from .merginproject import MerginProject
from .client_pull import DownloadJob, pull_project_async, pull_project_wait, pull_project_finalize, pull_project_cancel
from .client_push import UploadJob, push_project_async, push_project_wait, push_project_finalize, push_next_change, \
    push_project_cancel, push_project_is_running


class Syncer:
    def __init__(self, client, directory, download_job: DownloadJob = None, upload_job: UploadJob = None, total_upload_size: int = 0, uploaded_size: int = 0):
        self.client = client
        self.directory = directory
        self.mp = MerginProject(directory)
        self.download_job = download_job
        self.upload_job = upload_job
        self.total_upload_size = total_upload_size
        self.uploaded_size = uploaded_size

    def _one_cycle(self):
        """Pull remote, then kick off an upload (if there are changes).
        Returns an UploadJob (with .transferred_size) or None if up‑to‑date."""
        download_job = pull_project_async(self.client, self.directory)
        if download_job:
            pull_project_wait(download_job)
            pull_project_finalize(download_job)

        changes = self.mp.get_push_changes()
        if not changes:
            return None

        upload_job = push_next_change(self.client, self.mp, changes)
        return upload_job

    def sync_loop(self, progress_callback=None):
        """
        Repeatedly do pull → push cycles until no more changes.
        If progress_callback is provided, it’s called as:
            progress_callback(upload_job, last_transferred, new_transferred)
        """
        while True:
            upload_job = self._one_cycle()
            if upload_job is None:
                break

            # no UI: just wait & finalize
            if progress_callback is None:
                push_project_wait(upload_job)
                push_project_finalize(upload_job)
                continue

            # drive the callback
            last = 0
            while push_project_is_running(upload_job):
                time.sleep(0.1)
                now = upload_job.transferred_size
                progress_callback(upload_job, last, now)
                last = now

            push_project_wait(upload_job)
            push_project_finalize(upload_job)

    def cancel(self):
        if self.download_job is not None:
            pull_project_cancel(self.download_job)
        if self.upload_job is not None:
            push_project_cancel(self.upload_job)

    def estimate_total_upload(self):
        """
        One‑shot estimate of the _current_ total upload size.
        """
        changes = self.mp.get_push_changes()
        return calculate_uploads_size(changes) if changes else 0

def calculate_uploads_size(changes: Dict[str, Any]) -> int:
    files = changes.get("added", []) + changes.get("updated", [])
    return sum(
        f.get("diff", {}).get("size", f.get("size", 0))
        for f in files
    )
