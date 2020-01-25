"""
To download projects asynchronously. Start download: (does not block)

job = download_project_async(mergin_client, 'user/project', '/tmp/my_project)

Then we need to wait until we are finished downloading - either by periodically
calling download_project_is_running(job) that will just return True/False or by calling
download_project_wait(job) that will block the current thread (not good for GUI).
To finish the download job, we have to call download_project_finalize(job).
"""

import math
import os
import shutil

import concurrent.futures
import threading

from .client import MerginClient, MerginProject, CHUNK_SIZE
from .utils import save_to_file


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
    """ Keeps all the important data about a pending download job """
    
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
    chunks = math.ceil(file['size'] / CHUNK_SIZE)
    
    items = []
    for part_index in range(chunks):
        download_file_path = os.path.join(file_dir, basename + ".{}".format(part_index))
        size = min(CHUNK_SIZE, file['size'] - part_index * CHUNK_SIZE)
        items.append(DownloadQueueItem(file['path'], size, file['version'], diff_only, part_index, download_file_path))
        
    return items


def _do_download(item, mc, project_path, job):
    """ runs in worker thread """
    #print(threading.current_thread(), "downloading", item.file_path)
    if job.is_cancelled:
        #print(threading.current_thread(), "downloading", item.file_path, "cancelled")
        return
    
    # TODO: make download_blocking / save_to_file cancellable so that we can cancel as soon as possible

    item.download_blocking(mc, project_path)
    job.transferred_size += item.size
    #print(threading.current_thread(), "downloading", item.file_path, "finished")


def download_project_async(mc, project_path, directory):
    """
    Starts project download in background and returns handle to the pending project download.
    Using that object it is possible to watch progress or cancel the ongoing work.
    
    """

    if os.path.exists(directory):
        raise Exception("Project directory already exists")
    os.makedirs(directory)
    mp = MerginProject(directory)

    project_info = mc.project_info(project_path)
    version = project_info['version'] if project_info['version'] else 'v0'

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
    
    job = DownloadJob(project_path, total_size, version, update_tasks, download_list, directory, mp, project_info)
    
    # start download
    job.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    job.futures = []
    for item in download_list:
        future = job.executor.submit(_do_download, item, mc, project_path, job)
        job.futures.append(future)

    return job


def download_project_wait(job):
    """ blocks until all download tasks are finished """
    
    concurrent.futures.wait(job.futures)

    # handling of exceptions
    for future in job.futures:
        if future.exception() is not None:
            raise future.exception()


def download_project_is_running(job):
    """ Returns true/false depending on whether we have some pending downloads """
    for future in job.futures:
        if future.running():
            return True
    return False


def download_project_finalize(job):
    """
    To be called when download in the background is finished and we need to do the finalization (merge chunks etc.)
    
    This probably should not be called from a worker thread (e.g. directly from a handler when download is complete)
    """

    job.executor.shutdown(wait=True)
    
    assert job.transferred_size == job.total_size

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
    
    # set job as cancelled
    job.is_cancelled = True

    job.executor.shutdown(wait=True)



class UpdateTask:
    """
    Entry for each file that will be updated. At the end of a successful download of new data, all the tasks are executed.
    """
    
    # TODO: methods other than COPY
    def __init__(self, file_path, download_queue_items):
        self.file_path = file_path
        self.download_queue_items = download_queue_items
        
    def apply(self, directory, mp):
        """ assemble downloaded chunks into a single file """
        
        basename = os.path.basename(self.file_path)   #file['diff']['path']) if diff_only else os.path.basename(file['path'])
        file_dir = os.path.dirname(os.path.normpath(os.path.join(directory, self.file_path)))
        dest_file_path = os.path.join(file_dir, basename)
        
        # merge chunks together (and delete them afterwards)
        with open(dest_file_path, 'wb') as final:
            for item in self.download_queue_items:
                file_part = item.download_file_path
                with open(file_part, 'rb') as chunk:
                    shutil.copyfileobj(chunk, final)
                os.remove(file_part)

        if mp.is_versioned_file(self.file_path):
            shutil.copy(mp.fpath(self.file_path), mp.fpath_meta(self.file_path))



class DownloadQueueItem:
    """ a piece of data from a project that should be downloaded - it can be either a chunk or it can be a diff """
    
    def __init__(self, file_path, size, version, diff_only, part_index, download_file_path):
        self.file_path = file_path     # relative path to the file within project
        self.size = size               # size of the item in bytes
        self.version = version         # version of the file ("v123")
        self.diff_only = diff_only     # whether downloading diff or full version
        self.part_index = part_index    # index of the chunk
        self.download_file_path = download_file_path   # full path to a temporary file which will receive the content

    def download_blocking(self, mc, project_path):
        """ Starts download and only returns once the file has been fully downloaded and saved """

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
            save_to_file(resp, self.download_file_path)
        else:
            raise ClientError('Failed to download part {} of file {}'.format(part, basename))


if __name__ == '__main__':
    auth_token = 'Bearer XXXX_replace_XXXX'
    mc = MerginClient("https://public.cloudmergin.com/", auth_token)
    test_dir = "/tmp/_mergin_fibre"
    shutil.rmtree(test_dir)
    job = download_project_async(mc, "martin/fibre", test_dir)
    job.dump()
    download_project_wait(job)
    download_project_finalize(job)
