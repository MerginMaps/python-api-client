from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Tuple

from .utils import is_versioned_file
from .common import MAX_UPLOAD_MEDIA_SIZE, MAX_UPLOAD_VERSIONED_SIZE

MAX_UPLOAD_CHANGES = 100


# The custom exception
class ChangesValidationError(Exception):
    def __init__(self, message, invalid_changes=[], max_media_upload_size=None, max_versioned_upload_size=None):
        super().__init__(message)
        self.invalid_changes = invalid_changes if invalid_changes is not None else []
        self.max_media_upload_size = max_media_upload_size
        self.max_versioned_upload_size = max_versioned_upload_size


@dataclass
class BaseLocalChange:
    path: str
    checksum: str
    size: int
    mtime: datetime


@dataclass
class LocalChange(BaseLocalChange):
    origin_checksum: Optional[str] = None
    chunks: List[str] = field(default_factory=list)
    diff: Optional[dict] = None
    upload_file: Optional[str] = None
    # some functions (MerginProject.compare_file_sets) are adding version to the change from project info
    version: Optional[str] = None
    # some functions (MerginProject.compare_file_sets) are adding history dict to the change from project info
    history: Optional[dict] = None
    # some functions (MerginProject.compare_file_sets) are adding location dict to the change from project info
    location: Optional[str] = None

    def get_diff(self) -> Optional[BaseLocalChange]:
        if self.diff:
            return BaseLocalChange(
                path=self.diff.get("path", ""),
                checksum=self.diff.get("checksum", ""),
                size=self.diff.get("size", 0),
                mtime=self.diff.get("mtime", datetime.now()),
            )

    def to_server_data(self) -> dict:
        result = {
            "path": self.path,
            "checksum": self.checksum,
            "size": self.size,
            "chunks": self.chunks,
        }
        if self.diff:
            result["diff"] = {
                "path": self.diff.get("path", ""),
                "checksum": self.diff.get("checksum", ""),
                "size": self.diff.get("size", 0),
            }
        return result


@dataclass
class LocalChanges:
    added: List[LocalChange] = field(default_factory=list)
    updated: List[LocalChange] = field(default_factory=list)
    removed: List[LocalChange] = field(default_factory=list)

    def __post_init__(self):
        """
        Enforce a limit of changes combined from `added` and `updated`.
        """
        upload_changes = self.get_upload_changes()
        total_changes = len(upload_changes)
        oversize_changes = []
        for change in upload_changes:
            if not is_versioned_file(change.path) and change.size > MAX_UPLOAD_MEDIA_SIZE:
                oversize_changes.append(change)
            elif not change.diff and change.size > MAX_UPLOAD_VERSIONED_SIZE:
                oversize_changes.append(change)
        if oversize_changes:
            error = ChangesValidationError("Some files exceed the maximum upload size", oversize_changes)
            error.max_media_upload_size = MAX_UPLOAD_MEDIA_SIZE
            error.max_versioned_upload_size = MAX_UPLOAD_VERSIONED_SIZE
            raise error

        if total_changes > MAX_UPLOAD_CHANGES:
            # Calculate how many changes to keep from `added` and `updated`
            added_limit = min(len(self.added), MAX_UPLOAD_CHANGES)
            updated_limit = MAX_UPLOAD_CHANGES - added_limit
            self.added = self.added[:added_limit]
            self.updated = self.updated[:updated_limit]

    def to_server_payload(self) -> dict:
        return {
            "added": [change.to_server_data() for change in self.added],
            "updated": [change.to_server_data() for change in self.updated],
            "removed": [change.to_server_data() for change in self.removed],
        }

    def get_upload_changes(self) -> List[LocalChange]:
        """
        Get all changes that need to be uploaded.
        This includes added and updated files.
        """
        return self.added + self.updated

    def _map_unique_chunks(self, change_chunks: List[str], server_chunks: List[Tuple[str, str]]) -> List[str]:
        """
        Helper function to map and deduplicate chunk ids for a single change.
        """
        mapped = []
        seen = set()
        for chunk in change_chunks:
            for server_chunk in server_chunks:
                chunk_id = server_chunk[0]
                server_chunk_id = server_chunk[1]
                if chunk_id == chunk and server_chunk_id not in seen:
                    mapped.append(server_chunk_id)
                    seen.add(server_chunk_id)
        return mapped

    def update_chunks(self, server_chunks: List[Tuple[str, str]]) -> None:
        """
        Map chunk ids to chunks returned from server (server_chunk_id).

        This method updates the `chunks` attribute of each change in `added` and `updated`
        lists based on the provided `server_chunks` list, which contains tuples of (chunk_id, server_chunk_id).
        """
        for change in self.added:
            change.chunks = self._map_unique_chunks(change.chunks, server_chunks)

        for change in self.updated:
            change.chunks = self._map_unique_chunks(change.chunks, server_chunks)
