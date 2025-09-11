from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Tuple

from .utils import is_versioned_file

MAX_UPLOAD_CHANGES = 100


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
        total_changes = len(self.get_upload_changes())
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

    def get_media_upload_size(self) -> int:
        """
        Calculate the total size of media files in added and updated changes.
        """
        total_size = 0
        for change in self.get_upload_changes():
            if not is_versioned_file(change.path):
                total_size += change.size
        return total_size

    def get_gpgk_upload_size(self) -> int:
        """
        Calculate the total size of gpgk files in added and updated changes.
        Do not calculate diffs (only new or overwriten files).
        """
        total_size = 0
        for change in self.get_upload_changes():
            if is_versioned_file(change.path) and not change.diff:
                total_size += change.size
        return total_size
