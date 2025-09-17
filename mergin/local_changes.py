from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, List, Tuple


@dataclass
class FileDiffChange:
    path: str
    checksum: str
    size: int
    mtime: datetime


@dataclass
class FileChange:
    # path to the file relative to the project root
    path: str
    # file checksum
    checksum: str
    # file size in bytes
    size: int
    # file modification time
    mtime: datetime
    # original file checksum used for compairison
    origin_checksum: Optional[str] = None
    # list of chunk ids that make up this file
    chunks: List[str] = field(default_factory=list)
    # optional diff information for geopackage files with geodiff metadata
    diff: Optional[dict] = None
    upload_file: Optional[str] = None
    # some functions (MerginProject.compare_file_sets) are adding version to the change from project info
    version: Optional[str] = None
    # some functions (MerginProject.compare_file_sets) are adding history dict to the change from project info
    history: Optional[dict] = None
    # some functions (MerginProject.compare_file_sets) are adding location dict to the change from project info
    location: Optional[str] = None

    def get_diff(self) -> Optional[FileDiffChange]:
        if self.diff:
            return FileDiffChange(
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
class LocalPojectChanges:
    added: List[FileChange] = field(default_factory=list)
    updated: List[FileChange] = field(default_factory=list)
    removed: List[FileChange] = field(default_factory=list)

    def to_server_payload(self) -> dict:
        return {
            "added": [change.to_server_data() for change in self.added],
            "updated": [change.to_server_data() for change in self.updated],
            "removed": [change.to_server_data() for change in self.removed],
        }

    def get_upload_changes(self) -> List[FileChange]:
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
            for chunk_id, server_chunk_id in server_chunks:
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
