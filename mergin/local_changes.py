from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, List, Tuple

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

    def get_diff(self) -> Optional[BaseLocalChange]:
        if self.diff:
            return BaseLocalChange(
                path=self.diff.get("path", ""),
                checksum=self.diff.get("checksum", ""),
                size=self.diff.get("size", 0),
                mtime=self.diff.get("mtime", datetime.now())
            )
        return None

    def get_server_data(self) -> dict:
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
                "size": self.diff.get("size", 0)
            }
        return result

@dataclass
class LocalChanges:
    added: List[LocalChange] = field(default_factory=list)
    updated: List[LocalChange] = field(default_factory=list)
    removed: List[LocalChange] = field(default_factory=list)

    def get_server_request(self) -> dict:
        return {
            "added": [change.get_server_data() for change in self.added],
            "updated": [change.get_server_data() for change in self.updated],
            "removed": [change.get_server_data() for change in self.removed],
        }
    
    def get_upload_changes(self) -> List[LocalChange]:
        """
        Get all changes that need to be uploaded.
        This includes added and updated files.
        """
        return self.added + self.updated

    def update_chunks(self, chunks: List[Tuple[str, str]]) -> None:
        """
        Map chunk ids to file checksums.

        This method updates the `chunks` attribute of each change in `added` and `updated`
        lists based on the provided `chunks` list, which contains tuples of (checksum, chunk_id).
        """
        for change in self.added:
            change.chunks = [
                chunk[1]
                for chunk in chunks
                if chunk[0] == change.checksum
            ]

        for change in self.updated:
            change.chunks = [
                chunk[1]
                for chunk in chunks
                if chunk[0] == change.checksum
            ]