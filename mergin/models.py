from dataclasses import dataclass, field
from typing import List, Optional
from .common import DeltaChangeType, PullActionType


@dataclass
class ProjectDeltaItemDiff:
    """
    Single diff file info.
    """

    id: str
    size: int = 0
    version: Optional[str] = None


@dataclass
class ProjectDeltaItem:
    """
    Single file change presented in project delta items.
    """

    change: DeltaChangeType
    path: str
    version: str
    size: int
    checksum: str
    diffs: List[ProjectDeltaItemDiff] = field(default_factory=list)

    def __post_init__(self):
        self.change = DeltaChangeType(self.change)


@dataclass
class ProjectDelta:
    """
    Structure for project delta (changes between versions).
    """

    to_version: str
    items: List[ProjectDeltaItem] = field(default_factory=list)


@dataclass
class PullAction:
    """
    Action to be performed during pull.
    """

    type: PullActionType
    pull_delta_item: ProjectDeltaItem
    local_delta_item: Optional[ProjectDeltaItem] = None

    def __post_init__(self):
        self.type = PullActionType(self.type)


@dataclass
class ProjectFile:
    """
    File info in project response.
    """

    checksum: str
    mtime: str
    path: str
    size: int


@dataclass
class ProjectWorkspace:
    """
    Workspace info in project response.
    """

    id: int
    name: str


@dataclass
class ProjectResponse:
    """
    Project info response.
    """

    created_at: str
    files: List[ProjectFile]
    id: str
    name: str
    public: bool
    role: str
    size: int
    updated_at: str
    version: str
    workspace: ProjectWorkspace
