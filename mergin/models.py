from dataclasses import dataclass, field
from typing import List, Optional
from .common import DeltaChangeType, PullActionType


@dataclass
class ProjectDeltaItemDiff:
    id: str
    version: Optional[str] = None
    size: Optional[int] = None


@dataclass
class ProjectDeltaItem:
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
    to_version: str
    items: List[ProjectDeltaItem] = field(default_factory=list)


@dataclass
class PullAction:
    type: PullActionType
    pull_delta_item: ProjectDeltaItem
    local_delta_item: Optional[ProjectDeltaItem] = None

    def __post_init__(self):
        self.type = PullActionType(self.type)
