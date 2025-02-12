from itertools import filterfalse
from typing import Callable, Dict, List

from .utils import is_mergin_config, is_qgis_file, is_versioned_file

EDITOR_ROLE_NAME = "editor"

"""
Determines whether a given file change should be disallowed based on the file path.

Returns:
    bool: True if the file change should be disallowed, False otherwise.
"""
_disallowed_changes: Callable[[dict], bool] = lambda change: is_qgis_file(change["path"])


def is_editor_enabled(mc, project_info: dict) -> bool:
    """
    The function checks if the server supports editor access, and if the current user's project role matches the expected role name for editors.
    """
    server_support = mc.has_editor_support()
    project_role = project_info.get("role")

    return server_support and project_role == EDITOR_ROLE_NAME


def _apply_editor_filters(changes: Dict[str, List[dict]]) -> Dict[str, List[dict]]:
    """
    Applies editor-specific filters to the changes dictionary, removing any changes to files that are not in the editor's list of allowed files.

    Args:
        changes (dict[str, list[dict]]): A dictionary containing the added, updated, and removed changes.

    Returns:
        dict[str, list[dict]]: The filtered changes dictionary.
    """
    updated = changes.get("updated", [])

    changes["updated"] = list(filterfalse(_disallowed_changes, updated))
    return changes


def filter_changes(mc, project_info: dict, changes: Dict[str, List[dict]]) -> Dict[str, List[dict]]:
    """
    Filters the given changes dictionary based on the editor's enabled state.

    If the editor is not enabled, the changes dictionary is returned as-is. Otherwise, the changes are passed through the `_apply_editor_filters` method to apply any configured filters.

    Args:
        changes (dict[str, list[dict]]): A dictionary mapping file paths to lists of change dictionaries.

    Returns:
        dict[str, list[dict]]: The filtered changes dictionary.
    """
    if not is_editor_enabled(mc, project_info):
        return changes
    return _apply_editor_filters(changes)


def prevent_conflicted_copy(path: str, mc, project_info: dict) -> bool:
    """
    Decides whether a file path should be blocked from creating a conflicted copy.
    Note: This is used when the editor is active and attempting to modify files (e.g., .ggs) that are also updated on the server, preventing unnecessary conflict files creation.

    Args:
        path (str): The file path to check.
        mc: The Mergin client object.
        project_info (dict): Information about the Mergin project from server.

    Returns:
        bool: True if the file path should be prevented from ceating conflicted copy, False otherwise.
    """
    return is_editor_enabled(mc, project_info) and any([is_qgis_file(path)])
