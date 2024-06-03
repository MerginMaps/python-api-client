from itertools import filterfalse
from typing import Callable

from .utils import is_mergin_config, is_qgis_file, is_versioned_file

EDITOR_ROLE_NAME = "editor"

"""
Determines whether a given file change should be disallowed based on the file path.

Returns:
    bool: True if the file change should be disallowed, False otherwise.
"""
disallowed_added_changes: Callable[[dict], bool] = lambda change: is_qgis_file(change["path"]) or is_mergin_config(
    change["path"]
)
"""
Determines whether a given file change should be disallowed from being updated.

The function checks the following conditions:
- If the file path matches a QGIS file
- If the file path matches a Mergin configuration file
- If the file is versioned and the change does not have a diff

Returns:
    bool: True if the change should be disallowed, False otherwise.
"""
_disallowed_updated_changes: Callable[[dict], bool] = (
    lambda change: is_qgis_file(change["path"])
    or is_mergin_config(change["path"])
    or (is_versioned_file(change["path"]) and change.get("diff") is None)
)
"""
Determines whether a given file change should be disallowed from being removed.

The function checks if the file path of the change matches any of the following conditions:
- The file is a QGIS file (e.g. .qgs, .qgz)
- The file is a Mergin configuration file (mergin-config.json)
- The file is a versioned file (.gpkg, .sqlite)

If any of these conditions are met, the change is considered disallowed from being removed.
"""
_disallowed_removed_changes: Callable[[dict], bool] = (
    lambda change: is_qgis_file(change["path"]) or is_mergin_config(change["path"]) or is_versioned_file(change["path"])
)


def is_editor_enabled(mc, project_info: dict) -> bool:
    """
    The function checks if the server supports editor access, and if the current user's project role matches the expected role name for editors.
    """
    server_support = mc.has_editor_support()
    project_role = project_info.get("role")

    return server_support and project_role == EDITOR_ROLE_NAME


def _apply_editor_filters(changes: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """
    Applies editor-specific filters to the changes dictionary, removing any changes to files that are not in the editor's list of allowed files.

    Args:
        changes (dict[str, list[dict]]): A dictionary containing the added, updated, and removed changes.

    Returns:
        dict[str, list[dict]]: The filtered changes dictionary.
    """
    added = changes.get("added", [])
    updated = changes.get("updated", [])
    removed = changes.get("removed", [])

    # filter out files that are not in the editor's list of allowed files
    changes["added"] = list(filterfalse(disallowed_added_changes, added))
    changes["updated"] = list(filterfalse(_disallowed_updated_changes, updated))
    changes["removed"] = list(filterfalse(_disallowed_removed_changes, removed))
    return changes


def filter_changes(mc, project_info: dict, changes: dict[str, list[dict]]) -> dict[str, list[dict]]:
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
    return is_editor_enabled(mc, project_info) and any([is_qgis_file(path), is_mergin_config(path)])
