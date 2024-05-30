from itertools import filterfalse

from .utils import is_mergin_config, is_qgis_file, is_versioned_file


class EditorHandler:
    def __init__(self, mergin_client, project_info: dict):
        """
        Initializes the editor instance with the provided Mergin client and project information.

        Args:
            mergin_client (MerginClient): The Mergin client instance.
            project_info (dict): The project information dictionary.
        """
        self.mc = mergin_client
        self.project_info = project_info
        self.ROLE_NAME = "editor"

    _disallowed_added_changes = lambda self, change: is_qgis_file(change["path"]) or is_mergin_config(change["path"])
    _disallowed_updated_changes = (
        lambda self, change: is_qgis_file(change["path"])
        or is_mergin_config(change["path"])
        or (is_versioned_file(change["path"]) and change.get("diff") is None)
    )
    _disallowed_removed_changes = (
        lambda self, change: is_qgis_file(change["path"])
        or is_mergin_config(change["path"])
        or is_versioned_file(change["path"])
    )

    def is_enabled(self):
        """
        Returns True if the current user has editor access to the project, False otherwise.

        The method checks if the server supports editor access, and if the current user's project role matches the expected role name for editors.
        """
        server_support_editor = self.mc.has_editor_support()
        project_role = self.project_info.get("role")

        return server_support_editor and project_role == self.ROLE_NAME

    def _apply_editor_filters(self, changes: dict[str, list[dict]]) -> dict[str, list[dict]]:
        added = changes.get("added", [])
        updated = changes.get("updated", [])
        removed = changes.get("removed", [])

        # filter out files that are not in the editor's list of allowed files
        changes["added"] = list(filterfalse(self._disallowed_added_changes, added))
        changes["updated"] = list(filterfalse(self._disallowed_updated_changes, updated))
        changes["removed"] = list(filterfalse(self._disallowed_removed_changes, removed))
        return changes

    def filter_changes(self, changes: dict[str, list[dict]]) -> dict[str, list[dict]]:
        if not self.is_enabled():
            return changes
        return self._apply_editor_filters(changes)
