# Changelog

## 0.8.3

- Clean up temporary files in .mergin folder (#47)
- Improved hint how to set MERGIN_AUTH on Windows (#170)
- Better error reporting of HTTP errors (#174)

## 0.8.2

- Updated to pygeodiff 2.0.2

## 0.8.1

- Use new endpoint to retrieve detailed information about project version (#167)

## 0.8.0

- Added workspace API to list available workspaces, creating a new workspace and listing projects (#150, #152, #158)
- Removed check for available storage when syncing to personal namespace (#159)
- Added storing project IDs to project metadata files (#154)

## 0.7.4

- Added set_tables_to_skip() to optionally configure which tables to ignore in geodiff
- Updated to pygeodiff 2.0.1

## 0.7.3

- New default public server URL (#136)
- Fix issues with files and changesets downloading (#140)
- Fix handling of conflict files (MerginMaps/qgis-mergin-plugin#382)
- CLI: Fix mergin-py-client list project (#133)
- CLI: Fix individual file download (#140)

## 0.7.2

- Reporting can be run without end version specified (lutraconsulting/qgis-mergin-plugin#365)
- Fixed reporting when some tables were missing (lutraconsulting/qgis-mergin-plugin#362)

## 0.7.1

- Added has_writing_permissions() API call

## 0.7.0

- Added support for reporting of changes based on project history
- Fixed sync issues on Windows when schema changes (#117)
- Better naming of conflict files (#62)
- Unhandled exceptions during pull get written to the client log (#103)
- Fixed download of earlier version of GeoPackage files (#119)

## 0.6.6

- Add user_service() API call (#113)
- Updated to pygeodiff 1.0.5

## 0.6.5

- CLI: add, remove and list user permissions in a project (#98, #110)
- Update to pygeodiff 1.0.4

## 0.6.4

- Allow any namespace in create_project_and_push() (#104)
- CLI: create project & push data using --from-dir (#105)
- Update to pygeodiff 1.0.3

## 0.6.3

- Update to pygeodiff 1.0.2

## 0.6.2

- Fixed python error when pushing changes

## 0.6.1

- Added APIs to download individual files at any version (download_file()) and diffs (get_file_diff()) (#93)
- Robustness fixes (#30, #53, #96)
- Always require geodiff to be available (#92, #63)

## 0.6.0

- Moved to pygeodiff 1.0.0 - adding various new APIs

## 0.5.12

- CLI: simplified authentication, with multiple options now (#76)
  - user can pass MERGIN_USERNAME and MERGIN_PASSWORD env variables to commands
  - user can pass --username (and optionally --password) command line arguments to commands
  - user can still use "mergin login" command to first get auth token and set MERGIN_AUTH_TOKEN or pass --auth-token command line argument
- CLI: it is possible to create a project in a different namespace (#81)
- Fixed removal of projects in CLI (#82)
- Fixed possible error when removing project on Windows (#57)
- Fixed issue when a file was deleted both locally and on the server (qgis-mergin-plugin#232)
- Added optional global log (in addition to per-project logs)
- Improved handling of auth token problems
- Better error reporting

## 0.5.11

- Update to geodiff 0.8.8 (fixed issues with unicode paths, better error reporting)

## 0.5.10

- Added API (paginated_projects_list()) for paginated listing of projects (#77)
- Fixed sync error that have happened when moving to version >= 10 (#79, fixes lutraconsulting/qgis-mergin-plugin#219)
- Added more details to diagnostic logs to help finding errors

## 0.5.9

- Added API to download older version of a project (#74)
- Added API to query project details of an older version of a project

## 0.5.8

- Added API (get_projects_by_names()) to request project info for specified projects

## 0.5.7

- Fixed quota check when uploading to organisations (#70)
- Added API to manage collaborators in a project

## 0.5.6

- Added API for cloning of projects
- Projects can be created on behalf of an organisation

## 0.5.5

- Update to geodiff 0.8.6 (fixes non-ascii paths, quotes in paths, rebase with gpkg triggers)
- Fix "transferred size and expected total size do not match" (qgis-mergin-plugin#142)

## 0.5.4

- Disable upload of renames of files to prevent corrupt projects (#27)

## 0.5.3

- Fix download/pull of subdirs with just empty file(s) (qgis-mergin-plugin#160)
- Add version info to user agent + log versions in log file (qgis-mergin-plugin#150)
- Raise exception when username is passed but no password
- Automatic running of tests in continuous integration on GitHub (#64)

## 0.5.2

- Release fix

## 0.5.1

- Update to geodiff 0.8.5 (leaking file handles in hasChanges() call)
- Better error reporting for CLI, detect expired access token
- Project push: fail early if user does not have write permissions
- Support more endpoints with project info + CLI integration
- Raise ClientError when request fails when trying to log in

## 0.5.0

- Update to geodiff 0.8.4 (fixes a rebase bug, adds logging, supports conflict files)
- More detailed logging
- Better handling of exceptions
- CLI improvements and fixes
- Fixed sync error with missing basefile
- Changed version because the module is going to have also independent releases from QGIS plugin
  (they had synchronized version numbers until now)

## 2020.4.1

- Fixed load error if pip is not available (#133)

## 2020.4.0

- Asynchronous push/pull/download (#44)
- Basic logging of sync oprations to .mergin/client-log.txt
- Modified create_project() API - does not do project upload anymore

## 2020.3.1

- Second fix of the issue with sync (#119)

## 2020.3

- Fixed an issue with synchronization (#119)

## 2020.2

- Added checkpointing for GeoPackages (#93)
- More details in exceptions (#106)
- Fix SSL issues on macOS (#70)
- Handling of expired token (#108)
- Fix issue with checksums

## 2020.1

- Check for enough free space (#25)
- Added user agent in requests (#31)
- Fix problems with empty files

## 2019.6

- show list of local changes

## 2019.5

- Support for geodiff
- Added server compatibility checks (#1)
- Added tests (#16)

## 2019.4.1
- Improved push/pull speed using parallel requests
- Added possibility to create blank mergin project

## 2019.4

- Added filters for listing projects (owner, shared, query)
- Changed Basic auth to Bearer token-based auth
- Improved CLI: added login, credentials in env variables, delete project
- Download/upload files with multiple sequential requests (chunked transfer)
- Fixed push with no changes
- Blacklisting various temp files from sync

## 2019.3

Initial release containing following actions for project:

   - list
   - create (from local dir)
   - download
   - sync

with basic CLI icluded.
