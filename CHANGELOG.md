# Changelog

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
