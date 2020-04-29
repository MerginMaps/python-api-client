# Changelog

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
