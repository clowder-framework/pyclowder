# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) 
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Changed
- files.upload_preview and collections.upload_preview does not require section_id for metadata [CATS-935](https://opensource.ncsa.illinois.edu/jira/browse/CATS-935)

### Added
- Docker compose file for starting up the Clowder stack [BD-2226](https://opensource.ncsa.illinois.edu/jira/browse/BD-2226)

## 2.1.1 - 2018-07-12

### Fixed
- RABBITMQ_QUEUE variable/flag was ignored when set and would connect
  to default queue.

## 2.1.0 - 2018-07-06

### Added
- [Simple extractor](https://opensource.ncsa.illinois.edu/confluence/display/CATS/Simple+Extractor+wrapper+for+basic+functions),
  now can create a extractor from a single function.

### Fixed
- Acks were not always send due to racing condition
  [CATS-886](https://opensource.ncsa.illinois.edu/jira/browse/CATS-886)
- Queue name was not set from extractor_info.json
  [CATS-896](https://opensource.ncsa.illinois.edu/jira/browse/CATS-896)

## 2.0.3 - 2018-04-18

### Added
- push to pypi (can now do pip install pyclowder)
- added binary-preview-extractor which can be configured using environment
  variables (see sample-extractors/binary-preview/README.rst).

### Changed
- Now has onbuild version of pyclowder
- release.sh will now tag images (e.g. this will be tagged 2.0.3 2.0 and 2)
- RABBITMQ_URI is now set to amqp://guest:guest@rabbitmq/%2F to allow easy deployment
  using docker-compose of clowder