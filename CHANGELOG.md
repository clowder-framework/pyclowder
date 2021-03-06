# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) 
and this project adheres to [Semantic Versioning](http://semver.org/).

## 2.3.4 - 2020-10-04

### Fixed
- Extractor would not run in case clowder_url was ""

## 2.3.3 - 2020-10-02

### Fixed
- Thread of heartbeat was set as daemon after start.

## 2.3.2 - 2020-09-24

### Fixed
- When rabbitmq restarts the extractor would not stop and restart, resulting 
  in the extractor no longer receiving any messages. #17

### Added
- Can specify url to use for extractor downloads, this is helpful for instances
  that have access to the internal URL for clowder, for example in docker/kubernetes.
  
### Removed
- Removed ability to run multiple connectors in the same python process. If
  parallelism is needed, use multiple processes (or containers).

## 2.3.1 - 2020-09-18

With this version we no longer gurantee support for versions of python below 3.

### Fixed
- There was an issue where status messages could cause an exception. This would
  prevent most extractors from running correctly.

## 2.3.0 - 2020-09-15

**CRITICAL BUG IN THIS VERSION. PLEASE USE VERSION 2.3.1**

Removed develop branch, all pull requests will need to be against master from now
forward. Please update version number in setup.py in each PR.

From this version no more docker images are build, please use pip install to
install pyclowder.

### Added
- Simple extractors now support datasets, can also create new datasets.
- Ability to add tags from simple extractor to files and datasets.
- Ability to add additional files (outputs) to dataset in simple extractor.
- Use pipenv to manage dependencies.
- Add job_id to each status message returned by pyclowder.
- PyClowderExtractionAbort to indicate the message shoudl not be retried.

### Changed
- Better handling of status messages

## 2.2.3 - 2019-10-14

### Fixed
- Heartbeat of 5 minutes would cause timeouts for RabbitMQ
  [CATSPYC-30](https://opensource.ncsa.illinois.edu/jira/browse/CATSPYC-30)
- support uploading large file to dataset.
  [CATSPYC-29](https://opensource.ncsa.illinois.edu/jira/browse/CATSPYC-29)

## 2.2.2 - 2019-09-27

### Changed
- Heartbeat is now every five minutes, was every 5 seconds

### Fixed
- the python3 images were actually python2 images

## 2.2.1 - 2019-08-02

### Changed
- sample extractors had bad extractor_info.json. registry needs to be an array.

## 2.2.0 - 2019-04-03

### Fixed
- Code is now compatible with both Python 2 and Python 3.
- RabbitMQ queue name can be different from extractor name.
- Updated dependencies.
- A race condition existed where an ACK could be lost, resulting in an extractor not processing more messages
  [CATSPYC-1](https://opensource.ncsa.illinois.edu/jira/browse/CATSPYC-1)
- Error decoding messages where filename contains non-ascii characters
  [CATSPYC-18](https://opensource.ncsa.illinois.edu/jira/browse/CATSPYC-18)

### Changed
- files.upload_preview and collections.upload_preview does not require section_id for metadata
  [CATS-935](https://opensource.ncsa.illinois.edu/jira/browse/CATS-935)
- Extractors will not register by default to clowder
  [CATSPYC-1](https://opensource.ncsa.illinois.edu/jira/browse/CATSPYC-1)

### Added
- Simple R extractor. Allows wrapping R code as a clowder extractor, see sample-extractors/wordcount-simple-r-extractor for an example.
- Email notification when extraction is done
  [CATSPYC-17](https://opensource.ncsa.illinois.edu/jira/browse/CATSPYC-17)
- Docker compose file for starting up the Clowder stack
  [BD-2226](https://opensource.ncsa.illinois.edu/jira/browse/BD-2226)
- PyClowder will now send heartbeats on extractors exchange, processes can listen for broadcast to get notified when new extractors come online.
- Monitor application to leverage new heartbeat send out by extractors
- Extractors now send version and name to clowder as part of the agent information.

## 2.1.1 - 2018-07-12

### Fixed
- Error decoding json body from Clowder when filename had special characters
  [CATSPYC-18] (https://opensource.ncsa.illinois.edu/jira/browse/CATSPYC-18) 
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
