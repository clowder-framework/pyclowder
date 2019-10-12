#!/bin/sh

# exit on error, with error code
set -e

# use DEBUG=echo ./release.sh to print all commands
export DEBUG=${DEBUG:-""}

# build docker container based on python 2
${DEBUG} docker build --build-arg PYTHON_VERSION=2.7 --tag clowder/pyclowder:latest .
${DEBUG} docker build --build-arg PYCLOWDER_PYTHON="" --tag clowder/pyclowder:onbuild --file Dockerfile.onbuild .
${DEBUG} docker build --build-arg PYCLOWDER_PYTHON="" --tag clowder/extractors-binary-preview:onbuild sample-extractors/binary-preview
${DEBUG} docker build --build-arg PYCLOWDER_PYTHON="" --tag clowder/extractors-simple-extractor:onbuild sample-extractors/simple-extractor
${DEBUG} docker build --build-arg PYCLOWDER_PYTHON="" --tag clowder/extractors-simple-r-extractor:onbuild sample-extractors/simple-r-extractor

# build docker container based on python 3
${DEBUG} docker build --build-arg PYTHON_VERSION=3.5 --tag clowder/pyclowder-python3:latest .
${DEBUG} docker build --build-arg PYCLOWDER_PYTHON="-python3" --tag clowder/pyclowder-python3:onbuild --file Dockerfile.onbuild .
${DEBUG} docker build --build-arg PYCLOWDER_PYTHON="-python3" --tag clowder/extractors-binary-preview-python3:onbuild sample-extractors/binary-preview
${DEBUG} docker build --build-arg PYCLOWDER_PYTHON="-python3" --tag clowder/extractors-simple-extractor-python3:onbuild sample-extractors/simple-extractor
${DEBUG} docker build --build-arg PYCLOWDER_PYTHON="-python3" --tag clowder/extractors-simple-r-extractor-python3:onbuild sample-extractors/simple-r-extractor


# build sample extractors
${DEBUG} docker build --build-arg PYCLOWDER_PYTHON="-python3" --tag clowder/extractors-wordcount:latest sample-extractors/wordcount
${DEBUG} docker build --build-arg PYCLOWDER_PYTHON="-python3" --tag clowder/extractors-wordcount-simple-extractor:latest sample-extractors/wordcount-simple-extractor
${DEBUG} docker build --build-arg PYCLOWDER_PYTHON="-python3" --tag clowder/extractors-wordcount-simple-r-extractor:latest sample-extractors/wordcount-simple-r-extractor
