#!/bin/bash

# exit on error, with error code
set -e

# can use the following to push to isda-registry for testing:
# BRANCH="master" SERVER=isda-registry.ncsa.illinois.edu/ ./release.sh

# use DEBUG=echo ./release.sh to print all commands
export DEBUG=${DEBUG:-""}

# use SERVER=XYZ/ to push to a different server
SERVER=${SERVER:-""}

# what branch are we on
BRANCH=${BRANCH:-"$(git rev-parse --abbrev-ref HEAD)"}

# make sure docker is build
$(dirname $0)/docker.sh

# check branch and set version
if [ "${BRANCH}" = "master" ]; then
    VERSION=${VERSION:-"2.1.1 2.1 2 latest"}
elif [ "${BRANCH}" = "develop" ]; then
    VERSION="develop"
else
    exit 0
fi

# tag all images and push if needed
for i in pyclowder extractors-wordcount; do
    for v in ${VERSION}; do
        if [ "$v" != "latest" -o "$SERVER" != "" ]; then
            ${DEBUG} docker tag clowder/${i}:latest ${SERVER}clowder/${i}:${v}
        fi
        ${DEBUG} docker push ${SERVER}clowder/${i}:${v}
    done
done

for i in pyclowder extractors-binary-preview extractors-simple-extractor extractors-simple-r-extractor; do
    for v in ${VERSION}; do
        if [ "$v" != "latest" ]; then
            ${DEBUG} docker tag clowder/${i}:onbuild ${SERVER}clowder/${i}:${v}-onbuild
            ${DEBUG} docker push ${SERVER}clowder/${i}:${v}-onbuild
        elif [ "$SERVER" != "" ]; then
            ${DEBUG} docker tag clowder/${i}:onbuild ${SERVER}clowder/${i}:onbuild
            ${DEBUG} docker push ${SERVER}clowder/${i}:onbuild
        else
            ${DEBUG} docker push clowder/${i}:onbuild
        fi
    done
done
