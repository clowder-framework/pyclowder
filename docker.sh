#!/bin/sh

#DEBUG=echo

# build docker container
${DEBUG} docker build  --tag clowder/pyclowder:2 .
${DEBUG} docker build  --tag clowder/pyclowder:onbuild --file Dockerfile.onbuild .

# build sample extractors
${DEBUG} docker build  --tag clowder/extractors-wordcount:2 sample-extractors/wordcount


if [ "$(git rev-parse --abbrev-ref HEAD)" == "master" ]; then
  ${DEBUG} docker push clowder/pyclowder:2
  ${DEBUG} docker push clowder/pyclowder:onbuild
  ${DEBUG} docker push clowder/extractors-wordcount:2
fi
