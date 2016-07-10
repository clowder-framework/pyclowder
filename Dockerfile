FROM ubuntu:16.04
MAINTAINER Rob Kooper <kooper@illinois.edu>

COPY clowder/* /tmp/clowder/clowder/
COPY setup.py requirements.txt /tmp/clowder/

RUN apt-get update && apt-get install -y --no-install-recommends \
    python \
    python-pip && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --upgrade  -r /tmp/clowder/requirements.txt && \
    pip install --upgrade /tmp/clowder && \
    rm -rf /tmp/clowder && \
    adduser --system clowder
