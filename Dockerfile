FROM ubuntu:16.04
MAINTAINER Rob Kooper <kooper@illinois.edu>

RUN apt-get -q -q update && apt-get install -y --no-install-recommends \
        netcat \
        python \
        python-pip \
    && pip install --upgrade setuptools \
    && rm -rf /var/lib/apt/lists/* \
    && adduser --system clowder

COPY pyclowder /tmp/pyclowder/pyclowder
COPY setup.py requirements.txt /tmp/pyclowder/

RUN pip install --upgrade  -r /tmp/pyclowder/requirements.txt \
    && pip install --upgrade /tmp/pyclowder \
    && rm -rf /tmp/pyclowder
