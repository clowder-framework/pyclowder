# syntax=docker/dockerfile:1

# FROM python:3.9-slim-buster

FROM ubuntu:20.04

WORKDIR /extractor
COPY requirements.txt ./

ARG PYTHON_VERSION="3.9"

# install python
RUN apt-get -q -q update \
  && apt-get install -y --no-install-recommends python${PYTHON_VERSION} curl \
  && if [ ! -e /usr/bin/python ]; then ln -s /usr/bin/python${PYTHON_VERSION} /usr/bin/python; fi \
  && curl -k https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py \
  && python /tmp/get-pip.py \
  && pip install --upgrade setuptools \
  && rm -rf /var/lib/apt/lists/* /tmp/get-pip.py

RUN python -m pip install --upgrade pip

RUN apt-get -q -q --allow-releaseinfo-change update \
  && apt-get install -y build-essential curl unzip psmisc \
  && pip install cython==0.29.0 pytest 



# install Ray.io
RUN git clone https://github.com/ray-project/ray.git
# Install Bazel.
RUN ray/ci/travis/install-bazel.sh
RUN cd ray/python
RUN pip install -e . --verbose

# Build the dashboard
# (requires Node.js, see https://nodejs.org/ for more information).
# pushd ray/dashboard/client
# npm install
# npm run build
# popd


RUN pip3 install -r requirements.txt

COPY rayWorkflow.py extractor_info.json ./
CMD python rayWorkflow.py