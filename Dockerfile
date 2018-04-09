FROM ubuntu:16.04

# environment variables
ENV RABBITMQ_URI="" \
    RABBITMQ_EXCHANGE="clowder" \
    RABBITMQ_QUEUE="" \
    REGISTRATION_ENDPOINTS="https://clowder.ncsa.illinois.edu/extractors" \
    RABBITMQ_USERNAME="guest"\
    RABBITMQ_PASSWD="guest"\
    RABBITMQ_PORT_5672_TCP_ADDR="rabbitmq"\
    RABBITMQ_PORT_5672_TCP_PORT="5672"\
    RABBITMQ_VHOST="%2F"\
    MAIN_SCRIPT=""

# install python
RUN apt-get -q -q update && apt-get install -y --no-install-recommends \
        netcat \
        python \
        python-pip \
    && pip install --upgrade setuptools \
    && rm -rf /var/lib/apt/lists/* \
    && adduser --system clowder

# instal pyclowder2
COPY pyclowder /tmp/pyclowder/pyclowder
COPY setup.py description.rst /tmp/pyclowder/

RUN pip install --upgrade /tmp/pyclowder \
    && rm -rf /tmp/pyclowder

# change folder
WORKDIR /home/clowder/
