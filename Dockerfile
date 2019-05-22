FROM ubuntu:16.04

# environment variables
ENV RABBITMQ_URI="amqp://guest:guest@rabbitmq:5672/%2F" \
    RABBITMQ_EXCHANGE="clowder" \
    RABBITMQ_QUEUE="" \
    REGISTRATION_ENDPOINTS="" \
    EMAIL_SERVER="" \
    EMAIL_SENDER="extractor" \
    MAIN_SCRIPT=""

# install python
RUN apt-get -q -q update && apt-get install -y --no-install-recommends \
        python \
        python-pip \
    && pip install --upgrade setuptools \
    && rm -rf /var/lib/apt/lists/*

# instal pyclowder2
COPY pyclowder /tmp/pyclowder/pyclowder
COPY setup.py description.rst /tmp/pyclowder/

RUN pip install --upgrade /tmp/pyclowder \
    && rm -rf /tmp/pyclowder

# folder for pyclowder code
WORKDIR /home/clowder
COPY notifications.json /home/clowder

# command to run when starting container
CMD python "./${MAIN_SCRIPT}"
