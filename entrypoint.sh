#!/bin/bash
set -e

# rabbitmq
if [ "${RABBITMQ_URI}" == "" ]; then

    # configure RABBITMQ_URI if started using docker-compose or --link flag
    if [ -n "${RABBITMQ_PORT_5672_TCP_ADDR}" ]; then
        RABBITMQ_URI="amqp://${RABBITMQ_PORT_5672_TCP_ADDR}:${RABBITMQ_PORT_5672_TCP_PORT}/%2F"
    fi
fi

# start server if asked
if [ "$1" = 'extractor' ]; then
    # make sure main script exists
    if [ "${MAIN_SCRIPT}" == "" ]; then
        echo "No main script specified, can not run code."
        exit -1
    fi
    if [ ! -e "${MAIN_SCRIPT}" ]; then
        echo "Main script specified does not exist."
        exit -1
    fi

    # check to make sure rabbitmq is up
    if [ "${RABBITMQ_PORT_5672_TCP_ADDR}" != "" ]; then
        # start extractor after rabbitmq is up
        for i in `seq 1 10`; do
            if nc -z ${RABBITMQ_PORT_5672_TCP_ADDR} ${RABBITMQ_PORT_5672_TCP_PORT} ; then
                break
            fi
            sleep 1
        done
    fi

    # launch extractor and see what happens
    exec python "./${MAIN_SCRIPT}"
fi

exec "$@"
