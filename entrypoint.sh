#!/bin/bash
set -e

# use rabbitmq env variables to construct RABBITMQ_URI.
if [ "${RABBITMQ_URI}" == "" ]; then

    # if empty, then set to default rabbitmq username
    if [ "$RABBITMQ_USERNAME" == "" ]; then
        RABBITMQ_USERNAME="guest"
    fi
    # if empty, then set to default rabbitmq passwd
    if [ "$RABBITMQ_PASSWD" == "" ]; then
        RABBITMQ_PASSWD="guest"
    fi
    # if empty, then set to default rabbitmq hostname
    if [ "$RABBITMQ_PORT_5672_TCP_ADDR" == "" ]; then
        RABBITMQ_PORT_5672_TCP_ADDR="rabbitmq"
    fi
    # if empty, then set to default rabbitmq port
    if [ "$RABBITMQ_PORT_5672_TCP_PORT" == "" ]; then
        RABBITMQ_PORT_5672_TCP_PORT="5672"
    fi
    # if empty, then set to default rabbitmq vhost
    if [ "$RABBITMQ_VHOST" == "" ]; then
        RABBITMQ_VHOST="%2F"
    fi

    # configure RABBITMQ_URI if started using docker-compose or --link flag
    RABBITMQ_URI="amqp://${RABBITMQ_USERNAME}:${RABBITMQ_PASSWD}@${RABBITMQ_PORT_5672_TCP_ADDR}:${RABBITMQ_PORT_5672_TCP_PORT}/${RABBITMQ_VHOST}"
fi
#TODO, else branch, if RABBITMQ_URI is not empty, then pasrse RABBITMQ_URI to set rabbitmq envs.

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
