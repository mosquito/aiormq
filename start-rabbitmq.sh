#!/usr/bin/env bash

docker pull mosquito/aiormq-rabbitmq
docker kill aiormq_rabbitmq 2>&1 > /dev/null

exec docker run --rm -d \
    --name aiormq_rabbitmq \
    -p 5671:5671 \
    -p 5672:5672 \
    -p 15671:15671 \
    -p 15672:15672 \
    mosquito/aiormq-rabbitmq
