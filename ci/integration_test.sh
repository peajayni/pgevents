#!/bin/bash
set -eufx

DB_IMAGE_NAME=integration_db
TEST_IMAGE_NAME=test
NETWORK_NAME=test

function clean_up {
  set +e
  docker stop ${DB_IMAGE_NAME}
  docker network rm ${NETWORK_NAME}
  set -e
}

clean_up

docker network create ${NETWORK_NAME}

docker run \
    --rm \
    -d \
    --name ${DB_IMAGE_NAME} \
    --network ${NETWORK_NAME} \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    postgres

echo "Waiting for postgres to start"
sleep 30s

docker build .. --tag ${TEST_IMAGE_NAME}

set +e
docker run --rm --network ${NETWORK_NAME} ${TEST_IMAGE_NAME} pytest tests/integration
TEST_EXIT=$?
set -e

clean_up

exit ${TEST_EXIT}
