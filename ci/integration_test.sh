#!/bin/bash
set -eufx

BASE_DIR=$(dirname "$0")

DB_CONTAINER_NAME="integration_db"

TEST_IMAGE_NAME="test"
TEST_CONTAINER_NAME="test_runner"

NETWORK_NAME="test"

function clean_up {
  set +e
  docker stop ${DB_CONTAINER_NAME}
  docker network rm ${NETWORK_NAME}
  set -e
}

trap clean_up EXIT

clean_up

docker network create ${NETWORK_NAME}

docker run \
    --rm \
    -d \
    --name ${DB_CONTAINER_NAME} \
    --network ${NETWORK_NAME} \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    postgres

echo "Waiting for postgres to start"
sleep 10s

docker build --tag ${TEST_IMAGE_NAME} "${BASE_DIR}/.."

docker run \
  --rm \
  --network ${NETWORK_NAME} \
  --name ${TEST_CONTAINER_NAME} \
  ${TEST_IMAGE_NAME} \
  pytest tests/integration
