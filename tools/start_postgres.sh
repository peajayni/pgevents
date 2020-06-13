#!/bin/bash
set -euf

DB_CONTAINER_NAME="test_db"

set +e
docker stop ${DB_CONTAINER_NAME}
set -e

docker run \
    --rm \
    -d \
    --name ${DB_CONTAINER_NAME} \
    -e POSTGRES_PASSWORD=test \
    -e POSTGRES_USER=test \
    -e POSTGRES_PASSWORD=test \
    -p "5432:5432" \
    postgres

echo "======================="
echo "Postgres is now running"
echo "Run the command:"
echo
echo "docker stop ${DB_CONTAINER_NAME}"
echo
echo "to stop it"
echo "======================="
