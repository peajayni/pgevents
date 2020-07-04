#!/bin/bash
set -eufx

PROJECT_DIRECTORY=$(dirname $(dirname "$(readlink -f "$0")"))

psql -U postgres -c "create database test"
psql -U postgres -c "create user test with encrypted password 'test'"
psql -U postgres -c "grant all privileges on database test to test"

export PGMIGRATIONS_DSN="dbname=test user=test password=test host=localhost"
export PGMIGRATIONS_BASE_DIRECTORY="${PROJECT_DIRECTORY}/pgevents/migrations"
pgmigrations init
pgmigrations apply

pytest --cov=pgevents --cov-append tests/integration
