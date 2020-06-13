#!/bin/bash
set -eufx

psql -U postgres -c "create database test"
psql -U postgres -c "create user test with encrypted password 'test'"
psql -U postgres -c "grant all privileges on database test to test"

pytest --cov=pgevents --cov-append tests/integration
