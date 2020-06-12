#!/bin/bash
set -x

psql -c "create database test_db;" -U postgres
psql -c "create user test_user with encrypted password 'test_password';" -U postgres
psql -c "grant all privileges on database test_db to test_user;" -U postgres
