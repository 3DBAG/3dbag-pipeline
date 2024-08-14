#!/bin/bash
pg_restore -U ${POSTGRES_USER} -d ${POSTGRES_DB} --no-owner /var/lib/postgresql/data/baseregisters.tar
psql -d postgres -U ${POSTGRES_USER} -c "create database baseregisters_empty;"
psql -d baseregisters_empty -c "create extension postgis;"


