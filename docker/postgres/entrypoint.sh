#!/bin/bash
pg_restore -U ${POSTGRES_USER} -d ${POSTGRES_DB} --no-owner /var/lib/postgresql/data/baseregisters.tar
psql -d postgres -U ${POSTGRES_USER} -c "create database baseregisters_old;"
pg_restore -U ${POSTGRES_USER} -d baseregisters_old --no-owner /var/lib/postgresql/data/baseregisters_old.tar
psql -d postgres -U ${POSTGRES_USER} -c "create database ${DAGSTER_PG_DB};"
psql -d ${DAGSTER_PG_DB} -U ${POSTGRES_USER} -c "CREATE USER ${DAGSTER_PG_USER} WITH PASSWORD '${DAGSTER_PG_PASSWORD}';"
psql -d postgres -U ${POSTGRES_USER} -c "create database baseregisters_empty;"
psql -d baseregisters_empty -c "create extension postgis;"


