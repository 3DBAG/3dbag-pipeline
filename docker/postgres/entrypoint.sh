#!/bin/bash
pg_restore -U ${POSTGRES_USER} -d ${POSTGRES_DB} --no-owner /var/lib/postgresql/data/baseregisters.tar

