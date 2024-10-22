#!/bin/bash
pg_restore -U ${POSTGRES_USER} -d ${POSTGRES_DB} --no-owner /data/volume/baseregisters.tar

