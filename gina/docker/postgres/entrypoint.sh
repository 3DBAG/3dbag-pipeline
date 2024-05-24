#!/bin/bash
pg_restore -U baseregisters_test_user -d baseregisters_test --no-owner /var/lib/postgresql/data/baseregisters.tar
