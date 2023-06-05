#!/bin/bash
psql -d postgres -c "create database baseregisters;"
psql -d baseregisters -c "create extension postgis;"