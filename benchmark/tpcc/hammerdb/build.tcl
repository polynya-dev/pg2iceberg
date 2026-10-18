#!/bin/tclsh
# Builds the TPROC-C schema: WAREHOUSES warehouses, loaded by BUILD_VUS
# virtual users.
puts "SETTING CONFIGURATION"
dbset db pg
dbset bm TPC-C

diset connection pg_host $::env(PG_HOST)
diset connection pg_port 5432
diset connection pg_sslmode disable

diset tpcc pg_count_ware $::env(WAREHOUSES)
diset tpcc pg_num_vu $::env(BUILD_VUS)
diset tpcc pg_superuser postgres
diset tpcc pg_superuserpass postgres
diset tpcc pg_defaultdbase postgres
diset tpcc pg_user tpcc
diset tpcc pg_pass tpcc
diset tpcc pg_dbase tpcc
diset tpcc pg_tspace pg_default
diset tpcc pg_storedprocs true
diset tpcc pg_partition false

puts "SCHEMA BUILD STARTED"
buildschema
puts "SCHEMA BUILD COMPLETED"
