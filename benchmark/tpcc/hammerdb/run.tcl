#!/bin/tclsh
# Runs the timed TPROC-C driver: VUS virtual users, RAMPUP then DURATION
# minutes, no keying/thinking time.
puts "SETTING CONFIGURATION"
dbset db pg
dbset bm TPC-C

diset connection pg_host $::env(PG_HOST)
diset connection pg_port 5432
diset connection pg_sslmode disable

diset tpcc pg_superuser postgres
diset tpcc pg_superuserpass postgres
diset tpcc pg_defaultdbase postgres
diset tpcc pg_user tpcc
diset tpcc pg_pass tpcc
diset tpcc pg_dbase tpcc
diset tpcc pg_storedprocs true
diset tpcc pg_driver timed
diset tpcc pg_total_iterations 10000000
diset tpcc pg_rampup $::env(RAMPUP)
diset tpcc pg_duration $::env(DURATION)
diset tpcc pg_vacuum false
diset tpcc pg_timeprofile true
diset tpcc pg_allwarehouse true

loadscript
puts "TEST STARTED"
vuset vu $::env(VUS)
vucreate
tcstart
tcstatus
set jobid [ vurun ]
vudestroy
tcstop
puts "TEST COMPLETE"
set of [ open $::env(TMP)/pg_tprocc w ]
puts $of $jobid
close $of
