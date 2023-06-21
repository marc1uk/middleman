#!/bin/bash

#systemctl start postgresql-12

# export path to zmq, pqxx and boost
export LD_LIBRARY_PATH=/opt/libpqxx-6.4.5/install/lib:/opt/boost_1_66_0/install/lib:/opt/zeromq-4.0.7/lib:$LD_LIBRARY_PATH

# setup database environmental variables
# (probably not required as they're overridden in the config file anyway)
export PG_COLOR=always
export PGHOST=/tmp
export PGPORT=5432
#export PGUSER=admin
export PGDATABASE=daq
export PGDATA=/var/lib/pgsql/data

# wait up to 30s for the postgres server to be ready
#pg_isready -t 30

# run the middleman
cd /opt/middleman
while [ true ]; do
	echo -n "starting middleman at " >> middleman_runs.log
	date >> middleman_runs.log
	/opt/middleman/middleman /opt/middleman/ReceiveSQLConfig
	echo -n "middleman exited with code $? at "
	date >> middleman_runs.log
	sleep 1
done

