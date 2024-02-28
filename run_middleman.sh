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

# running the middleman in a loop enables reloading/recovery
# but we need to provide some means to fully terminate.
# the configuration specifies a quit file; if found, we quit
# default is '$PWD/quit', but check the config for user a override
STOPFILE="stop"
QUITFILE="quit"
while read -r -a LINE; do 
	#echo "next line is '${LINE}'"
	if [ "${LINE[0]}" == "quitfile" ]; then
		#echo "quitfile: ${LINE[1]}";
		QUITFILE="${LINE[1]}";
	fi;
	if [ "${LINE[0]}" == "stopfile" ]; then
		#echo "stopfile: ${LINE[1]}";
		STOPFILE="${LINE[1]}";
	fi;
done < <(cat ReceiveSQLConfig)

if [ -f ${QUITFILE} ]; then
	rm ${QUITFILE}
fi
if [ -f ${STOPFILE} ]; then
	rm ${STOPFILE}
fi

# run the middleman
cd /opt/middleman
while [ true ]; do
	echo -n "starting middleman at " >> middleman_runs.log
	date >> middleman_runs.log
	/opt/middleman/middleman /opt/middleman/ReceiveSQLConfig
	echo -n "middleman exited with code $? at " >> middleman_runs.log
	date >> middleman_runs.log
	sleep 1
	if [ -f ${QUITFILE} ]; then
		touch ${STOPFILE}; # trigger the middleman to stop, redundant
		rm $QUITFILE
		break;
	fi
done

