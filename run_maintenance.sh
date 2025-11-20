#!/bin/sh
set -x
while [ /bin/true ]; do
	psql -c "CALL partman.partition_data_proc('public.monitoring', p_batch := 1);"
	NDEFAULT=$(psql -At -c "SELECT * FROM partman.check_default();"  | cut -d'|' -f 2 );
	#if [ $? -ne 0 ]; then
	if [ ! -z "${NDEFAULT}" ] && [ ${NDEFAULT} -eq 0 ]; then
		break;
	else
		sleep 1200
	fi
done
sleep 1200
#psql -c "VACUUM ANALYZE monitoring;"
