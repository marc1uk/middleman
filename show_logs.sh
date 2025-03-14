#!/bin/bash
if [ $# -gt 0 ]; then
	if [ "$1" == "-f" ]; then
		journalctl -f  _SYSTEMD_INVOCATION_ID=`systemctl show --value -p InvocationID middleman`
	fi
fi
journalctl  _SYSTEMD_INVOCATION_ID=`systemctl show --value -p InvocationID middleman` | less
