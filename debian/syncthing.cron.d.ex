#
# Regular cron jobs for the syncthing package
#
0 4	* * *	root	[ -x /usr/bin/syncthing_maintenance ] && /usr/bin/syncthing_maintenance
