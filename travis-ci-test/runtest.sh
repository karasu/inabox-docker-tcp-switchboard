#!/bin/bash -ex

# start the switchboard
../docker-tcp-switchboard.py config.ini &
DAEMONPID=$!
function cleanup {
  echo "Cleaning up..."
  kill -9 $DAEMONPID
}
trap cleanup EXIT

sleep 2 # give time to startup

./client.py 4 10 7 10

# Show logfile
cat /tmp/logfile

if [ $(docker ps -aq|wc -l) -eq 0 ]; 
then 
	echo "Success: All containers are gone"; 
else 
	echo "Fail: Some containers remain"; 
	docker ps -a; 
	false; 
fi
