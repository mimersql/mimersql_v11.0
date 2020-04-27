#!/usr/bin/env bash

# set up a SIGTERM handler to stop MimerSQL gracefully
cleanup()
{
  echo "caught trap - exiting"
  mimcontrol -t mimerdb
  exit 0
}

trap cleanup SIGTERM

# start networking
service xinetd start

# install a license if there is one
if [[ -e /usr/local/MimerSQL/mimerdb/key.mcfg ]]
then
  mimlicense -n -f /usr/local/MimerSQL/mimerdb/key.mcfg
fi

# report the license status
mimlicense -c

# start Mimer SQL
mimcontrol -s mimerdb

# keep looping and looping and looping and…
while true; do :; done
