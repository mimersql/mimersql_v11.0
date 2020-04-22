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

# start Mimer SQL and return the current version on stdout
mimcontrol -s mimerdb
miminfo -V

 # keep looping and looping and looping and…
while true; do :; done
