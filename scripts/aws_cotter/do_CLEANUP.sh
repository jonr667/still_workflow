#!/bin/bash

obs=$1
pwd=$(pwd)

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

if [ ! -d $obs ]; then
   echo "Obs working dir : $pwd/$obs does not exist!"
   exit 1
fi

rm -r $obs
return_code=$?

if [ $return_code -eq 0 ]; then
   exit 0
else
   echo "Could not clean up directory : $pwd/$obs"
   exit 1
fi
