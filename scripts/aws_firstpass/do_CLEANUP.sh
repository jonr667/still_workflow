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
rm -rf $obs
return_code=$?
   
if [ $return_code -ne 0 ]; then
   echo "Could not clean directory $pwd/$obs"
   exit 1
fi
