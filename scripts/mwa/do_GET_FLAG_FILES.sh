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
cd $obs

flag_file=$(fetch_file_locs.py -o $obs -f)

if [ -f NO_FLAG_FILES ]; then
   rm NO_FLAG_FILES
fi

if [ ! $flag_file ]; then
   echo "Could not find flag file for obs : $obs"
   touch NO_FLAG_FILES
   exit 0
   
else
   echo "Flag file found, extracting $flag_file"
   unzip /nfs$flag_file -d $pwd/$obs
   exit 0  
fi
