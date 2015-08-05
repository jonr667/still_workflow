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
   file_host=$( echo $flag_file | awk -F'/' '{print $2}' )
   scp -c arcfour128 $file_host:$flag_file ./
   return_code=$?
                  
   if [ $return_code -ne 0 ]; then
      echo "Could not grab $flag_file from host : $file_host"
      exit 1
   fi
      
   unzip *.zip
   return_code=$?
                  
   if [ $return_code -ne 0 ]; then
      echo "Could not unzip flag file $flag_file"
      exit 1
   fi

   exit 0  
fi
