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

for gpufile in $( cat gpu_file_locations_list_$obs.txt ); do
   file_host=$( echo $gpufile | awk -F'/' '{print $2}' )
   #if [ -f $gpufile ]; then
   #   cp $gpufile ./
   scp -c arcfour128 $file_host:$gpufile ./
   return_code=$?
      
   if [ $return_code -ne 0 ]; then
      echo "Could not scp file $gpufile from host $file_host"
      exit 1
   fi
done

exit 0

