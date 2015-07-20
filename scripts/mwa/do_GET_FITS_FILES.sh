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
   if [ -f $gpufile ]; then
      cp $gpufile ./
   else
      echo "Could not fetch file $gpufile"
      exit 1
   fi
done

exit 0
