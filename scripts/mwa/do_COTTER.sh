#!/bin/bash

cotter_args="-timeres 2 -freqres 80 -usepcentre -initflag 2 -noflagautos -absmem 40 -j 5"

obs=$1

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

if [ ! -d $obs ]; then
   echo "Obs working dir : $pwd/$obs does not exist!"
   exit 1
fi
cd $obs

gpu_files=$(cat gpu_file_locations_list_$obs.txt | tr -d "\n")

if [ -f NO_FLAG_FILES ]; then
   echo "running cotter in flaggin mode" >>$obslog
   cotter -m $obs.metafits $cotter_args $gpu_files
   return_code=$?
else
   echo "running cotter using Randall flags" >>$obslog
   cotter -m $obs.metafits $cotter_args -flagfiles $obs_%%.mwaf $gpu_files
   return_code=$?
fi

if [ $return_code -eq 0 ]; then
   echo "Cotter completed with return code 0"
   exit 0
else
   echo "Cotter experienced an error with obs $obs, args: $cotter_args"
   exit 1
fi
