#!/bin/bash

#version=4
#subversion=1
hostname=$(hostname -s)

production_dir=$(echo $production_dir | sed s/{hostname}/$hostname/)

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

write_uvfits_loc.py -o $obs -v $wf_version -s $wf_subversion -f $production_dir/$obs.uvfits
return_code=$?

if [ $return_code -eq 0 ]; then
   echo "Successuflly wrote uvfits file location to db"
   exit 0
else
   echo "write_uvfits_loc.py -o $obs -v $wf_version -s $wf_subversion -f $production_dir/$obs.uvfits : returned with error code : $return_code"
   exit 1
fi

