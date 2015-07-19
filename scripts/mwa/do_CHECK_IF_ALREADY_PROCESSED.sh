#!/bin/bash
version=4
subversion=1

obs=$1

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

file=$(read_uvfits_loc.py -v $version -s $subversion -o $obs)

if [ ! $file ]; then
   echo "Has not been processed."
   exit 0
else
   echo "This obs $obs has already been processed according to file : $file"
   exit 1
fi
