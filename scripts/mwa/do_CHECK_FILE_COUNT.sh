#!/bin/bash

obs=$1

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi
nfiles_mandc=$(get_nfiles.py -o $obs)
nfiles_ngas=$(fetch_file_locs.py -n -o $obs)

if [ $nfiles_mandc -ne $nfiles_ngas ]; then
   echo "File count did not match, MANDC: $nfiles_mandc - NAGS: $nfiles_ngas"
   exit 1
else
  echo "File count for MANDC and NGAS matched at : $nfiles_mandc"
  exit 0
fi
