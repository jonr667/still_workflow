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

file_locations=$(fetch_file_locs.py -o $obs)

if [ -f gpu_file_locations_list_$obs.txt ]; then

   rm gpu_file_locations_list_$obs.txt
fi

for file in $file_locations; do
   echo "$file" >> gpu_file_locations_list_$obs.txt
done

line_count=$(cat gpu_file_locations_list_$obs.txt | wc -l)
if [ $line_count -lt 24 ]; then
   echo "There were fewer than 24 files listed"
   exit 1
else
   echo "Files found : $line_count"
   exit 0
fi

