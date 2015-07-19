#!/bin/bash

obs=$1
pwd=$(pwd)

production_dir="/nfs/eor-11/r1/EoRuvfits/batch"

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

if [ ! -d $obs ]; then
   echo "Obs working dir : $pwd/$obs does not exist!"
   exit 1
fi
cd $obs

LIST_OF_FILES="uvfits metafits qs"

for file_type in $LIST_OF_FILES; do
   if [ -f $obs.$file_type ]; then
      cp $obs.$file_type $production_dir/
      if [ ! -f $production_dir/$obs.$file_type ]; then
         echo "File $obs.$file_type could not be copied to $production_dir"
         exit 1
      fi 
   else
      echo "Could not find file $obs.$file_type"
      exit 1
   fi  
done
exit 0
       