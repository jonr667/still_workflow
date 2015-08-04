#!/bin/bash

obs=$1
pwd=$(pwd)
hostname=$(hostname -s)

production_dir=$(echo $production_dir | sed s/{hostname}/$hostname/)

if [ ! -d $production_dir ]; then
   echo "Creating directory : $production_dir"
   mkdir -p $production_dir
fi

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
   if [ -e $obs.$file_type ]; then
      cp -r $obs.$file_type $production_dir/
      if [ ! -e $production_dir/$obs.$file_type ]; then
         echo "File $obs.$file_type could not be copied to $production_dir"
         exit 1
      fi 
   else
      echo "Could not find file $obs.$file_type"
      exit 1
   fi  
done
exit 0
       