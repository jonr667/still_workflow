#!/bin/bash

obs=$1
pwd=$(pwd)

# Only commenting this out and hardcoding a host because eor-14 is out of space on /r1
#hostname=$(hostname -s)
hostname='eor-13'

production_dir=$(echo $production_dir | sed s/{hostname}/$hostname/)


real_prod_dir=$(echo $production_dir | cut -f 2 -d '/' --complement)

# Only commenting this out and hardcoding a host because eor-14 is out of space on /r1
#if [ ! -d $real_prod_dir ]; then
#   echo "Creating directory : $real_prod_dir"
#   mkdir -p $real_prod_dir
#  if [ $? -ne 0 ]; do
#    echo "Mkdir -p $real_prod_dir failed"
#    exit 1
#  fi
#fi

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
       # Commneted out the cp because no space on eor-14
       #cp -r $obs.$file_type $real_prod_dir/
       scp -r $obs.$file_type $hostname:$real_prod_dir/
       # Only commenting this out and hardcoding a host because eor-14 is out of space on /r1
       # if [ ! -e $real_prod_dir/$obs.$file_type ]; then
       #    echo "File $obs.$file_type could not be copied to $real_prod_dir"
       #    exit 1
       # fi 
   else
      echo "Could not find file $obs.$file_type"
      exit 1
   fi  
done
exit 0
       
