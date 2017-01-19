#!/bin/bash

obs=$1
pwd=$(pwd)

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

mkdir $obs
return_code=$?

if [ $return_code -eq 0 ]; then
   echo "Directory created : $pwd/$obs"
   exit 0
else
   echo "Directory already exists, please clean up : $pwd/$obs"
   exit 1
fi

