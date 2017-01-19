#!/bin/bash

obs=$1
pwd=$(pwd)

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

if [ ! -d $obs ]; then
   echo "Obs working dir : $pwd/$obs does not exist, creating."
   mkdir $obs
fi
cd $obs

aws s3 cp s3://mwatest/fits/$obs ./ --recursive

exit 0

