#!/bin/bash

obs=$1
pwd=$(pwd)

cd /usr/local/mwa/anaconda/bin
source activate /usr/local/mwa/anaconda/
cd $pwd

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

if [ ! -d $pwd/$obs ]; then
   echo "Obs working dir : $pwd/$obs does not exist!"
   exit 1
fi

cd $obs

