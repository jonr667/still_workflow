#!/bin/bash

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

make_metafits.py -u http://ngas01.ivec.org  -g $obs
return_code=$?

if [ $return_code -eq 0 ]; then
  if [ -f $obs.metafits ]; then
     echo "Metafits file created : $obs.metafits"
     exit 0
  else
     echo "Metafits file $obs.metafits was not found"
     exit 1
  fi
else
   echo "make_metafits.py -u http://ngas01.ivec.org  -g $obs returned with error code : $return_code"
   exit 1
fi

