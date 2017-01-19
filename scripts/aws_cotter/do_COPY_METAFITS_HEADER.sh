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


copy_metafitsheader.py -i $obs.uvfits -m $obs.metafits

return_code=$?

if [ $return_code -eq 0 ]; then
   echo "Metafits header copied successfully"
else
   echo "copy_metafitsheader.py -i $obs.uvfits -m $obs.metafits : returned with error code : $return_code"
   exit 1
fi

