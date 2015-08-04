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

populate_qc.py -v $obs 

return_code=$?

if [ $return_code -eq 0 ]; then
   echo "Successfully populated QC DB"
   exit 0
else
   echo "populate_qc.py -v $obs : returned with error code : $return_code"
   exit 1
fi

