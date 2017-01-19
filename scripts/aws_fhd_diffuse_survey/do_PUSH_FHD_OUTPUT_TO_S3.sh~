#!/bin/bash

obs=$1
pwd=$(pwd)
#export AWS_ACCESS_KEY_ID="${aws_id}"
#export AWS_SECRET_ACCESS_KEY="${aws_passwd}"

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi
      

if [ ! -d $obs ]; then
   echo "Obs working dir : $pwd/$obs does not exist!"
   exit 1
fi
#cd $1


aws s3 cp ./${obs}/output s3://mwatest/FHD_FIRST_PASS/ --recursive

return_code=$?
   
if [ $return_code -ne 0 ]; then
   echo "Could not push firstpass files for obs : $obs"
   exit 1
fi
