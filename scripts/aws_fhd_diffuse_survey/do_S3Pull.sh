#!/bin/bash

obs=$1
pwd=$(pwd)

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi
      
mkdir -p $1

if [ ! -d $obs ]; then
   echo "Obs working dir : $pwd/$obs does not exist!"
   exit 1
fi
cd $1

aws s3 cp s3://mwapublic/uvfits/5.1/$1.uvfits ./

return_code=$?
   
if [ $return_code -ne 0 ]; then
   echo "Could not grab file $1.uvfits from THE CLOUD"
   exit 1
fi
aws s3 cp s3://mwapublic/metafits/5.1/$1.metafits ./


return_code=$?
   
if [ $return_code -ne 0 ]; then
   echo "Could not grab file $1.metafits from THE CLOUD"
   exit 1
fi
