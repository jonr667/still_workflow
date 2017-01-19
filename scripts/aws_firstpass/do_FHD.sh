#!/bin/bash

obs=$1
pwd=$(pwd)
output_path="${pwd}/${obs}/output"
fhd_path="/usr/local/MWA/FHD"
#version_tag="arn_new_cube_defaults"
version_tag="jonr_barebones_aws"
nslots="14"

mkdir -p $output_path
# cd $pwd

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

if [ ! -d $pwd/$obs ]; then
   echo "Obs working dir : $pwd/$obs does not exist!"
   exit 1
fi

cd $fhd_path/Observations

/usr/local/bin/idl -IDL_DEVICE ps -IDL_CPU_TPOOL_NTHREADS $nslots -e eor_firstpass_versions -args $obs $output_path $version_tag

return_code=$?

if [ $return_code -ne 0 ]; then
   echo "FHD had a problem!"
   exit 1
fi
