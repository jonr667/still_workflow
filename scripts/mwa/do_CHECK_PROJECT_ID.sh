#!/bin/bash

obs=$1

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

project_id=$(get_projid.py -o $obs)
if [ project_id ]; then
   if [ $project_id != "G0009" ] && [ $project_id != "G0010" ]; then
      echo "Project ID for obs: $obs, does not match either G0009 nor G0010"
      exit 1
   else
     echo "Project ID matches G0009 or G0010"
     exit 0
   fi
else
  echo "Could not get project ID for obs : $obs"
  exit 1
fi
