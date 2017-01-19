#!/bin/bash

#cotter_args="-timeres 2 -freqres 80 -usepcentre -initflag 2 -noflagautos -absmem 40 -j 5 -o $obs.uvfits -saveqs $obs.qs"
obs=$1

full_cotter_args="${cotter_args} -o ${obs}.uvfits -saveqs ${obs}.qs"

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

if [ ! -d $obs ]; then
   echo "Obs working dir : $pwd/$obs does not exist!"
   exit 1
fi
cd $obs

gpu_files=$(ls *gpu*.fits | tr "\n" " ")

if [ -f NO_FLAG_FILES ]; then
   echo "running cotter in flaggin mode"
   echo "cotter -m $obs.metafits $full_cotter_args $gpu_files"
   cotter -m $obs.metafits $full_cotter_args $gpu_files
   return_code=$?
else
   echo "running cotter using Randall flags"
   echo "cotter -m $obs.metafits $full_cotter_args -flagfiles ${obs}_%%.mwaf $gpu_files"
   cotter -m $obs.metafits $full_cotter_args -flagfiles ${obs}_%%.mwaf $gpu_files   # the %% are Cotters way of passing in gpuname to flagfiles
   return_code=$?
fi

if [ $return_code -eq 0 ]; then
   echo "Cotter completed with return code 0"
   exit 0
else
   echo "Cotter experienced an error with obs $obs, args: $full_cotter_args"
   exit 1
fi
