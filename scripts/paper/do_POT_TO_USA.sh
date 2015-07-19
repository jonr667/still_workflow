#! /bin/bash
POT=$1
DESTINATION=$2
UVCRRE_FILE=$3
NPZ_FILE=$4
# 
#rsync -rvuP -e 'ssh -c arcfour256' ${UVCRRE_FILE} ${NPZ_FILE} ${DESTINATION}  
ssh ${POT} "rsync -rvuP -e 'ssh -c arcfour256' ${UVCRRE_FILE} ${NPZ_FILE} ${DESTINATION}"
#now do the checksum to test XXX what does this actually do?  ask DMM
#rsync -rvuP -e 'ssh -c arcfour256' ${UVCRRE_FILE} ${NPZ_FILE} ${DESTINATION}
ssh ${POT} "rsync -rvuP -e 'ssh -c arcfour256' ${UVCRRE_FILE} ${NPZ_FILE} ${DESTINATION}"
#XFERSTATUS=$?
 

#rsync -r -c arcfour256 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $1 $2
