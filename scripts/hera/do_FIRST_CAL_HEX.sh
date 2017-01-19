#! /bin/bash
set -e
CALBASE=hsa7458_v000
#CALFILE_HH=hsa7458_v000_HH.py #hsa7458_v000
#CALFILE_PH=hsa7458_v000_PH.py #psa...

f=$(basename $1 uvc)
bad_ant_file=$1.bad_ants
ex_ants=`cat ${bad_ant_file}`

for ext in HH PH
    do
        echo firstcal.py -C ${CALBASE}_${ext} --ex_ants=${ex_ants} ${f}$ext.uvc 
        firstcal.py -C ${CALBASE}_${ext} --ex_ants=${ex_ants} ${f}$ext.uvc 
done

