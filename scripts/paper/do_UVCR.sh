#! /bin/bash
FLAGCHANS="0_65,377_388,510,770,840,852,913,921_922,932_934,942_1023"
rm -rf $1R
#echo xrfi_simple.py -a 1 --combine -t 80 --df=6 -c 0_65,377_388,510,770,840,852,913,921_922,932_934,942_1023 $1
#xrfi_simple.py -a 1 --combine -t 80 --df=6 -c 0_65,377_388,510,770,840,852,913,921_922,932_934,942_1023 $1
# XXX need to figure out why above command fails
echo xrfi_simple.py -a 1 --df=6 -c ${FLAGCHANS} --combine -t 80 $1
xrfi_simple.py -a 1 --df=6 -c ${FLAGCHANS} --combine -t 80 $1
