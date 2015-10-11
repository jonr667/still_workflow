
#!/bin/bash

obs=$1
pwd=$(pwd)

cd /usr/local/mwa/anaconda/bin
source activate /usr/local/mwa/anaconda/
cd $pwd

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

if [ ! -d $pwd/$obs ]; then
   echo "Obs working dir : $pwd/$obs does not exist!"
   exit 1
fi

cd $obs
python /shared/mwa_wedge/wedge.py --baselines_file /shared/mwa_wedge/MWA_128T_antenna_locations.txt -o ./$1.uvfits

return_code=$?

if [ $return_code -ne 0 ]; then
   echo "Wedge had a problem!"
   exit 1
fi
      