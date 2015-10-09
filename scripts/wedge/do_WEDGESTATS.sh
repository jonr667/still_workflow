#!/bin/bash

obs=$1
pwd=$(pwd)

if [ ! $1 ]; then
   echo "No observation ID given."
   exit 1
fi

if [ ! -d $obs ]; then
   echo "Obs working dir : $pwd/$obs does not exist!"
   exit 1
fi
cd $1

WEDGESTATS=`python ~/scripts/wedgestats.py $obs.p.npz`

echo found $WEDGESTATS

return_code=$?

if [ $return_code = 0 ]; then
   window_x=`python -c    "print '${WEDGESTATS}'.split()[3]"`
   window_y=`python -c    "print '${WEDGESTATS}'.split()[4]"`
   wedge_res_x=`python -c "print '${WEDGESTATS}'.split()[5]"`
   wedge_res_y=`python -c "print '${WEDGESTATS}'.split()[6]"`
   gal_wedge_x=`python -c "print '${WEDGESTATS}'.split()[7]"`
   gal_wedge_y=`python -c "print '${WEDGESTATS}'.split()[8]"`
   ptsrc_wedge_x=`python -c "print '${WEDGESTATS}'.split()[9]"`
   ptsrc_wedge_y=`python -c "print '${WEDGESTATS}'.split()[10]"`

   previous_window=`psql -h eor-00 mwa_qc -U mwa -c "select count(*) from qs where obsid=${obsid}" -t -A`
   if [ $previous_window -eq 0 ]; then
      #this is the first time, do an insert
      echo "Creating new wedge record for $obsid"
      psql -h eor-00 mwa_qc -U mwa -c "insert into qs (obsid,window_x,window_y,wedge_res_x,wedge_res_y,gal_wedge_x,gal_wedge_y,ptsrc_wedge_x,ptsrc_wedge_y,wedge_timestamp) values (${obsid},${window_x},${window_y},${wedge_res_x},${wedge_res_y},${gal_wedge_x},${gal_wedge_y},${ptsrc_wedge_x},${ptsrc_wedge_y},current_timestamp)"
   else
      #this is a redo, do an update
      echo "updating old wedge record for $obsid"
      psql -h eor-00 mwa_qc -U mwa -c "update qs set (window_x,window_y,wedge_res_x,wedge_res_y,gal_wedge_x,gal_wedge_y,ptsrc_wedge_x,ptsrc_wedge_y,wedge_timestamp) = (${window_x},${window_y},${wedge_res_x},${wedge_res_y},${gal_wedge_x},${gal_wedge_y},${ptsrc_wedge_x},${ptsrc_wedge_y},current_timestamp) where obsid=${obsid}"
   fi    
fi


if [ $return_code -ne 0 ]; then
   echo "Could not perform wedgestats.py"
   exit 1
fi