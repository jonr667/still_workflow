#! /bin/bash
#copy the test data to the pot
echo "clearing the database for a fresh run"
PW=`python -c "from ddr_compress.dbi import dbinfo; print dbinfo['password']"`
DB=`python -c "from ddr_compress.dbi import dbinfo; print dbinfo['dbname']"`
mysql ${DB} -h qmaster --password=${PW} -e "set foreign_key_checks=0; truncate  neighbors; truncate observation; truncate file; set foreign_key_checks=1;"
echo "initializing the pot for the test"
ssh pot0 rm -rf /data/pot0/*{RRE,npz}
ssh still4 rm -rf /data/still4/*
ssh still5 rm -rf /data/still5/*
rsync -avP -e "ssh -c arcfour128"  ~/test_pot/sim4/z*uv pot0:/data/pot0
echo "starting the task servers on still machines"
echo "starting qdaemon"
echo "loading the data into the db"
#ssh pot0 "add_observations.py /data/pot0/z*892.[2-3]*.xx.uv " #A small test that runs in about 10 minutes
ssh pot0 "add_observations.py /data/pot0/z*.uv "  #this test takes about half an hour
echo "DONE"
