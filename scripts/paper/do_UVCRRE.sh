#! /bin/bash

rm -rf $2E
# XXX are we generating UVCRRE only, or D and F also?
ddr_filter_coarse.py -a all -p xx,xy,yx,yy --maxbl=301 --clean=1e-3 --output=ddr --nsections=20 $1 $2 $3
