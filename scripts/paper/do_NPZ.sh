#! /bin/bash

rm -rf $1.npz
xrfi_simple.py -a 1 --combine -t 80 -n 5 --to_npz=$1.npz $1
