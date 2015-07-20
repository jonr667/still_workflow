#! /bin/bash

rm -rf $1R
xrfi_simple.py -a all --combine -t 80 --from_npz=$1E.npz $1
