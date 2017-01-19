#! /bin/bash
rm -rf $1_rfi.npz
rm -rf $1_rfi.png
echo flagreader.py $1 -F --npz=$1_rfi.npz --fimg=$1_rfi.png
flagreader.py $1 -F --npz=$1_rfi.npz --fimg=$1_rfi.png
