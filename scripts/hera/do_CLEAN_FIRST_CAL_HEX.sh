#! /bin/bash
set -e
f=$(basename $1 uvc)
for ext in HH PH
    do
        echo rm -rf ${f}$ext.uvc.npz
        rm -rf ${f}$ext.uvc.npz
done
