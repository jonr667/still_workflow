#! /bin/bash
set -e
sitename=${1}
librarian_path=${3}
f=$(basename ${2} uvc)
#only the hexes get firstcal'd
for ext in HH PH
    do
        local_file=${f}$ext.uvc.npz
        librarian_file=${f}$ext.uvc.firstcal.npz
        echo upload_to_librarian.py ${sitename} ${local_file} ${librarian_path}/${librarian_file}
        upload_to_librarian.py ${sitename} ${local_file} ${librarian_path}/${librarian_file}
done
