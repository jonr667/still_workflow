#! /bin/bash
set -e
sitename=${1}
librarian_path=${3}
f=$(basename ${2} uvc)
for ext in HH PH PI PP
    do 
        FILE=${f}${ext}.uvc
        echo upload_to_librarian.py ${sitename} ${FILE} ${librarian_path}/${FILE}
        upload_to_librarian.py ${sitename} ${FILE} ${librarian_path}/${FILE}
done

