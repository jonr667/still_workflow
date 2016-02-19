#! /bin/bash

# example call from config: args = ['onsite', '%s/%s' % (path,basename)]
# first argument is the "site" -- used to decide how to connect to librarian
# second argument is the local file name with path
jd=$(basename $(dirname $2))
basename=$(basename $2)

mkdir -p $jd
cp -r $basename $jd/$basename"_test"

python -m hera_librarian.uploader $1 $jd/$basename"_test"
