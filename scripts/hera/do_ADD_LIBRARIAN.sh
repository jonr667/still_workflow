#! /bin/bash
#
# example configuration: args = ['onsite', '%s/%s' % (parent_dirs,basename)]
#
# first argument is the "connection" -- used to decide how to connect to librarian
# second argument is the local file name with store path prefix
#
# Note that the actual file of interest is present in PWD *without* the
# path prefix.
#
# XXX redundant with the other do_*_LIBRARIAN.sh scripts

conn="$1"
store_path="$2"
#store_path tells the librarian where the data came within the librarian
#basename of the store_path is just the filename
#ie in a librarian path like /data2/stuff/2456789/zen.2456789.34775.uv
# /data2/stuff is the "store"
# 2456789/zen.2456789.34775.uv is the store_path
# zen.2456789.34775.uv is the basename
exec upload_to_librarian.py $conn $(basename $store_path) $store_path
