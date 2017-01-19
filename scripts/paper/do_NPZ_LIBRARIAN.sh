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
exec upload_to_librarian.py $conn $(basename $store_path) $store_path
