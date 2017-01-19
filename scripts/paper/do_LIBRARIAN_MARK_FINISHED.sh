#! /bin/bash
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

# This takes three arguments:
#
# $1 argument is the "connection" name -- used to decide how to connect to the Librarian.
#
# $2 local file name, with or without path prefix.
#
# Example configuration: args = ['onsite', basename]

# Specifying the hostname as event payload since something's required and it's
# the first thing I thought of ...

conn="$1"
path="$2"

exec add_librarian_file_event.py $conn $path rtp.successfully_processed host=\"$(hostname)\"
