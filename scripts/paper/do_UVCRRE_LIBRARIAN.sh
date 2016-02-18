#! /bin/bash

# XXX is a cleanup necessary here?
# first argument is the site
# second argument is the local file name with path
python -m hera_librarian.uploader $1 $2
