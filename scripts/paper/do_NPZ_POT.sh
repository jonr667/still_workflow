#! /bin/bash

# XXX is a cleanup necessary here?
scp -r -c arcfour256 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $1 $2
