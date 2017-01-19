#! /bin/bash
set -e
rm -rf $1
echo scp -r -c arcfour256 $2 .
scp -r -c arcfour256 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $2 .
