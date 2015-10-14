#!/usr/bin/env python

import psycopg2
import psycopg2.extras
import numpy as np
import os
import sys
import argparse

from still import get_dbi_from_config
from still import SpawnerClass
from still import WorkFlow
from still import process_client_config_file

basedir = os.path.dirname(os.path.realpath(__file__))[:-3]
sys.path.append(basedir + 'lib')


def main():

    parser = argparse.ArgumentParser(description='MWA - Add observations to Workflow Manager')

    parser.add_argument('--config_file', dest='config_file', required=False,
                        help="Specify the complete path to the config file, by default we'll use etc/still.cfg")
    parser.add_argument('-o', dest='obsnums', required=False, nargs='+',
                        help="List of obervations seperated by spaces")


    parser.set_defaults(config_file="%setc/still.cfg" % basedir)

    args, unknown = parser.parse_known_args()

    sg = SpawnerClass()
    wf = WorkFlow()

    sg.config_file = args.config_file
    process_client_config_file(sg, wf)
    dbi = get_dbi_from_config(args.config_file)
    dbi.test_db()  # Testing the database to make sure we made a connection, its fun..
    for obsid in args.obsnums:
        print("Obsid: %s") % obsid
        dbi.add_observation(obsid, obsid, "GPS", None, None, None, outputhost=None, length=None, status='NEW')


if __name__ == "__main__":
    main()
