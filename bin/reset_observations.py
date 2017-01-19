#! /usr/bin/env python
"""
Reset observations corresponding to the input files. Backconverts from input file names to unique observations.
NB in the future this might break with the introduction of tracking files produced by the still
NB filenames must be FULL PATH. If the root is not '/' for all files it will exit


"""
import argparse
import os
import sys
import re
import logging


basedir = os.path.dirname(os.path.realpath(__file__))[:-3]
sys.path.append(basedir + 'lib')

from dbi import File
from still import process_client_config_file, WorkFlow, SpawnerClass, StillDataBaseInterface


def file2jd(zenuv):
    return re.findall(r'\d+\.\d+', zenuv)[0]


def file2pol(zenuv):
    return re.findall(r'\.(.{2})\.', zenuv)[0]


parser = argparse.ArgumentParser(description='Reset Observations')

parser.add_argument('-v', dest='debug', action='store_true',
                    help='set log level to debug')

parser.add_argument('--status', dest='status', required=False, default='',
                    help='set the observation to this status, default will be the first item in the config workflow_actions')

parser.add_argument('--config_file', dest='config_file', required=False,
                    help="Specify the complete path to the config file")

parser.add_argument('--file', nargs='*', dest='files', required=True,
                    help="File name to reset, can use wildcard in directory but you **MUST USE QUOTES AROUND IT**")

parser.set_defaults(config_file="%setc/still.cfg" % basedir)

args, unknown = parser.parse_known_args()

sg = SpawnerClass()
wf = WorkFlow()

sg.config_file = args.config_file
process_client_config_file(sg, wf)
if args.status == '':
    args.status = wf.workflow_actions[0]

# connect to the database
dbi = StillDataBaseInterface(sg.dbhost, sg.dbport, sg.dbtype, sg.dbname, sg.dbuser, sg.dbpasswd, test=False)

# Setup logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('reset_observations.py')

if args.debug:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


# for each file get the obsnum, then reset the status to first item in config files workflow_actions

obsnums = []

# for filename in glob.glob(args.files):

for filename in args.files:
    logger.debug("looking for file {filename}".format(filename=filename))

    try:
        s = dbi.Session()
        FILE = s.query(File).filter(File.filename == filename).one()  # XXX note assumes we are not noting that this file is copied.

        obsnum = FILE.obsnum
        logger.debug("found obsnum {obsnum}".format(obsnum=obsnum))
        s.close()
        logger.debug("setting status to {status}".format(status=args.status))
        dbi.set_obs_status(obsnum, args.status)
        dbi.set_obs_pid(obsnum, None)
        dbi.set_obs_still_host(obsnum, None)
        dbi.add_log(obsnum, args.status, "issuing a reset_observations", 0)
    except Exception as e:
        print("failed on file %s: %s") % (filename, e)
        continue
