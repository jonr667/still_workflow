#!/usr/bin/python
"""
Prints the logs for a given obsnum or input file

"""

import optparse
import os
import sys
import re
import logging
from datetime import datetime, timedelta
import numpy as n

#  Setup the lib path ./lib/  as a spot to check for python libraries
basedir = os.path.dirname(os.path.realpath(__file__))[:-3]
sys.path.append(basedir + 'lib')

from dbi import DataBaseInterface
# from dbi import gethostname
from dbi import Observation
# from dbi import File
from dbi import Log


def file2jd(zenuv):
    return re.findall(r'\d+\.\d+', zenuv)[0]


def file2pol(zenuv):
    return re.findall(r'\.(.{2})\.', zenuv)[0]
o = optparse.OptionParser()
o.set_usage('summarize_night.py obsnum')
o.set_description(__doc__)
# o.add_option('--length',type=float,
#        help='length of the input observations in minutes [default=average difference between filenames]')
o.add_option('-v', action='store_true', help='set log level to debug')
o.add_option('--status', default='UV_POT', help='set the observation to this status [default=UV_POT]')
opts, args = o.parse_args(sys.argv[1:])

# connect to the database
if opts.v:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger('summarize_still')
dbi = DataBaseInterface()
s = dbi.Session()
print "summarizing Distiller"

OBSs = s.query(Observation)
JDs = [OBS.julian_date for OBS in OBSs]
nights = n.sort(list(set(map(int, JDs))))
print "number of nights ingested:", len(nights)

Nobs = s.query(Observation).count()
Nprogress = s.query(Observation).filter(Observation.status != 'NEW', Observation.status != 'UV_POT',
                                        Observation.status != 'NEW', Observation.status != 'COMPLETE').count()

Ncomplete = s.query(Observation).filter(Observation.status == 'COMPLETE').count()
print "Total observations in still:", Nobs
print "Number complete:", Ncomplete
print "Number in progress:", Nprogress
print "broken down by night [most recent activity]"
pending = 0
for night in nights:
    Night_complete = s.query(Observation).filter(Observation.julian_date.like(str(night) + '%'), Observation.status == 'COMPLETE').count()
    Night_total = s.query(Observation).filter(Observation.julian_date.like(str(night) + '%')).count()
    OBSs = s.query(Observation).filter(Observation.julian_date.like(str(night) + '%')).all()
    obsnums = [OBS.obsnum for OBS in OBSs]
    if s.query(Log).filter(Log.obsnum.in_(obsnums)).count() < 1 and opts.v:
        print night, ':', 'completeness', 0, '/', Night_total, '[Pending]'
    if s.query(Log).filter(Log.obsnum.in_(obsnums)).count() < 1:
        pending += Night_total
        continue
    LOG = s.query(Log).filter(Log.obsnum.in_(obsnums)).order_by(Log.timestamp.desc()).limit(1).one()
    if LOG.timestamp > (datetime.utcnow() - timedelta(5.0)) or opts.v:
        print night, ':', 'completeness', Night_complete, '/', Night_total, LOG.timestamp

# find all obses that have failed in the last 12 hours
print("observations pending: %s") % pending
FAIL_LOGs = s.query(Log).filter(Log.exit_status > 0, Log.timestamp > (datetime.utcnow() - timedelta(0.5))).all()
logger.debug("found %d FAILURES" % len(FAIL_LOGs))

# break it down by stillhost
fail_obsnums = [LOG.obsnum for LOG in FAIL_LOGs]
print("fails in the last 12 hours")
if len(fail_obsnums) < 1:
    print("None")
else:
    FAIL_OBSs = s.query(Observation).filter(Observation.obsnum.in_(fail_obsnums)).all()
    fail_stills = list(set([OBS.stillhost for OBS in FAIL_OBSs]))  # list of stills with fails
    for fail_still in fail_stills:
        # get failed obsnums broken down by still
        fail_count = s.query(Observation).filter(Observation.obsnum.in_(fail_obsnums), Observation.stillhost == fail_still).count()
        print("Fail Still : %s , Fail Count %s") % (fail_still, fail_count)
    print("most recent fails")
    for FAIL_OBS in FAIL_OBSs:
        print FAIL_OBS.obsnum, FAIL_OBS.status, FAIL_OBS.stillhost
print "Number of observations completed in the last 24 hours"
good_obscount = s.query(Log).filter(Log.exit_status == 0, Log.timestamp > (datetime.utcnow() - timedelta(1.0)), Log.stage == 'CLEAN_UVCRE').count()  # HARDWF
print("Good count: %s") % good_obscount
s.close()
sys.exit(0)
