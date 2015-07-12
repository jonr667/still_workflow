#! /usr/bin/env python

import curses
import time
import os
import sys
import numpy as n

basedir = os.path.dirname(os.path.realpath(__file__))[:-3]
sys.path.append(basedir + 'lib')

from dbi import Observation
from still import get_dbi_from_config
from still import SpawnerClass
from still import WorkFlow
from still import process_client_config_file

# setup my curses stuff following
# https://docs.python.org/2/howto/curses.html
stdscr = curses.initscr()
curses.noecho()
curses.cbreak()
stdscr.keypad(1)
stdscr.nodelay(1)

# setup my db connection
# Jon : set this up correctly, read conf file
config_file = basedir + 'etc/still.cfg'

sg = SpawnerClass()
wf = WorkFlow()

sg.config_file = config_file
process_client_config_file(sg, wf)

dbi = get_dbi_from_config(config_file)
dbi.test_db()  # Testing the database to make sure we made a connection, its fun..

stdscr.addstr("DiStiller Status Board. Monitoring")
stdscr.addstr(1, 0, "Press 'q' to exit")
statheight = 50
statusscr = curses.newwin(statheight, 400, 5, 0)
statusscr.keypad(1)
statusscr.nodelay(1)
curline = 2
colwidth = 50
obslines = 20
stat = ['\\', '|', '/', '-', '.']
i = 0
#try:
while(1):

    heigh, width = stdscr.getmaxyx()
    curline = 2
    i += 1
    stdscr.addstr(0, 50, stat[i % len(stat)])
    s = dbi.Session()
    stills = dbi.get_available_stills()
    totalobs = s.query(Observation).count()
    stdscr.addstr(curline, 0, "Number of observations currently in the database: {totalobs}".format(totalobs=totalobs))
    curline += 1
    OBSs = s.query(Observation).filter(Observation.current_stage_in_progress != "", Observation.status != 'COMPLETE', Observation.currentpid > 0).all()  # HARDWF
    POTCOUNT = s.query(Observation).filter(Observation.status == wf.workflow_actions[0]).count()  # HARDWF
    failed_obs = s.query(Observation).filter(Observation.current_stage_in_progress == "FAILED").order_by(Observation.current_stage_start_time)
    killed_obs = s.query(Observation).filter(Observation.current_stage_in_progress == "KILLED").order_by(Observation.current_stage_start_time)
    obsnums = [OBS.obsnum for OBS in OBSs]
    obshosts = [OBS.stillhost for OBS in OBSs]
#    print("Obshosts: %s") % obshosts
 #   sys.exit(0)
    s.close()
    hosts = list(set(obshosts))
    stdscr.addstr(curline, 0, "Number of observations currently being processed: {num}".format(num=len(obsnums)))
    curline += 1
    stdscr.addstr(curline, 0, "Observations waiting to be processed: {num}".format(num=POTCOUNT))
    curline += 1
    for still in stills:
        stdscr.addstr(curline, 0, "Still : %s, DataDir : %s, CPU LOAD : %s" % (still.hostname, still.data_dir, still.current_load))
        curline += 1
        for obs in dbi.get_obs_assigned_to_still(still.hostname):
            if obs.current_stage_in_progress != "KILLED" and obs.current_stage_in_progress != "FAILED":
                stdscr.addstr(curline, 0, "     Obs# : %s,  Currently Processing : %s,  PID: %s,  Started : %s"
                              % (obs.obsnum, obs.current_stage_in_progress, obs.currentpid, obs.current_stage_start_time))
                curline += 1
    curline += 1
    stdscr.addstr(curline, 0, "Killed Observations: %s   |    Failed Observations : %s" % (killed_obs.count(), failed_obs.count()))
    curline += 2
    for killed in killed_obs:
        stdscr.addstr(curline, 0, "Obs# : %s, Current completed stage : %s,  Time of last attempt : %s,  Status of last attempt : %s"
                      % (killed.obsnum, killed.status, killed.current_stage_start_time, killed.current_stage_in_progress))
        curline += 2
    for failed in failed_obs:
        stdscr.addstr(curline, 0, "Obs# : %s, Current completed stage : %s,  Time of last attempt : %s,  Status of last attempt : %s"
                      % (failed.obsnum, failed.status, failed.current_stage_start_time, failed.current_stage_in_progress))
        curline += 1
    statusscr.erase()
    if len(obsnums) == 0 or len(hosts) == 0:
        statusscr.addstr(0, 0, "  ----  Still Idle  ----   ")

    statusscr.refresh()
    c = stdscr.getch()
    if c == ord('q'):
        sys.exit(0)

    time.sleep(2)
# except Exception:
#    pass
# terminate
curses.nocbreak()
stdscr.keypad(0)
curses.echo()
curses.endwin()
