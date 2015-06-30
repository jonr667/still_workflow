#! /usr/bin/env python

import curses
import time
import os
import sys
import numpy as n

basedir = os.path.dirname(os.path.realpath(__file__)).replace("unit_tests", "")
sys.path.append(basedir + 'lib')

from dbi import DataBaseInterface
from dbi import Observation


# setup my curses stuff following
# https://docs.python.org/2/howto/curses.html
stdscr = curses.initscr()
curses.noecho()
curses.cbreak()
stdscr.keypad(1)
stdscr.nodelay(1)

# setup my db connection
# Jon : set this up correctly, read conf file
dbi = DataBaseInterface()

stdscr.addstr("DiStiller Status Board. Monitoring : {dbname}".format(dbname=dbi.dbinfo['dbname']))
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
try:
    while(1):
        # get the screen dimensions
        #        resize = curses.is_term_resized(y, x)
        #        # Action in loop if resize is True:
        #        if resize is True:
        #            y, x = screen.getmaxyx()
        #            stdscr.clear()
        #            statusscr.clear()
        #            curses.resizeterm(y, x)
        #            stdscr.refresh()
        #            statussrc.refresh()
        # load the currently executing files
        heigh, width = stdscr.getmaxyx()
        curline = 2
        i += 1
        stdscr.addstr(0, 30, stat[i % len(stat)])
        s = dbi.Session()
        totalobs = s.query(Observation).count()
        stdscr.addstr(curline, 0, "Number of observations currently in the database: {totalobs}".format(totalobs=totalobs))
        curline += 1
        OBSs = s.query(Observation).filter(Observation.status != 'UV_POT', Observation.status != 'COMPLETE', Observation.currentpid > 0).all()  # HARDWF
        POTCOUNT = s.query(Observation).filter(Observation.status == 'UV_POT').count()  # HARDWF
        obsnums = [OBS.obsnum for OBS in OBSs]
        obshosts = [OBS.stillhost for OBS in OBSs]
        s.close()
        hosts = list(set(obshosts))
        stdscr.addstr(curline, 0,
                      "Number of observations currently being processed: {num}".format(num=len(obsnums)))
        curline += 1
        stdscr.addstr(curline, 0, "Observations waiting to be processed: {num}".format(num=POTCOUNT))
        curline += 1
        statusscr.erase()
        if len(obsnums) == 0 or len(hosts) == 0:
            statusscr.addstr(0, 0, "  ----  Still Idle  ----   ")
        else:
            colwidth = int(width / len(hosts))
            rowcount = n.zeros(len(hosts)).astype(int)
            for j, host in enumerate(hosts):
                statusscr.addstr(1, j * colwidth + int(colwidth / 2.), host)
                for m, obsnum in enumerate(obsnums):
                    pothost, path, filename = dbi.get_input_file(obsnum)
                    status = dbi.get_obs_status(obsnum)
                    # still_host = dbi.get_obs_still_host(obsnum)
                    still_host = obshosts[m]
                    if still_host != host:  # Jon : ?
                        continue
                    try:
                        statusscr.addstr(rowcount[j] + 2, j * colwidth,
                                         "{obsnum} {filename} {status}".format(filename=os.path.basename(filename), status=status, obsnum=obsnum))
                    except Exception, e:
                        curses.nocbreak()
                        stdscr.keypad(0)
                        curses.echo()
                        curses.endwin()
                        print j * colwidth, rowcount[j] + 2, len("{filename} {status}".format(filename=os.path.basename(filename), status=status))
                        print j, rowcount
                        print e
                        sys.exit()
                    rowcount[j] += 1
        statusscr.refresh()
        c = stdscr.getch()
        if c == ord('q'):
            break
        time.sleep(1)
except Exception, e:
    pass
# terminate
curses.nocbreak()
stdscr.keypad(0)
curses.echo()
curses.endwin()
print e
