#! /usr/bin/env python
import ddr_compress as ddr
import sys
import optparse
import os
import configparser
import logging
# import numpy as n
import affinity
import multiprocessing

logging.basicConfig(level=logging.DEBUG)
affinity.set_process_affinity_mask(0, 2 ** multiprocessing.cpu_count() - 1)
print ddr.__file__
# DATA_DIR = '/data' # where stills put the data they are working on
o = optparse.OptionParser()
o.set_usage('still_taskserver [options] *.uv')
o.set_description(__doc__)
o.add_option('--port', type=int,
             help='set port number [no default]')
o.add_option('--logfile',
             help="optionally send logs to a file instead")
o.add_option('--configfile',
             help='Input a configuration file. see ddr_compress/configs/ for template')
opts, args = o.parse_args(sys.argv[1:])
configfile = os.path.expanduser('~/.ddr_compress/still.cfg')
logger = logging.getLogger('taskserver')
# from ddr_compress.task_server import logger
logger.setLevel(logging.DEBUG)

if opts.logfile is not None:
    fh = logging.FileHandler(opts.logfile)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.debug('Starting log file')
if len(args) == 0 and os.path.exists(configfile):  # todo config file an inputable thing above
    hostname = ddr.dbi.gethostname()
    config = configparser.ConfigParser()
    configfile = os.path.expanduser(configfile)
    if os.path.exists(configfile):
        logger.info('loading file ' + configfile)
        config.read(configfile)
        DATA_DIR = config[hostname]['datadir']
        if opts.port is None:
            opts.port = int(config[hostname]['port'])
    else:
        logging.info(configfile + " Not Found. Exiting")
        sys.exit()
else:
    DATA_DIR = args[0]
dbi = ddr.dbi.DataBaseInterface()
logger.debug('testing db connection')
dbi.test_db()
logger.debug('starting task_server on {hostname}:{port}'.format(hostname=hostname, port=opts.port))
task_server = ddr.task_server.TaskServer(dbi, data_dir=DATA_DIR, port=opts.port)
task_server.start()
