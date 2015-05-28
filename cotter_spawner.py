#!/usr/bin/env python
# import sys
import argparse
import configparser
import os
import psycopg2
import psycopg2.extras
import numpy as np
from still.dbi import DataBaseInterface, Observation, logger
from still.scheduler import Scheduler



class SpawnerClass:
    CommandLineArgs = ''

    def __init__(self):
        self.data = []
        self.config_file = ''
        self.config_name = ''


class MWAScheduler(Scheduler):

    def __init__(self):
        self.time_last_run = 0
        
    def ext_command_hook(self):
        print("Ext_command_hook!")
        print("Time Last run %s" % self.time_last_run)
        self.time_last_run = self.time_last_run + 1
        return


class MWADataBaseInterface(DataBaseInterface):

    def add_observation(self, obsnum, date, date_type, pol, filename, host, length=2 / 60. / 24, status='UV_POT'):
        """
        create a new observation entry.
        returns: obsnum  (see jdpol2obsnum)
        Note: does not link up neighbors!
        """
        OBS = Observation(obsnum=obsnum, date=date, date_type=date_type, pol=0, status=status, length=length)
        s = self.Session()
        try:
            print("Adding Observation # ", obsnum)
            s.add(OBS)
            s.commit()
        except:
            print("Could not commit observation via add_observation.")
            exit(1)

        s.close()
        # *JON* Not sure I want to add files here yet...
        # self.add_file(obsnum, host, filename)  # todo test.
        # sys.stdout.flush()
        return obsnum


def sync_new_ops_from_ngas_to_still(db):
    # will maybe change this over to SQL alchemy later
    # Throwing it in now as straight SQL to get things working
    # so I can move onto other parts for the moment
    try:
        pgconn = psycopg2.connect("dbname='test' user='test' host='localhost' password='testme'")
    except:
        print("I am unable to connect to the database")
    print("Probably connected")
    #    cur = pgconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur = pgconn.cursor()
    cur.execute("""SELECT cast(substring(foreign_ngas_files.file_id, 1,10) AS character varying(30))
                   FROM
                      foreign_ngas_files
                   WHERE
                      cast(substring(foreign_ngas_files.file_id, 1,10) AS bigint) NOT IN (SELECT obsid FROM foreign_mwa_qc WHERE obsid IS NOT NULL)
                      AND cast(substring(foreign_ngas_files.file_id, 1,10) AS bigint) NOT IN (SELECT obsnum FROM observation WHERE obsnum IS NOT NULL)
                   """)

    rows = cur.fetchall()

    unique_obsids = np.unique(rows)
    print(unique_obsids)
    # db.add_observation(obsnum=obsnum, date=date, date_type=date_type, pol=0, legth=2 / 60. / 24)

    return 0


def read_config_file(SpawnerGlobal, config_file, config_name='testing'):
    if config_file is not None:
        config = configparser.ConfigParser()
        config_file = os.path.expanduser(config_file)
        if os.path.exists(config_file):
            #    logger.info('loading file ' + config_file)
            config.read(config_file)
            dbinfo = config[config_name]
            print(dbinfo)
    return 0


def main(SpawnerGlobal, args):
    # Instantiate a still db instance from our superclass
    SpawnerGlobal.db = MWADataBaseInterface(test=False, configfile='./cotter_still.cfg')
    if args.init is True:   # See if we were told to initiate the database
        SpawnerGlobal.db.createdb()
        exit(0)
    try:
        SpawnerGlobal.db.test_db()  # Testing the database to make sure we made a connection, its fun..
    except:
        print("We could not run a test on the database and are aborting.  Please check the DBI DB config")
        exit(1)
    sync_new_ops_from_ngas_to_still(SpawnerGlobal.db)
    myscheduler = MWAScheduler()
    MWAScheduler.init(myscheduler)
    # Will probably want to crank the sleep time up a bit in the future....
    myscheduler.start(dbi=SpawnerGlobal.db, sleeptime=2)
    return 0

# Spawner = SpawnerClass

parser = argparse.ArgumentParser(description='Process MWA data.')

SpawnerGlobal = SpawnerClass()

# Probably accept config file location and maybe config file section as command line arguments
# for the moment this is mostly just placeholder stuffs

parser = argparse.ArgumentParser(description='Process raw array data and cotterize the heck out of it')
parser.add_argument('--init', dest='init', action='store_true',
                    help='Initialize the database if this is the first time running this')
parser.add_argument('--config_file', dest='config_file', required=False,
                    help="Specify the complete path to the config file")

parser.set_defaults(config_file='./cotter_still.cfg')


args, unknown = parser.parse_known_args()
SpawnerGlobal.config_file = args.config_file
print(SpawnerGlobal.config_file)
main(SpawnerGlobal, args)

