#!/usr/bin/env python
# import sys
import argparse
import configparser
import os
import sys
import psycopg2
import psycopg2.extras
import numpy as np
from still.dbi import DataBaseInterface, Observation, logger
from still.scheduler import Scheduler
# from still.task_server import TaskClient
import still.task_server

class WorkFlow:
    #
    # Setup a class to handle the workflow elements and be able to pass the actions, prereqs, etc.. around willy nilly
    # This class should probably move over to the Scheduler
    #

    def __init__(self):
        self.workflow_actions = ''
        self.action_prereqs = {}
        self.action_args = {}
        self.workflow_actions_endfile = ''
        self.prioritize_obs = 0
        self.neighbors = 0


class SpawnerClass:
    #
    # Just create a class so I have a place to store some long lasting variables
    #
    def __init__(self):
        self.data = []
        self.config_file = ''
        self.config_name = ''


class MWAScheduler(Scheduler):
    #
    # Overload Scheduler class from still to be able to modify some functions
    #

    def ext_command_hook(self):
        #
        # Overloading existing class function to customize what happens after each run
        # Things like sleeping when nothing to do or loading more obsid's in from ngas
        # and stuff such as that should go here
        #
        print("Ext_command_hook!")
        return


class MWADataBaseInterface(DataBaseInterface):
    #
    # Overload DataBaseInterface class from still to be able to modify some functions
    #
    def add_observation(self, obsnum, date, date_type, pol, filename, host, length=2 / 60. / 24, status='UV_POT'):
        #
        # Overloading the existing class function to get MWA data in, though this might be generic enough to backport
        #
        OBS = Observation(obsnum=obsnum, date=date, date_type=date_type, pol=0, status=status, length=length)
        print(OBS.obsnum)
        s = self.Session()
        try:
            print("Adding Observation # ", obsnum)
            s.add(OBS)
            s.commit()
            s.close()
        except:
            print("Could not commit observation via add_observation.")
            exit(1)

        # Jon: Not sure I want to add files here yet...
        # self.add_file(obsnum, host, filename)  # todo test.
        # sys.stdout.flush()
        return obsnum


def sync_new_ops_from_ngas_to_still(sg):
    # will maybe change this over to SQL alchemy later
    # Throwing it in now as straight SQL to get things working
    # so I can move onto other parts for the moment
    try:
        pgconn = psycopg2.connect("dbname='test' user='test' host='localhost' password='testme'")
    except:
        print("I am unable to connect to the database")
        exit(1)
    print("Probably connected")
    cur_dict = pgconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)  # Get a sql cursor that supports returning data as a dict
    cur = pgconn.cursor()  # Normal cursor, non-dict
    # Get all the new OBS id's avaiable from the NGAS postgresql database and check against files that are currently in the still
    # as well as files that are in the mwa_qc db and have thus already been processed
    cur.execute("""SELECT cast(substring(foreign_ngas_files.file_id, 1,10) AS bigint)
                   FROM
                      foreign_ngas_files
                   WHERE
                      cast(substring(foreign_ngas_files.file_id, 1,10) AS varchar(10)) NOT IN (SELECT cast(obsid AS varchar(10)) FROM foreign_mwa_qc WHERE obsid IS NOT NULL)
                      AND cast(substring(foreign_ngas_files.file_id, 1,10) AS varchar(10)) NOT IN (SELECT cast(obsnum AS varchar(10)) FROM observation WHERE obsnum IS NOT NULL)
                   LIMIT 50""")

    rows = cur.fetchall()
    unique_obsids = np.unique(rows)  # Lets just make sure we trim out all the extra obs id's we get from each having multiple files associated with it

    for obsid in unique_obsids:  # We now need to add all the files that are associated with each obs id to the db as well as the primary entry
        print("Adding obsid: %s") % obsid
        SpawnerGlobal.db.add_observation(obsnum=obsid, date=obsid, date_type='GPS', pol=0, filename='none', host='none', length=0)  # Add primary entry for obsnum
        # Get all the files associated with each unique obsid, there is some redundency here but went for readability of logic over
        # most effecient
        cur_dict.execute("""SELECT file_name, host_id, file_id, mount_point
                            FROM foreign_ngas_files
                              INNER JOIN foreign_ngas_disks
                              USING (disk_id)
                            WHERE cast(substring(foreign_ngas_files.file_id, 1,10) AS varchar(10)) = '%(myobsid)s' LIMIT 100""", {'myobsid': obsid})
        rows = cur_dict.fetchall()
        for file_info in rows:  # build the full path to each file based on ngas info and push each one into the db as a file of a unique obsid
            print(file_info)
            path = file_info['mount_point'] + '/' + file_info['file_name']
            print(path)
            SpawnerGlobal.db.add_file(obsid, file_info['host_id'][:-5], path)
        exit(0)
    return 0


def process_config_file(sg, wf):
    #
    # We will read the entire cnofig file here and push it into a class
    #
    config = configparser.RawConfigParser()
    #        config_file = os.path.expanduser(config_file)
    if os.path.exists(sg.config_file):
        #    logger.info('loading file ' + config_file)
        config.read(sg.config_file)

        config_sections = config.sections()
        dbinfo = config['dbinfo']
        workflow = config['WorkFlow']  # Get workflow actions
        workflow_actions = workflow['actions'].replace(" ", "").split(",")
        wf.workflow_actions = tuple(workflow_actions)  # Get all the workflow actions and put them in a nice immutible tuple
        workflow_actions_endfile = workflow['actions_endfile'].replace(" ", "").split(",")
        wf.workflow_actions_endfile = tuple(workflow_actions_endfile)

        if config.has_option('WorkFlow', 'prioritize_obs'):
            wf.prioritize_obs = int(config.get('WorkFlow', 'prioritize_obs'))
        if config.has_option('WorkFlow', 'neighbors'):
            wf.neighbors = int(config.get('WorkFlow', 'neighbors'))

        for action in wf.workflow_actions:  # Collect all the prereqs and arg strings for any action of the workflow and throw them into a dict of keys and lists
            wf.action_args[action] = '\'%s:%s/%s\' % (pot, path, basename)'  # Put in a default host:path/filename for each actions arguements that get passed to do_ scripts
            if action in config_sections:
                if config.has_option(action, "prereqs"):
                    wf.action_prereqs[action] = config.get(action, "prereqs").replace(" ", "").split(",")
                if config.has_option(action, "args"):
                    wf.action_args[action] = config.get(action, "args")



    else:
        print("Config file does not appear to exist : %s") % sg.config_file
        sys.exit(1)
    return 0


def main(sg, wf, args):
    #
    # Instantiate a still db instance from our superclass
    #
    process_config_file(SpawnerGlobal, wf)
    sg.db = MWADataBaseInterface(test=False, configfile=sg.config_file)

    if args.init is True:   # See if we were told to initiate the database
        sg.db.createdb()
        sys.exit(0)
    try:
        sg.db.test_db()  # Testing the database to make sure we made a connection, its fun..
    except:
        print("We could not run a test on the database and are aborting.  Please check the DBI DB config")
        sys.exit(1)

#    sync_new_ops_from_ngas_to_still(sg)  # Lets get started and get a batch of new observations and push them into the db

#    MWAScheduler.init(myscheduler)
    # Will probably want to crank the sleep time up a bit in the future....
#    myscheduler.start(dbi=sg.db, sleeptime=2)  # Start the scheduler daemon
    
    # Throwing this in temporarily for testing, will put in config file as soon as I know its working.
    STILLS = ['still4', 'still5']
    PORTS = [14204, 14204]
    ACTIONS_PER_STILL = 8  # how many actions that run in parallel on a still
    BLOCK_SIZE = 10  # number of files that are sent together to a still
    TIMEOUT = 600  # seconds; how long a task is allowed to be running before it is assumed to have failed
    SLEEPTIME = 1.  # seconds; throttle on how often the scheduler polls the database

    task_clients = [still.task_server.TaskClient(sg.db, s, port=p) for (s, p) in zip(STILLS, PORTS)]
    # scheduler = ddr.task_server.Scheduler(task_clients, actions_per_still=ACTIONS_PER_STILL,blocksize=BLOCK_SIZE,nstills=len(STILLS))
#    def __init__(self, workflow, nstills=4, actions_per_still=8, transfers_per_still=2, blocksize=10):
    myscheduler = MWAScheduler(task_clients, wf, actions_per_still=ACTIONS_PER_STILL, blocksize=BLOCK_SIZE, nstills=len(STILLS))  # Init scheduler daemon
    myscheduler.start(dbi=sg.db, ActionClass=still.task_server.Action, action_args=(task_clients, TIMEOUT), sleeptime=SLEEPTIME)
    return 0


#
# Mostly placeholder stuff for reading in command line aruments
#

parser = argparse.ArgumentParser(description='Process MWA data.')

SpawnerGlobal = SpawnerClass()

workflow_objects = WorkFlow()

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

main(SpawnerGlobal, workflow_objects, args)
