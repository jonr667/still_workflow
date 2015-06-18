#!/usr/bin/env python
# import sys
import argparse
import configparser
import os
import sys

#  Setup the lib path ./lib/  as a spot to check for python libraries
basedir = os.path.dirname(os.path.realpath(__file__))[:-3]
sys.path.append(basedir + 'lib')
print basedir

import add_observations

from dbi import DataBaseInterface
from dbi import Observation
from dbi import logger
from scheduler import Scheduler
from task_server import TaskServer
from task_server import TaskClient
from task_server import Action


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
        self.dbname = ''
        self.dbpasswd = ''
        self.dbtype = ''
        self.dbhost = ''
        self.dbport = 0
        self.dbuser = ''
        self.config_file = ''
        self.config_name = ''
        self.data_dir = ''
        self.hosts = []
        self.port = 14204
        self.actions_per_still = 8
        self.timeout = 60
        self.sleep_time = 10
        self.block_size = 10


class StillScheduler(Scheduler):
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


class StillDataBaseInterface(DataBaseInterface):
    #
    # Overload DataBaseInterface class from still to be able to modify some functions
    #
    def add_observation(self, obsnum, date, date_type, pol, filename, host, length=2 / 60. / 24, status=''):
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


def process_client_config_file(sg, wf):
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
        if config.has_option('workflow', 'actions_endfile'):
            workflow_actions_endfile = workflow['actions_endfile'].replace(" ", "").split(",")
            wf.workflow_actions_endfile = tuple(workflow_actions_endfile)

        if config.has_option('dbinfo', 'dbhost'):
            sg.dbhost = config.get('dbinfo', 'dbhost').replace(" ", "")
        if config.has_option('dbinfo', 'dbport'):
            sg.dbport = int(config.get('dbinfo', 'dbport'))
        if config.has_option('dbinfo', 'dbtype'):
            sg.dbtype = config.get('dbinfo', 'dbtype').replace(" ", "")
        if config.has_option('dbinfo', 'dbuser'):
            sg.dbuser = config.get('dbinfo', 'dbuser').replace(" ", "")
        if config.has_option('dbinfo', 'dbpasswd'):
            sg.dbpasswd = config.get('dbinfo', 'dbpasswd').replace(" ", "")
        if config.has_option('dbinfo', 'dbname'):
            sg.dbname = config.get('dbinfo', 'dbname').replace(" ", "")

        if config.has_option('Still', 'hosts'):
            sg.hosts = config.get('Still', 'hosts').replace(" ", "").split(",")
        if config.has_option('Still', 'port'):
            sg.port = int(config.get('Still', 'port'))
        if config.has_option('Still', 'data_dir'):
            sg.data_dir = config.get('Still', 'data_dir')
        if config.has_option('Still', 'timeout'):
            sg.timeout = int(config.get('Still', 'timeout'))
        if config.has_option('Still', 'block_size'):
            sg.block_size = int(config.get('Still', 'block_size'))

        if config.has_option('WorkFlow', 'prioritize_obs'):
            wf.prioritize_obs = int(config.get('WorkFlow', 'prioritize_obs'))
        if config.has_option('WorkFlow', 'neighbors'):
            wf.neighbors = int(config.get('WorkFlow', 'neighbors'))

        for action in wf.workflow_actions:  # Collect all the prereqs and arg strings for any action of the workflow and throw them into a dict of keys and lists
            wf.action_args[action] = '[\'%s:%s/%s\' % (pot, path, basename)]'  # Put in a default host:path/filename for each actions arguements that get passed to do_ scripts
            if action in config_sections:
                if config.has_option(action, "prereqs"):
                    wf.action_prereqs[action] = config.get(action, "prereqs").replace(" ", "").split(",")
                if config.has_option(action, "args"):
                    wf.action_args[action] = config.get(action, "args")

    else:
        print("Config file does not appear to exist : %s") % sg.config_file
        sys.exit(1)
    return 0


def main_client(sg, wf, args):
    #
    # Instantiate a still client instance
    #

    if args.init is True:   # See if we were told to initiate the database
        sg.db.createdb()
        print("Database has been initialized")
        sys.exit(0)
    try:
        sg.db.test_db()  # Testing the database to make sure we made a connection, its fun..
    except:
        print("We could not run a test on the database and are aborting.  Please check the DBI DB config")
        sys.exit(1)
 #   add_observations.ingest_addtional_opsids(sg)
#    obsid = 1062453568
#    add_observations.get_all_nags_files_for_obsid(sg, obsid)
    #    sync_new_ops_from_ngas_to_still(sg)  # Lets get started and get a batch of new observations and push them into the db
 #   sys.exit(0)
    STILLS = sg.hosts

    ACTIONS_PER_STILL = sg.actions_per_still  # how many actions that run in parallel on a still
    BLOCK_SIZE = sg.block_size  # number of files that are sent together to a still
    TIMEOUT = sg.timeout  # seconds; how long a task is allowed to be running before it is assumed to have failed
    SLEEPTIME = sg.sleep_time  # seconds; throttle on how often the scheduler polls the database

    task_clients = [TaskClient(sg.db, s, wf, port=sg.port) for s in STILLS]
    myscheduler = StillScheduler(task_clients, wf, actions_per_still=ACTIONS_PER_STILL, blocksize=BLOCK_SIZE, nstills=len(STILLS))  # Init scheduler daemon

    myscheduler.start(dbi=sg.db, ActionClass=Action, action_args=(task_clients, TIMEOUT), sleeptime=SLEEPTIME)

    return 0


def main_server(sg):
    #
    # Instantiate a still server instance
    #

    task_server = TaskServer(sg.db, data_dir=sg.data_dir, port=14204)
    task_server.start()
    return


sg = SpawnerClass()
workflow_objects = WorkFlow()

# Probably accept config file location and maybe config file section as command line arguments
# for the moment this is mostly just placeholder stuffs

parser = argparse.ArgumentParser(description='STILL workflow management software')
parser.add_argument('--init', dest='init', action='store_true',
                    help='Initialize the database if this is the first time running this')
parser.add_argument('--server', dest='server', action='store_true',
                    help='Start a Still Task Server')
parser.add_argument('--client', dest='client', action='store_true',
                    help='Start a Still Task Client')
parser.add_argument('--config_file', dest='config_file', required=False,
                    help="Specify the complete path to the config file")


parser.set_defaults(config_file="%setc/still.cfg" % basedir)


args, unknown = parser.parse_known_args()
sg.config_file = args.config_file
process_client_config_file(sg, workflow_objects)

# Create database interface with SQL Alchemy
sg.db = StillDataBaseInterface(sg.dbhost, sg.dbport, sg.dbtype, sg.dbname, sg.dbuser, sg.dbpasswd, test=False)

if args.client is True:
    main_client(sg, workflow_objects, args)
elif args.server is True:
    main_server(sg)
else:
    print("You must specify to start this as a client or server (--client or --server)")
