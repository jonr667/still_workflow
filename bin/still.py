#!/usr/bin/env python
# import sys
import argparse
import configparser
import os
import sys

#  Setup the lib path ./lib/  as a spot to check for python libraries
basedir = os.path.dirname(os.path.realpath(__file__))[:-3]
sys.path.append(basedir + 'lib')

import add_observations

# from dbi import DataBaseInterface
# from dbi import Observation

import dbi
from scheduler import Scheduler
from task_server import TaskServer
from task_server import TaskClient
from scheduler import Action


class WorkFlow:
    #
    # Setup a class to handle the workflow elements and be able to pass the actions, prereqs, etc.. around willy nilly
    # This class should probably move over to the Scheduler
    #

    def __init__(self):
        self.name = ''
        self.workflow_actions = ''
        self.action_prereqs = {}
        self.action_args = {}
        self.workflow_actions_endfile = ''
        self.prioritize_obs = 0
        self.neighbors = 0
        self.still_locked_after = ''
#        self.start_trigger_status_state = ''


class SpawnerClass:
    #
    # Just create a class so I have a place to store some long lasting variables
    #
    def __init__(self):
        self.data = []
        self.dbi = ''
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


class StillDataBaseInterface(dbi.DataBaseInterface):
    #
    # Overload DataBaseInterface class from still to be able to modify some functions
    #
    def add_observation(self, obsnum, date, date_type, pol, filename, host, length=2 / 60. / 24, status=''):
        #
        # Overloading the existing class function to get MWA data in, though this might be generic enough to backport
        #
        OBS = dbi.Observation(obsnum=obsnum, date=date, date_type=date_type, pol=0, status=status, length=length)
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


def get_config_entry(config, heading, item, reqd=False, remove_spaces=True, default_val=''):
    if config.has_option(heading, item):
        if remove_spaces:
            config_item = config.get(heading, item).replace(" ", "")
        else:
            config_item = config.get(heading, item)
    elif reqd:
        print("The required config file setting \'%s\' under [%s] is missing") % (item, heading)
        sys.exit(1)
    else:
        config_item = default_val
    return config_item


def process_client_config_file(sg, wf):
    #
    # We will read the entire cnofig file here and push it into a class
    #
    config = configparser.RawConfigParser()
    if os.path.exists(sg.config_file):
        config.read(sg.config_file)

        config_sections = config.sections()

        # Read in all the database information
        sg.dbhost = get_config_entry(config, 'dbinfo', 'dbhost', reqd=True, remove_spaces=True)
        sg.dbport = get_config_entry(config, 'dbinfo', 'dbport', reqd=True, remove_spaces=True)
        sg.dbtype = get_config_entry(config, 'dbinfo', 'dbtype', reqd=True, remove_spaces=True)
        sg.dbuser = get_config_entry(config, 'dbinfo', 'dbuser', reqd=True, remove_spaces=True)
        sg.dbpasswd = get_config_entry(config, 'dbinfo', 'dbpasswd', reqd=True, remove_spaces=True)
        sg.dbname = get_config_entry(config, 'dbinfo', 'dbname', reqd=True, remove_spaces=True)

        # Read in all the STILL information
        sg.hosts = get_config_entry(config, 'Still', 'hosts', reqd=True, remove_spaces=True).split(",")
        sg.port = int(get_config_entry(config, 'Still', 'port', reqd=True, remove_spaces=True))
        sg.data_dir = get_config_entry(config, 'Still', 'data_dir', reqd=True, remove_spaces=False)
        sg.timeout = int(get_config_entry(config, 'Still', 'timeout', reqd=False, remove_spaces=True))
        sg.block_size = int(get_config_entry(config, 'Still', 'block_size', reqd=False, remove_spaces=True))
        sg.actions_per_still = int(get_config_entry(config, 'Still', 'actions_per_still', reqd=False, remove_spaces=True, default_val=8))

        # Read in all the workflow information
        wf.workflow_actions = tuple(get_config_entry(config, 'WorkFlow', 'actions', reqd=True, remove_spaces=True).split(","))
        wf.workflow_actions_endfile = tuple(get_config_entry(config, 'WorkFlow', 'actions_endfile', reqd=False, remove_spaces=True).split(","))
        wf.prioritize_obs = int(get_config_entry(config, 'WorkFlow', 'prioritize_obs', reqd=False, remove_spaces=True, default_val=0))
        wf.still_locked_after = get_config_entry(config, 'WorkFlow', 'still_locked_after', reqd=False, remove_spaces=True)
        wf.name = get_config_entry(config, 'WorkFlow', 'name', reqd=True, remove_spaces=True)
        wf.neighbors = int(get_config_entry(config, 'WorkFlow', 'neighbors', reqd=False, remove_spaces=False, default_val=0))

        for action in wf.workflow_actions:  # Collect all the prereqs and arg strings for any action of the workflow and throw them into a dict of keys and lists
            wf.action_args[action] = '[\'%s:%s/%s\' % (pot, path, basename)]'  # Put in a default host:path/filename for each actions arguements that get passed to do_ scripts
            if action in config_sections:
                wf.action_prereqs[action] = get_config_entry(config, action, 'prereqs', reqd=False, remove_spaces=True).split(",")
                wf.action_args[action] = get_config_entry(config, action, 'args', reqd=False, remove_spaces=True)
    else:
        print("Config file does not appear to exist : %s") % sg.config_file
        sys.exit(1)
    return 0


def get_dbi_from_config(config_file):
    sg = SpawnerClass()
    wf = WorkFlow()
    sg.config_file = config_file
    process_client_config_file(sg, wf)

    # Create database interface with SQL Alchemy
    sg.dbi = StillDataBaseInterface(sg.dbhost, sg.dbport, sg.dbtype, sg.dbname, sg.dbuser, sg.dbpasswd, test=False)
    return sg.dbi


def start_client(sg, wf, args):
    #
    # Instantiate a still client instance
    #

    if args.init is True:   # See if we were told to initiate the database
        sg.dbi.createdb()
        print("Database has been initialized")
        sys.exit(0)
    try:
        sg.dbi.test_db()  # Testing the database to make sure we made a connection, its fun..
    except:
        print("We could not run a test on the database and are aborting.  Please check the DB config settings")
        sys.exit(1)

    task_clients = [TaskClient(sg.dbi, s, wf, port=sg.port) for s in sg.hosts]
    myscheduler = StillScheduler(task_clients, wf, dbi=sg.dbi, actions_per_still=sg.actions_per_still, blocksize=sg.block_size, nstills=len(sg.hosts), timeout=sg.sleep_time)  # Init scheduler daemon

    myscheduler.start(dbi=sg.dbi, ActionClass=Action)

    return 0


def start_server(sg, wf):
    #
    # Instantiate a still server instance
    #

    task_server = TaskServer(sg.dbi, workflow_name=wf.name, data_dir=sg.data_dir, port=sg.port)
    task_server.start()
    return


def main():
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

    sg.dbi = get_dbi_from_config(sg.config_file)
    if args.client is True:
        start_client(sg, workflow_objects, args)
    elif args.server is True:
        start_server(sg, workflow_objects)
    else:
        print("You must specify to start this as a client or server (--client or --server)")

    pass

if __name__ == "__main__":
    main()
