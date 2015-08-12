#!/usr/bin/env python
# import sys
import argparse
import configparser
import os
import sys

#  Setup the lib path ./lib/  as a spot to check for python libraries
basedir = os.path.dirname(os.path.realpath(__file__))[:-3]
sys.path.append(basedir + 'lib')

import dbi
from scheduler import Scheduler
from task_server import TaskServer
from task_server import TaskClient
from scheduler import Action
from still_shared import setup_logger


class WorkFlow:
    #
    # Setup a class to handle the workflow elements and be able to pass the actions, prereqs, etc.. around willy nilly
    # This class should probably move over to the Scheduler
    #

    def __init__(self):

        self.workflow_actions = ''
        self.action_prereqs = {}
        self.action_args = {}
        self.drmaa_args = {}
        self.drmaa_queue_by_task = {}
        self.workflow_actions_endfile = ''
        self.prioritize_obs = 0
        self.neighbors = 0
        self.still_locked_after = ''
        self.drmma_args = []   # I think this will be useful but will want to be cautious of -o and -e being passed overriding our settings.
        self.default_drmma_queue = ''
        self.drmma_queue_by_task = []

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
        self.timeout = 3600
        self.sleep_time = 10
        self.block_size = 10
        self.lock_all_neighbors_to_same_still = 0
        self.transfers_per_still = 2
        self.log_path = ''
        self.path_to_do_scripts = ''
        self.logger = ''
        self.env_vars = ''
        self.ip_addr = ''
        self.cluster_scheduler = 0

    def preflight_check_scheduler(self):
        # Nothing to do here at the moment, just a place holder
        return

    def preflight_check_ts(self, wf):
        if self.check_path("Data_Dir", self.data_dir) != 0:  # Check data_dir path exists and is writeable
            sys.exit(1)

        workflow_list = list(wf.workflow_actions)[1:]  # Remove the first task as its a dummy task to set an obs status to for processing to start
        for task in workflow_list:
            if self.check_script_path(task) != 0:
                sys.exit(1)

    def check_path(self, dir_type, dir_path):

        if os.path.isdir(dir_path):
            if os.access(dir_path, os.W_OK):
                return 0
            else:
                print("ERROR: %s path : %s is not writeable by this program") % (dir_type, dir_path)
        else:
            print("ERROR: %s path : %s does not seem to exist.") % (dir_type, dir_path)
        return 1

    def check_script_path(self, task):
        logger = self.logger

        if task == "COMPLETE":
            return 0
        if self.path_to_do_scripts[-1:] == '/':
            self.path_to_do_scripts = self.path_to_do_scripts[:-1]
        full_path = self.path_to_do_scripts + '/do_' + task + '.sh'
        if os.path.isfile(full_path):
            if os.access(full_path, os.X_OK):
                return 0
            else:
                logger.critical("Script : %s is not set as executable, please run chmod +x %s" % (full_path, full_path))
        else:
            logger.critical("Count not find workflow script : %s" % full_path)

        return 1


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
        # print("Ext_command_hook!")
        return


class StillDataBaseInterface(dbi.DataBaseInterface):
    #
    # Overload DataBaseInterface class from still to be able to modify some functions
    #
    def add_observation2(self, obsnum, date, date_type, pol, filename, host, length=2 / 60. / 24, status=''):
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
            s.close()
            exit(1)

        # Jon: Not sure I want to add files here yet...
        # self.add_file(obsnum, host, filename)  # todo test.
        # sys.stdout.flush()
        s.close()
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
    basedir = os.path.dirname(os.path.realpath(__file__))[:-3]

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
        sg.port = int(get_config_entry(config, 'Still', 'port', reqd=False, remove_spaces=True))
        sg.ip_addr = get_config_entry(config, 'Still', 'ip_addr', reqd=False, remove_spaces=True)
        sg.data_dir = get_config_entry(config, 'Still', 'data_dir', reqd=False, remove_spaces=False)
        sg.path_to_do_scripts = get_config_entry(config, 'Still', 'path_to_do_scripts', reqd=False, remove_spaces=False, default_val=basedir + 'scripts/')
        sg.timeout = int(get_config_entry(config, 'Still', 'timeout', reqd=False, remove_spaces=True))
        sg.block_size = int(get_config_entry(config, 'Still', 'block_size', reqd=False, remove_spaces=True))
        sg.actions_per_still = int(get_config_entry(config, 'Still', 'actions_per_still', reqd=False, remove_spaces=True, default_val=8))
        sg.sleep_time = int(get_config_entry(config, 'Still', 'sleep_time', reqd=False, remove_spaces=True))
        sg.cluster_scheduler = int(get_config_entry(config, 'Still', 'cluster_scheduler', reqd=False, remove_spaces=True))
        sg.log_path = get_config_entry(config, 'Still', 'log_path', reqd=False, remove_spaces=False, default_val=basedir + 'log/')

        if "ScriptEnvironmentVars" in config_sections:  # Read in allow the env vars for the do_ scripts
            sg.env_vars = dict(config.items('ScriptEnvironmentVars'))  # Put the vars into a dict that we will later pickle

        # Read in all the workflow information
        wf.workflow_actions = tuple(get_config_entry(config, 'WorkFlow', 'actions', reqd=True, remove_spaces=True).split(","))
        wf.workflow_actions_endfile = tuple(get_config_entry(config, 'WorkFlow', 'actions_endfile', reqd=False, remove_spaces=True).split(","))
        wf.prioritize_obs = int(get_config_entry(config, 'WorkFlow', 'prioritize_obs', reqd=False, remove_spaces=True, default_val=0))
        wf.still_locked_after = get_config_entry(config, 'WorkFlow', 'still_locked_after', reqd=False, remove_spaces=True)  # Do I still use this?
        wf.default_drmaa_queue = get_config_entry(config, 'WorkFlow', 'default_drmaa_queue', reqd=False, remove_spaces=True)
        wf.neighbors = int(get_config_entry(config, 'WorkFlow', 'neighbors', reqd=False, remove_spaces=False, default_val=0))
        wf.lock_all_neighbors_to_same_still = int(get_config_entry(config, 'WorkFlow', 'lock_all_neighbors_to_same_still', reqd=False, remove_spaces=False, default_val=0))

        for action in wf.workflow_actions or wf.workflow_actions_endfile:      # Collect all the prereqs and arg strings for any action of the workflow and throw them into a dict of keys and lists
            wf.action_args[action] = '[\'%s:%s/%s\' % (pot, path, basename)]'  # Put in a default host:path/filename for each actions arguements that get passed to do_ scripts

            if action in config_sections:

                wf.action_prereqs[action] = get_config_entry(config, action, 'prereqs', reqd=False, remove_spaces=True).split(",")
                wf.action_args[action] = get_config_entry(config, action, 'args', reqd=False, remove_spaces=False)
                wf.drmaa_args[action] = get_config_entry(config, action, 'drmaa_args', reqd=False, remove_spaces=False)
                wf.drmaa_queue_by_task[action] = get_config_entry(config, action, 'drmaa_queue', reqd=False, remove_spaces=False)
    else:
        print("Config file does not appear to exist : %s") % sg.config_file
        sys.exit(1)

    if sg.check_path("Logging", sg.log_path) != 0:  # Check logging path exists and is writeable
        sys.exit(1)

    return 0


def get_dbi_from_config(config_file, Spawner=None, still_startup=0):

    if still_startup != 1:
        sg = SpawnerClass()
        wf = WorkFlow()
        sg.config_file = config_file
        process_client_config_file(sg, wf)
    else:
        sg = Spawner
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
        if sg.dbi.test_db() is False:  # Testing the database to make sure we made a connection, its fun..
            print("Incorrect number of tables read from the database")
            sys.exit(1)
    except:
        print("We could not run a test on the database and are aborting.  Please check the DB config settings")
        sys.exit(1)

    print("My Log Path : %s") % sg.log_path
    sg.logger = setup_logger("Scheduler", "DEBUG", sg.log_path)
    task_clients = [TaskClient(sg.dbi, s, wf, sg.port, sg) for s in sg.hosts]

    # Screw it going to just break a bunch of the unittest stuff and simplify the calling of the scheduler to take SpawnerClass
    myscheduler = StillScheduler(task_clients, wf, sg)  # Init scheduler daemon
    myscheduler.start(dbi=sg.dbi, ActionClass=Action)

    return 0


def start_server(sg, wf, args):
    #
    # Instantiate a still server instance
    #
    if args.data_dir:
        mydata_dir = args.data_dir
    else:
        mydata_dir = sg.data_dir
    if args.port:
        my_port = int(args.port)
    else:
        my_port = sg.port
    sg.logger = setup_logger("TS", "DEBUG", sg.log_path)
    sg.preflight_check_ts(wf)

    task_server = TaskServer(sg.dbi, sg, data_dir=mydata_dir, port=my_port, path_to_do_scripts=sg.path_to_do_scripts)
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
                        help="Specify the complete path to the config file, by default we'll use etc/still.cfg")
    parser.add_argument('-d', dest='data_dir', required=False,
                        help="For use with --server only, specifies a data_dir for still server, *overrides value in config file*")
    parser.add_argument('-p', dest='port', required=False,
                        help="For use with --server only, specifies a port for still server, *overrides value in config file*")

    parser.set_defaults(config_file="%setc/still.cfg" % basedir)

    args, unknown = parser.parse_known_args()
    sg.config_file = args.config_file

    process_client_config_file(sg, workflow_objects)

    # Assign command line arquments over conf file arguments here
    if args.data_dir:
        sg.data_dir = args.data_dir
    if args.port:
        sg.port = args.port

    # Create database interface with SQL Alchemy
    get_dbi_from_config(sg.config_file, Spawner=sg, still_startup=1)

    if args.client is True:
        start_client(sg, workflow_objects, args)
    elif args.server is True:
        start_server(sg, workflow_objects, args)
    else:
        print("You must specify to start this as a client or server (--client or --server)")


if __name__ == "__main__":
    main()
