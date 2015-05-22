#!/usr/bin/env python
# import sys
import argparse
import configparser

from still.dbi import DataBaseInterface, Observation
from still.scheduler import Scheduler


class SpawnerClass:
    CommandLineArgs = ''

    def __init__(self):
        self.data = []
        self.config_file = ''
        self.config_name = ''


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
    obsnum = 0
    date = 0
    db.add_observation(obsnum=obsnum, date=date, date_type=date_type, pol=0, legth=2/60./24)
    return 0


def read_config_file(SpawnerGlobal, config_file, config_name='testing'):
    if configfile is not None:
        config = configparser.ConfigParser()
        configfile = os.path.expanduser(configfile)
        if os.path.exists(configfile):
            logger.info('loading file '+configfile)
            config.read(configfile)
            self.dbinfo = config[config_name]
    return 0


def main(SpawnerGlobal, args):
    SpawnerGlobal.db = MWADataBaseInterface(test=False, configfile='/Users/wintermute/Desktop/Dropbox/Research/mwa_cotter_spawner/cotter_still.cfg')
    if args.init is True:
        SpawnerGlobal.db.createdb()
        exit(0)
    SpawnerGlobal.db.test_db()
    myscheduler = Scheduler()
    # Will probably want to crank the sleep time up a bit in the future....
    Scheduler.start(myscheduler, dbi=SpawnerGlobal.db, sleeptime=10)
    return 0

# Spawner = SpawnerClass

parser = argparse.ArgumentParser(description='Process MWA data.')

SpawnerGlobal = SpawnerClass()

# Probably accept config file location and maybe config file section as command line arguments

parser = argparse.ArgumentParser(description='Process raw array data and cotterize the heck out of it')
parser.add_argument('--init', dest='init', action='store_true',
                    help='Initialize the database if this is the first time running this')
parser.add_argument('--config_file', dest='config_file', required=True,
                    help="Specify the complete path to the config file")
parser.add_argument('--config_name', dest='config_name', default='test',
                    help="Specify header name to use in the config file (examples: test, production)")

args = parser.parse_args()
SpawnerGlobal.config_file = args.config_file
print(SpawnerGlobal.config_file)
main(SpawnerGlobal, args)

