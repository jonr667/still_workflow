import unittest
import time
import os
import sys
import numpy as n
import logging
# import ddr_compress.scheduler as sch
# from ddr_compress.dbi import File, Observation, Log
# from ddr_compress.dbi import DataBaseInterface, jdpol2obsnum
# from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
basedir = os.path.dirname(os.path.realpath(__file__)).replace("unit_tests", "")
sys.path.append(basedir + 'lib')
from dbi import File
from dbi import DataBaseInterface
from dbi import Observation
from dbi import Log
from dbi import jdpol2obsnum


# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('dbi_test')

# class MYSQLTest(unittest.TestCase):
#    def setUp(self):
#        self.dbi = DataBaseInterface()
#        print "NOTE: MYSQLTest will only work if the sql db setup referenced in ddr_compress.dbi is setup"
#        self.dbi.createdb()
#        self.jd = 2456892.20012000
#        self.pol = 'xx'
#        self.filename='/data0/zen.2456785.123456.uv'
#        self.host = 'pot0'
#        self.length = 10.16639/60./24
#    def test_db(self):
#        dbi.test_db()
#    def test_obsnum_increment(self):
#        obsnum = self.dbi.add_observation(self.jd,self.pol,self.filename,self.host,length=self.length)
#        print obsnum
#        self.assertEqual(obsnum,jdpol2obsnum(self.jd,self.pol,self.length))


class TestDBI(unittest.TestCase):
    def setUp(self):
        """
        create an in memory DB and open a connection to it
        """
        # filename = os.path.dirname(__file__) + '/../configs/test.cfg'
        self.dbi = DataBaseInterface(dbhost="localhost", dbport="5432", dbtype="postgresql", dbname="test", dbuser="test", dbpasswd="testme")
        #self.dbi = DataBaseInterface("", "", "", "", "", "", test=True)  # Jon: Change me
        self.session = self.dbi.Session()
        self.jd = 2456892.20012000
        self.pol = 'xx'
        self.filename = '/data0/zen.2456785.123456.uv'
        self.host = 'pot0'
        self.length = 10 / 60. / 24
        self.date_type = 'julian'
        self.obsnum = jdpol2obsnum(self.jd, self.pol, self.length)

    def add_an_obs(self):
        self.dbi.delete_test_obs()
        obsnum = self.dbi.add_observation(str(self.obsnum), self.jd, self.date_type, self.pol, self.filename, self.host, length=self.length, status="NEW", outputhost="UNITTEST")

        return obsnum

    def test_add_log(self):
        obsnum = self.add_an_obs()
        self.dbi.add_log(obsnum, 'UV', 'test123', 0)
        logtext = self.dbi.get_logs(obsnum)
        self.assertEqual(logtext.strip(), 'test123')
        LOG = self.session.query(Log).filter(Log.obsnum == obsnum).one()

    def test_update_log(self):
        obsnum = self.add_an_obs()
        self.dbi.add_log(obsnum, 'UV', 'test123', None)
        logtext = self.dbi.get_logs(obsnum)
        self.assertEqual(logtext.strip(), 'test123')
        LOG = self.session.query(Log).filter(Log.obsnum == obsnum).one()
        self.dbi.update_log(obsnum, exit_status=0)
        newsession = self.dbi.Session()
        LOG = newsession.query(Log).filter(Log.obsnum == obsnum).one()
        self.assertEqual(LOG.exit_status, 0)

# Config file is no longer handled here, its been moved to still.py and a test of it should be moved there as well
#    def test_configparsing(self):
#        logger.info('Note: did you remember to do "cp configs/test.cfg ~/.paperstill/db.cfg" ? ')
#        self.assertEqual(self.dbi.dbinfo['hostip'], 'memory')

    def test_obsnum_increment(self):
        dt = self.length
        jds = n.arange(0, 10) * dt + self.jd
        obsnums = []
        for jd in jds:
            obsnums.append(jdpol2obsnum(jd, self.pol, dt))
        delta = n.diff(obsnums)
        for d in delta:
            self.assertEqual(d, 1)
        obsnum = self.add_an_obs()

        # obsnum = self.dbi.add_observation(self.obsnum, self.jd, self.date_type, self.pol, self.filename, self.host, length=self.length, status='NEW')
        self.assertEqual(float(obsnum), jdpol2obsnum(self.jd, self.pol, self.length))

    def test_add_observation(self):
        """
        use the dbi to create a record.
        basically tests the same as test_Observation_and_file
        but with the dbi wrapper
        """
        obsnum = self.add_an_obs()

        # obsnum = self.dbi.add_observation(self.obsnum, self.jd, self.date_type, self.pol, self.filename, self.host, length=self.length, status='NEW')
        OBS = self.session.query(Observation).filter(Observation.obsnum == obsnum).one()
        self.assertEqual(float(OBS.date), self.jd)
        self.assertEqual(float(OBS.obsnum), jdpol2obsnum(self.jd, self.pol, self.length))

    def test_add_file(self):
        """
        todo update from code
        """
        # first add the observatio
        obsnum = self.add_an_obs()
#        obsnum = self.dbi.add_observation(self.obsnum, self.jd, self.date_type, self.pol, self.filename, self.host, '', '')
        # then add a file to it
        filenum = self.dbi.add_file(obsnum, self.host, self.filename + 'cRRE')
        # then grab the file record back
        FILE = self.session.query(File).filter(File.filenum == filenum).one()
        # and check that its right
        self.assertEqual(FILE.filename, self.filename + 'cRRE')

    def test_add_files(self):
        # first add the observation
        obsnum = self.add_an_obs()
        # obsnum = self.dbi.add_observation(self.obsnum, self.jd, self.date_type, self.pol, self.filename, self.host, self.length, status='NEW')

        # add two subsequent products
        files = ['/data0/zen.2456785.123456.uvc', '/data0/zen.2456785.323456.uvcR']
        for filename in files:
            filenum = self.dbi.add_file(obsnum, self.host, filename)
        # how I get all the files for a given obs
        OBS = self.session.query(Observation).filter(Observation.obsnum == obsnum).one()

        self.assertEqual(len(OBS.files), 3)  # check that we got three files

    def test_add_observations(self):
        # form up the observation list
        obslist = []
        jds = n.arange(0, 10) * self.length + 2456446.1234
        pols = ['xx', 'yy', 'xy', 'yx']
        for pol in pols:
            for jdi in xrange(len(jds)):
                obsnum = jdpol2obsnum(jdi, pol, self.length)
                self.dbi.delete_obs(str(obsnum))  # Delete obseration if it exists before adding a new one
                obslist.append({'obsnum': str(obsnum),
                                'outputhost': "UNITTEST",
                                'date': str(jds[jdi]),
                                'date_type': self.date_type,
                                'pol': pol,
                                'host': self.host,
                                'filename': self.filename,
                                'length': self.length,
                                'status': 'UV_POT'})
                if jdi != 0:
                    obslist[-1]['neighbor_low'] = jds[jdi - 1]
                if jdi < len(jds[:-1]):
                    obslist[-1]['neighbor_high'] = jds[jdi + 1]
        obsnums = self.dbi.add_observations(obslist, status='UV_POT')
        nobs = self.session.query(func.count(Observation.obsnum)).scalar()
        self.assertEqual(len(obslist) + 1, int(nobs))  # did we add observations?
        # did they get the proper neighbor assignments
        for obsnum in obsnums:
            OBS = self.session.query(Observation).filter(Observation.obsnum == obsnum).one()
            # find the original record we put into add_observations and check that the neighbors match
            for obs in obslist:
                if float(obs['date']) == float(OBS.date):
                    if 'neighbor_low' in obs:
                        self.assertEqual(float(OBS.low_neighbors[0].date), round(float(obs['neighbor_low']), 5))
                    if 'neighbor_high' in obs:
                        self.assertEqual(float(OBS.high_neighbors[0].date), round(float(obs['neighbor_high']), 5))
                    break

    def test_list_observations(self):
        # form up the observation list
        obslist = []
        jds = n.arange(0, 10) * self.length + 2456446.1234
        pols = ['xx', 'yy', 'xy', 'yx']
        for pol in pols:
            for jdi in xrange(len(jds)):
                obsnum = jdpol2obsnum(jdi, pol, self.length)
                self.dbi.delete_obs(str(obsnum))  # Delete obseration if it exists before adding a new one
                obslist.append({'obsnum': str(obsnum),
                                'outputhost': "UNITTEST",
                                'date': str(jds[jdi]),
                                'date_type': self.date_type,
                                'pol': pol,
                                'host': self.host,
                                'filename': self.filename,
                                'length': self.length,
                                'status': 'UV_POT'})
                if jdi != 0:
                    obslist[-1]['neighbor_low'] = jds[jdi - 1]
                if jdi < len(jds[:-1]):
                    obslist[-1]['neighbor_high'] = jds[jdi + 1]
        obsnums = self.dbi.add_observations(obslist)
        float_obsnums = []
        # Jon : I have to convert some stuffs back to float as obsnum is now type VarChar(100) to handle different types of obsid's
        for obs in obsnums:
            float_obsnums.append(float(obs))

        # tic = time.time()
        observations = self.dbi.list_observations()
        float_observations = []
        for obs in observations:
            float_observations.append(float(obs))

        self.assertEqual(n.sum(n.array(float_observations) - n.array(float_obsnums)), 0)

    def test_get_neighbors(self):
        """
        First set up a likely triplet of observations
        """
        # form up the observation list
        obslist = []
        jds = n.arange(0, 10) * self.length + 2456446.1234
        pols = ['xx', 'yy', 'xy', 'yx']
        for i, pol in enumerate(pols):
            for jdi in xrange(len(jds)):
                obsnum = jdpol2obsnum(jdi, pol, self.length)
                self.dbi.delete_obs(str(obsnum))  # Delete obseration if it exists before adding a new one
                obslist.append({'obsnum': str(obsnum),
                                'outputhost': "UNITTEST",
                                'date': str(jds[jdi]),
                                'date_type': self.date_type,
                                'pol': pol,
                                'host': self.host,
                                'filename': self.filename,
                                'length': self.length,
                                'status': 'UV_POT'})
                if jdi != 0:
                    obslist[-1]['neighbor_low'] = jds[jdi - 1]
                if jdi != (len(jds) - 1):
                    obslist[-1]['neighbor_high'] = jds[jdi + 1]
        obsnums = self.dbi.add_observations(obslist)
        obsnums.sort()
        i = 5  # I have ten time stamps. this guys should have plenty of neighbors
        mytestobsnum = obsnums[i]  # choose a middle obs
        tic = time.time()
        neighbors = self.dbi.get_neighbors(mytestobsnum)
        # print "time to execute get_neighbors",time.time()-tic,'s'
        self.assertEqual(len(neighbors), 2)

        self.assertEqual(neighbors[0], obsnums[i - 1])  # low
        self.assertEqual(neighbors[1], obsnums[i + 1])  # high

    def test_set_obs_status(self):
        """
        set the status with the dbi function then check it with
        under the hood stuff
        """
        # first create an observation in the first place
#        obsnum = self.dbi.add_observation(self.obsnum, self.jd, self.date_type, self.pol, self.filename, self.host)
        obsnum = self.add_an_obs()
        # then set the status to something else
        self.dbi.set_obs_status(obsnum, 'UV')
        # get the status back out
        OBS = self.session.query(Observation).filter(Observation.obsnum == obsnum).one()
        self.assertEqual(OBS.status, 'UV')

    def test_get_obs_status(self):
        """
        set the status with the dbi function (cause I tested it with more basic tests already)
        """
        # first create an observation in the first place
#        obsnum = self.dbi.add_observation(self.obsnum, self.jd, self.date_type, self.pol, self.filename, self.host)
        obsnum = self.add_an_obs()
        # then set the status to something else
        self.dbi.set_obs_status(obsnum, 'UV')
        # then get the status back
        tic = time.time()
        status = self.dbi.get_obs_status(obsnum)
        # print "time to execute get_obs_status",time.time()-tic,'s'
        self.assertEqual(status, 'UV')

    def test_time_transaction(self):
        """
        """
        # first create an observation in the first place
        obsnum = self.add_an_obs()
#        obsnum = self.dbi.add_observation(self.obsnum, self.jd, self.date_type, self.pol, self.filename, self.host)
        # then set the status to something else
        tic = time.time()
        self.dbi.set_obs_status(obsnum, 'UV')
        print "time to execute set_obs_status", time.time() - tic, 's'

    def test_get_input_file(self):
        """
        create an observation
        add the initial file
        get the pot host and path
        """
        # first add the observation
        obsnum = self.add_an_obs()
#        obsnum = self.dbi.add_observation(self.obsnum, self.jd, self.date_type, self.pol, self.filename, self.host)
        # then add a file to it
        host, path, filename = self.dbi.get_input_file(obsnum)
        self.assertEqual(host, self.host)
        self.assertEqual(path, os.path.dirname(self.filename))
        self.assertEqual(filename, os.path.basename(self.filename))

    def test_get_output_location(self):
        """
        create an observation
        add the initial file
        get the pot host and path
        """
        # first add the observation
        obsnum = self.add_an_obs()
#        obsnum = self.dbi.add_observation(self.obsnum, self.jd, self.date_type, self.pol, self.filename, self.host)
        host, path = self.dbi.get_output_location(obsnum)
        self.assertEqual(host, self.host)
        self.assertEqual(path, os.path.dirname(self.filename))

    def test_still_path(self):
        """
        """
        obsnum = self.add_an_obs()
#        obsnum = self.dbi.add_observation(self.obsnum, self.jd, self.date_type, self.pol, self.filename, self.host)
        self.dbi.set_obs_still_path(obsnum, '/data/')
        still_path = self.dbi.get_obs_still_path(obsnum)
        self.assertEqual(still_path, '/data/')

    def test_obs_pid(self):
        """
        """
        obsnum = self.add_an_obs()
#        obsnum = self.dbi.add_observation(self.obsnum, self.jd, self.date_type, self.pol, self.filename, self.host)
        self.dbi.set_obs_pid(obsnum, 9999)
        pid = self.dbi.get_obs_pid(obsnum)
        self.assertEqual(pid, 9999)

if __name__ == '__main__':
    unittest.main()
