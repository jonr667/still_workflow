import unittest
import threading
import time
import logging
import os
import sys

basedir = os.path.dirname(os.path.realpath(__file__)).replace("unit_tests", "")
sys.path.append(basedir + 'lib')
sys.path.append(basedir + 'bin')

from dbi import jdpol2obsnum
from dbi import DataBaseInterface
from still import process_client_config_file, WorkFlow, SpawnerClass
import scheduler as sch
from task_server import TaskClient
import numpy as n
# from sqlalchemy.orm.exc import NoResultFound

logger = logging.basicConfig(level=logging.INFO)
TEST_PORT = 14204
FILE_PROCESSING_STAGES = ['NEW',
                          'UV_POT',
                          'UV',
                          'UVC',
                          'CLEAN_UV',
                          'UVCR',
                          'CLEAN_UVC',
                          'ACQUIRE_NEIGHBORS',
                          'UVCRE',
                          'NPZ',
                          'UVCRR',
                          'NPZ_POT',
                          'CLEAN_UVCRE',
                          'UVCRRE',
                          'CLEAN_UVCRR',
                          'CLEAN_NPZ',
                          'CLEAN_NEIGHBORS',
                          'UVCRRE_POT',
                          'CLEAN_UVCRRE',
                          'CLEAN_UVCR',
                          'COMPLETE']

FILE_PROCESSING_LINKS = {'ACQUIRE_NEIGHBORS': 'UVCRE',
                         'CLEAN_NEIGHBORS': 'UVCRRE_POT',
                         'CLEAN_NPZ': 'CLEAN_NEIGHBORS',
                         'CLEAN_UV': 'UVCR',
                         'CLEAN_UVC': 'ACQUIRE_NEIGHBORS',
                         'CLEAN_UVCR': 'COMPLETE',
                         'CLEAN_UVCRE': 'UVCRRE',
                         'CLEAN_UVCRR': 'CLEAN_NPZ',
                         'CLEAN_UVCRRE': 'CLEAN_UVCR',
                         'COMPLETE': None,
                         'NEW': 'UV_POT',
                         'NPZ': 'UVCRR',
                         'NPZ_POT': 'CLEAN_UVCRE',
                         'UV': 'UVC',
                         'UVC': 'CLEAN_UV',
                         'UVCR': 'CLEAN_UVC',
                         'UVCRE': 'NPZ',
                         'UVCRR': 'NPZ_POT',
                         'UVCRRE': 'CLEAN_UVCRR',
                         'UVCRRE_POT': 'CLEAN_UVCRRE',
                         'UV_POT': 'UV'}


class NullAction(sch.Action):

    def _command(self):
        return


class PopulatedDataBaseInterface(DataBaseInterface):

    def __init__(self, nobs, npols, test=True):
        DataBaseInterface.__init__(self, "", "", "", "", "", "", test=test)
        # self.dbi = DataBaseInterface("", "", "", "", "", "", test=True)  # Jon: Change me
        self.length = 10 / 60. / 24
        self.host = 'localhost'
        self.defaultstatus = 'UV_POT'
        self.date_type = 'julian'
        self.Add_Fake_Observations(nobs, npols)

    def Add_Fake_Observations(self, nobs, npols):
            # form up the observation list
        obslist = []
        jds = n.arange(0, nobs) * self.length + 2456446.1234
        pols = ['xx', 'yy', 'xy', 'yx']
        for i, pol in enumerate(pols):
            if i >= npols:
                continue
            for jdi in xrange(len(jds)):
                obsnum = jdpol2obsnum(jdi, pol, self.length)

                obslist.append({'obsnum': obsnum,
                                'date': jds[jdi],
                                'date_type': self.date_type,
                                'pol': pol,
                                'host': self.host,
                                'filename': 'zen.{jd}.uv'.format(jd=n.round(jds[jdi], 5)),
                                'length': self.length})

                if jdi != 0:
                    obslist[-1]['neighbor_low'] = jds[jdi - 1]
                if jdi < len(jds[:-1]):
                    obslist[-1]['neighbor_high'] = jds[jdi + 1]
        obsnums = self.add_observations(obslist, status=self.defaultstatus)


class FakeDataBaseInterface:
    def __init__(self, nfiles=10):
        self.files = {}
        for i in xrange(nfiles):
            self.files[str(i)] = 'UV-POT'

    def get_obs_status(self, filename):
        return self.files[filename]

    def get_obs_index(self, filename):  # not used
        return int(filename)

    def list_observations(self):
        files = self.files.keys()
        files.sort()
        return files

    def get_neighbors(self, filename):
        n = int(filename)
        n1, n2 = str(n - 1), str(n + 1)
        if n1 not in self.files:
            n1 = None
        if n2 not in self.files:
            n2 = None
        return (n1, n2)

# need Action's that update the db
# scheduler reads the state, decides which action to do and launches.
#  the action sends a message to the taskserver to launch a task
# the task runs a script and then updates the db

# I need an Action that does not launch a task, it just sets the current state to Action.state
# taskserver sets the PID


class TestSchedulerDB(unittest.TestCase):
    def setUp(self):
        self.ntimes = 10
        self.npols = 4
        self.dbi = PopulatedDataBaseInterface(self.ntimes, self.npols, test=True)
        self.files = self.dbi.list_observations()
        self.sg = SpawnerClass()
        self.sg.config_file = "still_test_paper.cfg"
        self.wf = WorkFlow()
        process_client_config_file(self.sg, self.wf)
        self.task_clients = TaskClient(self.dbi, 'localhost', self.wf, port=TEST_PORT)

    def test_populated(self):  # do a couple of quick checks on my db population
        obsnums = self.dbi.list_observations()
        self.assertEqual(len(obsnums), self.ntimes * self.npols)
        self.assertEqual(len(set(obsnums)), self.ntimes * self.npols)

    def test_get_new_active_obs(self):
        # s = sch.Scheduler(nstills=1, actions_per_still=1, blocksize=10)
        s = sch.Scheduler(self.task_clients, self.wf, nstills=1, actions_per_still=1, blocksize=10)
        tic = time.time()
        s.get_new_active_obs(self.dbi)
        print("time to execute get_new_active_obs: %s") % (time.time() - tic)
        self.assertEqual(len(s.active_obs), self.ntimes * self.npols)

    def test_get_action(self):
        """
        """
        obsnum = self.files[5]
        # s = sch.Scheduler(nstills=1, actions_per_still=1)
        s = sch.Scheduler(self.task_clients, self.wf, nstills=1, actions_per_still=1, blocksize=10)
        tic = time.time()
        a = s.get_action(self.dbi, obsnum, ActionClass=NullAction)
        print("time to execute get_action: %s") % (time.time() - tic)
        self.assertNotEqual(a, None)  # everything is actionable in this test
        self.assertEqual(a.task, FILE_PROCESSING_LINKS[self.dbi.defaultstatus])  # check this links to the next step

    def test_start(self):
        self.dbi = PopulatedDataBaseInterface(3, 1, test=True)
        obsnums = self.dbi.list_observations()

        class SuccessAction(sch.Action):

            def _command(me):
                me.dbi = self.dbi
                # print "Action setting {obsnum} status to {status}".format(
                #        status=me.task,obsnum=me.obs)
                me.dbi.set_obs_status(me.obs, me.task)

        def all_done():
            for obsnum in obsnums:
                if self.dbi.get_obs_status(obsnum) != 'COMPLETE':
                    return False
                return True

        # s = sch.Scheduler(nstills=1, actions_per_still=1, blocksize=10)
        s = sch.Scheduler(self.task_clients, self.wf, nstills=1, actions_per_still=1, blocksize=10)
        t = threading.Thread(target=s.start, args=(self.dbi, SuccessAction))
        t.start()
        tstart = time.time()
        completion_time = len(FILE_PROCESSING_STAGES) * 3 * 0.2  # 0.2 s per file per step
        # print "time to completion:",completion_time,'s'
        while not all_done():
            if time.time() - tstart > completion_time:
                break
            time.sleep(1)
        s.quit()
        for obsnum in obsnums:
            self.assertEqual(self.dbi.get_obs_status(obsnum), 'COMPLETE')

    def test_clean_completed_actions(self):
        """
        todo
        """
        self.dbi = PopulatedDataBaseInterface(3, 1, test=True)

        class SuccessAction(sch.Action):

            def _command(me):
                me.dbi = self.dbi
                me.dbi.set_obs_status(me.obs, me.task)
                print("Action has status: %s") % (me.dbi.get_obs_status(me.obs))
                return None
        # s = sch.Scheduler(nstills=1, actions_per_still=1, blocksize=10)
        s = sch.Scheduler(self.task_clients, self.wf, nstills=1, actions_per_still=1, blocksize=10)
        s.get_new_active_obs(self.dbi)
        s.update_action_queue(self.dbi, ActionClass=SuccessAction)
        a = s.pop_action_queue(0)
        s.launch_action(a)
        self.assertEqual(len(s.launched_actions[0]), 1)
        time.sleep(1)
        s.clean_completed_actions(self.dbi)
        self.assertEqual(len(s.launched_actions[0]), 0)


if __name__ == '__main__':
    unittest.main()
