import unittest, threading
import subprocess, os, time, socket
import ddr_compress.task_server as ts
import ddr_compress.scheduler as sch

class SleepTask(ts.Task):
    def _run(self):
        return subprocess.Popen(['sleep','100'], stdout=open(os.devnull,'w'))

class NullTask(ts.Task):
    def _run(self):
        return subprocess.Popen(['ls'], stdout=open(os.devnull,'w'), cwd=self.cwd)

class FakeDataBaseInterface:
    def __init__(self, nfiles=10):
        self.files = {}
        self.pids = {}
        self.stills = {}
        self.paths = {}
        for i in xrange(nfiles):
            self.files[i] = 'UV_POT'
            self.pids[i] = -1
            self.stills[i] = 'localhost'
            self.paths[i] = os.path.abspath('.')
    def get_obs_status(self, obsnum):
        return self.files[obsnum]
    def list_observations(self):
        files = self.files.keys()
        files.sort()
        return files
    def get_neighbors(self, obsnum):
        n1,n2 = obsnum-1, obsnum+1
        if not self.files.has_key(n1): n1 = None
        if not self.files.has_key(n2): n2 = None
        return (n1,n2)
    def set_obs_status(self, obs, status):
        self.files[obs] = status
    def set_obs_pid(self, obs, pid):
        self.pids[obs] = pid
    def get_obs_pid(self, obs):
        return self.pids[obs]
    def get_input_file(self, obsnum):
        return 'localhost','.','test.uv'
    def get_output_location(self, obsnum):
        return 'localhost','.'
    def get_obs_still_host(self, obsnum):
        return self.stills[obsnum]
    def get_obs_still_path(self, obsnum):
        return self.paths[obsnum]
    def set_obs_still_host(self, obsnum, host):
        self.stills[obsnum] = host
    def set_obs_still_path(self, obsnum, path):
        self.paths[obsnum] = path

class TestFunctions(unittest.TestCase):
    def test_pad(self):
        self.assertEqual(len(ts.pad('', 80)), 80)
        self.assertEqual(len(ts.pad('abc'*10, 30)), 30)
    def test_to_pkt(self):
        pkt = ts.to_pkt('UV', 5, 'still', ['1','2','3'])
        self.assertEqual(len(pkt), 7*ts.PKT_LINE_LEN)
        self.assertEqual(pkt[:ts.PKT_LINE_LEN], ts.pad('7'))
    def test_from_pkt(self):
        pkt = ts.pad('5') + ts.pad('UV') + ts.pad('4') + ts.pad('still') + ts.pad('1')
        task, obs, still, args = ts.from_pkt(pkt)
        self.assertEqual(task, 'UV')
        self.assertEqual(obs, 4)
        self.assertEqual(still, 'still')
        self.assertEqual(args, ['1'])
    def test_to_from_pkt(self):
        pkt = ts.to_pkt('UV', 5, 'still', ['1','2','3'])
        task, obs, still, args = ts.from_pkt(pkt)
        self.assertEqual(task, 'UV')
        self.assertEqual(obs, 5)
        self.assertEqual(still, 'still')
        self.assertEqual(args, ['1','2','3'])

class TestTask(unittest.TestCase):
    def setUp(self):
        self.var = 0
        class VarTask(ts.Task):
            def _run(me):
                self.var += 1
                return subprocess.Popen(['ls'], stdout=open(os.devnull,'w'),
                    cwd=me.cwd)
        self.VarTask = VarTask
    def test_run(self):
        dbi = FakeDataBaseInterface()
        t = self.VarTask('UV', 1, 'still', ['filename'], dbi)
        self.assertEqual(t.process, None)
        var = self.var
        t.run()
        self.assertEqual(self.var, var+1)
        self.assertTrue(type(t.process) is subprocess.Popen)
        t.finalize()
        self.assertEqual(dbi.get_obs_status(1), 'UV')
        self.assertRaises(RuntimeError, t.run)
    def test_kill(self):
        dbi = FakeDataBaseInterface()
        t = SleepTask('UV',1,'still',[],dbi)
        start_t = time.time()
        t.run()
        t.kill()
        t.finalize()
        end_t = time.time()
        self.assertEqual(t.poll(), -9)
        self.assertLess(end_t-start_t, 100)
        self.assertEqual(dbi.get_obs_status(1), 'UV_POT')

class TestTaskServer(unittest.TestCase):
    def setUp(self):
        self.dbi = FakeDataBaseInterface()
    def test_basics(self):
        s = ts.TaskServer(self.dbi)
        t = SleepTask('UV',1,'still',[],self.dbi)
        s.append_task(t)
        self.assertEqual(len(s.active_tasks), 1)
        t.run()
        s.kill(t.process.pid)
        while t.poll() is None: time.sleep(.01)
        self.assertEqual(t.poll(), -9)
        thd = threading.Thread(target=s.finalize_tasks, args=(.1,))
        s.is_running = True
        thd.start()
        s.is_running = False
        thd.join()
        self.assertEqual(len(s.active_tasks),0)
    def test_shutdown(self):
        s = ts.TaskServer(self.dbi)
        t = threading.Thread(target=s.start)
        t.start()
        s.shutdown()
        t.join()
        self.assertFalse(s.is_running)
    def test_send_task(self):
        self.var = 0
        class SleepHandler(ts.TaskHandler):
            def handle(me):
                self.var += 1
                t = SleepTask('UV',1,'still',[],self.dbi)
                t.run()
                me.server.append_task(t)
        s = ts.TaskServer(self.dbi, handler=SleepHandler)
        thd = threading.Thread(target=s.start)
        thd.start()
        try:
            self.assertEqual(len(s.active_tasks), 0)
            self.assertEqual(self.var, 0)
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto('test', ('localhost', ts.STILL_PORT))
            while self.var != 1: time.sleep(.1)
            self.assertEqual(self.var, 1)
            self.assertEqual(len(s.active_tasks), 1)
        finally:
            s.shutdown()
            thd.join()
    def test_dbi(self):
        self.var = 0
        for f in self.dbi.files: self.dbi.files[f] = 'UV_POT'
        class NullHandler(ts.TaskHandler):
            def handle(me):
                task, obs, still, args = me.get_pkt()
                t = NullTask(task, obs, still, args, self.dbi)
                me.server.append_task(t)
                t.run()
                self.var += 1
        s = ts.TaskServer(self.dbi, handler=NullHandler)
        thd = threading.Thread(target=s.start)
        thd.start()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(ts.to_pkt('UV',1,'still',[]), ('localhost', ts.STILL_PORT))
            while self.var != 1: time.sleep(.6)
            self.assertEqual(self.var, 1)
            self.assertEqual(self.dbi.get_obs_status(1), 'UV')
        finally:
            s.shutdown()
            thd.join()

class TestTaskClient(unittest.TestCase):
    def setUp(self):
        self.dbi = FakeDataBaseInterface()
    def test_attributes(self):
        tc = ts.TaskClient(self.dbi, 'localhost')
        self.assertEqual(tc.host_port, ('localhost', ts.STILL_PORT))
    def test__tx(self):
        self.pkt = ''
        class SleepHandler(ts.TaskHandler):
            def handle(me):
                self.pkt = me.get_pkt()
        s = ts.TaskServer(self.dbi, handler=SleepHandler)
        thd = threading.Thread(target=s.start)
        thd.start()
        try:
            tc = ts.TaskClient(self.dbi, 'localhost')
            tc._tx('UV', 1, ['a','b','c'])
        finally:
            s.shutdown()
            thd.join()
        self.assertEqual(self.pkt, ('UV',1,'localhost',['a','b','c']))
    def test_gen_args(self):
        tc = ts.TaskClient(self.dbi, 'localhost')
        for task in sch.FILE_PROCESSING_STAGES[2:-1]:
            args = tc.gen_args(task, 2)
            if task in ['UVCRE','UVCRRE']:
                self.assertEqual(len(args), 3)
            elif task in ['UVC','CLEAN_UV','CLEAN_UVC','NPZ','UVCRR',
                    'CLEAN_UVCRE','CLEAN_UVCRR','CLEAN_NPZ','CLEAN_UVCR',
                    'CLEAN_UVCRRE']:
                self.assertEqual(len(args), 1)
            elif task in ['ACQUIRE_NEIGHBORS','CLEAN_NEIGHBORS']:
                self.assertEqual(len(args), 0)
            elif task in ['UV','NPZ_POT','UVCRRE_POT']:
                self.assertEqual(len(args), 2)
    def test_tx(self):
        self.pkt = ''
        class SleepHandler(ts.TaskHandler):
            def handle(me):
                self.pkt = me.get_pkt()
        s = ts.TaskServer(self.dbi, handler=SleepHandler)
        thd = threading.Thread(target=s.start)
        thd.start()
        try:
            tc = ts.TaskClient(self.dbi, 'localhost')
            tc.tx('UV', 1)
        finally:
            s.shutdown()
            thd.join()
        self.assertEqual(self.pkt, ('UV',1,'localhost',['test.uv', 'localhost:./test.uv']))


if __name__ == '__main__':
    unittest.main()
