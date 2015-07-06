import SocketServer
import logging
import threading
import time
import socket
import os
import tempfile
import scheduler
import string
import sys
import psutil

# from still_shared import logger

logger = logging.getLogger('taskserver')
logger.setLevel(logging.DEBUG)
logger.propagate = True
PKT_LINE_LEN = 160
STILL_PORT = 14204


def pad(s, line_len=PKT_LINE_LEN):

    return (s + ' ' * line_len)[:line_len]


def to_pkt(task, obs, still, args):
    nlines = len(args) + 4
    return ''.join(map(pad, [str(nlines), task, str(obs), still] + args))


def from_pkt(pkt, line_len=PKT_LINE_LEN):
    nlines, pkt = pkt[:line_len].rstrip(), pkt[line_len:]
    nlines = int(nlines)
    task, pkt = pkt[:line_len].rstrip(), pkt[line_len:]
    obs, pkt = int(pkt[:line_len].rstrip()), pkt[line_len:]
    still, pkt = pkt[:line_len].rstrip(), pkt[line_len:]
    args = []
    for i in xrange(nlines - 4):
        arg, pkt = pkt[:line_len].rstrip(), pkt[line_len:]
        args.append(arg)
    return task, obs, still, args


class Task:
    def __init__(self, task, obs, still, args, dbi, cwd='.'):
        self.task = task
        self.obs = obs
        self.still = still
        self.args = args
        self.dbi = dbi
        self.cwd = cwd
        self.process = None
        self.OUTFILE = tempfile.TemporaryFile()
        self.outfile_counter = 0

    def run(self):
        if self.process is not None:
            raise RuntimeError('Cannot run a Task that has been run already.')
        self.process = self._run()
        if self.process is None:
            self.record_failure()
        else:
            self.record_launch()

    def _run(self):
        process = None
        logger.info('Task._run: (%s, %s) %s cwd=%s' % (self.task, self.obs, ' '.join(['do_%s.sh' % self.task] + self.args), self.cwd))
        # process= subprocess.Popen(['do_%s.sh' % self.task] + self.args, cwd=self.cwd,stderr=subprocess.PIPE,stdout=subprocess.PIPE) # XXX d something with stdout stderr
        # create a temp file descriptor for stdout and stderr
        self.OUTFILE = tempfile.TemporaryFile()
        self.outfile_counter = 0
        try:
            process = psutil.Popen(['/Users/wintermute/mwa_pipeline/scripts/do_%s.sh' % self.task] + self.args, cwd=self.cwd, stderr=self.OUTFILE, stdout=self.OUTFILE)
            process.cpu_affinity(range(psutil.cpu_count()))
            self.dbi.add_log(self.obs, self.task, ' '.join(['do_%s.sh' % self.task] + self.args + ['\n']), None)
        except Exception, e:
            logger.error('Task._run: (%s,%s) %s error="%s"' % (self.task, self.obs, ' '.join(['do_%s.sh' % self.task] + self.args), e))
#            sys.exit(1)
        return process

    def poll(self):
        # logger.debug('Task.pol: (%s,%s)  reading to log position %d'%(self.task,self.obs,self.outfile_counter))
        if self.process is None:
            return None
        self.OUTFILE.seek(self.outfile_counter)
        logtext = self.OUTFILE.read()
        # logger.debug('Task.pol: (%s,%s) found %d log characters' % (self.task,self.obs,len(logtext)))
        if len(logtext) > self.outfile_counter:
            logger.debug('Task.pol: ({task},{obsnum}) adding {d} log characeters'.format(task=self.task, obsnum=self.obs, d=len(logtext)))
            self.dbi.update_log(self.obs, self.task, logtext=logtext, exit_status=self.process.poll())
            self.outfile_counter += len(logtext)
            # logger.debug('Task.pol: (%s,%s) setting next log position to %d' % (self.task,self.obs,self.outfile_counter))
        # logger.debug('Task.pol: (%s,%s) post log addition' % (self.task,self.obs))
        return self.process.poll()

    def finalize(self):
        logger.info('Task.finalize waiting: ({task},{obsnum})'.format(task=self.task, obsnum=self.obs))
        self.process.communicate()
        # try:
        #    stdout,stderr=self.process.communicate()
        #    if stderr is None:
        #        stderr='<no stderr>'
        #    if stdout is None:
        #        stdout = '<no stdout>'
        #    logtext=stdout + stderr
        # except Exception,e:
        #        logger.error(e)
        # logger.info('Task.finalize writing log: ({task},{obsnum})'.format(task=self.task,obsnum=self.obs))
        # self.dbi.add_log(self.obs,self.task,logtext=logtext,exit_status=self.poll())
        logger.debug('Task.finalize closing out log: ({task},{obsnum})'.format(task=self.task, obsnum=self.obs))
        self.dbi.update_log(self.obs, exit_status=self.process.poll())
        if self.poll():
            self.record_failure()
        else:
            self.record_completion()

    def kill(self):
        # myproc = psutil.Process(pid=self.process.pid)
        # print("My Process pid in kill : %s") % myproc.children(recursive=True)
        self.record_failure()
        logger.debug('Task.kill Trying to kill: ({task},{obsnum}) pid={pid}'.format(task=self.task, obsnum=self.obs, pid=self.process.pid))
        logger.debug('Task.kill Killing {n} children to prevent orphans: ({task},{obsnum})'.format(n=len(self.process.children(recursive=True)), task=self.task, obsnum=self.obs))
        for child in self.process.children(recursive=True):
            child.kill()
        logger.debug('Task.kill Killing shell script: ({task},{obsnum})'.format(task=self.task, obsnum=self.obs))
        self.process.kill()
        os.wait()
        logger.debug('Task.kill Successfully killed ({task},{obsnum})'.format(task=self.task, obsnum=self.obs))

    def record_launch(self):
        self.dbi.set_obs_pid(self.obs, self.process.pid)

    def record_failure(self):
        self.dbi.set_obs_pid(self.obs, -9)
        logger.error('Task.record_failure.  TASK FAIL ({task},{obsnum})'.format(task=self.task, obsnum=self.obs))

    def record_completion(self):
        self.dbi.set_obs_status(self.obs, self.task)
        self.dbi.set_obs_pid(self.obs, 0)


class TaskClient:
    def __init__(self, dbi, host, workflow, port=STILL_PORT):
        self.dbi = dbi
        self.host_port = (host, port)
        self.wf = workflow
        self.error_count = 0

    def _tx(self, task, obs, args):
        recieved = ''
        status = ''
        print("my args : %s") % args
        logger.debug('TaskClient._tx: sending (%s,%s) with args=%s' % (task, obs, ' '.join(args)))

        pkt = to_pkt(task, obs, self.host_port[0], args)
        print(self.host_port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            print("connecting to TaskServer %s") % self.host_port[0]
            try:
                sock.connect(self.host_port)
                sock.sendall(pkt + "\n")
                recieved = sock.recv(1024)
            except socket.error, exc:
                print "Caught exception socket.error : %s" % exc
        finally:
            sock.close()

        if recieved != "OK\n":
            # Jon: Put more stuff here to handle when the server doesn't respond back correctly that its taken the task
            print("!! We had a problem sending data to %s") % self.host_port[0]
            self.error_count += 1
            print("Host : %s  has error count :%s") % (self.host_port[0], self.error_count)
            status = "FAILED_TO_CONNECT"
        else:
            status = "OK"
        return status, self.error_count

    def gen_args(self, task, obs):
        pot, path, basename = self.dbi.get_input_file(obs)  # Jon: Pot I believe is host where file to process is, basename is just the file name
        outhost, outpath = self.dbi.get_output_location(obs)
        # hosts and paths are not used except for ACQUIRE_NEIGHBORS and CLEAN_NEIGHBORS
        stillhost, stillpath = self.dbi.get_obs_still_host(obs), self.dbi.get_obs_still_path(obs)
        neighbors = [(self.dbi.get_obs_still_host(n), self.dbi.get_obs_still_path(n)) + self.dbi.get_input_file(n)
                     for n in self.dbi.get_neighbors(obs) if n is not None]
        neighbors_base = list(self.dbi.get_neighbors(obs))
        if not neighbors_base[0] is None:
            neighbors_base[0] = self.dbi.get_input_file(neighbors_base[0])[-1]
        if not neighbors_base[1] is None:
            neighbors_base[1] = self.dbi.get_input_file(neighbors_base[1])[-1]

        # Jon : closurs are a bit weird but cool, should get rid of appendage HARDWF
        def interleave(filename, appendage='cR'):
            # make sure this is in sync with do_X.sh task scripts.
            rv = [filename]
            if neighbors_base[0] is not None:
                rv = [neighbors_base[0] + appendage] + rv
            if neighbors_base[1] is not None:
                rv = rv + [neighbors_base[1] + appendage]
            return rv

        try:
            args = eval(self.wf.action_args[task])
        except:
            print("Could not process arguments for task %s please check args for this task in config file") % task
            print(self.wf.action_args)
            sys.exit(1)
        print("My Args!!! : %s") % args

        return args

    def tx(self, task, obs):
        args = self.gen_args(task, obs)
        print("Got here, that's exciting... Args: %s, task: %s") % (args, task)
        self._tx(task, obs, args)

    def tx_kill(self, obs):
        pid = self.dbi.get_obs_pid(obs)
        if pid is None:
            logger.debug('ActionClient.tx_kill: task running on %s is not alive' % obs)
        else:
            self._tx('KILL', obs, [str(pid)])

# XXX consider moving this class to a separate file


class Action(scheduler.Action):
    # def __init__(self, obs, task, neighbor_status, still, task_clients, timeout=3600.):
        # scheduler.Action.__init__(self, obs, task, neighbor_status, still, timeout=timeout)
        # self.task_client = task_clients[still]

    def _command(self):
        logger.debug('Action: task_client(%s,%s)' % (self.task, self.obs))
        self.task_client.tx(self.task, self.obs)

# Jon : I don't think we need to handle this in this way, I added the self.task_clients to the normal class.
#       If this is needed it might be wroth just renaming it something other than Scheduler or something that
#       imports this file and scheduler.py can get conflicts.
# class Scheduler(scheduler.Scheduler):
#     def __init__(self, task_clients, workflow, actions_per_still=8, blocksize=10):
#         scheduler.Scheduler.__init__(self, nstills=len(task_clients), actions_per_still=actions_per_still, blocksize=blocksize)
#         self.task_clients = task_clients
# Jon : Bring back kill action
#     def kill_action(self, a):
#         scheduler.Scheduler.kill_action(self, a)
#         still = self.obs_to_still(a.obs)
#         self.task_clients[still].tx_kill(a.obs)


# class TaskHandler(SocketServer.BaseRequestHandler):
class TaskHandler(SocketServer.StreamRequestHandler):

    def get_pkt(self):
        pkt = self.data
        task, obs, still, args = from_pkt(pkt)
        return task, obs, still, args

    def handle(self):

        self.data = self.rfile.readline().strip()
        self.wfile.write("OK\n")

        print "{} wrote:".format(self.client_address[0])
        print self.data
        print("I got stuffs!")
        task, obs, still, args = self.get_pkt()
        logger.info('TaskHandler.handle: received (%s,%s) with args=%s' % (task, obs, ' '.join(args)))
        if task == 'KILL':
            self.server.kill(int(args[0]))  # TODO I THINK THIS IS WHERE WE HAVE A PROBLEM. RUN and maybe COMPLETE need to clean up existing threads.
        elif task == 'COMPLETE':  # HARDWF, JON: This one should be ok but complete still needs to be in conf file.  Though Maybe just make last thing in conf file what we use here?
            self.server.dbi.set_obs_status(obs, task)
        else:
            t = Task(task, obs, still, args, self.server.dbi, self.server.data_dir)
            self.server.append_task(t)
            t.run()


class TaskServer(SocketServer.TCPServer):
    allow_reuse_address = True

    def __init__(self, dbi, data_dir='.', port=STILL_PORT, handler=TaskHandler):
        SocketServer.TCPServer.__init__(self, ('', port), handler)
        self.active_tasks_semaphore = threading.Semaphore()
        self.active_tasks = []
        self.dbi = dbi
        self.data_dir = data_dir
        self.is_running = False
        self.watchdog_count = 0
        self.port = port
#        self.rfile = self.rfile

    def append_task(self, t):
        self.active_tasks_semaphore.acquire()
        self.active_tasks.append(t)
        self.active_tasks_semaphore.release()

    def finalize_tasks(self, poll_interval=5.):
        while self.is_running:
            self.active_tasks_semaphore.acquire()
            new_active_tasks = []
            for t in self.active_tasks:
                if t.poll() is None:  # not complete
                    new_active_tasks.append(t)
                    try:
                        c = t.process.children()[0]
                        # Check the affinity!
                        if len(c.cpu_affinity()) < psutil.cpu_count():
                            c.cpu_affinity(range(psutil.cpu_count()))
                        logger.debug('Proc info on {obsnum}:{task}:{pid} - cpu={cpu:.1f}%%, mem={mem:.1f}%%, Naffinity={aff}'.format(
                            obsnum=t.obs, task=t.task, pid=c.pid, cpu=c.cpu_percent(interval=1.0), mem=c.memory_percent(), aff=len(c.cpu_affinity())))
                    except:
                        continue
                else:
                    t.finalize()
            self.active_tasks = new_active_tasks
            self.active_tasks_semaphore.release()

            #  Jon: I think we can get rid of the watchdog as I'm already throwing this at the db
            time.sleep(poll_interval)
            if self.watchdog_count == 100:
                logger.debug('TaskServer is alive')
                self.watchdog_count = 0
            else:
                self.watchdog_count += 1

    def kill(self, pid):
        for task in self.active_tasks:
            if task.process.pid == pid:
                task.kill()
                break

    def checkin_timer(self):
        #
        # Just a timer that will update that its last_checkin time in the database every 5min
        #
        while True:
            hostname = socket.gethostname()
            ip_addr = socket.gethostbyname(hostname)
            self.dbi.still_checkin(hostname, ip_addr)
            time.sleep(30)
        return 0

    def start(self):
        self.is_running = True
        t = threading.Thread(target=self.finalize_tasks)
        t.start()
        logger.debug('this is scheduler.py')
        logger.debug("using code at: " + __file__)
        self.dbi.still_checkin("localhost", "127.0.0.1")
        try:
            # Setup a thread that just updates the last checkin time for this still every 5min
            timer_thread = threading.Thread(target=self.checkin_timer)
            timer_thread.daemon = True  # Make it a daemon so that when ctrl-c happens this thread goes away
            timer_thread.start()
            # Start the lisetenser server
            self.serve_forever()
        finally:
            self.shutdown()
            t.join()

    def shutdown(self):
        print("getting to shutdown!!")
        self.is_running = False
        for t in self.active_tasks:
            try:
                t.process.kill()
            except(OSError):
                pass
        SocketServer.TCPServer.shutdown(self)
