import SocketServer
import logging
import threading
import time
import socket
import os
import tempfile
import platform

# import scheduler
# import string
import sys
import psutil
from still_shared import setup_logger

HOSTNAME = socket.gethostname()

# logger = logging.getLogger('ts')
# format = '%(asctime)s - {0} - %(name)s - %(levelname)s - %(message)s'.format(HOSTNAME)


# formating = logging.Formatter(format)
# logger.setLevel(logging.DEBUG)

# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# ch.setFormatter(formating)

# fh = logging.FileHandler("%s_ts.log" % HOSTNAME)
# fh.setLevel(logging.DEBUG)
# fh.setFormatter(formating)

# logger.addHandler(fh)
# logger.addHandler(ch)

# logger.propagate = True
logger = True
PKT_LINE_LEN = 160
STILL_PORT = 14204
PLATFORM = platform.system()
FAIL_ON_ERROR = 0


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
    def __init__(self, task, obs, still, args, dbi, TaskServer, cwd='.', path_to_do_scripts="."):
        self.task = task
        self.obs = obs
        self.still = still
        self.args = args
        self.dbi = dbi
        self.cwd = cwd
        self.process = None
        self.OUTFILE = tempfile.TemporaryFile()
        self.outfile_counter = 0
        self.path_to_do_scripts = path_to_do_scripts
        self.ts = TaskServer

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
        # create a temp file descriptor for stdout and stderr
        self.OUTFILE = tempfile.TemporaryFile()
        self.outfile_counter = 0
        try:
            process = psutil.Popen(['%sdo_%s.sh' % (self.path_to_do_scripts, self.task)] + self.args, cwd=self.cwd, stderr=self.OUTFILE, stdout=self.OUTFILE)
            process.nice(10)
            if PLATFORM != "Darwin":  # Jon : cpu_affinity doesn't exist for the mac, testing on a mac... yup... good story.
                process.cpu_affinity(range(psutil.cpu_count()))
            # process.set_nice(10)  # Jon : I want to set all the processes evenly so they don't compete against core OS functionality slowing things down.
            self.dbi.update_obs_current_stage(self.obs, self.task)
            self.dbi.add_log(self.obs, self.task, ' '.join(['%sdo_%s.sh' % (self.path_to_do_scripts, self.task)] + self.args + ['\n']), None)

        except Exception:
            logger.exception('Task._run: (%s,%s) error="%s"' % (self.task, self.obs, ' '.join(['%sdo_%s.sh' % (self.path_to_do_scripts, self.task)] + self.args)))
            self.record_failure()
            if FAIL_ON_ERROR == 1:
                self.ts.shutdown()

        return process

    def poll(self):
        # logger.debug('Task.pol: (%s,%s)  reading to log position %d'%(self.task,self.obs,self.outfile_counter))
        if self.process is None:
            return None
        self.OUTFILE.seek(self.outfile_counter)
        logtext = self.OUTFILE.read()
        # logger.debug('Task.pol: (%s,%s) found %d log characters' % (self.task,self.obs,len(logtext)))
        if len(logtext) > self.outfile_counter:
            # logger.debug('Task.pol: ({task},{obsnum}) adding {d} log characeters'.format(task=self.task, obsnum=self.obs, d=len(logtext)))
            logger.debug("Output -> Task : %s, Obsnum: %s, Output: %s" % (self.task, self.obs, logtext))
            self.dbi.update_log(self.obs, self.task, logtext=logtext, exit_status=self.process.poll())
            self.outfile_counter += len(logtext)
        return self.process.poll()

    def finalize(self):
        logger.info('Task.finalize waiting: ({task},{obsnum})'.format(task=self.task, obsnum=self.obs))
        self.process.communicate()

        logger.debug('Task.finalize closing out log: ({task},{obsnum})'.format(task=self.task, obsnum=self.obs))
        self.dbi.update_log(self.obs, exit_status=self.process.poll())
        if self.poll():
            self.record_failure()
        else:
            self.record_completion()

    def kill(self):
        # myproc = psutil.Process(pid=self.process.pid)
        self.record_failure(failure_type="KILLED")
        if self.process.pid:
            logger.debug('Task.kill Trying to kill: ({task},{obsnum}) pid={pid}'.format(task=self.task, obsnum=self.obs, pid=self.process.pid))

            for child in self.process.children(recursive=True):
                child.kill()
            self.process.kill()

        os.wait()

    def record_launch(self):
        self.dbi.set_obs_pid(self.obs, self.process.pid)

    def record_failure(self, failure_type="FAILED"):
        for task in self.ts.active_tasks:
            if task.obs == self.obs:
                self.ts.active_tasks.remove(task)  # Remove the killed task from the active task list
                logger.debug("Removed task : %s from active list" % task.task)
        self.dbi.set_obs_pid(self.obs, -9)
        self.dbi.update_obs_current_stage(self.obs, failure_type)
        logger.error("Task.record_failure: Task: %s, Obsnum: %s, Type: %s" % (self.task, self.obs, failure_type))

    def record_completion(self):
        self.dbi.set_obs_status(self.obs, self.task)
        self.dbi.set_obs_pid(self.obs, 0)


class TaskClient:
    def __init__(self, dbi, host, workflow, port, sg):
        self.dbi = dbi
        self.sg = sg
        self.host_port = (host, port)
        self.wf = workflow
        self.error_count = 0
        self.logger = sg.logger
        global logger
        logger = sg.logger

    def transmit(self, task, obs):
        recieved = ''
        status = ''
        args = self.gen_args(task, obs)
        logger.debug('TaskClient.transmit: sending (%s,%s) with args=%s' % (task, obs, args))

        pkt = to_pkt(task, obs, self.host_port[0], args)
        # print(self.host_port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:  # Attempt to open a socket to a server and send over task instructions
            logger.debug("connecting to TaskServer %s" % self.host_port[0])
            try:
                sock.connect(self.host_port)
                sock.sendall(pkt + "\n")
                recieved = sock.recv(1024)
            except socket.error, exc:
                logger.exception("Caught exection trying to contact : %s socket.error : %s" % (self.host_port[0], exc))
        finally:
            sock.close()

        if recieved != "OK\n":  # Check if we did not recieve an OK that the server accepted the task
            logger.debug("!! We had a problem sending data to %s" % self.host_port[0])
            self.error_count += 1
            logger.debug("Host : %s  has error count :%s" % (self.host_port[0], self.error_count))
            status = "FAILED_TO_CONNECT"
        else:
            status = "OK"
        return status, self.error_count

    def gen_args(self, task, obs):
        args = []
        pot, path, basename = self.dbi.get_input_file(obs)  # Jon: Pot I believe is host where file to process is, basename is just the file name

        outhost, outpath = self.dbi.get_output_location(obs)
        # hosts and paths are not used except for ACQUIRE_NEIGHBORS and CLEAN_NEIGHBORS
        # stillhost, stillpath = self.dbi.get_obs_still_host(obs), self.dbi.get_obs_still_path(obs)
        stillhost, stillpath = self.dbi.get_obs_still_host(obs), self.dbi.get_still_info(self.host_port[0]).data_dir

        neighbors = [(self.dbi.get_obs_still_host(n), self.dbi.get_still_info(self.host_port[0]).data_dir) + self.dbi.get_input_file(n)
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

        if task != "STILL_KILL_OBS":
            try:
                args = eval(self.wf.action_args[task])
            except:
                logger.exception("Could not process arguments for task %s please check args for this task in config file, ARGS: %s" % (task, self.wf.action_args))
                args = []
                # sys.exit(1)
        return args

    def tx_kill(self, obs):
        pid = self.dbi.get_obs_pid(obs)
        if pid is None:
            logger.debug('ActionClient.tx_kill: task running on %s is not alive' % obs)
        else:
            self.transmit('KILL', obs)


class TaskHandler(SocketServer.StreamRequestHandler):

    def get_pkt(self):
        pkt = self.data
        task, obsnum, still, args = from_pkt(pkt)
        return task, obsnum, still, args

    def handle(self):
        task_already_exists = False
        self.data = self.rfile.readline().strip()
        self.wfile.write("OK\n")

        task, obsnum, still, args = self.get_pkt()
        logger.info('TaskHandler.handle: received (%s,%s) with args=%s' % (task, obsnum, ' '.join(args)))
        if task == "STILL_KILL_OBS":  # We should only be killing a process...
            pid_of_obs_to_kill = self.server.dbi.get_obs_pid(obsnum)
            logger.debug("We recieved a kill request for obsnum: %s, shutting down pid: %s" % (obsnum, pid_of_obs_to_kill))
            self.server.kill(pid_of_obs_to_kill)

        elif task == 'COMPLETE':
            self.server.dbi.set_obs_status(obsnum, task)

        else:
            for active_task in self.server.active_tasks:
                logger.debug("  Active Task: %s, For Obs: %s" % (active_task.task, active_task.obs))
                if active_task.task == task and active_task.obs == obsnum:  # We now check to see if the task is already in the list before we go crazy and try to run a second copy
                    logger.debug("We are currently running this task already. Task: %s , Obs: %s" % (active_task.task, active_task.obs))
                    task_already_exists = True
                    break

            if task_already_exists is False:
                t = Task(task, obsnum, still, args, self.server.dbi, self.server, self.server.data_dir, self.server.path_to_do_scripts)
                self.server.append_task(t)
                t.run()
        return


class TaskServer(SocketServer.TCPServer):
    allow_reuse_address = True

    def __init__(self, dbi, sg, data_dir='.', port=STILL_PORT, handler=TaskHandler, path_to_do_scripts="."):

        SocketServer.TCPServer.__init__(self, ('', port), handler)
        self.active_tasks_semaphore = threading.Semaphore()
        self.active_tasks = []
        self.dbi = dbi
        self.data_dir = data_dir
        self.is_running = False
        self.watchdog_count = 0
        self.port = port
        self.path_to_do_scripts = path_to_do_scripts
        self.logger = sg.logger
        global logger
        logger = sg.logger
        logger.debug("Path to do_ Scripts : %s" % self.path_to_do_scripts)
        logger.debug("Data_dir : %s" % self.data_dir)
        logger.debug("Port : %s" % self.port)

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
                        if PLATFORM != "Darwin":  # Jon : cpu_affinity doesn't exist for the mac, testing on a mac... yup... good story.

                            if len(c.cpu_affinity()) < psutil.cpu_count():
                                logger.debug('Proc info on {obsnum}:{task}:{pid} - cpu={cpu:.1f}%%, mem={mem:.1f}%%, Naffinity={aff}'.format(
                                    obsnum=t.obs, task=t.task, pid=c.pid, cpu=c.cpu_percent(interval=1.0), mem=c.memory_percent(), aff=len(c.cpu_affinity())))
                                c.cpu_affinity(range(psutil.cpu_count()))
                    except:

                        continue
                else:
                    t.finalize()
            self.active_tasks = new_active_tasks
            self.active_tasks_semaphore.release()

            #  Jon: I think we can get rid of the watchdog as I'm already throwing this at the db
            time.sleep(poll_interval)
            if self.watchdog_count == 30:
                logger.debug('TaskServer is alive')
                for t in self.active_tasks:
                    try:
                        c = t.process.children()[0]
                        if psutil.pid_exists(c.pid):
                            logger.debug('Proc info on {obsnum}:{task}:{pid} - cpu={cpu:.1f}%%, mem={mem:.1f}%%, Naffinity={aff}'.format(
                                obsnum=t.obs, task=t.task, pid=c.pid, cpu=c.cpu_percent(interval=1.0), mem=c.memory_percent(), aff=len(c.cpu_affinity())))
                    except:
                        pass
                self.watchdog_count = 0
            else:
                self.watchdog_count += 1

    def kill(self, pid):
        try:
            for task in self.active_tasks:
                if task.process.pid == pid:
                    task.kill()
#                    self.active_tasks.remove(task)  # Remove the killed task from the active task list
                    break
        except:
            logger.exception("Problem killing off task: %s  w/  pid : %s" % (task, pid))

    def kill_all(self):
        for task in self.active_tasks:
                task.kill()
                break

    def checkin_timer(self):
        #
        # Just a timer that will update that its last_checkin time in the database every 5min
        #
        while self.is_running is True:
            hostname = socket.gethostname()
            ip_addr = socket.gethostbyname(hostname)
            cpu_usage = psutil.cpu_percent()

            self.dbi.still_checkin(hostname, ip_addr, self.port, int(cpu_usage), self.data_dir, status="OK")
            time.sleep(60)
        return 0

    def start(self):
        psutil.cpu_percent()
        time.sleep(1)
        self.is_running = True
        t = threading.Thread(target=self.finalize_tasks)
        t.daemon = True
        t.start()
        logger.debug('Starting Task Server')
        logger.debug("using code at: " + __file__)

        try:
            # Setup a thread that just updates the last checkin time for this still every 5min
            timer_thread = threading.Thread(target=self.checkin_timer)
            timer_thread.daemon = True  # Make it a daemon so that when ctrl-c happens this thread goes away
            timer_thread.start()
            # Start the lisetenser server
            self.serve_forever()
        finally:
            self.shutdown()
#            t.join()

    def shutdown(self):
        logger.debug("Shutting Down task_server")
        hostname = socket.gethostname()
        ip_addr = socket.gethostbyname(hostname)
        cpu_usage = psutil.cpu_percent()
        self.dbi.still_checkin(hostname, ip_addr, self.port, int(cpu_usage), self.data_dir, status="OFFLINE")

        self.is_running = False
        for t in self.active_tasks:
            try:
                t.process.kill()
            except(OSError):
                pass
        SocketServer.TCPServer.shutdown(self)
