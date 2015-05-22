import SocketServer
import logging, threading, subprocess, time
import socket, os,tempfile,psutil

logger = logging.getLogger('taskserver')
logger.setLevel(logging.DEBUG)
logger.propagate = True
PKT_LINE_LEN = 160
STILL_PORT = 14204
def pad(s, line_len=PKT_LINE_LEN):
    return (s + ' '*line_len)[:line_len]

def to_pkt(task, obs, still, args):
    nlines = len(args) + 4
    return ''.join(map(pad, [str(nlines), task, str(obs), still] + args))

def from_pkt(pkt, line_len=PKT_LINE_LEN):
    nlines,pkt = pkt[:line_len].rstrip(), pkt[line_len:]
    nlines = int(nlines)
    task,pkt = pkt[:line_len].rstrip(), pkt[line_len:]
    obs,pkt = int(pkt[:line_len].rstrip()), pkt[line_len:]
    still,pkt = pkt[:line_len].rstrip(), pkt[line_len:]
    args = []
    for i in xrange(nlines-4):
        arg,pkt = pkt[:line_len].rstrip(), pkt[line_len:]
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
        if not self.process is None:
            raise RuntimeError('Cannot run a Task that has been run already.')
        if self.task == 'UV': # on first copy of data to still, record in db that obs is assigned here
            self.dbi.set_obs_still_host(self.obs, self.still)
            self.dbi.set_obs_still_path(self.obs, os.path.abspath(self.cwd))
        self.process = self._run()
        self.record_launch()
    def _run(self):
        logger.info('Task._run: (%s,%d) %s cwd=%s' % (self.task,self.obs,' '.join(['do_%s.sh' % self.task] + self.args),self.cwd))
        #process= subprocess.Popen(['do_%s.sh' % self.task] + self.args, cwd=self.cwd,stderr=subprocess.PIPE,stdout=subprocess.PIPE) # XXX d something with stdout stderr
        #create a temp file descriptor for stdout and stderr
        self.OUTFILE = tempfile.TemporaryFile()
        self.outfile_counter = 0
        try:
            process= psutil.Popen(['do_%s.sh' % self.task] + self.args, cwd=self.cwd,stderr=self.OUTFILE,stdout=self.OUTFILE)
            process.cpu_affinity(range(psutil.cpu_count()))
            self.dbi.add_log(self.obs,self.task,' '.join(['do_%s.sh' % self.task] + self.args+['\n']),None)
        except Exception,e:
            logger.error('Task._run: (%s,%d) %s error="%s"' % (self.task,self.obs,' '.join(['do_%s.sh' % self.task] + self.args),e))
        return process
    def poll(self):
        #logger.debug('Task.pol: (%s,%d)  reading to log position %d'%(self.task,self.obs,self.outfile_counter))
        if self.process is None: return None
        self.OUTFILE.seek(self.outfile_counter)
        logtext = self.OUTFILE.read()
        #logger.debug('Task.pol: (%s,%d) found %d log characters' % (self.task,self.obs,len(logtext)))
        if len(logtext)>self.outfile_counter:
            logger.debug('Task.pol: ({task},{obsnum}) adding {d} log characeters'.format(task=self.task,obsnum=self.obs,d=len(logtext)))
            self.dbi.update_log(self.obs,self.task,logtext=logtext,exit_status=self.process.poll())
            self.outfile_counter += len(logtext)
            #logger.debug('Task.pol: (%s,%d) setting next log position to %d' % (self.task,self.obs,self.outfile_counter))
        #logger.debug('Task.pol: (%s,%d) post log addition' % (self.task,self.obs))
        return self.process.poll()
    def finalize(self):
        logger.info('Task.finalize waiting: ({task},{obsnum})'.format(task=self.task,obsnum=self.obs))
        self.process.communicate()
        #try:
        #    stdout,stderr=self.process.communicate()
        #    if stderr is None:
        #        stderr='<no stderr>'
        #    if stdout is None:
        #        stdout = '<no stdout>'
        #    logtext=stdout + stderr
        #except Exception,e:
        #        logger.error(e)
        #logger.info('Task.finalize writing log: ({task},{obsnum})'.format(task=self.task,obsnum=self.obs))
        #self.dbi.add_log(self.obs,self.task,logtext=logtext,exit_status=self.poll())
        logger.debug('Task.finalize closing out log: ({task},{obsnum})'.format(task=self.task,obsnum=self.obs))
        self.dbi.update_log(self.obs,exit_status=self.process.poll())
        if self.poll(): self.record_failure()
        else: self.record_completion()
    def kill(self):
        self.record_failure()
        logger.debug('Task.kill Trying to kill: ({task},{obsnum}) pid={pid}'.format(task=self.task,obsnum=self.obs,pid=self.process.pid))
        logger.debug('Task.kill Killing {n} children to prevent orphans: ({task},{obsnum})'.format(n=len(self.process.children(recursive=True)),task=self.task,obsnum=self.obs))
        for child in self.process.children(recursive=True):
            child.kill()
        logger.debug('Task.kill Killing shell script: ({task},{obsnum})'.format(task=self.task,obsnum=self.obs))
        self.process.kill()
        os.wait()
        logger.debug('Task.kill Successfully killed ({task},{obsnum})'.format(task=self.task,obsnum=self.obs))
    def record_launch(self):
        self.dbi.set_obs_pid(self.obs, self.process.pid)
    def record_failure(self):
        self.dbi.set_obs_pid(self.obs, -9)
        logger.error('Task.record_failure.  TASK FAIL ({task},{obsnum})'.format(task=self.task,obsnum=self.obs))
    def record_completion(self):
        self.dbi.set_obs_status(self.obs, self.task)
        self.dbi.set_obs_pid(self.obs,0)
class TaskClient:
    def __init__(self, dbi, host, port=STILL_PORT):
        self.dbi = dbi
        self.host_port = (host,port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    def _tx(self, task, obs, args):
        logger.debug('TaskClient._tx: sending (%s,%d) with args=%s' % (task, obs, ' '.join(args)))
        pkt = to_pkt(task, obs, self.host_port[0], args)
        self.sock.sendto(pkt, self.host_port)
    def gen_args(self, task, obs):
        pot,path,basename = self.dbi.get_input_file(obs)
        outhost,outpath = self.dbi.get_output_location(obs)
        # hosts and paths are not used except for ACQUIRE_NEIGHBORS and CLEAN_NEIGHBORS
        stillhost,stillpath = self.dbi.get_obs_still_host(obs), self.dbi.get_obs_still_path(obs)
        neighbors = [(self.dbi.get_obs_still_host(n),self.dbi.get_obs_still_path(n)) + self.dbi.get_input_file(n)
            for n in self.dbi.get_neighbors(obs) if not n is None]
        neighbors_base = list(self.dbi.get_neighbors(obs))
        if not neighbors_base[0] is None: neighbors_base[0] = self.dbi.get_input_file(neighbors_base[0])[-1]
        if not neighbors_base[1] is None: neighbors_base[1] = self.dbi.get_input_file(neighbors_base[1])[-1]
        def interleave(filename, appendage='cR'):
            # make sure this is in sync with do_X.sh task scripts.
            rv = [filename]
            if not neighbors_base[0] is None: rv = [neighbors_base[0]+appendage] + rv
            if not neighbors_base[1] is None: rv = rv + [neighbors_base[1]+appendage]
            return rv
        args = {
            'UV': [basename, '%s:%s/%s' % (pot,path,basename)],
            'UVC': [basename],
            'CLEAN_UV': [basename],
            'UVCR': [basename+'c'],
            'CLEAN_UVC': [basename+'c'],
            'ACQUIRE_NEIGHBORS': ['%s:%s/%s' % (n[0], n[1], n[-1]+'cR') for n in neighbors if n[0] != stillhost or n[1] != stillpath],
            'UVCRE': interleave(basename+'cR'),
            'NPZ': [basename+'cRE'],
            'UVCRR': [basename+'cR'],
            'NPZ_POT': [basename+'cRE.npz', '%s:%s' % (pot,path)],
            'CLEAN_UVCRE': [basename+'cRE'],
            'UVCRRE': interleave(basename+'cRR'),
            'CLEAN_UVCRR': [basename+'cRR'],
            'CLEAN_NPZ': [basename+'cRE.npz'],
            'CLEAN_NEIGHBORS': [n[-1]+'cR' for n in neighbors if n[0] != stillhost],
            'UVCRRE_POT': [basename+'cRRE', '%s:%s' % (pot,path)],
            'CLEAN_UVCR': [basename+'cR'],
            'CLEAN_UVCRRE': [basename+'cRRE'],
            'POT_TO_USA': [pot, '%s:%s'%(outhost,outpath), '%s/%s'%(path,basename+'cRRE'), '%s/%s'%(path,basename+'cRE.npz')], # XXX add destination here? if so, need to decide how dbi distinguishes between location of pot and location of usa
            'COMPLETE': [],
        }
        return args[task]
    def tx(self, task, obs):
        args = self.gen_args(task, obs)
        self._tx(task, obs, args)
    def tx_kill(self, obs):
        pid = self.dbi.get_obs_pid(obs)
        if pid is None:
            logger.debug('ActionClient.tx_kill: task running on %d is not alive' % obs)
        else:
            self._tx('KILL', obs, [str(pid)])

# XXX consider moving this class to a separate file
import scheduler
class Action(scheduler.Action):
    def __init__(self, obs, task, neighbor_status, still, task_clients, timeout=3600.):
        scheduler.Action.__init__(self, obs, task, neighbor_status, still, timeout=timeout)
        self.task_client = task_clients[still]
    def _command(self):
        logger.debug('Action: task_client(%s,%d)' % (self.task, self.obs))
        self.task_client.tx(self.task, self.obs)

class Scheduler(scheduler.Scheduler):
    def __init__(self, task_clients, actions_per_still=8, blocksize=10):
        scheduler.Scheduler.__init__(self, nstills=len(task_clients),
            actions_per_still=actions_per_still, blocksize=blocksize)
        self.task_clients = task_clients
    def kill_action(self, a):
        scheduler.Scheduler.kill_action(self, a)
        still = self.obs_to_still(a.obs)
        self.task_clients[still].tx_kill(a.obs)

class TaskHandler(SocketServer.BaseRequestHandler):
    def setup(self):
        #logger.debug('Connect: %s\n' % str(self.client_address))
        return
    def finish(self):
        #logger.debug('Disconnect: %s\n' % str(self.client_address))
        return
    def get_pkt(self):
        pkt = self.request[0]
        task, obs, still, args = from_pkt(pkt)
        return task, obs, still, args
    def handle(self):
        task, obs, still, args = self.get_pkt()
        logger.info('TaskHandler.handle: received (%s,%d) with args=%s' % (task,obs,' '.join(args)))
        if task == 'KILL':
            self.server.kill(int(args[0])) #TODO I THINK THIS IS WHERE WE HAVE A PROBLE. RUN and maybe COMPLETE need to clean up existing threads.
        elif task == 'COMPLETE':
            self.server.dbi.set_obs_status(obs, task)
        else:
            t = Task(task, obs, still, args, self.server.dbi, self.server.data_dir)
            self.server.append_task(t)
            t.run()

class TaskServer(SocketServer.UDPServer):
    allow_reuse_address = True
    def __init__(self, dbi, data_dir='.', port=STILL_PORT, handler=TaskHandler):
        SocketServer.UDPServer.__init__(self, ('', port), handler)
        self.active_tasks_semaphore = threading.Semaphore()
        self.active_tasks = []
        self.dbi = dbi
        self.data_dir = data_dir
        self.is_running = False
        self.watchdog_count = 0
    def append_task(self, t):
        self.active_tasks_semaphore.acquire()
        self.active_tasks.append(t)
        self.active_tasks_semaphore.release()
    def finalize_tasks(self, poll_interval=5.):
        while self.is_running:
            self.active_tasks_semaphore.acquire()
            new_active_tasks = []
            for t in self.active_tasks:
                if t.poll() is None: # not complete
                    new_active_tasks.append(t)
                    try:
                        c = t.process.children()[0]
                        #Check the affinity!
                        if len(c.cpu_affinity())<psutil.cpu_count():c.cpu_affinity(range(psutil.cpu_count()))
                        logger.debug('Proc info on {obsnum}:{task}:{pid} - cpu={cpu:.1f}%%, mem={mem:.1f}%%, Naffinity={aff}'.format(
                                    obsnum=t.obs,task=t.task,pid=c.pid,cpu=c.cpu_percent(interval=1.0),mem=c.memory_percent(),aff=len(c.cpu_affinity())))
                    except:
                        continue
                else:
                    t.finalize()
            self.active_tasks = new_active_tasks
            self.active_tasks_semaphore.release()
            time.sleep(poll_interval)
            if self.watchdog_count==100:
                logger.debug('TaskServer is alive')
                self.watchdog_count=0
            else:
                self.watchdog_count += 1
    def kill(self, pid):
        for task in self.active_tasks:
            if task.process.pid == pid:
                task.kill()
                break
    def start(self):
        self.is_running = True
        t = threading.Thread(target=self.finalize_tasks)
        t.start()
        logger.debug('this is scheduler.py')
        logger.debug("using code at: "+__file__)
        try:
            self.serve_forever()
        finally:
            self.shutdown()
            t.join()
    def shutdown(self):
        self.is_running = False
        for t in self.active_tasks:
            try: t.process.kill()
            except(OSError): pass
        SocketServer.UDPServer.shutdown(self)

