import threading
import time
import socket
import os
import tempfile
import platform
import urlparse
import cgi
import httplib
import urllib
import sys
import psutil

from string import upper

from BaseHTTPServer import BaseHTTPRequestHandler
from BaseHTTPServer import HTTPServer

from still_shared import InputThread
from still_shared import handle_keyboard_input

logger = True  # This is just here because the jedi syntax checker is dumb.

HOSTNAME = socket.gethostname()

PKT_LINE_LEN = 160
STILL_PORT = 14204
PLATFORM = platform.system()
FAIL_ON_ERROR = 0


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
        self.sg = TaskServer.sg

    def run(self):
        if self.process is not None:
            raise RuntimeError('Cannot run a Task that has been run already.')
        self.process = self._run()
        if self.process is None:
            self.record_failure()
        else:
            self.record_launch()
        return

    def _run(self):
        process = None

        logger.info('Task._run: (%s, %s) %s cwd=%s' % (self.task, self.obs, ' '.join(['do_%s.sh' % self.task] + self.args), self.cwd))
        # create a temp file descriptor for stdout and stderr
        self.OUTFILE = tempfile.TemporaryFile()
        self.outfile_counter = 0
        try:
            current_env = os.environ
            full_env = current_env  # Add obsnum and task to all
            process = psutil.Popen(['%s/do_%s.sh' % (self.path_to_do_scripts, self.task)] + self.args, cwd=self.cwd, env=full_env, stderr=self.OUTFILE, stdout=self.OUTFILE)
            process.nice(2)  # Jon : I want to set all the processes evenly so they don't compete against core OS functionality slowing things down.
            if PLATFORM != "Darwin":  # Jon : cpu_affinity doesn't exist for the mac, testing on a mac... yup... good story.
                process.cpu_affinity(range(psutil.cpu_count()))

            self.dbi.update_obs_current_stage(self.obs, self.task)
            self.dbi.add_log(self.obs, self.task, ' '.join(['%sdo_%s.sh' % (self.path_to_do_scripts, self.task)] + self.args + ['\n']), None)

        except Exception:
            logger.exception('Task._run: (%s,%s) error="%s"' % (self.task, self.obs, ' '.join(['%s/do_%s.sh' % (self.path_to_do_scripts, self.task)] + self.args)))
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

        os.wait()  # Might need to think about this one, communicate might be a better option but not sure

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

    def transmit(self, task, obs, action_type):
        conn_headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        status = ''
        response_status = -1
        response_reason = "Failed to connect"
        # respose_data = ""

        if action_type == "NEW_TASK":
            conn_type = "POST"
            conn_path = "/NEW_TASK"
            args = self.gen_args(task, obs)
            args_string = ' '.join(args)
            conn_params = urllib.urlencode({'obsnum': obs, 'task': task, 'args': args_string, 'env_vars': self.sg.env_vars})
            logger.debug('TaskClient.transmit: sending (%s,%s) with args=%s' % (task, obs, args_string))

        elif action_type == "KILL_TASK":
            conn_type = "GET"
            conn_path = "/KILL_TASK?" + obs
            conn_params = ""

        try:  # Attempt to open a socket to a server and send over task instructions
            logger.debug("connecting to TaskServer %s" % self.host_port[0])

            conn = httplib.HTTPConnection(self.host_port[0], self.host_port[1], timeout=20)
            conn.request(conn_type, conn_path, conn_params, conn_headers)
            response = conn.getresponse()
            response_status = response.status
            response_reason = response.reason
            response_data = response.read()
        except:
            logger.exception("Could not connect to server %s on port : %s" % (self.host_port[0], self.host_port[1]))
        finally:
            conn.close()

        if response_status != 200:  # Check if we did not recieve 200 OK
            self.error_count += 1
            logger.debug("Problem connecting to host : %s  has error count :%s" % (self.host_port[0], self.error_count))
            status = "FAILED_TO_CONNECT"
        else:
            status = "OK"
            logger.debug("Connection status : %s : %s" % (response_status, response_reason))
        return status, self.error_count

    def gen_args(self, task, obs):
        args = []
        pot, path, basename = self.dbi.get_input_file(obs)  # Jon: Pot I believe is host where file to process is, basename is just the file name
        outhost, outpath = self.dbi.get_output_location(obs)

        #  These varibles are here to be accessible to the arguments variable in the config file
        stillhost = self.dbi.get_obs_still_host(obs)
        stillpath = self.dbi.get_still_info(self.host_port[0]).data_dir
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
            try:  # Jon: Check if we actually have any custom args to process, if not then defaulting is normal behavior and not an exception
                args = eval(self.wf.action_args[task])
            except:
                logger.exception("Could not process arguments for task %s please check args for this task in config file, ARGS: %s" % (task, self.wf.action_args))
                args = [obs]
                # sys.exit(1)

        return args


class TaskHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)  # Return a response of 200, OK to the client
        self.end_headers()
        parsed_path = urlparse.urlparse(self.path)

        if upper(parsed_path.path) == "/KILL_TASK":
            try:
                obsnum = str(parsed_path.query)
                pid_of_obs_to_kill = self.server.dbi.get_obs_pid(obsnum)
                logger.debug("We recieved a kill request for obsnum: %s, shutting down pid: %s" % (obsnum, pid_of_obs_to_kill))
                self.server.kill(pid_of_obs_to_kill)
                self.send_response(200)  # Return a response of 200, OK to the client
                self.end_headers()
                logger.debug("Task killed for obsid: %s" % obsnum)
            except:
                logger.exception("Could not kill observation, url path called : %s" % self.path)
                self.send_response(400)  # Return a response of 200, OK to the client
                self.end_headers()
        elif upper(parsed_path.path) == "/INFO_TASKS":
            message = ""
            for mytask in self.server.active_tasks:
                try:
                    child_proc = mytask.process.children()[0]
                    if psutil.pid_exists(child_proc.pid):
                        message += mytask.obs + ':' + mytask.task + ':' + str(child_proc.pid) + \
                            ':' + str(child_proc.cpu_percent(interval=1.0)) + ':' + \
                            str(child_proc.memory_info_ex()[0]) + ':' + str(child_proc.cpu_times()[0]) + "\n"
                except:
                    logger.exception("Trying to send response to INFO request")

            self.wfile.write(message)

        return

    def do_POST(self):
        task_already_exists = False

        form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ={'REQUEST_METHOD': 'POST', 'CONTENT_TYPE': self.headers['Content-Type']})

        self.send_response(200)  # Return a response of 200, OK to the client
        self.end_headers()

        if upper(self.path) == "/NEW_TASK":                # New task recieved, grab the relavent bits out of the POST
            task = form.getfirst("task", "")
            obsnum = str(form.getfirst("obsnum", ""))
            still = form.getfirst("still", "")
            args = form.getfirst("args", "").split(' ')
            env_vars = form.getfirst("env_vars", "")
            logger.info('TaskHandler.handle: received (%s,%s) with args=%s' % (task, obsnum, ' '.join(args)))  # , ' '.join(env_vars)))

        if task == 'COMPLETE':
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


class TaskServer(HTTPServer):
    allow_reuse_address = True

    def __init__(self, dbi, sg, data_dir='.', port=STILL_PORT, handler=TaskHandler, path_to_do_scripts="."):
        global logger
        logger = sg.logger

        HTTPServer.__init__(self, (HOSTNAME, port), handler)  # Class us into HTTPServer so we can make calls from TaskHandler into this class via self.server.
        self.active_tasks_semaphore = threading.Semaphore()
        self.active_tasks = []
        self.dbi = dbi
        self.sg = sg
        self.data_dir = data_dir
        self.keep_running = False
        self.watchdog_count = 0
        self.port = port
        self.path_to_do_scripts = path_to_do_scripts
        self.logger = sg.logger

        logger.debug("Path to do_ Scripts : %s" % self.path_to_do_scripts)
        logger.debug("Data_dir : %s" % self.data_dir)
        logger.debug("Port : %s" % self.port)

    def append_task(self, t):
        self.active_tasks_semaphore.acquire()
        self.active_tasks.append(t)
        self.active_tasks_semaphore.release()

    def finalize_tasks(self, poll_interval=5.):
        self.user_input = InputThread()
        self.user_input.start()

        while self.keep_running:
            self.active_tasks_semaphore.acquire()
            new_active_tasks = []
            for mytask in self.active_tasks:
                if mytask.poll() is None:  # not complete
                    new_active_tasks.append(mytask)
                    try:
                        c = mytask.process.children()[0]
                        # Check the affinity!
                        if PLATFORM != "Darwin" and len(c.cpu_affinity()) < psutil.cpu_count():  # Jon : cpu_affinity doesn't exist for the mac, testing on a mac... yup... good story.
                            c.cpu_affinity(range(psutil.cpu_count()))
                    except:
                        continue
                else:
                    mytask.finalize()
            self.active_tasks = new_active_tasks
            self.active_tasks_semaphore.release()

            #  Jon: I think we can get rid of the watchdog as I'm already throwing this at the db
            time.sleep(poll_interval)
            if self.watchdog_count == 30:
                logger.debug('TaskServer is alive')
                for mytask in self.active_tasks:
                    try:
                        child_proc = mytask.process.children()[0]
                        if psutil.pid_exists(child_proc.pid):
                            logger.debug('Proc info on {obsnum}:{task}:{pid} - cpu={cpu:.1f}%%, mem={mem:.1f}%%, Naffinity={aff}'.format(
                                obsnum=mytask.obs, task=mytask.task, pid=child_proc.pid, cpu=child_proc.cpu_percent(interval=1.0),
                                mem=child_proc.memory_percent(), aff=len(child_proc.cpu_affinity())))
                    except:
                        pass
                self.watchdog_count = 0
            else:
                self.watchdog_count += 1

            self.keyboard_input = self.user_input.get_user_input()
            if self.keyboard_input is not None:
                handle_keyboard_input(self, self.keyboard_input)
        return

    def kill(self, pid):
        try:
            for task in self.active_tasks:
                if task.process.pid == pid:
                    task.kill()
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
        while self.keep_running is True:
            hostname = socket.gethostname()
            ip_addr = socket.gethostbyname(hostname)
            cpu_usage = psutil.cpu_percent()

            self.dbi.still_checkin(hostname, ip_addr, self.port, int(cpu_usage), self.data_dir, status="OK")
            time.sleep(60)
        return 0

    def start(self):
        psutil.cpu_percent()
        time.sleep(1)
        self.keep_running = True
        t = threading.Thread(target=self.finalize_tasks)
        t.daemon = True
        t.start()
        logger.debug('Starting Task Server')
        logger.debug("using code at: " + __file__)

        try:
            # Setup a thread that just updates the last checkin time for this still every 5min
            timer_thread = threading.Thread(target=self.checkin_timer)
            timer_thread.daemon = True  # Make it a daemon so that when ctrl-c happens this thread goes away
            timer_thread.start()  # Start heartbeat
            self.serve_forever()  # Start the lisetenser server
        finally:
            self.shutdown()
        return

    def shutdown(self):
        logger.debug("Shutting down task_server")
        hostname = socket.gethostname()
        ip_addr = socket.gethostbyname(hostname)
        cpu_usage = psutil.cpu_percent()
        self.dbi.still_checkin(hostname, ip_addr, self.port, int(cpu_usage), self.data_dir, status="OFFLINE")

        self.keep_running = False
        for t in self.active_tasks:
            try:
                t.process.kill()
            except():
                pass
        HTTPServer.shutdown(self)
        sys.exit(0)
