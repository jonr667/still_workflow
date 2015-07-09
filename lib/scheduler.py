import time
import sys
import logging
# import readchar
import os
# import datetime


from task_server import TaskClient
# import datetime
# from still_shared import logger

#  Setup the lib path ./lib/  as a spot to check for python libraries
# basedir = os.path.dirname(os.path.realpath(__file__))[:-3]
# sys.path.append(basedir + 'bin')


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('scheduler')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('scheduler.log')
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.addHandler(fh)

MAXFAIL = 5  # Jon : move this into config
TIME_INT_FOR_STILL_CHECK = 100


def action_cmp(x, y):
    return cmp(x.priority, y.priority)


class Action:
    '''An Action performs a task on an observation, and is scheduled by a Scheduler.'''
    # def __init__(self, obs, task, neighbor_status, still, workflow, timeout=3600., task_clients=[]):  # PD
    def __init__(self, obs, task, neighbor_status, task_client, workflow, still, timeout=3600.):

        '''f:obs, task:target status,
        neighbor_status:status of adjacent obs (do not enter a status for a non-existent neighbor
        still:still action will run on.'''
        self.obs = obs
        self.task = task
        self.is_transfer = False  # = (task == 'POT_TO_USA')  # XXX don't like hardcoded value here HARDWF JON: commented out POT_TO_USA part, I don't think its used anymore
        self.neighbor_status = neighbor_status
        self.still = still  # PD
        self.priority = 0
        self.launch_time = -1
        self.timeout = timeout
        self.wf = workflow
        # self.task_client = task_clients[still]  # PD
        self.task_client = task_client

    def set_priority(self, p):
        '''Assign a priority to this action.  Highest priorities are scheduled first.'''
        self.priority = p

    def has_prerequisites(self):
        '''For the given task, check that neighbors are in prerequisite state.
        We don't check that the center obs is in the prerequisite state,
        s this action could not have been generated otherwise.'''

        # Jon: I'm leaving this, it only accepting 2 at the moment but it would probably be nice to come back and clean this up
        # to support however many
        # index1, index2 = FILE_PROCESSING_PREREQS[self.task]

        try:
            index1, index2 = self.wf.workflow_actions.index(self.wf.action_prereqs[self.task])
        except:
            return True

        print("has_prerequisites : Task : %s - Index1 : %s - Index2 : %s") % (self.task, self.wf.workflow_actions[index1], self.wf.workflow_actions[index2])
        logger.debug('Action.has_prerequisites: checking (%s,%s) neighbor_status=%s' % (self.task, self.obs, self.neighbor_status))

        for status_of_neighbor in self.neighbor_status:
            if status_of_neighbor is None:  # indicates that obs hasn't been entered into DB yet
                return False

            index_of_neighbor_status = self.wf.workflow_actions.index(status_of_neighbor)
            if index1 is not None and index_of_neighbor_status < index1:
                return False
            if index2 is not None and index_of_neighbor_status >= index2:
                return False
            # logger.debug('Action.has_prerequisites: (%s,%s) prerequisites met' % (self.task, self.obs))

        return True

    def launch(self, launch_time=None):
        '''Run this task.'''
        if launch_time is None:
            launch_time = time.time()
        self.launch_time = launch_time
        logger.debug('Action: launching (%s,%s) on still %s' % (self.task, self.obs, self.task_client.host_port[0]))
        return self.run_remote_task()

    def timed_out(self, curtime=None):
        assert(self.launch_time > 0)  # Error out if action was not launched
        if curtime is None:
            curtime = time.time()
        logger.debug("curtime %s, launch_time : %s, timeout : %s" % (curtime, self.launch_time, self.timeout))
        return curtime > self.launch_time + self.timeout

    def run_remote_task(self):
        logger.debug('Action: task_client(%s,%s)' % (self.task, self.obs))
        self.task_client.transmit(self.task, self.obs)


class Scheduler:
    '''A Scheduler reads a DataBaseInterface to determine what Actions can be
    taken, and then schedules them on stills according to priority.'''

    # to make instantiating the object little nicer
    def __init__(self, task_clients, workflow, dbi='', nstills=4, actions_per_still=8, transfers_per_still=2, blocksize=10, timeout=3600., sleep=10):  # PD
        '''nstills:           # of stills in system,
           actions_per_still: # of actions that can be scheduled simultaneously per still.'''
        self.nstills = nstills  # preauto
        self.actions_per_still = actions_per_still
        self.transfers_per_still = transfers_per_still  # Jon : Not overly sure on this one
        self.blocksize = blocksize  # preauto
        self.active_obs = []
        self._active_obs_dict = {}
        self.action_queue = []
        self.dbi = dbi
        self.launched_actions = {}  # XPD
#        for still in xrange(nstills):  # preauto
#            self.launched_actions[still] = []  # preauto
        self._run = False
        self.failcount = {}
        self.wf = workflow  # Jon: Moved the workflow class to instantiated on object creation, should do the same for dbi probably
        self.task_clients = {}
        # If task_clients is set to AUTO then check the db for still servers
        if task_clients[0].host_port[0] == "AUTO":
            self.find_all_stills()
            self.auto = 1

        else:
            self.auto = 0
            self.task_clients = task_clients

        self.timeout = timeout
        self.sleep = sleep

    def find_all_stills(self):
        print("looking for stills...")
        stills = self.dbi.get_available_stills()

        while stills.count() < 1:
            print("Can't find any stills! Waiting for 10sec and trying again")
            time.sleep(10)
            stills = self.dbi.get_available_stills(self.wf.name)

        for still in stills:
            print(still.hostname)
            if still.hostname not in self.task_clients:
                logger.debug("Did not found host %s in task_clients dict, adding" % still.hostname)
                print("Found still : %s") % still.hostname
                self.task_clients[still.hostname] = TaskClient(self.dbi, still.hostname, self.wf, port=still.port)
                self.launched_actions[still.hostname] = []  # XPD
                print(self.task_clients)

        return

    def quit(self):
        self._run = False

    def ext_command_hook(self):
        return

    def start(self, dbi, ActionClass=None, action_args=()):
        '''Begin scheduling (blocking).
        dbi: DataBaseInterface'''
        print(self.wf.action_prereqs)
        self._run = True
        logger.info('Scheduler.start: entering loop')
        self.dbi = dbi
        last_checked_for_stills = time.time()

        while self._run:
            if (time.time() - last_checked_for_stills) > TIME_INT_FOR_STILL_CHECK:
                self.find_all_stills()
                last_checked_for_stills = time.time()
            logger.debug("Number of stills : %s" % len(self.task_clients))
            # tic = time.time()
            self.ext_command_hook()
            logger.info("getting active obs")
            self.get_new_active_obs(dbi)
            logger.info('updating action queue')
            self.update_action_queue(dbi, ActionClass, action_args)
            # Launch actions that can be scheduled
            logger.info('launching actions')
            for still in self.launched_actions:  # PD
                while len(self.get_launched_actions(still, tx=False)) < self.actions_per_still:
                    try:
                        a = self.pop_action_queue(still, tx=False)
                    except(IndexError):  # no actions can be taken on this still
                        logger.info('No actions available for still : %s\n' % still)
                        break  # move on to next still
                    self.launch_action(a)
                while len(self.get_launched_actions(still, tx=True)) < self.transfers_per_still:
                    try:
                        a = self.pop_action_queue(still, tx=True)
                    except(IndexError):  # no actions can be taken on this still
                        logger.info('No actions available for still : %s\n' % still)
                        break  # move on to next still
                    self.launch_action(a)
            self.clean_completed_actions(self.dbi)

#            if window.getch() == 'q':
#                self.quit()

            time.sleep(self.sleep)

    def pop_action_queue(self, still, tx=False):
        '''Return highest priority action for the given still.'''
        # Seems like we're going through all the actions to find the ones for the particular still..
        for i in xrange(len(self.action_queue)):
            a = self.action_queue[i]
            if a.still == still and a.is_transfer == tx:
                return self.action_queue.pop(i)
        raise IndexError('No actions available for still : %s\n' % still)

    def get_launched_actions(self, still, tx=False):
        return [a for a in self.launched_actions[still] if a.is_transfer == tx]  # PD

    def launch_action(self, a):
        '''Launch the specified Action and record its launch for tracking later.'''
        self.launched_actions[a.still].append(a)
        a.launch()

    def kill_action(self, a):
        '''Subclass this to actually kill the process.'''
        logger.info('Scheduler.kill_action: called on (%s,%s)' % (a.task, a.obs))

    def clean_completed_actions(self, dbi):
        '''Check launched actions for completion, timeout or fail'''
        for still in self.launched_actions:  # PD
            updated_actions = []
            for cnt, a in enumerate(self.launched_actions[still]):  # PD
                status = dbi.get_obs_status(a.obs)
                pid = dbi.get_obs_pid(a.obs)
                try:
                    self.failcount[str(a.obs) + status]
                except(KeyError):
                    self.failcount[str(a.obs) + status] = 0
                if status == a.task:
                    logger.info('Task %s for obs %s on still %s completed successfully.' % (a.task, a.obs, still))
                    # Jon: Going to use this space to lock a task to a specific server instead of the taskserver
                    # this should keep the taskserver more generic to eventually accomidate multiple workflows simultaniously

                    # not adding to updated_actions removes this from list of launched actions
                    if status == self.wf.still_locked_after:  # on first copy of data to still, record in db that obs is assigned here
                        dbi.set_obs_still_host(a.obs, a.still)
                        # dbi.set_obs_still_path(a.obs, os.path.abspath(self.cwd))  # Jon: Not sure how to get this over yet *FIXME*
                elif a.timed_out():
                    logger.info('Task %s for obs %s on still %s TIMED OUT.' % (a.task, a.obs, still))
                    self.kill_action(a)
                    self.failcount[str(a.obs) + status] += 1
                    # XXX make db entry for documentation
                elif pid == -9:
                    self.failcount[str(a.obs) + status] += 1
                    logger.info('Task %s for obs %s on still %s HAS DIED. failcount=%d' % (a.task, a.obs, still, self.failcount[str(a.obs) + status]))
                else:  # still active
                    updated_actions.append(a)
            self.launched_actions[still] = updated_actions  # PD

    def already_launched(self, action):
        '''Determine if this action has already been launched.  Enforces
        fact that only one valid action can be taken for a given obs
        at any one time.'''
        for a in self.launched_actions[action.still]:  # PD
            if a.obs == action.obs:
                return True
        return False

    def get_new_active_obs(self, dbi):
        '''Check for any new obs that may have appeared.  Actions for
        these obs may potentially take priority over ones currently
        active.'''
        # XXX If actions have been launched since the last time this
        # was called, clean_completed_actions() must be called first to ensure
        # that cleanup occurs before.  Is this true? if so, should add mechanism
        # to ensure ordering
        observations = dbi.list_open_observations()  # Get only observations that are NOT :  NEW OR COMPLETE

        for open_obs in observations:

            if open_obs not in self._active_obs_dict:
                    self._active_obs_dict[open_obs] = len(self.active_obs)
                    self.active_obs.append(open_obs)
        return

    def update_action_queue(self, dbi, ActionClass=None, action_args=()):
        '''Based on the current list of active obs (which you might want
        to update first), generate a prioritized list of actions that
        can be taken.'''
        failed = dbi.get_terminal_obs()
        for f in self.active_obs:
            print("My active obs...: %s") % f
        actions = [self.get_action(dbi, f, ActionClass=ActionClass, action_args=action_args) for f in self.active_obs]
        actions = [a for a in actions if a is not None]  # remove unactionables
        actions = [a for a in actions if not self.already_launched(a)]  # filter actions already launched
        actions = [a for a in actions if self.failcount.get(str(a.obs) + dbi.get_obs_status(a.obs), 0) < MAXFAIL]  # filter actions that have utterly failed us
        actions = [a for a in actions if a.obs not in failed]  # Filter actions that have failed before

        if self.wf.prioritize_obs == 1:
            for a in actions:
                a.set_priority(self.determine_priority(a, dbi))

        actions.sort(action_cmp, reverse=True)  # place most important actions first

        self.action_queue = actions  # completely throw out previous action list
        for i in actions:
            print("Actions - obs: %s : task: %s") % (i.obs, i.task)

        return

    def get_action(self, dbi, obs, ActionClass=None, action_args=()):
        '''Find the next actionable step for obs f (one for which all
        prerequisites have been met.  Return None if no action is available.
        This function is allowed to return actions that have already been
        launched.
        ActionClass: a subclass of Action, for customizing actions.
            None defaults to the standard Action'''
        status = dbi.get_obs_status(obs)
        print("Obsid : %s    Status %s") % (obs, status)
        if status == 'COMPLETE':  # Jon: May be worth adding some code here to make sure to pop this observation out of the queue so we don't keep hitting it
            print("COMPLETE for obsid : %s") % obs
            return None  # obs is complete
        neighbors = dbi.get_neighbors(obs)
        print("Neighbors for obs %s : ") % obs
        print("The neighbors : ", neighbors)

        if None in neighbors:  # is this an end-file that can't be processed past UVCR?
            # next_step = ENDFILE_PROCESSING_LINKS[status]
            print("Status : %s") % status
            cur_step_index = self.wf.workflow_actions_endfile.index(status)
            next_step = self.wf.workflow_actions_endfile[cur_step_index + 1]
        else:  # this is a normal file
            # next_step = FILE_PROCESSING_LINKS[status]
            cur_step_index = self.wf.workflow_actions.index(status)
            next_step = self.wf.workflow_actions[cur_step_index + 1]

        neighbor_status = [dbi.get_obs_status(n) for n in neighbors if n is not None]
        # Jon : just for some info on neighbors, remove later
        print("Neighbor Status : ") % neighbor_status

        still = self.obs_to_still(obs)  # Get a still for a new obsid if one doesn't already exist, TODO: CHECK NEIGHBORS!
        if still != 0:  # If the obsnum is assigned to a server that doesn't exist at the moment we need to skip it, maybe reassign later
            if ActionClass is None:
                ActionClass = Action

            a = ActionClass(obs, next_step, neighbor_status, self.task_clients[still], self.wf, still, timeout=self.timeout)  # XPD

            if self.wf.neighbors == 1:
                if a.has_prerequisites():
                    return a
        # logging.debug('scheduler.get_action: (%s,%s) does not have prereqs' % (a.task, a.obs))
        return None

    def determine_priority(self, action, dbi):
        '''Assign a priority to an action based on its status and the time
        order of the obs to which this action is attached.'''
    #    print("From determine_priority, action.obs : %s : ") % action.obs
        pol, jdcnt = int(action.obs) / 2 ** 32, int(action.obs) % 2 ** 32  # XXX maybe not make this have to explicitly match dbi bits
        return jdcnt * 4 + pol  # prioritize first by time, then by pol
        # XXX might want to prioritize finishing a obs already started before
        # moving to the latest one (at least, up to a point) to avoid a
        # build up of partial obs.  But if you prioritize obs already
        # started too excessively, then the queue could eventually fill with
        # partially completed tasks that are failing for some reason

    def obs_to_still(self, obs):
        ##############
        #   Check if a obsid has a still already, if it does simply return it.  If it does not then lets find the lowest
        #   loaded (cpu) one and assign it.  If none are under 80% then lets just wait around, they're busy enough as is.
        ##############
        mystill = self.dbi.get_obs_still_host(obs)
        if mystill:
            if mystill in self.task_clients:
                return mystill
            else:  # We couldn't find its still server as its not in task_clients for whatever reason so punt for now
                logger.debug("Obs attached to non-existant STILL OBS : %s, STILL %s" % (obs, mystill))
                return 0

        else:
            self.find_all_stills()  # Update stills to make sure we have them all loaded
            still = self.dbi.get_most_available_still()
            while not still:

                logger.info("Can't find any available still servers, they are all above 80% usage or have gone offline.  Waiting...")
                time.sleep(10)
                still = self.dbi.get_most_available_still()

            return still.hostname
