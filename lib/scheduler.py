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

logger = logging.getLogger('scheduler')
formating = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formating)

fh = logging.FileHandler("scheduler.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formating)

logger.addHandler(fh)
logger.addHandler(ch)

MAXFAIL = 5  # Jon : move this into config
TIME_INT_FOR_STILL_CHECK = 100


def action_cmp(x, y):
    return cmp(x.priority, y.priority)


class Action:
    '''An Action performs a task on an observation, and is scheduled by a Scheduler.'''

    def __init__(self, obs, task, neighbor_status, task_client, workflow, still, timeout=3600.):

        '''f:obs, task:target status,
        neighbor_status:status of adjacent obs (do not enter a status for a non-existent neighbor
        still:still action will run on.'''
        self.obs = obs
        self.task = task
        self.is_transfer = False
        self.neighbor_status = neighbor_status
        self.still = still
        self.priority = 0
        self.launch_time = -1
        self.timeout = timeout
        self.wf = workflow
        self.task_client = task_client

    def set_priority(self, p):
        '''Assign a priority to this action.  Highest priorities are scheduled first.'''
        self.priority = p

    def has_prerequisites(self):
        '''For the given task, check that neighbors are in prerequisite state.
        We don't check that the center obs is in the prerequisite state,
        s this action could not have been generated otherwise.'''

        try:
            index1 = self.wf.workflow_actions.index(self.wf.action_prereqs[self.task][0])
        except:
            return True
        try:
            index2 = self.wf.workflow_actions.index(self.wf.action_prereqs[self.task][1])
        except:
            index2 = None

        # logger.debug("has_prerequisites : Task : %s - Index1 : %s" % (self.task, self.wf.workflow_actions[index1]))
        # if index2 is not None:
        #     logger.debug("has_prerequisites : Task : %s - Index2 : %s" % (self.task, self.wf.workflow_actions[index2]))
        # logger.debug('Action.has_prerequisites: checking (%s,%s) neighbor_status=%s' % (self.task, self.obs, self.neighbor_status))

        for status_of_neighbor in self.neighbor_status:
            if status_of_neighbor is None:  # indicates that obs hasn't been entered into DB yet
                return False
            # logger.debug("Neighbor Status: %s" % status_of_neighbor)
            index_of_neighbor_status = self.wf.workflow_actions.index(status_of_neighbor)
            # logger.debug("Index1: %s, Index of neighbor status : %s" % (index1, index_of_neighbor_status))
            if index1 is not None and index_of_neighbor_status < index1:
                return False
            if index2 is not None:
                logger.debug("Index1: %s, Index of neighbor status : %s" % (index2, index_of_neighbor_status))
            if index2 is not None and index_of_neighbor_status >= index2:
                return False
            # logger.debug('Action.has_prerequisites: (%s,%s) prerequisites met' % (self.task, self.obs))
        # logger.debug("PreREqs: Going to return true...")
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
        return curtime > self.launch_time + self.timeout

    def run_remote_task(self, task=""):
        if task == "":
            task = self.task

        logger.debug('Action: task_client(%s,%s)' % (task, self.obs))
        connect_returned = self.task_client.transmit(task, self.obs)
        return connect_returned

class Scheduler:
    ###
    # A Scheduler reads a DataBaseInterface to determine what Actions can be
    # taken, and then schedules them on stills according to priority.'''
    ###
    def __init__(self, task_clients, workflow, sg):
        self.sg = sg  # Might as well have it around in case I find I need something from it...  Its just a little memory
        self.nstills = len(sg.hosts)  # preauto
        self.actions_per_still = sg.actions_per_still
        self.transfers_per_still = sg.transfers_per_still  # Jon : This isn't used...
        self.block_size = sg.block_size  # preauto
        self.timeout = sg.timeout
        self.sleep_time = sg.sleep_time

        self.lock_all_neighbors_to_same_still = workflow.lock_all_neighbors_to_same_still
        self.active_obs = []
        self._active_obs_dict = {}
        self.action_queue = []
        self.dbi = sg.dbi
        self.launched_actions = {}

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

    def find_all_stills(self):
        ###
        # find_all_stills : Check the database for all available stills with status OK
        ###
        logger.debug("looking for stills...")
        stills = self.dbi.get_available_stills()

        while stills.count() < 1:
            logger.debug("Can't find any stills! Waiting for 10sec and trying again")
            time.sleep(10)
            stills = self.dbi.get_available_stills()

        for still in stills:
            if still.hostname not in self.task_clients:
                logger.debug("Discovery of new still : %s" % still.hostname)
                self.task_clients[still.hostname] = TaskClient(self.dbi, still.hostname, self.wf, port=still.port)
                self.launched_actions[still.hostname] = []

        return

    def quit(self):
        self._run = False

    def ext_command_hook(self):
        return

    def start(self, dbi, ActionClass=None, action_args=()):
        '''Begin scheduling (blocking).
        dbi: DataBaseInterface'''

        self._run = True
        logger.info('Starting Scheduler')
        self.dbi = dbi
        last_checked_for_stills = time.time()

        while self._run:
            if (time.time() - last_checked_for_stills) > TIME_INT_FOR_STILL_CHECK:
                self.find_all_stills()
                last_checked_for_stills = time.time()
                logger.debug("Number of stills : %s" % len(self.task_clients))

            self.ext_command_hook()
            self.get_new_active_obs()
            self.update_action_queue(ActionClass, action_args)

            # Launch actions that can be scheduled
            for still in self.launched_actions:
                while len(self.get_launched_actions(still, tx=False)) < self.actions_per_still:
                    action_from_queue = self.pop_action_queue(still, tx=False)
                    if action_from_queue is not False:
                        self.launch_action(action_from_queue)
                    else:
                        logger.info("No actions available for still : %s" % still)
                        break  # move on to next still

            self.clean_completed_actions(self.dbi)
            time.sleep(self.sleep_time)

    def get_all_neighbors(self, obsnum):
        ###
        # get_all_neighbors: Go down (and up) the rabbit hole and find ALL the neighbors of a particular obsid
        ###
        neighbor_obs_nums = []
        neighbor_obs_nums.append(obsnum)  # Go ahead and add the current obsid to the list

        low_obs, high_obs = self.dbi.get_neighbors(obsnum)
        while high_obs is not None:  # Traverse the list UP to find all neighbors above this one
            neighbor_obs_nums.append(high_obs)
            high_obs = self.dbi.get_neighbors(high_obs)[1]

        while low_obs is not None:  # Traverse the list DOWN to find all neighbors above this one
            neighbor_obs_nums.append(low_obs)
            low_obs = self.dbi.get_neighbors(low_obs)[0]
        return neighbor_obs_nums

    def pop_action_queue(self, still, tx=False):
        '''Return highest priority action for the given still.'''
        # Seems like we're going through all the actions to find the ones for the particular still..
        # Should think about obtimizing at some point

        for i in xrange(len(self.action_queue)):
            a = self.action_queue[i]
            if a.still == still and a.is_transfer == tx:
                return self.action_queue.pop(i)
        return False

    def get_launched_actions(self, still, tx=False):
        return [a for a in self.launched_actions[still] if a.is_transfer == tx]

    def launch_action(self, a):
        '''Launch the specified Action and record its launch for tracking later.'''
        self.launched_actions[a.still].append(a)
        a.launch()

    def kill_action(self, a):
        logger.info('Scheduler.kill_action: called on (%s,%s)' % (a.task, a.obs))
        connect_returned = a.run_remote_task(task="STILL_KILL_OBS")
        if connect_returned == "FAILED_TO_CONNECT":
            logger.debug("We had an issue connecting to still : %s to kill task: %s for obsnum : %s" % (a.still, a.task, a.obs))
            return 1
        else:
            return 0

    def clean_completed_actions(self, dbi):
        '''Check launched actions for completion, timeout or fail'''
        for still in self.launched_actions:
            updated_actions = []
            for action in self.launched_actions[still]:
                # status = dbi.get_obs_status(action.obs)
                obs = dbi.get_obs(action.obs)
                status = obs.status
                pid = dbi.get_obs_pid(action.obs)

                try:
                    self.failcount[str(action.obs) + status]

                except(KeyError):
                    self.failcount[str(action.obs) + status] = 0

                if status == action.task:
                    logger.info('Task %s for obs %s on still %s completed successfully.' % (action.task, action.obs, still))

                elif action.timed_out():
                    logger.info('Task %s for obs %s on still %s TIMED OUT.' % (action.task, action.obs, still))
                    self.kill_action(action)
                    self.failcount[str(action.obs) + status] += 1
                    # XXX make db entry for documentation

                elif pid == -9:
                    self.failcount[str(action.obs) + status] += 1
                    logger.info('Task %s for obs %s on still %s HAS DIED. failcount=%d' % (action.task, action.obs, still, self.failcount[str(action.obs) + status]))

                else:  # still active
                    updated_actions.append(action)

            self.launched_actions[still] = updated_actions

    def already_launched(self, action):
        '''Determine if this action has already been launched.  Enforces
        fact that only one valid action can be taken for a given obs
        at any one time.'''
        for a in self.launched_actions[action.still]:
            if a.obs == action.obs:
                return True
        return False

    def get_new_active_obs(self):
        '''Check for any new obs that may have appeared.  Actions for
        these obs may potentially take priority over ones currently
        active.'''
        # XXX If actions have been launched since the last time this
        # was called, clean_completed_actions() must be called first to ensure
        # that cleanup occurs before.  Is this true? if so, should add mechanism
        # to ensure ordering
        observations = self.dbi.list_open_observations()  # Get only observations that are NOT :  NEW OR COMPLETE

        for open_obs in observations:

            if open_obs not in self._active_obs_dict:
                    self._active_obs_dict[open_obs] = len(self.active_obs)
                    self.active_obs.append(open_obs)
        return

    def update_action_queue(self, ActionClass=None, action_args=()):
        '''Based on the current list of active obs (which you might want
        to update first), generate a prioritized list of actions that
        can be taken.'''

        actions = []
        for myobs in self.active_obs:
            if self.dbi.get_obs(myobs).current_stage_in_progress == "FAILED":
                self.active_obs.remove(myobs)

            else:
                myaction = self.get_action(myobs, ActionClass=ActionClass, action_args=action_args)
                if (myaction is not None) and (self.already_launched(myaction) is not True):
                    if self.wf.prioritize_obs == 1:
                        myaction.set_priority(self.determine_priority(myaction))

                    actions.append(myaction)
                    #logger.debug("Actions - obs: %s : task: %s" % (myaction.obs, myaction.task))

        actions.sort(action_cmp, reverse=True)  # place most important actions first
        self.action_queue = actions  # completely throw out previous action list

        return

    def get_action(self, obsnum, ActionClass=None, action_args=()):
        '''Find the next actionable step for obs f (one for which all
        prerequisites have been met.  Return None if no action is available.
        This function is allowed to return actions that have already been
        launched.
        ActionClass: a subclass of Action, for customizing actions.
            None defaults to the standard Action'''
        obsinfo = self.dbi.get_obs(obsnum)
        status = obsinfo.status
        if obsinfo.current_stage_in_progress == "FAILED":
            return None

        # logger.debug("Obsid : %s    Status %s" % (obsnum, status))
        if status == 'COMPLETE':  # Jon: May be worth adding some code here to make sure to pop this observation out of the queue so we don't keep hitting it
            logger.debug("COMPLETE for obsid : %s" % obsnum)
            return None  # obs is complete

        neighbors = self.dbi.get_neighbors(obsnum)

        if None in neighbors:  # is this an end-file that can't be processed past UVCR?
            cur_step_index = self.wf.workflow_actions_endfile.index(status)
            next_step = self.wf.workflow_actions_endfile[cur_step_index + 1]

        else:  # this is a normal file
            cur_step_index = self.wf.workflow_actions.index(status)
            next_step = self.wf.workflow_actions[cur_step_index + 1]

        neighbor_status = [self.dbi.get_obs_status(n) for n in neighbors if n is not None]
        still = self.dbi.get_obs_still_host(obsnum)

        if not still:
            still = self.obs_to_still(obsnum)  # Get a still for a new obsid if one doesn't already exist.
            if self.lock_all_neighbors_to_same_still == 1:
                for neighbor in self.get_all_neighbors(obsnum):
                    self.dbi.set_obs_still_host(neighbor, still)

        if still != 0:  # If the obsnum is assigned to a server that doesn't exist at the moment we need to skip it, maybe reassign later
            if ActionClass is None:
                ActionClass = Action

            a = ActionClass(obsnum, next_step, neighbor_status, self.task_clients[still], self.wf, still, timeout=self.timeout)
            if self.wf.neighbors == 1:
                if a.has_prerequisites() is True:
                    return a
        # logging.debug('scheduler.get_action: (%s,%s) does not have prereqs' % (a.task, a.obs))
        return None

    def determine_priority(self, action):
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
            still = self.dbi.get_most_available_still()
            while not still:
                logger.info("Can't find any available still servers, they are all above 80% usage or have gone offline.  Waiting...")
                time.sleep(10)
                still = self.dbi.get_most_available_still()

            return still.hostname
