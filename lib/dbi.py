import os
import sys
import logging
# import hashlib
import psutil
import datetime
import numpy as np
from contextlib import contextmanager

# from subprocess import Popen, PIPE

from sqlalchemy import Table, BigInteger, Column, String, Integer, ForeignKey
from sqlalchemy import Float, func, DateTime, BigInteger, Text
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

# from still_shared import logger

# Based on example here: http://www.pythoncentral.io/overview-sqlalchemys-expression-language-orm-queries/
Base = declarative_base()

# Jon : Not sure why the logger is defined here?

logger = logging.getLogger('dbi')
formating = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formating)

fh = logging.FileHandler("dbi.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formating)

logger.addHandler(fh)
logger.addHandler(ch)
#########
#
#   Helper functions
#   Jon : Should move both of these to the add_observations for paper instead of here, they are workflow specific and don't interact with the database
#####


def jdpol2obsnum(jd, pol, djd):
    # Jon : I think at some point I want to move this function to a paper specific file
    """
    input: julian date float, pol string. and length of obs in fraction of julian date
    output: a unique index
    """
    import aipy as a
    dublinjd = jd - 2415020  # use Dublin Julian Date
    # print("JD: %s  dublinjd : %s") % (jd, dublinjd)
    # obsint = int(dublinjd / djd)  # divide up by length of obs
    obsint = int(round(dublinjd / djd))  # divide up by length of obs
    polnum = a.miriad.str2pol[pol] + 10
    # print("JD: %s  dublinjd : %s  polnum : %s") % (jd, dublinjd, polnum)
    assert(obsint < 2 ** 31)
    return int(obsint + polnum * (2 ** 32))


#############
#
#   The basic definition of our database
#
########

neighbors = Table("neighbors", Base.metadata,
                  Column("low_neighbor_id", String(100), ForeignKey("observation.obsnum"), primary_key=True),
                  Column("high_neighbor_id", String(100), ForeignKey("observation.obsnum"), primary_key=True)
                  )


class Observation(Base):
    __tablename__ = 'observation'
    # date = Column(BigInteger)  # Jon: Changed this to a biginteger for now... Though I can probably just pad my date
    date = Column(String(100))  # Jon: Changed this to a biginteger for now... Though I can probably just pad my date
    date_type = Column(String(100))
    pol = Column(String(4))
    # JON: removed default=updateobsnum, late, should figure out how to just override the alchamy base class thinggie.
    # obsnum = Column(BigInteger, default=updateobsnum, primary_key=True)
    # obsnum = Column(BigInteger, primary_key=True)
    obsnum = Column(String(100), primary_key=True)
    # status = Column(Enum(*FILE_PROCESSING_STAGES, name='FILE_PROCESSING_STAGES'))
    # Jon: There may be a very good reason to not just make this a string and I'm sure I will find out what it is soon enough
    status = Column(String(200))
    # last_update = Column(DateTime,server_default=func.now(),onupdate=func.current_timestamp())
    length = Column(Float)  # length of observation in fraction of a day
    currentpid = Column(Integer)
    stillhost = Column(String(100))
    stillpath = Column(String(200))
    outputpath = Column(String(200))
    outputhost = Column(String(100))
    current_stage_in_progress = Column(String(200))
    current_stage_start_time = Column(DateTime)
    high_neighbors = relationship("Observation",
                                  secondary=neighbors,
                                  primaryjoin=obsnum == neighbors.c.low_neighbor_id,
                                  secondaryjoin=obsnum == neighbors.c.high_neighbor_id,
                                  backref="low_neighbors",
                                  cascade="all, delete-orphan",
                                  single_parent=True)


class File(Base):
    __tablename__ = 'file'
    filenum = Column(Integer, primary_key=True)
    filename = Column(String(200))
    path_prefix = Column(String(200))
    host = Column(String(100))
    obsnum = Column(String(100), ForeignKey('observation.obsnum'))
    # this next line creates an attribute Observation.files which is the list of all
    #  files associated with this observation
    observation = relationship(Observation, backref=backref('files', uselist=True), cascade="all, delete-orphan", single_parent=True)
    md5sum = Column(Integer)


class Log(Base):
    __tablename__ = 'log'
    lognum = Column(BigInteger, primary_key=True)
#    obsnum = Column(BigInteger, ForeignKey('observation.obsnum'))
    # Jon: obsnum = Column(String(100), ForeignKey('observation.obsnum'))
    # Jon: There may be a very good reason to not just make this a string and I'm sure I will find out what it is soon enough
    obsnum = Column(String(100))
    stage = Column(String(200))
    # stage = Column(Enum(*FILE_PROCESSING_STAGES, name='FILE_PROCESSING_STAGES'))
    exit_status = Column(Integer)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    timestamp = Column(DateTime, nullable=False, default=func.current_timestamp())
    logtext = Column(Text)
    # observation = relationship(Observation, backref=backref('logs', uselist=True), cascade="all, delete-orphan", single_parent=True)


class Still(Base):
    __tablename__ = 'still'
    hostname = Column(String(100), primary_key=True)
    ip_addr = Column(String(50))
    port = Column(BigInteger)
    data_dir = Column(String(200))
    last_checkin = Column(DateTime, default=datetime.datetime.now(), onupdate=datetime.datetime.now())
    status = Column(String(100))
    current_load = Column(Integer)
    number_of_cores = Column(Integer)  # Jon : Placeholder for future expansion
    free_memory = Column(Integer)      # Jon : Placeholder for future expansion
    total_memory = Column(Integer)     # Jon : Placeholder for future expansion
    cur_num_of_tasks = Column(Integer)
    max_num_of_tasks = Column(Integer)
    free_disk = Column(BigInteger) # measured in bytes


class DataBaseInterface(object):
    def __init__(self, dbhost="", dbport="", dbtype="", dbname="", dbuser="", dbpasswd="", test=False):
        """
        Connect to the database and initiate a session creator.
         or
        create a FALSE database
        """

        if test:
            self.engine = create_engine('sqlite:///', connect_args={'check_same_thread': False}, poolclass=StaticPool)
            self.createdb()
        elif dbtype == 'postgresql':
            try:
                self.engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dbuser, dbpasswd, dbhost, dbport, dbname), echo=False, pool_size=20, max_overflow=100)  # Set echo=True to Enable debug mode
            except:
                logger.exception("Could not connect to the postgresql database.")
                sys.exit(1)
        elif dbtype == 'mysql':
            try:
                self.engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(dbuser, dbpasswd, dbhost, dbport, dbname), pool_size=20, max_overflow=40, echo=False)
            except:
                logger.exception("Could not connect to the mysql database.")
                sys.exit(1)
        try:
            self.Session = sessionmaker(bind=self.engine)
        except:
            logger.exception("Could not create database binding, please check database settings")
            sys.exit(1)

    @contextmanager
    def session_scope(self):
        '''
        creates a session scope
        can use 'with'

        Returns
        -------
        object: session scope to be used to access database with 'with'
        '''
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def test_db(self):
        tables = Base.metadata.tables.keys()
        return (len(tables) == 5)

    def list_observations(self):
        s = self.Session()
        # todo tests
        obsnums = [obs.obsnum for obs in s.query(Observation).filter(Observation.status != 'NEW')]
        s.close()
        return obsnums

    def list_observations_with_status(self, status):
        #
        # Get all observations with a given status
        #
        obsnums = []
        s = self.Session()
        try:
            obsnums = [obs.obsnum for obs in s.query(Observation).filter(Observation.status == status)]
        except:
            logger.debug("No new observations found.")
        s.close()
        return obsnums

    def list_open_observations(self):
        s = self.Session()
        try:
            obsnums = [obs.obsnum for obs in s.query(Observation).
                       filter((Observation.current_stage_in_progress != 'FAILED') | (Observation.current_stage_in_progress.is_(None))).
                       filter((Observation.current_stage_in_progress != 'KILLED') | (Observation.current_stage_in_progress.is_(None))).
                       filter(Observation.status != 'NEW').
                       filter(Observation.status != 'COMPLETE').all()]

        except:
            logger.debug("No open observations found.")
        s.close()
        return obsnums

    def list_observations_with_cur_stage(self, cur_stage):
        s = self.Session()
        obsnums = []
        try:
            obsnums = [obs.obsnum for obs in s.query(Observation).
                       filter(Observation.current_stage_in_progress == cur_stage).all()]
        except:
            logger.exception("No open observations found.")
        s.close()
        return obsnums

    def list_open_observations_on_tm(self, tm_hostname=None):
        s = self.Session()
        try:
            obsnums = [obs.obsnum for obs in s.query(Observation).
                       filter((Observation.current_stage_in_progress != 'FAILED') | (Observation.current_stage_in_progress.is_(None))).
                       filter((Observation.current_stage_in_progress != 'KILLED') | (Observation.current_stage_in_progress.is_(None))).
                       filter(Observation.status != 'NEW').
                       filter(Observation.status != 'COMPLETE').
                       filter(Observation.stillhost == tm_hostname).all()]

        except:
            logger.debug("No open observations found.")
        s.close()
        return obsnums

    def get_obs(self, obsnum):
        """
        retrieves an observation object.
        Errors if there are more than one of the same obsnum in the db. This is bad and should
        never happen

        todo:test
        """
        s = self.Session()
        # print("My obsnum! %s") % obsnum
        OBS = s.query(Observation).filter(Observation.obsnum == str(obsnum)).one()
        s.close()
        return OBS

    def update_obs(self, OBS):
        """
        Sends an observation object back to the db

        todo:test
        """
        s = self.Session()
        s.add(OBS)
        s.commit()
        s.close()
        return True

    def createdb(self):
        """
        creates the tables in the database.
        """
        Base.metadata.bind = self.engine
        Base.metadata.create_all()


    def add_log(self, obsnum, status, logtext, exit_status):
        """
        add a log entry about an obsnum, appears to only be run when a task is first started
        """
        current_datetime = datetime.datetime.now()
        LOG = Log(obsnum=obsnum, stage=status, logtext=logtext, exit_status=exit_status, start_time=current_datetime)
        s = self.Session()
        s.add(LOG)
        s.commit()
        s.close()

    def update_log(self, obsnum, status=None, logtext=None, exit_status=None, append=True):
        """
        replace the contents of the most recent log
        """
        s = self.Session()
        current_datetime = datetime.datetime.now()
        if s.query(Log).filter(Log.obsnum == str(obsnum)).count() == 0:
            s.close()
            self.add_log(str(obsnum), status, logtext, exit_status)
            return
        LOG = s.query(Log).filter(Log.obsnum == str(obsnum)).order_by(Log.timestamp.desc()).limit(1).one()
        LOG.end_time = current_datetime
        if exit_status is not None:
            LOG.exit_status = exit_status
        if logtext is not None:
            if append:
                LOG.logtext += logtext
            else:
                LOG.logtext = logtext
        if status is not None:
            LOG.status = status

        s.add(LOG)
        s.commit()
        s.close()

        return None

    def get_logs(self, obsnum, good_only=True):
        """
        return
        """
        s = self.Session()
        if good_only:
            LOGs = s.query(Log).filter(Log.obsnum == obsnum, Log.exit_status == 0)
        LOGs = s.query(Log).filter(Log.obsnum == obsnum)
        logtext = '\n'.join([LOG.logtext for LOG in LOGs])
        s.close()
        return logtext  # maybe this isn't the best format to be giving the logs

    def get_terminal_obs(self, nfail=5):
        """
        Get the obsids of things that have failed nfail times or more (and never completed).
        select obsnum from (select obsnum as obsnum, count(obsnum) as cnt from log where exit_status!=0
        group by obsnum) as myalias where cnt>5
        """
        s = self.Session()
        FAILED_LOG_COUNT_Q = s.query(Log.obsnum,
                                     func.count('*').label('cnt')).filter(Log.exit_status != 0).group_by(Log.obsnum).subquery()
        FAILED_LOGS = s.query(FAILED_LOG_COUNT_Q).filter(FAILED_LOG_COUNT_Q.c.cnt >= nfail)
        FAILED_OBSNUMS = map(int, [FAILED_LOG.obsnum for FAILED_LOG in FAILED_LOGS])
        s.close()
        return FAILED_OBSNUMS

    def add_observation(self, obsnum, date, date_type, pol, filename, host, outputhost='',
                        length=10 / 60. / 24, status='NEW', path_prefix=None):
        """
        create a new observation entry.
        returns: obsnum  (see jdpol2obsnum)
        Note: does not link up neighbors!
        """
        OBS = Observation(obsnum=obsnum, date=date, date_type=date_type, pol=pol, status=status, outputhost=outputhost, length=length)
        s = self.Session()
        s.add(OBS)
        s.commit()
        obsnum = OBS.obsnum
        s.close()
        self.add_file(obsnum, host, filename, path_prefix=path_prefix)
        return obsnum

    def add_file(self, obsnum, host, filename, path_prefix=None):
        """
        Add a file to the database and associate it with an observation.
        """
        if path_prefix is not None and not filename.startswith (path_prefix):
            raise Exception ('if using path_prefix, filename must start with it; got %s, %s' % (path_prefix, filename))

        FILE = File(filename=filename, host=host, path_prefix=(path_prefix or ''))
        # get the observation corresponding to this file
        s = self.Session()
        OBS = s.query(Observation).filter(Observation.obsnum == obsnum).one()
        FILE.observation = OBS  # associate the file with an observation
        s.add(FILE)
        s.commit()
        filenum = FILE.filenum  # we gotta grab this before we close the session.
        s.close()  # close the session
        return filenum

    def add_observations(self, obslist, status=''):  # HARDWF
        """
        Add a whole set of observations.
        Handles linking neighboring observations.

        input: list of dicts where the dict has the parameters needed as inputs to add_observation:
        julian_date
        pol (anything in a.miriad.str2pol)
        host
        file
        length (in fractional jd)
        neighbor_high (julian_date)
        neighbor_low  (julian_date)

        What it does:
        adds observations with status NEW
        Links neighboring observations in the database
        """
        neighbors = {}
        for obs in obslist:
            obsnum = self.add_observation(obs['obsnum'], obs['date'], obs['date_type'], obs['pol'],
                                          obs['filename'], obs['host'], outputhost=obs['outputhost'],
                                          length=obs['length'], status=obs['status'],
                                          path_prefix=obs.get('path_prefix'))

            neighbors[obsnum] = (obs.get('neighbor_low', None), obs.get('neighbor_high', None))

        s = self.Session()
        for middleobsnum in neighbors:
            OBS = s.query(Observation).filter(Observation.obsnum == middleobsnum).one()
            if neighbors[middleobsnum][0] is not None:

                L = s.query(Observation).filter(
                    Observation.date == str(neighbors[middleobsnum][0]),
                    Observation.pol == OBS.pol).one()
                OBS.low_neighbors = [L]
            if not neighbors[middleobsnum][1] is None:
                H = s.query(Observation).filter(
                    Observation.date == str(neighbors[middleobsnum][1]),
                    Observation.pol == OBS.pol).one()
                OBS.high_neighbors = [H]
                sys.stdout.flush()
            OBS.status = status
            s.add(OBS)
            s.commit()
        s.close()
        return neighbors.keys()

    def delete_test_obs(self):
        s = self.Session()
        obsnums = [obs.obsnum for obs in s.query(Observation).filter(Observation.outputhost == "UNITTEST")]
        s.close()
        for obsnum in obsnums:
            self.delete_obs(obsnum)

    def delete_obs(self, obsnum):
        #
        # Delete an obseration and its associated entry in File table
        # Jon: Does not seem to want to auto delete assocaited file, need to fix

        s = self.Session()
        obslist = s.query(Log).filter(Log.obsnum == obsnum)
        for obs in obslist:
            s.delete(obs)
            s.commit()
        obslist = s.query(File).filter(File.obsnum == obsnum)
        for obs in obslist:
            s.delete(obs)
            s.commit()

        try:
            OBS = s.query(Observation).filter(Observation.obsnum == obsnum).one()
            s.delete(OBS)
            s.commit()
        except:
            pass

        s.close()

    def get_neighbors(self, obsnum):
        """
        get the neighbors given the input obsnum
        input: obsnum
        return: list of two obsnums
        If no neighbor, returns None the list entry

        Todo: test. no close!!
        """
        s = self.Session()
        OBS = s.query(Observation).filter(Observation.obsnum == obsnum).one()
        try:
            high = OBS.high_neighbors[0].obsnum
        except(IndexError):
            high = None
        try:
            low = OBS.low_neighbors[0].obsnum
        except(IndexError):
            low = None
        s.close()
        return (low, high)

    def get_obs_still_host(self, obsnum):
        """
        input: obsnum
        output: host
        """

        OBS = self.get_obs(obsnum)
        return OBS.stillhost

    def set_obs_still_host(self, obsnum, host):
        """
        input: obsnum, still host
        retuns: True for success, False for failure
        """
        OBS = self.get_obs(obsnum)
        OBS.stillhost = host
        yay = self.update_obs(OBS)
        return yay

    def get_obs_still_path(self, obsnum):
        """
        input: obsnum
        returns: path to assigned scratch space on still
        """
        OBS = self.get_obs(obsnum)
        return OBS.stillpath

    def set_obs_still_path(self, obsnum, path):
        """
        input: obsnum, path to assigned scratch space on still
        returns: True for success, False for failure
        """
        OBS = self.get_obs(obsnum)
        OBS.stillpath = path
        yay = self.update_obs(OBS)
        return yay

    def get_obs_pid(self, obsnum):
        """
        todo
        """
        OBS = self.get_obs(obsnum)
        return OBS.currentpid

    def set_obs_pid(self, obsnum, pid):
        """
        set to -1 if no task is running
        """
        OBS = self.get_obs(obsnum)
        OBS.currentpid = pid
        yay = self.update_obs(OBS)
        return yay

    def get_input_file(self, obsnum, apply_path_prefix=False):
        """
        input:observation number
        return: host,path (the host and path of the initial data set on the pot)

        todo:test
        """
        mypath = ""
        myhost = ""
        myfile = ""
        path_prefix = ""
        s = self.Session()
        OBS = s.query(Observation).filter(Observation.obsnum == obsnum).one()
        # Jon: Maybe make the like statement a variable in the config file? for now I will cheat for a bit
        # try:
        #    POTFILE = s.query(File).filter(
        #        File.observation == OBS,
        #        # File.host.like('%pot%'), # XXX temporarily commenting this out.
        #        # need a better solution for finding original file
        #        File.filename.like('%uv')).one()
        # except:
        #    logger.exception("Could not get input file for OBS: %s " % obsnum)

        # Jon : FIX ME!!!!!
        try:
            POTFILE = s.query(File).filter(File.observation == OBS).first()
            myhost = POTFILE.host
            mypath = os.path.dirname(POTFILE.filename)
            myfile = os.path.basename(POTFILE.filename)
            path_prefix = POTFILE.path_prefix
        except:
            pass
        s.close()

        if not apply_path_prefix:
            return myhost, mypath, myfile

        if not mypath.startswith (path_prefix):
            raise Exception ('internal consistency failure: filename should start with %s but got %s'
                             % (path_prefix, mypath))

        if path_prefix == '':
            return myhost, '', mypath, myfile

        parent_dirs = mypath[len (path_prefix)+1:]
        return myhost, path_prefix, parent_dirs, myfile

    def get_output_location(self, obsnum):
        """
        input: observation number
        return: host,path
        TODO: test
        """
        # right now we're pointing the output at the input location (nominally whatever pot
        #    the data came from
        host, path, inputfile = self.get_input_file(obsnum)
        return host, path

    def set_obs_status(self, obsnum, status):
        """
        change the satus of obsnum to status
        input: obsnum (key into the observation table, returned by add_observation and others)
        """
        OBS = self.get_obs(obsnum)
        OBS.status = status
        self.update_obs(OBS)
        return True

    def get_obs_status(self, obsnum):
        """
        retrieve the status of an observation
        """
        OBS = self.get_obs(obsnum)
        status = OBS.status

        return status

    def get_obs_latest_log(self, obsnum):
        """Return the latest log item associated with an obsnum. Returns None
        if the obsnum has no log items.

        """
        with self.session_scope() as s:
            item = s.query(Log).filter (Log.obsnum == obsnum).order_by(Log.timestamp.desc ()).first ()
            if item is None:
                return None
            return {
                'stage': item.stage,
                'exit_status': item.exit_status,
                'start_time': item.start_time,
                'end_time': item.end_time,
                'logtext': item.logtext,
            }

    def get_available_stills(self):
        ###
        # get_available_stills : Retrun all stills that have checked in within the past 3min and have status of "OK"
        ###
        since = datetime.datetime.now() - datetime.timedelta(minutes=3)
        s = self.Session()
        stills = s.query(Still).filter(Still.last_checkin > since, Still.status == "OK").all()
        s.close()
        return stills

    def get_still_info(self, hostname):
        ###
        # get_still_info : Return all the information of a still given its hostname
        ###
        s = self.Session()
        still = s.query(Still).filter(Still.hostname == hostname).first()
        s.close()
        return still

    def still_checkin(self, hostname, ip_addr, port, load, data_dir, status="OK", max_tasks=2, cur_tasks=0):
        """Check to see if the still entry already exists in the database, if it does
        update the timestamp, port, data_dir, and load. If does not exist then
        go ahead and create an entry.

        """
        # Collect load statistics and classify ourselves as under duress if
        # anything is too excessive.

        current_load = os.getloadavg()[1] #use the 5 min load average

        vmem = psutil.virtual_memory()
        free_memory = vmem.available / (1024 ** 3)
        total_memory = vmem.total / (1024 ** 3)

        fs_info = os.statvfs (data_dir)
        free_disk = fs_info.f_frsize * fs_info.f_bavail

        duress = (
            current_load >= 80 or # normalize to n_cpu()?
            free_memory < 1 or # measured in gigs
            free_disk < 2147483648 # measured in bytes; = 3 gigs
        )

        if duress and status == 'OK':
            logger.warn ('still is under duress: load %s, free mem %s, free disk %s',
                         current_load, free_memory, free_disk / 1024**3)
            status = 'DURESS'

        # Now actually update the database.

        s = self.Session()

        if s.query(Still).filter(Still.hostname == hostname).count() > 0:  # Check if the still already exists, if so just update the time
            still = s.query(Still).filter(Still.hostname == hostname).one()
            still.last_checkin = datetime.datetime.now()
            still.status = status
            # print("STILL_CHECKIN, test mode, setting load = 0, change back before release")
            # still.current_load = 0
            still.current_load = current_load
            still.number_of_cores = psutil.cpu_count()
            still.free_memory = round(free_memory, 2)
            still.total_memory = round(total_memory, 2)
            still.data_dir = data_dir
            still.port = port
            still.max_num_of_tasks = max_tasks
            still.cur_num_of_tasks = cur_tasks
            still.free_disk = free_disk

            s.add(still)
        else:  # Still doesn't exist, lets add it
            still = Still(hostname=hostname, ip_addr=ip_addr, port=port, current_load=load,
                          data_dir=data_dir, status=status, max_num_of_tasks=max_tasks, cur_num_of_tasks=cur_tasks)
            s.add(still)

        s.commit()
        s.close()
        return 0

    def mark_still_offline(self, hostname):
        s = self.Session()
        still = s.query(Still).filter(Still.hostname == hostname).one()
        still.status = "OFFLINE"
        s.add(still)
        s.commit()
        s.close()

    def get_most_available_still(self):
        ###
        # get_most_available_still : Grab the still with the least load that has checked in within the past 3min and has status OK and load under 80%
        ###
        s = self.Session()
        since = datetime.datetime.now() - datetime.timedelta(minutes=3)
        still = s.query(Still.hostname).filter(Still.last_checkin > since, Still.status == "OK").order_by(Still.current_load).all()
        np.random.shuffle(still)
        s.close()
        # JON ADD: Return least used and none if over the limit

        return str(still[0][0])

    def get_obs_assigned_to_still(self, still_hostname):
        s = self.Session()
        observations = s.query(Observation).filter(Observation.stillhost == still_hostname)
        s.close()
        return observations

    def update_obs_current_stage(self, obsnum, current_stage_in_progress):
        s = self.Session()
        obs = s.query(Observation).filter(Observation.obsnum == str(obsnum)).one()

        obs.current_stage_in_progress = current_stage_in_progress
        obs.current_stage_start_time = datetime.datetime.now()

        s.add(obs)
        s.commit()
        s.close()
