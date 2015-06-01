import os
import sys
import logging
import hashlib
import configparser
# import numpy as n

from subprocess import Popen, PIPE

from sqlalchemy import Table, Column, String, Integer, ForeignKey
from sqlalchemy import Float, func, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
# from sqlalchemy.pool import QueuePool

# Jon added this for some testing and it needs to be removed

from still.scheduler import FILE_PROCESSING_STAGES

# Uncomment the following line after you hose Jon's stuff above
# from ddr_compress.scheduler import FILE_PROCESSING_STAGES

# Based on example here: http://www.pythoncentral.io/overview-sqlalchemys-expression-language-orm-queries/
Base = declarative_base()

# Jon : Not sure why the logger is defined here?
logger = logging.getLogger('dbi')

# dbinfo = {'username':'obs',
#          'password':'',
#          'hostip':'qmaster',
#          'port':3306,
#          'dbname':'test'}


#########
#
#   Useful helper functions
#
#####
def jdpol2obsnum(jd, pol, djd):
    """
    input: julian date float, pol string. and length of obs in fraction of julian date
    output: a unique index
    """
    import aipy as a
    dublinjd = jd - 2415020  # use Dublin Julian Date
    obsint = int(dublinjd/djd)  # divide up by length of obs
    polnum = a.miriad.str2pol[pol]+10
    assert(obsint < 2**31)
    return int(obsint + polnum*(2**32))


def updateobsnum(context):
    """
    helper function for Observation sqlalchemy object.
    used to calculate the obsnum on creation of the record
    """
    return jdpol2obsnum(context.current_parameters['date'],
                        context.current_parameters['pol'],
                        context.current_parameters['length'])


def md5sum(fname):
    """
    calculate the md5 checksum of a file whose filename entry is fname.
    """
    fname = fname.split(':')[-1]
    BLOCKSIZE = 65536
    hasher = hashlib.md5()
    try:
        afile = open(fname, 'rb')
    except(IOError):
        afile = open("%s/visdata" % fname, 'rb')
    buf = afile.read(BLOCKSIZE)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()


def gethostname():
    hn = Popen(['bash', '-cl', 'hostname'], stdout=PIPE).communicate()[0].strip()
    return hn
#############
#
#   The basic definition of our database
#
########

neighbors = Table("neighbors", Base.metadata,
                  Column("low_neighbor_id", BigInteger, ForeignKey("observation.obsnum"), primary_key=True),
                  Column("high_neighbor_id", BigInteger, ForeignKey("observation.obsnum"), primary_key=True)
                  )


class Observation(Base):
    __tablename__ = 'observation'
    date = Column(BigInteger)  # Jon: Changed this to a biginteger for now... Though I can probably just pad my date
    date_type = Column(String(100))
    pol = Column(String(4))
    # JON: removed default=updateobsnum, late, should figure out how to just override the alchamy base class thinggie.
    # obsnum = Column(BigInteger, default=updateobsnum, primary_key=True)
    obsnum = Column(BigInteger, primary_key=True)
    status = Column(Enum(*FILE_PROCESSING_STAGES, name='FILE_PROCESSING_STAGES'))
    # last_update = Column(DateTime,server_default=func.now(),onupdate=func.current_timestamp())
    length = Column(Float)  # length of observation in fraction of a day
    currentpid = Column(Integer)
    stillhost = Column(String(100))
    stillpath = Column(String(200))
    outputpath = Column(String(200))
    outputhost = Column(String(100))
    high_neighbors = relationship("Observation",
                                  secondary=neighbors,
                                  primaryjoin=obsnum == neighbors.c.low_neighbor_id,
                                  secondaryjoin=obsnum == neighbors.c.high_neighbor_id,
                                  backref="low_neighbors")


class File(Base):
    __tablename__ = 'file'
    filenum = Column(Integer, primary_key=True)
    filename = Column(String(200))
    host = Column(String(100))
    obsnum = Column(BigInteger, ForeignKey('observation.obsnum'))
    # this next line creates an attribute Observation.files which is the list of all
    #  files associated with this observation
    observation = relationship(Observation, backref=backref('files', uselist=True))
    md5sum = Column(Integer)


class Log(Base):
    __tablename__ = 'log'
    lognum = Column(Integer, primary_key=True)
    obsnum = Column(BigInteger, ForeignKey('observation.obsnum'))
    stage = Column(Enum(*FILE_PROCESSING_STAGES, name='FILE_PROCESSING_STAGES'))
    exit_status = Column(Integer)
    timestamp = Column(DateTime, nullable=False, default=func.current_timestamp())
    logtext = Column(Text)


# note the Cal object/table is added here
# to provide support for omnical.
# the DataBaseInterface Class does not currently support Cal
class Cal(Base):
    __tablename__ = 'cal'
    calnum = Column(Integer, primary_key=True)
    obsnum = Column(BigInteger, ForeignKey('observation.obsnum'))
    last_activity = Column(DateTime, nullable=False, default=func.current_timestamp())
    cal_date = Column(DateTime)
    calfile = Column(Text)
    output_dir = Column(Text)
    input_file = Column(Text)
    logtext = Column(Text)
    observation = relationship(Observation, backref=backref('cals', uselist=True))


class DataBaseInterface(object):
    def __init__(self, configfile='~/.ddr_compress/still.cfg', test=False):
        """
        Connect to the database and initiate a session creator.
         or
        create a FALSE database

        db.cfg is the default setup. Config files live in ddr_compress/configs
        To use a config file, copy the desired file ~/.paperstill/db.cfg
        """
        if configfile is not None:
            config = configparser.ConfigParser()
            configfile = os.path.expanduser(configfile)
            if os.path.exists(configfile):
                logger.info('loading file ' + configfile)
                config.read(configfile)
                self.dbinfo = config['dbinfo']
                # .decode is not needed in python 3
                # self.dbinfo['password'] = self.dbinfo['password'].decode('string-escape')
            else:
                logging.info(configfile + " Not Found")
        if test:
            self.engine = create_engine('sqlite:///',
                                        connect_args={'check_same_thread': False},
                                        poolclass=StaticPool)
            self.createdb()
        else:
            if self.dbinfo['dbtype'] == 'postgresql':
                try:
                    print(self.dbinfo['password'])
                    self.engine = create_engine(
                        'postgresql+psycopg2://{username}:{password}@{hostip}:{port}/{dbname}'.format(**self.dbinfo), echo=True)
                except:
                    print("Could not connect to the postgresql database.")
                    sys.exit(1)
            else:
                self.engine = create_engine(
                    'mysql://{username}:{password}@{hostip}:{port}/{dbname}'.format(
                                **self.dbinfo),
                    pool_size=20,
                    max_overflow=40)
        self.Session = sessionmaker(bind=self.engine)

    def test_db(self):
        tables = Base.metadata.tables.keys()
        print("found %i tables" % len(tables))
        s = self.Session()
        count = s.query(Observation).count()
        print("found %i records" % (count))
        return (len(tables) == 3)

    def list_observations(self):
        s = self.Session()
        # todo tests
        obsnums = [obs.obsnum for obs in s.query(Observation).filter(Observation.status != 'NEW')]
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
        OBS = s.query(Observation).filter(Observation.obsnum == obsnum).one()
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
        add a log entry about an obsnum
        """
        LOG = Log(obsnum=obsnum, stage=status, logtext=logtext, exit_status=exit_status)
        s = self.Session()
        s.add(LOG)
        s.commit()
        s.close()

    def update_log(self, obsnum, status=None, logtext=None, exit_status=None, append=True):
        """
        replace the contents of the most recent log
        """
        s = self.Session()
        if s.query(Log).filter(Log.obsnum == obsnum).count() == 0:
            s.close()
            self.add_log(obsnum, status, logtext, exit_status)
            return
        LOG = s.query(Log).filter(Log.obsnum == obsnum).order_by(Log.timestamp.desc()).limit(1).one()
        if exit_status is not None:
            LOG.exit_status = exit_status
        if logtext is not None:
            if append:
                LOG.logtext += logtext
            else:
                LOG.logtext = logtext
        if status is not None:
            LOG.status = status
        print("LOG.exit_status = ", LOG.exit_status)
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

    def add_observation(self, date, date_type, pol, filename, host, length=10 / 60. / 24, status='UV_POT'):
        """
        create a new observation entry.
        returns: obsnum  (see jdpol2obsnum)
        Note: does not link up neighbors!
        """
        OBS = Observation(date=date, date_type=date_type, pol=pol, status=status, length=length)
        print(OBS)
        s = self.Session()
        s.add(OBS)
        s.commit()
        obsnum = OBS.obsnum
        s.close()
        self.add_file(obsnum, host, filename)  # todo test.
        sys.stdout.flush()
        return obsnum

    def add_file(self, obsnum, host, filename):
        """
        Add a file to the database and associate it with an observation.
        """
        FILE = File(filename=filename, host=host)
        # get the observation corresponding to this file
        s = self.Session()
        OBS = s.query(Observation).filter(Observation.obsnum == obsnum).one()
        FILE.observation = OBS  # associate the file with an observation
        s.add(FILE)
        s.commit()
        filenum = FILE.filenum  # we gotta grab this before we close the session.
        s.close()  # close the session
        return filenum

    def add_observations(self, obslist, status='UV_POT'):
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
            obsnum = self.add_observation(obs['date'], obs['date_type'], obs['pol'],
                                          obs['filename'], obs['host'],
                                          length=obs['length'], status='NEW')
            neighbors[obsnum] = (obs.get('neighbor_low', None), obs.get('neighbor_high', None))
        s = self.Session()
        for middleobsnum in neighbors:
            OBS = s.query(Observation).filter(Observation.obsnum == middleobsnum).one()
            if not neighbors[middleobsnum][0] is None:
                L = s.query(Observation).filter(
                    Observation.date == neighbors[middleobsnum][0],
                    Observation.pol == OBS.pol).one()
                OBS.low_neighbors = [L]
            if not neighbors[middleobsnum][1] is None:
                H = s.query(Observation).filter(
                    Observation.date == neighbors[middleobsnum][1],
                    Observation.pol == OBS.pol).one()
                OBS.high_neighbors = [H]
                sys.stdout.flush()
            OBS.status = status
            s.add(OBS)
            s.commit()
        s.close()
        return neighbors.keys()

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

    # todo this functions
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

    def get_input_file(self, obsnum):
        """
        input:observation number
        return: host,path (the host and path of the initial data set on the pot)

        todo:test
        """
        s = self.Session()
        OBS = s.query(Observation).filter(Observation.obsnum == obsnum).one()
        POTFILE = s.query(File).filter(
            File.observation == OBS,
            # File.host.like('%pot%'), # XXX temporarily commenting this out.
            # need a better solution for finding original file
            File.filename.like('%uv')).one()
        host = POTFILE.host
        path = os.path.dirname(POTFILE.filename)
        file = os.path.basename(POTFILE.filename)
        return host, path, file

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

# def get_neighbors(self,obsnum):
#        """
#        for now lets search for neighbors based on time
#        formally, look for observations within 1.2 of the length of the input
#
#        return: list of obsnums. Always len=2.  None indicates no neighbor
#        """
#        s = self.Session()
#        OBS = s.query(Observation).filter(Observation.obsnum==obsnum).one()
#        NEIGHBORS = s.query(Observation).filter(
#                        func.abs(Observation.julian_date-OBS.julian_date)<(1.2*OBS.length),Observation.obsnum!=OBS.obsnum,Observation.pol==OBS.pol)
#        neighborobsnums = [o.obsnum for o in NEIGHBORS]
#        while len(neighborobsnums)<2:
#            neighborobsnums.append(None)
#        return neighborobsnums
