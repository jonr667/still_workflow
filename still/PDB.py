#! /usr/bin/env python
"""
Creates a database object for parsing and descending into the paper database.

DFM
"""

import MySQLdb,sqlite3
import hashlib,logging
logger = logging.getLogger('PDB')
#global definitions
TEST=True
HOSTIP='10.0.1.20'
USER='obs'
PASSWD='P9ls4R*@'
if TEST:
    DBNAME='test'
else:
    DBNAME='psa128'

#construct objects to lay out database schema.

def unpack(xin):
    """
    descends into nested tuples and recasts as list
    """
    if xin==None:
        return []
    else:
        xout = []
        for i in xin:
            if not type(i) == tuple:
                xout.append(i)
            else:
                xout.append(unpack(i))
        return xout

def gethostname():
    from subprocess import Popen,PIPE
    hn = Popen(['bash','-cl','hostname'], stdout=PIPE).communicate()[0].strip()
    return hn
def file2jd(zenuv):
    return re.findall(r'\d+\.\d+', zenuv)[0]

class column(object):
    """
    Container for information to a single column in a database.
    """
    def __init__(self, name, dtype, key=None):
        self.name = name
        self.dtype = dtype
        self.key = key

    def parse_key(self):
        """
        For initializing pdb. Adds the proper syntax for denoting the primary key of a table.
        """
        try:
            if self.key.startswith("pk"):
                return " PRIMARY KEY,"
            else:
                return ","
        except(AttributeError):
            return ","

    def init(self):
        """
        Add an entry to the INSERT statment for the column using proper mysql syntax.
        """
        q = " %s %s"%(self.name, self.dtype)
        q += self.parse_key()
        return q

    def link(self,tabname):
        """
        Adds a link (foreign key) to the column.
        """
        try:
            if self.key.startswith("fk"):
                ref = self.key.split(':')[-1]
                return "ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s;"%(tabname, self.name, ref)
            else:
                return None
        except(AttributeError):
            return None

class table(object):
    """
    An object for information of a table in pdb.
    """
    def __init__(self, name):
        self.name = name
        self.cols = {}

    def __getitem__(self, key):
        return self.cols[key]

    def addcol(self, name, dtype, key=None):
        """
        Adds a column to the table object (Not the db). If it's the primary key, table.pk gets populated with the column name.
        """
        self.cols[name] = column(name,dtype,key)
        if key=='pk':
            self.pk = name

    def init(self):
        """
        Write the command for creating the table in the database.
        """
        q = """CREATE TABLE IF NOT EXISTS %s ("""%self.name
        for c in self.cols:
            q += self[c].init()
        q = q[:-1] + ");"
        return q

    def link(self):
        """
        Generate mysql command for adding links among tables.
        """
        q = ""
        for c in self.cols:
            _q = self[c].link(self.name)
            if not _q is None:
                q += _q
        return q

class db(object):
    """
    An object to handle the paper database.
    """
    def __init__(self,name,verbose=False,test=False):
        self.name = name
        self.verbose = verbose
        self.tabs = {}
        self.test = test
        if test: self.db = sqlite3.connect(':memory:')
        else: self.db = MySQLdb.connect(HOSTIP, USER, PASSWD, DBNAME,autocommit=True)
        for tab in self.get_tables():
            self.addtab(tab)
            for col,dtype,key in self.get_cols(tab):
                self[tab].addcol(col,dtype,key=key)

    def __getitem__(self, key):
        return self.tabs[key]

    def __del__(self):
        self.db.close()

    def get_tables(self):
        """
        return a list of table names in pdb.
        """
        c = self.db.cursor()
        if self.test: c.execute("select name from sqlite_master;")
        else: c.execute("SHOW TABLES;")
        return [t[0] for t in unpack(c.fetchall())]

    def get_cols(self, table):
        """
        return a list of column name/datatype pairs for table 'table'
        """
        c = self.db.cursor()
        if self.test: c.execute('pragma table_info({table}'.format(table=table))
        else: c.execute("SHOW COLUMNS IN %s;"%table)
        _cols = c.fetchall()
        cols = []
        for name,dtype,null,key,default,extra in _cols:
            if key == "PRI":
                key = "pk"
            else:
                key = None
            cols.append([name,dtype,key])
        return cols

    def addtab(self,name):
        """
        Add a table to the database object --- this doesn't add a new table to the actual database.
        """
        self.tabs[name] = table(name)

    def print_schema(self):
        """
        Sends a human-readable schema of the database to stdout.
        """
        cout = "%s\n"%self.name
        for tname in self.tabs.keys():
            cout += " --- %s\n"%tname
            for col in self[tname].cols:
                cout += "\t --- %s (%s)"%(self[tname][col].name,self[tname][col].dtype)
                if not self[tname][col].key is None:
                    cout += " [%s]"%self[tname][col].key
                cout += "\n"
        print cout

    def populate(self):
        """
        Populate an empty database with properly-linked tables.
        """
        #first create the tables:
        for t in self.tabs.keys():
            q = self[t].init()
            self.db.query(q)
        #next link foreign keys:
        for t in self.tabs.keys():
            q = self[t].link()
            for _q in q.split(';'):
                #a wrapper to deal with _mysql's inability to parse multiple commands on the same line.
                if not _q == "":
                    _q += ";"
                    if self.verbose: print _q
                    self.db.query(_q)

    def drop_tables(self):
        """
        Deletes all tables from the database. Good for testing.
        """
        for t in self.tabs:
            q = "DROP TABLE IF EXISTS %s;"%t
            if self.verbose: print q
            self.db.query(q)

    def has_record(self, tabname, key, col=None):
        """
        Returns true if table pdb.tabname contains a row whose entry for the column 'col' given by key. If no column is given, the primary key is
        assumed.
        """
        cursor = self.db.cursor()
        if col is None:
            q = "SELECT EXISTS(SELECT 1 FROM %s WHERE %s='%s');"%(tabname, self[tabname].pk, key)
        else:
            q = "SELECT EXISTS(SELECT 1 FROM %s WHERE %s='%s');"%(tabname, col, key)
        if self.verbose: print q
        cursor.execute(q)
        result = cursor.fetchone()[0]
        return bool(int(result))

    def addrow(self, tabname, values):
        """
        Adds a row to pdb.tabname whose column/value pairs are given as the key/value pairs of dictionary 'values'
        """
        q = """INSERT INTO %s ("""%tabname
        q += ", ".join(values.keys())
        q += ") VALUES ("
        q += ", ".join(self.format_values(tabname, values))
        q += ");"
        if self.verbose: print q
        self.db.query(q)

    def delrow(self, tabname, pk):
        """
        deletes a record from pdb.tabname whose primary key is pk
        """
        pk = self.format_values(tabname, {self[tabname].pk:pk})[0]
        q = """DELETE FROM %s WHERE %s=%s"""%(tabname, self[tabname].pk, pk)
        if self.verbose: print q
        self.db.query(q)

    def format_values(self, tabname, v):
        """
        Converts python strings into something that mysql understands.
          --- tabname is the table you're updating
          --- v is a dictionary with {'column name': column value} pairs.
              ---- example
              ---- >>> python_hostname = "qmaster"
              ---- >>> v = {'hostname': python_hostname}
        """
        vret = []
        for vi in v.keys():
            if self[tabname][vi].dtype in ['varchar(256)','mediumtext']:
                vret.append("'%s'"%v[vi])
            else:
                vret.append(v[vi])
        return vret

    def get(self, target, tab, col, val):
        """
        retrieve target column of entries in table tab, whose column is equal to val.
        """
        q = """SELECT %s FROM %s WHERE"""%(target,tab)
        if type(col) is list:
            constraints = [" %s=%s "%(c, self.format_values(tab, {c:v})[0]) for (c,v) in zip(col,val)]
            q += 'and'.join(constraints)[:-1]+';'
        else:
            q+=" %s=%s;"%(col, self.format_values(tab, {col:val})[0])
        if self.verbose: print q
        cursor = self.db.cursor()
        cursor.execute(q)
        return unpack(cursor.fetchall())

    def update(self, target_col, target_val, tab, col, val):
        """
        Change the entry of target_col to target_val in table tab for rows with col=val.
        """
        target_val = self.format_values(tab, {target_col: target_val})[0]
        val = self.format_values(tab, {col: val})[0]
        q = """UPDATE %s SET %s=%s WHERE %s=%s;"""%(tab, target_col, target_val, col, val)
        if self.verbose: print q
        self.db.query(q)
    def count(self, target,table,column,value):
        """
        count the number of entries where table.column=value
        """
        q = """select count(*) from {table} where {column}={value};""".format(
            table=table,
            column=column,
            value=value)
        if self.verbose: print q
        cursor = self.db.cursor()
        cursor.execute(q)
        return unpack(cursor.fetchall())[0][0]
    def query(self,q):
        cursor = self.db.cursor()
        cursor.execute(q)
        return unpack(cursor.fetchall())
class StillDB(db):
    def add_observations():
        """
        bring over from the python script of the same name.
        """
    def get_obs_index(basefile):
        """
        input:basefile
        return: unique index based on JD and Pol
        """

    def list_observations(limit=1e5):
        """
        Get a list of files  return type is list of (observations or files. not sure)
        note that filenames are defined as host:path pairs to convey the complete location coordinates.
        inputs:none
        outputs:all basefiles in db
        """
        #return self.query("""select filename from history where exit_status=0 and 
        #            basefile not in (select basefile from history where status='COMPLETE');""")
        return self.query("""select basefiles from observations;""")
#    def is_completed(basefile):
#        """
#        checks the execution status of the input filename. 
#        Note: filename is not a primary key. Here we return only the most recent status
#        returns True (completed = succesful or error) or False (execution ongoing)
#        """
#        #select count(*) from history where filename=filename;
#        #if count>0
#        status = self.query("""select exit_status from history where filename={filename} order by stoptime desc
#        count=1;""".format(filename=filename))[0][0]
#        if status is not None: return True
#        else: return False
    def get_obs_status(basefile):
        """
        returns the current status of the requested file.  NULL indicates currently running, None indicates that file
        does not exist and is not currently scheduled
        input: full basefile "host:/path/to/file"
        return: valid and most recently complete FILE_PROCESSING_STAGE
        TODO: update for new return. no more NULL
        """
        #first check if the filename exists
        if self.count("history","filename",filename)==0:
            return None
        #select status from history where filename=filename
        status = self.get('exit_status','history','status',filename)[0][0]
        return status

    def get_neighbors(basefile):
        """
        returns neighbors of the input filename 
        #input: full filename "host:/path/to/file"
        input: basefile
        return: neighboring basefiles. Always return two items.
        TODO: update for new i/o
        """
        host=filename.split(':')[0]
        #select neighbor observations
        basefile    =   self.get('basefile','history','filename',filename)[0][0]
        jd_lo       =   self.get('jd_lo','observations','basefile',basefile)[0][0]
        basefile_lo =   self.get('basefile','observations','JD',jd_lo)[0][0]
        jd_hi       =   self.get('jd_hi','observations','basefile',basefile)[0][0]
        basefile_hi =   self.get('basefile','observations','JD',jd_hi)[0][0]
        
        #select neighbor files (if available)
        q="""select basefile from history where (basefile='{basefile_hi}' or basefile='{basefile_lo}')
        and host={host}';""".format(
        basefile_hi=basefile_hi,
        basefile_lo=basefile_lo,
        filename=filename,
        host=host)
        neighbors = [b[0] for b in self.query(q)] #this line uncompresses the 2d result to a 1d list
        return neighbors

    def begin_task(infile,outfile,operation,pid):
        """
        registers a task with the database. 
        input: infile, outfile, operation, pid
        """
        hostname = outfile.split(':')[0]
        if not self.has_record('hosts', hostname):
            if self.verbose:
                logger.info("Unidentified host %s"%hostname)
            return 1
        if not self.has_record('files',infile):
            if self.verbose:
                logger.info("Unidentified file %s"%filename)
            return 1
        self.update('last_modified',"NOW()",'files','filename',infile) #update the files table entry
        
        #update history table
        histcols = {}
        histcols['input']  = infile
        histcols['output'] = outfile
        histcols['host'] = hostname
        histcols['operation'] = operation
        histcols['starttime'] = "NOW()"
        #get the infile basefile, note that we already checked that it exists in the files table.
        histcols['basefile'] = self.get('basefile','files','filename',infile)[0][0] #get basefile from files where filename=infile
        
        self.addrow('history', histcols)        
        return True
    def conclude_task(filename,log=None,exit_status=1):
        """
        registers a task as concluded. 
        filename = host:/path/to/file
        log=string log output from task
        exit_status = exit status of script. $? environment variable in bash default=1 (generic error state)
        returns True if successful
        """
        if not self.has_record('history',outfile,col='output'):
            logger.info("entry in the history column doesn't exist!")
        q = """update history set exit_status={exit_status} and stoptime=NOW() where filename={filename} and 
                exit_status is NULL;""".format(
            exit_status=exit_status,
            filename=filename)
        self.query(q)
        if not opts.log is None:
            self.update('log',log,'history','output',outfile)
        return True
    def add_file(filename):
        """
        adds a file to the file table and ties it back to the observations table.
        This table is THE RECORD of data and where it can be found.
        filename = host:/path/to/file
        retunrs True if successful 
        """
        hostname = filename.split(':')[0]
        
        if not self.has_record('hosts', hostname):
            print 'host not in pdb.hosts'
            sys.exit(1)
        JD = file2jd(filename)
        pol = file2pol(filename)
        filecols = {}
        filecols['JD'] = JD
        filecols['filename'] = filename
        filecols['host'] = hostname
        filecols['basefile'] = self.query("""select basefile from observations where JD={JD} and pol={pol};""")
        filecols['md5'] = md5sum(hostname)
        filecols['last_modified']="NOW()"
        self.addrow('files', filecols)
        return True
    
    def kill_task(filename):
        """
        kills any currently running task generating a file matching "filename"
        """
        tasks = self.get('PID','history',['filename','exit_status'],[filename,'NULL'])
        host = filename.split(':')[0]
        for task in tasks:
            os.Popen("ssh {host} 'kill -9 {pid}'".format(host=host,pid=pid)) #note I am not 100% on this line here
        self.conclude_task(filename,status="ERROR")
        return True
    
pdb = StillDB(DBNAME,verbose=False,test=True)

def md5sum(fname):
    """
    calculate the md5 checksum of a file whose filename entry is fname.
    """
    fname = fname.split(':')[-1]
    BLOCKSIZE=65536
    hasher=hashlib.md5()
    try:
        afile=open(fname, 'rb')
    except(IOError):
        afile=open("%s/visdata"%fname, 'rb')
    buf=afile.read(BLOCKSIZE)
    while len(buf) >0:
        hasher.update(buf)
        buf=afile.read(BLOCKSIZE)
    return hasher.hexdigest()

if __name__ == "__main__":
    pdb.print_schema()
