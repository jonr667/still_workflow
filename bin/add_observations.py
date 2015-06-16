import psycopg2
import psycopg2.extras
import numpy as np


def ingest_addtional_opsids(sg):
    # will maybe change this over to SQL alchemy later
    # Throwing it in now as straight SQL to get things working
    # so I can move onto other parts for the moment
    try:
        pgconn = psycopg2.connect(database=sg.dbname, user=sg.dbuser, host=sg.dbhost, port=sg.dbport, password=sg.dbpasswd)
    except:
        print("I am unable to connect to the database")
        exit(1)
    print("Probably connected")
#    cur_dict = pgconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)  # Get a sql cursor that supports returning data as a dict
    cur = pgconn.cursor()  # Normal cursor, non-dict
    # Get all the new OBS id's avaiable from the NGAS postgresql database and check against files that are currently in the still
    # as well as files that are in the mwa_qc db and have thus already been processed
    cur.execute("""SELECT cast(substring(foreign_ngas_files.file_id, 1,10) AS bigint)
                   FROM
                      foreign_ngas_files
                   WHERE
                      cast(substring(foreign_ngas_files.file_id, 1,10) AS varchar(10)) NOT IN (SELECT cast(obsid AS varchar(10)) FROM foreign_mwa_qc WHERE obsid IS NOT NULL)
                      AND cast(substring(foreign_ngas_files.file_id, 1,10) AS varchar(10)) NOT IN (SELECT cast(obsnum AS varchar(10)) FROM observation WHERE obsnum IS NOT NULL)
                   LIMIT 50""")

    rows = cur.fetchall()
    unique_obsids = np.unique(rows)  # Lets just make sure we trim out all the extra obs id's we get from each having multiple files associated with it

    for obsid in unique_obsids:  # We now need to add all the files that are associated with each obs id to the db as well as the primary entry
        print("Adding obsid: %s") % obsid
        sg.db.add_observation(obsnum=obsid, date=obsid, date_type='GPS', pol=0, filename='none', host='none', length=0, status='NEW')  # Add primary entry for obsnum
        sg.db.add_file(obsid, "myhost", "/some/path/do/something")
    return 0


def get_all_nags_files_for_obsid(sg, obsid):
        # Get all the files associated with each unique obsid, there is some redundency here but went for readability of logic over
        # most effecient
    myfiles = []

    try:
        pgconn = psycopg2.connect(database=sg.dbname, user=sg.dbuser, host=sg.dbhost, port=sg.dbport, password=sg.dbpasswd)
    except:
        print("I am unable to connect to the database")
        exit(1)
    cur_dict = pgconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)  # Get a sql cursor that supports returning data as a dict
    cur_dict.execute("""SELECT file_name, host_id, file_id, mount_point
                        FROM foreign_ngas_files
                        INNER JOIN foreign_ngas_disks
                        USING (disk_id)
                        WHERE cast(substring(foreign_ngas_files.file_id, 1,10) AS varchar(10)) = '%(myobsid)s' LIMIT 100""", {'myobsid': obsid})
    rows = cur_dict.fetchall()
    for file_info in rows:  # build the full path to each file based on ngas info and push each one into the db as a file of a unique obsid
        print(file_info)
        path = file_info['mount_point'] + '/' + file_info['file_name']
        myfiles.append(path)
        print(path)
        #            sg.db.add_file(obsid, file_info['host_id'][:-5], path)

    return 0
