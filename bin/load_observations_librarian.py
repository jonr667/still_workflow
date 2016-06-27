#! /usr/bin/env python
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License

"""Ask the Librarian for new files that we need to process, and load them
into our database. "Files that need processing" are:

  - have a "source" of the "Correlator"
  - do not have a history item with a "type" of "rtp.successfully-processed"
  - are not already in our database

"""
from __future__ import absolute_import, division, print_function

# add our local modules to the Python search path
import os.path, sys
basedir = os.path.dirname (os.path.dirname (os.path.realpath (__file__)))
sys.path.insert (0, os.path.join (basedir, 'lib'))

import optparse, re
import numpy as np

from dbi import jdpol2obsnum, DataBaseInterface, Still
from still import get_dbi_from_config, process_client_config_file, SpawnerClass, WorkFlow
import hera_librarian

librarian_source = 'Correlator'
rtp_processed_key = 'rtp.successfully-processed'
initial_status = 'NEW'


def path_to_jd (path):
    return re.findall(r'\d+\.\d+', path)[0]


def path_to_pol (path):
    return re.findall(r'\.(.{2})\.', path)[0]


def try_get_file_djd (filerec):
    # XXX open up the miriad file and look for a header, once we start
    # inserting the needed header!
    return None


def augmented_file_to_obsinfo (filerec):
    return {
        'obsnum': filerec['obsnum'],
        'date': filerec['jd'],
        'date_type': 'julian',
        'pol': filerec['pol'],
        'host': filerec['store_host'],
        'filename': filerec['name'],
        'outputhost': '',
        'status': initial_status,
        'length': filerec['djd'],
    }


def main (args):
    o = optparse.OptionParser ()
    o.set_usage ('load_observations_librarian.py')
    o.set_description (__doc__)
    o.add_option('--connection', help='the name of the Librarian connection to use (specified in .hl_client.cfg)')
    opts, args = o.parse_args (args)

    # Some boilerplate to set up the database interface ...
    spawner = SpawnerClass()
    workflow = WorkFlow()
    spawner.config_file = os.path.join (basedir, 'etc/still.cfg')
    process_client_config_file (spawner, workflow)
    dbi = get_dbi_from_config (spawner.config_file)
    dbi.test_db ()

    # Get the list of potentially-relevant files from the Librarian.

    lc = hera_librarian.LibrarianClient (opts.connection)
    try:
        listing = lc.list_files_without_history_item (librarian_source,
                                                      rtp_processed_key)
    except hera_librarian.RPCFailedError as e:
        print ('RPC to librarian failed: %s' % e.message)
        sys.exit (1)

    try:
        files = listing['files']
        len (files)
    except Exception as e:
        print ('unexpected response from librarian: %s' % e)
        sys.exit (1)

    if not len (files):
        print ('No new files.')
        return

    # for each file we should have a dict of at least:
    #
    # name              -- something like 2456892/zen.2456892.49664.xx.uv
    # obsid             -- the obsid associated with this file
    # create_time       -- the Unix timestamp that the file was sent to the Librarian
    # size              -- file size in bytes
    # type              -- file "type" stored in the Librarian; "uv" for UV data
    # md5               -- the MD5 of the file contents; XXX may be calculated weirdly by Librarian
    # store_ssh_prefix  -- the Librarian "ssh_prefix" of the file's storage location
    # store_path_prefix -- the Librarian "path_prefix" of the file's storage location
    #
    # We start by extracting a few useful pieces of meta-information:

    for f in files:
        f['jd'] = float (path_to_jd (f['name']))
        f['pol'] = path_to_pol (f['name'])

        # Extract the hostname of the store on which this file is stored from
        # its store's ssh_prefix. The prefix will look like "user@host",
        # but the "user@" part might not be present.
        f['store_host'] = f['store_ssh_prefix'].split ('@', 1)[-1]

        # Meanwhile, the RTP system expects the filenames to be absolute paths.
        f['name'] = f['store_path_prefix'] + '/' + f['name']

    # If at all possible, get a default observation length from the
    # separations between observations, in case we have any funky datasets
    # without the length embedded. We are somewhat recklessly assuming that
    # even if this batch of datasets spans different nights, they all will
    # have the same DJD; this doesn't seem too unreasonable.

    pols = list (set (f['pol'] for f in files))
    bestjds = []

    for pol in pols:
        jds = np.sort ([f['jd'] for f in files if f['pol'] == pol])
        if len (jds) > len (bestjds):
            bestjds = jds

    default_djd = None

    if len (bestjds) > 2:
        default_djd = np.median (np.diff (bestjds))
        print ('Inferring default djd = %.5f days' % default_djd)

        for f in files:
            f['djd'] = default_djd

    # Buuut let's get djd straight from the data if at all possible. If there
    # are any files for which we have no idea about the djd, we can't add
    # them. For everything else, no we can compute the 'obsnum' magic number
    # (which is not the same as obsid!).

    for f in files:
        djd = try_get_file_djd (f)
        if djd is not None:
            f['djd'] = djd

    files = [f for f in files if f.get ('djd') is not None]

    for f in files:
        f['obsnum'] = str (jdpol2obsnum (f['jd'], f['pol'], f['djd']))

    # Now let's fill in the "neighbor" information. XXX: if we only get, say,
    # a random subset of observations from one night, this information will be
    # grievously incomplete! I don't see a way around that given the way that
    # this aspect of things is handled at the moment.

    for pol in pols:
        sfiles = sorted ((f for f in files if f['pol'] == pol), key=lambda f: f['jd'])

        for i in xrange (len (sfiles)):
            f_this = sfiles[i]

            if i > 0:
                f_prev = sfiles[i - 1]

                if (f_this['jd'] - f_prev['jd']) < (1.2 * f_this['djd']):
                    f_this['neighbor_low'] = f_prev['jd']

            if i < len (sfiles) - 1:
                f_next = sfiles[i + 1]

                if (f_next['jd'] - f_this['jd']) < (1.2 * f_this['djd']):
                    f_this['neighbor_high'] = f_next['jd']

    # Now that we've computed everything, avoid duplicating files that we
    # already know about.

    from sqlalchemy.orm.exc import NoResultFound

    def not_already_seen (filerec):
        try:
            obs = dbi.get_obs (filerec['obsnum'])
            print (repr (obs))
            return False
        except NoResultFound:
            return True

    n_before = len (files)
    files = [f for f in files if not_already_seen (f)]
    if len (files) != n_before:
        print ('Dropping %d already-ingested files.' % (n_before - len (files)))

    if not len (files):
        print ('Nothing to add.')
        return

    # Let's go for it.

    try:
        print ('Attempting to add %d observations to the still ...' % len (files))
        dbi.add_observations ([augmented_file_to_obsinfo (f) for f in files], initial_status)
    except Exception as e:
        print ('addition failed! here\'s what was attempted:', file=sys.stderr)
        for f in files:
            print ('', file=sys.stderr)
            print (augmented_file_to_obsinfo (f), file=sys.stderr)
        raise


if __name__ == '__main__':
    main (sys.argv[1:])
