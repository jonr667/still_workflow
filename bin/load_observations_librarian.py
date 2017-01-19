#! /usr/bin/env python
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License

"""Ask the Librarian for new files that we need to process, and load them into
our database. "Files that need processing" (1) have a "source" of the
"Correlator"; (2) do not have an event with a "type" of
"rtp.ingested"; and (3) are not already in our database.

"""
from __future__ import absolute_import, division, print_function

# add our local modules to the Python search path
import os.path, sys
basedir = os.path.dirname (os.path.dirname (os.path.realpath (__file__)))
sys.path.insert (0, os.path.join (basedir, 'lib'))

import optparse, re
import numpy as np

from dbi import DataBaseInterface, Still
from still import get_dbi_from_config, process_client_config_file, SpawnerClass, WorkFlow
import hera_librarian

rtp_ingested_key = 'rtp.ingested'
initial_status = 'UV_POT'


def main (args):
    o = optparse.OptionParser ()
    o.set_usage ('load_observations_librarian.py')
    o.set_description (__doc__)
    o.add_option('--connection', help='the name of the Librarian connection to use (as in ~/.hl_client.cfg)')
    o.add_option('--config_file',help='RTP configuration file default=RTP/etc/still.cfg',default='etc/still.cfg')
    o.add_option('--source', help='Only load files originating from the named "source" (default "%default")',
                 default='correlator')
    opts, args = o.parse_args (args)

    # Some boilerplate to set up the database interface ...
    spawner = SpawnerClass()
    workflow = WorkFlow()
    spawner.config_file = os.path.join (basedir, opts.config_file)
    process_client_config_file (spawner, workflow)
    dbi = get_dbi_from_config (spawner.config_file)
    dbi.test_db ()

    # Get the list of potentially-relevant files from the Librarian.

    lc = hera_librarian.LibrarianClient (opts.connection)
    try:
        listing = lc.describe_session_without_event (opts.source,
                                                     rtp_ingested_key)
    except hera_librarian.RPCError as e:
        print ('RPC to librarian failed: %s' % e.message)
        sys.exit (1)

    if not listing['any_matching']:
        print ('No new sessions.')
        return

    # For each record we get a dict of at least
    #
    #   date        -- the start Julian Date of the observation
    #   pol         -- the polarization of the data ("xx" or "yy")
    #   store_path  -- the path of a file instance *within* a store
    #   path_prefix -- the store's path prefix, used to construct full paths
    #   host        -- the hostname of the store
    #   length      -- the duration of the observation in days
    #
    # This is a pretty good start ... because of course the Librarian's API
    # call has been engineered to give us what we need.

    def augment_record (r):
        return {
            'obsnum': os.path.basename (r['store_path']), # NOTE: this is actually free text
            'date': r['date'],
            'date_type': 'julian',
            'pol': r['pol'],
            'host': r['host'],
            'filename': os.path.join (r['path_prefix'], r['store_path']),
            'path_prefix': r['path_prefix'],
            'outputhost': '',
            'status': initial_status,
            'length': r['length'],
        }

    obsinfos = [augment_record (r) for r in listing['info']]

    # Now we need to fill in the "neighbor" information.

    pols = set (oi['pol'] for oi in obsinfos)

    for pol in pols:
        soi = sorted ((oi for oi in obsinfos if oi['pol'] == pol), key=lambda oi: oi['date'])

        for i in xrange (len (soi)):
            oi_this = soi[i]

            if i > 0:
                oi_prev = soi[i - 1]

                if (oi_this['date'] - oi_prev['date']) < (1.2 * oi_this['length']):
                    oi_this['neighbor_low'] = oi_prev['date']

            if i < len (soi) - 1:
                oi_next = soi[i + 1]

                if (oi_next['date'] - oi_this['date']) < (1.2 * oi_this['length']):
                    oi_this['neighbor_high'] = oi_next['date']

    # Now that we've computed everything, avoid duplicating files that we
    # already know about. We shouldn't end up ever trying to submit
    # duplicates, but in practice ...

    from sqlalchemy.orm.exc import NoResultFound

    def not_already_seen (oi):
        try:
            obs = dbi.get_obs (oi['obsnum'])
            return False
        except NoResultFound:
            return True

    n_before = len (obsinfos)
    obsinfos = [oi for oi in obsinfos if not_already_seen (oi)]
    if len (obsinfos) != n_before:
        print ('Dropping %d already-ingested records.' % (n_before - len (obsinfos)))

    if not len (obsinfos):
        print ('Nothing to add.')
        return

    # Try ingesting into the RTP.

    try:
        print ('Attempting to add %d observations to the still ...' % len (obsinfos))
        dbi.add_observations (obsinfos, initial_status)
    except Exception as e:
        print ('addition failed! here\'s what was attempted:', file=sys.stderr)
        print ('', file=sys.stderr)
        for oi in obsinfos:
            print (oi, file=sys.stderr)
        raise

    # Add events to the Librarian indicating that these files were
    # successfully ingested into the RTP.

    for oi in obsinfos:
        lc.create_file_event (os.path.basename (oi['filename']), rtp_ingested_key)


if __name__ == '__main__':
    main (sys.argv[1:])
