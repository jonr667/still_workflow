#!/usr/bin/env python
import aipy as a, numpy as n
import optparse, sys, os

o = optparse.OptionParser()
o.set_usage('correct_psa128.py [options] *.uv')
o.set_description(__doc__)
opts,args = o.parse_args(sys.argv[1:])

for uvfile in args:
    uvofile = uvfile+'c'
    print uvfile,'->',uvofile
    if os.path.exists(uvofile):
        print uvofile, 'exists, skipping.'
        continue

    ###########
    # CORRECT #
    ###########

    rewire = {}
    #rewire = {0:1, 1:24, 2:45, 3:100}

    aa = a.phs.ArrayLocation(('-30:43:17.5','21:25:41.9'))
    nints = 0
    curtime = None
    def mfunc(uv, preamble, data, flags):
        global curtime
        global nints
        uvw, t, (i,j) = preamble
        blp = a.pol.ijp2blp(i, j, uv['pol'])
        #m = mask[t]
        p1,p2 = a.miriad.pol2str[uv['pol']]

        try:
            i = rewire[i]
        except(KeyError):
            pass
        try:
            j = rewire[j]
        except(KeyError):
            pass

        if i > j:
            i, j = j, i
            d = np.conjugate(d)

        #if i == j and (p1,p2) == ('y','x'):
        #    return None, None, None

        #if same_col(i,j):
        #    return None, None, None

        if t != curtime:
            curtime = t
            aa.set_jultime(t)
            uvo['lst'] = uvo['ra'] = uvo['obsra'] = aa.sidereal_time()
            nints += 1

        preamble = (uvw, t, (i,j))
        #return preamble, n.where(m, 0, data), m
        return preamble, data, flags

    override = {
            'latitud': aa.lat,
            'dec': aa.lat,
            'obsdec': aa.lat,
            'longitu': aa.long,
            'nchan': 1024,
            'nants': 128,
            'ngains': 256,
            'nspect0': 128,
            'telescop': 'HERA',
            'nints': nints
            }

    uvi = a.miriad.UV(uvfile)
    uvo = a.miriad.UV(uvofile, status='new')
    uvo.init_from_uv(uvi, override=override)
    uvo.pipe(uvi, mfunc=mfunc, raw=True, append2hist=' '.join(sys.argv)+'\n')
