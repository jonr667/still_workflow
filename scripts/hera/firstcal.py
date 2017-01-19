#! /usr/bin/env python
import capo.hex as hx, capo.arp as arp, capo.red as red, capo.omni as omni
import numpy as n, pylab as p, aipy as a
import sys,optparse

o = optparse.OptionParser()
a.scripting.add_standard_options(o,cal=True,pol=True)
o.add_option('--plot', action='store_true', help='Plot things.')
o.add_option('--ex_ants', action='store', type='string', default='',
    help='Exclude these antennas when solving.')
opts,args = o.parse_args(sys.argv[1:])
PLOT=opts.plot

uv = a.miriad.UV(args[0])
opts.pol=a.miriad.pol2str[uv['pol']]
del(uv)

def flatten_reds(reds):
    freds = []
    for r in reds:
        freds += r
    return freds

def save_gains(s,f,name='fcgains'):
    s2 = {}
    for k,i in s.iteritems():
        s2[str(k)] = omni.get_phase(f,i)
    for k,i in s.iteritems():
        s2['d'+str(k)] = i
    import sys
    cmd = sys.argv
    s2['cmd'] = ' '.join(cmd)
    n.savez('%s.npz'%name,**s2)

def normalize_data(datadict):
    d = {}
    for key in datadict.keys():
        d[key] = datadict[key]/n.where(n.abs(datadict[key]) == 0., 1., n.abs(datadict[key]))
    return d



#hera info assuming a hex of 19 and 128 antennas
aa = a.cal.get_aa(opts.cal, n.array([.150]))
bad_ants = [ant for ant in map(int,opts.ex_ants)]
info = omni.aa_to_info(aa, fcal=True, ex_ants=bad_ants)
reds = flatten_reds(info.get_reds())
print len(reds)

#Read in data here.
ant_string =','.join(map(str,info.subsetant))
bl_string = ','.join(['_'.join(map(str,k)) for k in reds])
times, data, flags = arp.get_dict_of_uv_data(args, bl_string, opts.pol, verbose=True)
dataxx = {}
for (i,j) in data.keys():
    dataxx[(i,j)] = data[(i,j)][opts.pol]
fqs = n.linspace(.1,.2,1024)
dlys = n.fft.fftshift(n.fft.fftfreq(fqs.size, fqs[1]-fqs[0]))

#gets phase solutions per frequency.
fc = omni.FirstCal(dataxx,fqs,info)
#sols = fc.run(tune=True, verbose=True)
sols = fc.run(tune=True)
#import IPython; IPython.embed()
#save solutions
fname = args[0]
save_gains(sols,fqs,name=fname)


if PLOT:
    #For plotting a subset of baselines
    infotest = omni.aa_to_info(aa, fcal=True, ubls=[(80,96)],ex_ants=[81])
    redstest = infotest.get_reds()
    #########  To look at solutions and it's application to data. If only lookgin for solutions stop here  ##############
    dataxx_c = {}
    print dataxx.keys()
    for (a1,a2) in info.bl_order():
        if (a1,a2) in dataxx.keys():
            dataxx_c[(a1,a2)] = dataxx[(a1,a2)]*omni.get_phase(fqs,sols[a1])*n.conj(omni.get_phase(fqs,sols[a2]))
            #if a1==43 or a2==43:
            #    dataxx_c[(a1,a2)]*=n.exp(-2j*n.pi*fqs*n.pi)
        else:
            dataxx_c[(a1,a2)] = dataxx[(a2,a1)]*omni.get_phase(fqs,sols[a2])*n.conj(omni.get_phase(fqs,sols[a1]))
            #if a1==43 or a2==43:
            #    dataxx_c[(a1,a2)]*=n.exp(-2j*n.pi*fqs*n.pi)
    #plotting data
    redbls = []
    for r in redstest: redbls += r
    redbls = n.array(redbls)
    #print redbls.shape
    #dm = divmod(len(redbls), n.round(n.sqrt(len(redbls))))
    #nr,nc = int(dm[0]),int(dm[0]+n.ceil(float(dm[1])/dm[0]))
    #fig,ax = p.subplots(nrows=nr,ncols=nc,figsize=(14,10))
    #for i,bl in enumerate(redbls):
    #    bl = (bl[0],bl[1])
    #    try:
    #        waterfall(dataxx[bl], ax[divmod(i,nc)], mode='phs')
    #        ax[divmod(i,nc)].set_title('%d,%d'%(bl))
    #    except(KeyError):
    #        waterfall(dataxx[bl[::-1]], ax[divmod(i,nc)], mode='phs')
    #        ax[divmod(i,nc)].set_title('%d,%d'%(bl[::-1]), color='m')
    #fig.subplots_adjust(hspace=.5)

    if PLOT:
        for k, bl in enumerate(redbls):
            bl = tuple(bl)
            try:
                #p.subplot(211); arp.waterfall(dataxx[bl], mode='log',mx=0,drng=3); p.colorbar(shrink=.5)
                #p.subplot(212); arp.waterfall(dataxx_c[bl], mode='log',mx=0,drng=3); p.colorbar(shrink=.5)
                p.figure(k)
                p.subplot(211); arp.waterfall(dataxx[bl], mode='phs'); p.colorbar(shrink=.5)
                p.subplot(212); arp.waterfall(dataxx_c[bl], mode='phs'); p.colorbar(shrink=.5)
                p.title('%d_%d'%bl)
                print sols[bl[0]] - sols[bl[1]]
                print bl
            except(KeyError):
                p.subplot(211); arp.waterfall(dataxx[bl[::-1]], mode='phs'); p.colorbar(shrink=.5)
                p.subplot(212); arp.waterfall(dataxx_c[bl], mode='phs'); p.colorbar(shrink=.5)
                p.title('%d_%d'%bl)
                print bl

        p.show()


    data_norm = normalize_data(dataxx_c)

    if PLOT or True:
        for bl in redbls:
            bl = tuple(bl)
            try:
                print data_norm[bl].shape
                p.subplot(111); arp.waterfall(n.fft.fftshift(arp.clean_transform(data_norm[bl]),axes=1),extent=(dlys[0],dlys[-1],0,len(redbls))); p.colorbar()
                p.xlim(-50,50)
                p.title('%d,%d'%bl)
            except(KeyError):
                print 'Key Error on', bl

            p.show()
