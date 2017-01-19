#! /usr/bin/env python
"""
Read flags from MIRIAD .uv files from a given JD, and constructs a waterfall/occupancy plot from them. Saves grid of (integrations x frequencies) to an .npz file, along with information about the percentage of frequencies and times flagged over the files you hand it.

Written by: Saul Aryeh Kohn, 2015.
6/2016: more flexibility for outputs, can plot waterfall w.r.t. SAST, can just save frequency occupancies
"""

import sys, aipy, optparse, re, datetime
from astropy import time
from matplotlib import pylab
import numpy as np
from matplotlib.ticker import NullFormatter
nullfmt   = NullFormatter()

o = optparse.OptionParser()
o.set_usage('flagreader.py [options] *.uvR')
o.set_description(__doc__)
aipy.scripting.add_standard_options(o, ant=True, pol=True)
o.add_option('--verbose','-v',dest='verb',action='store_true',help='print intermediate info')
o.add_option('--wfall','-W',dest='save_wfall',action='store_true',help='Save occupancy waterfall .png file?')
o.add_option('--focc','-F',dest='save_freq',action='store_true',help='Save frequency occupancy .png file? Same naming convention as default .npz file.')
o.add_option('--npz','-N',dest='npz',default=None,help='Name of output .npz (neglecting the ".npz" ending) with flag grid and percentage info. Default will be <JD>_<ant>_<pol>_RFI.npz')
o.add_option('--wimg',dest='wimg',default=None,help='Name of waterfall .png file. Default will be same naming convention as default .npz file.')
o.add_option('--fimg',dest='fimg',default=None,help='Name of waterfall .png file. Default will be <first file>_f.png .')
o.add_option('--show','-S',dest='show',action='store_true',help='Show image.')
opts, args = o.parse_args(sys.argv[1:])

def file2jd(zenuv): return re.findall(r'\d+\.\d+', zenuv)[0]

def get_caldat(JD):
    #http://aa.usno.navy.mil/faq/docs/JD_Formula.php
    L= int(JD+68569)
    N= 4*L/146097
    L= L-(146097*N+3)/4
    I= 4000*(L+1)/1461001
    L= L-1461*I/4+31
    J= 80*L/2447
    K= L-2447*J/80
    L= J/11
    J= J+2-12*L
    I= 100*(N-49)+I+L
    return I,J,K
 
def jds2hrs(jdlist,hrs=False,sast=False):
    s = []
    for t in jdlist:
        T = time.Time(t,scale='utc',format='jd') 
        
        if not hrs: s.append(T.iso.rsplit()[1])
        else:
            tm = T.iso.rsplit()[1]
            hr = int(tm.split(':')[0])
            if not sast: s.append(hr)
            else: s.append((hr+2)%24)
    return s
    
t_arr, flg_arr = [], []
jd = args[0].split('.')[1]

## Read-in data

for n,f in enumerate(args):
	print 'Reading %s...'%f
	jdf = f.split('.')[1]
	assert(jdf==jd)
	uv = aipy.miriad.UV(f)
	aipy.scripting.uv_selector(uv, opts.ant, opts.pol)
	for preamble, data, flags in uv.all(raw=True):
		uvw, t, (i,j) = preamble
		t_arr.append(t)
		flg_arr.append(flags)
	del(uv)

t_arr = np.array(t_arr)
flg_arr=np.array(flg_arr)
pcnt_t, pcnt_f = [],[]
if opts.verb: print 'Calculating percentage occupations'
pcnt_f = 100.*np.sum(flg_arr,axis=0)/flg_arr.shape[0]
pcnt_t = 100.*np.sum(flg_arr,axis=1)/flg_arr.shape[1]

fqs = np.linspace(100.,200.,num=pcnt_f.shape[0])
tms = np.linspace(t_arr[0],t_arr[len(t_arr)-1],num=t_arr.shape[0])
str_tms = jds2hrs(t_arr,hrs=True,sast=True)

##Write data to npz

if opts.npz is not None: npzname=opts.npz
else: npzname='%s_%s_%s_RFI.npz'%(jd,opts.ant,opts.pol)

if opts.verb: print 'Writing data to %s'%npzname
np.savez(npzname,grid=flg_arr,dJDs=t_arr,percent_f=pcnt_f,percent_t=pcnt_t)

#If you don't want plots, let's save everyone a smidgen of time and quit now
if not opts.show and not opts.save_wfall and not opts.save_freq: sys.exit()

##Plotting freq occupancy

if opts.save_freq or opts.show:
    if opts.verb: print 'Plotting frequency-occupancy plot'
    pylab.step(fqs,pcnt_f,where='mid')
    pylab.fill_between(fqs,0,pcnt_f,color='blue',alpha=0.3)
    pylab.xlabel('Frequency [MHz]')
    pylab.ylabel('Occupancy [%]')
    if len(args)==1: pylab.suptitle(file2jd(args[0]),size=15)
    else: pylab.suptitle('%s - %s'%(file2jd(args[0]),file2jd(args[len(args)-1])),size=15)
    
    #pylab.savefig('%s_%s_%s_F.png'%(jd,opts.ant,opts.pol)) #hard to plug into RTP
    if not opts.fimg is None: pylab.savefig(opts.fimg)
    else: pylab.savefig('%s_f.png'%args[0]) #zen.2451234.12345.xx.HH.uvcR_f.png 
    #^ it's being run on each uvcR file in the RTP system
    if opts.show:
        pylab.show()
    else:
        pylab.close()

##Plotting waterfall
if opts.verb: print 'Plotting occupancy waterfall'
#Set-up plot format
left, width = 0.1, 0.65
bottom, height = 0.1, 0.65
bottom_h = left_h = left+width+0.02
rect_imshow = [left, bottom, width, height]
rect_histx = [left, bottom_h, width, 0.2]
rect_histy = [left_h, bottom, 0.2, height]
pylab.figure(1, figsize=(8,8))		
axImshow = pylab.axes(rect_imshow)
axHistx = pylab.axes(rect_histx)
axHisty = pylab.axes(rect_histy)
axHistx.xaxis.set_major_formatter(nullfmt)
axHisty.yaxis.set_major_formatter(nullfmt)

#actuallly interact with data
axImshow.imshow(flg_arr,aspect='auto',interpolation='nearest',cmap='binary',extent=[100.,200.,t_arr[len(t_arr)-1],t_arr[0]])
axHistx.plot(fqs,pcnt_f)
axHistx.fill_between(fqs,0,pcnt_f,color='blue',alpha=0.3)
axHisty.plot(pcnt_t,tms,'b-')

#More formatting

#Getting ticks in the right places
FF = lambda m, n: [i*n//m + n//(2*m) for i in range(m)]
new_str_tms,new_tms = [],[]
for i in FF(12,len(str_tms)):
    new_tms.append(tms[i])
    new_str_tms.append(str_tms[i])

axImshow.set_yticks(new_tms)
axImshow.set_yticklabels(new_str_tms)

axHistx.set_xlim( axImshow.get_xlim() )
axHisty.set_ylim( axImshow.get_ylim() )
axHisty.set_xlim( (0.,100.) )

axHistx.set_ylabel(r'% $\rm{flagged}$ $\nu\,\rm{over}\,t$')
axHisty.set_xlabel(r'% $\rm{flagged}$ $t\,\rm{over}\,\nu$')
axImshow.set_xlabel(r'Frequency [MHz]',size=15)
axImshow.set_ylabel(r'SAST',size=15)
gd = get_caldat(int(jd))
axHisty.set_title('%i/%i/%i'%(gd[0],gd[1],gd[2])+'\n\n\n',size=18)

if opts.save_wfall:
    if not opts.wimg is None: pylab.savefig(opts.wimg)
    else: pylab.savefig('%s_%s_%s_RFI.png'%(jd,opts.ant,opts.pol))
if opts.show: pylab.show()
pylab.close()
