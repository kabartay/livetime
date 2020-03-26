#!/usr/bin/env python
#############################################
##
##           Mukharbek Organokov, 2017
##
#############################################

## ==========================================
## Calculate livetime of DATA and MC using DB
## ==========================================

## Python 2.7 to be used!

import os

#from __future__ import unicode_literals

from jsondb import *


import numpy as np
import pandas as pd  

import matplotlib
matplotlib.rcParams['text.usetex'] = True        # Need for \displaystyle
matplotlib.rcParams['text.latex.unicode'] = True # Need for \displaystyle
import matplotlib.pyplot as plt  
from matplotlib.ticker import NullFormatter
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg

from scipy.stats import sem 
from scipy.stats.stats import pearsonr 

import pylab

# For plotly needs API key. Go account for that https://plot.ly/settings/api
# Note that generating a new API key will require changes to your ~/.plotly/.credentials file.
# My API KEY: AZr5hGpxnbLuV2ttQKfj
# API key has been regenerated! Don't forget to update your config file.AZr5hGpxnbLuV2ttQKfj

base_dir = "/media/muha/DATA/DATASCIENCE/FORGIT/PHYS/livetime"
sub_dir = "/data"
dir_data = "DATARemained"
dir_mc = "MCRemained"

## If all .txt files are placed in directory 'all'
## Set of DATA/MC runs by years
"""
run_lists = [
    "runs_data_remained_final_2008.txt",
    "runs_data_remained_final_2009.txt",
    "runs_data_remained_final_2010.txt",
    "runs_data_remained_final_2011.txt",
    "runs_data_remained_final_2012.txt",
    "runs_data_remained_final_2013.txt",
    "runs_data_remained_final_2014.txt",
    "runs_data_remained_final_2015.txt",
    "runs_mc_remained_final_2008.txt",
    "runs_mc_remained_final_2009.txt",
    "runs_mc_remained_final_2010.txt",
    "runs_mc_remained_final_2011.txt",
    "runs_mc_remained_final_2012.txt",
    "runs_mc_remained_final_2013.txt",
    "runs_mc_remained_final_2014.txt",
    "runs_mc_remained_final_2015.txt",
    #"test_runs.txt",
]
"""

run_lists_mc = [
    "runs_mc_remained_final_2008.txt",
    "runs_mc_remained_final_2009.txt",
    "runs_mc_remained_final_2010.txt",
    "runs_mc_remained_final_2011.txt",
    "runs_mc_remained_final_2012.txt",
    "runs_mc_remained_final_2013.txt",
    "runs_mc_remained_final_2014.txt",
    "runs_mc_remained_final_2015.txt",
]

run_lists_data = [
    "runs_data_remained_final_2008.txt",
    "runs_data_remained_final_2009.txt",
    "runs_data_remained_final_2010.txt",
    "runs_data_remained_final_2011.txt",
    "runs_data_remained_final_2012.txt",
    "runs_data_remained_final_2013.txt",
    "runs_data_remained_final_2014.txt",
    "runs_data_remained_final_2015.txt",
]


## Version if all together
"""
for run_list in run_lists:
    # Load ASCII column files with run and livetime info:
    run_lvt = jsondb(base_dir+sub_dir+"/run_ymd_hms_dur_lvt.txt",columns=["run","ymd","hms","dur","lvt"]).df()[['run','lvt']]
    # run_lvt=np.genfromtxt('run_ymd_hms_lvt.txt', delimiter = ' ', dtype=float)
   
    # Version if all together
    sel_run = jsondb(base_dir+sub_dir+'/all/'+run_list,columns=["run"]).df()
    total_lvt = run_lvt.loc[run_lvt['run'].isin(sel_run['run']),'lvt'].convert_objects(convert_numeric=True).sum()

    print "------------------------------------------------------------------------"
    print "WARNING! It's doubling the value!!! Reduce that effect by dividing by 2!"
    
    ## Version if all together
    print "LT calc: ",total_lvt
    total_lvt=total_lvt/2
    print "LT corr: ", total_lvt

    print "                                                                        "
    print "  | Livetime in seconds for run list \"{}\" is:".format(run_list)
    print "  | {0} years and {1} days".format(int(total_lvt*1./(365.25*24*60*60)),
                                                total_lvt*1./(24*60*60)-365.25*int(total_lvt*1./(365.25*24*60*60)))
    print "  | {:.3f} years".format(total_lvt*1./(365.25*24*60*60))
    print "  | {:.3f} days".format(total_lvt*1./(24*60*60))
    print "  | {:.3f} hours".format(total_lvt*1./(60*60))
    print "  | {:.2f} seconds".format(total_lvt)
    
    try:
        ##f = open(base_dir+'/antdb_livetime/'+run_list+'.txt', 'w')
        #f = open(base_dir+'/antdb_livetime/'+run_list, 'w')
        f = open(base_dir+'/'+'calc_'+run_list, 'w')
        ##f.write("{:.2f}".format(total_lvt))
        f.write("{:.3f}".format(total_lvt*1./(24*60*60)))
        f.close()
    except Exception as error:
        print(error)
        #print "  | Probably you have to built the folder..."
        #print "    mkdir -p",base_dir+'/antdb_livetime'
"""

## Version if separate

## For DATA
list_lvt_data = []
for run_list_data in run_lists_data:
    # Load ASCII column files with run and livetime info:
    run_lvt = jsondb(base_dir+sub_dir+"/run_ymd_hms_dur_lvt.txt",columns=["run","ymd","hms","dur","lvt"]).df()[['run','lvt']]
    
    sel_run_data = jsondb(base_dir+sub_dir+'/'+dir_data+'/'+run_list_data,columns=["run"]).df()
    # Calculate LT now
    total_lvt_data = run_lvt.loc[run_lvt['run'].isin(sel_run_data['run']),'lvt'].convert_objects(convert_numeric=True).sum()
    
    print "------------------------------------------------------------------------" 
    print "WARNING! It's doubling the value!!! Reduce that effect by dividing by 2!"
    
    print "LT calc DATA: ",total_lvt_data
    total_lvt_data=total_lvt_data/2
    print "LT corr MC: ",total_lvt_data
    
    print "                                                                        "
    print "  | Livetime in seconds for run list DATA \"{}\" is:".format(run_list_data)
    print "  | {0} years and {1} days".format(int(total_lvt_data*1./(365.25*24*60*60)),
                                                total_lvt_data*1./(24*60*60)-365.25*int(total_lvt_data*1./(365.25*24*60*60)))
    print "  | {:.3f} years".format(total_lvt_data*1./(365.25*24*60*60))
    print "  | {:.3f} days".format(total_lvt_data*1./(24*60*60))
    print "  | {:.3f} hours".format(total_lvt_data*1./(60*60))
    print "  | {:.2f} seconds".format(total_lvt_data)
    
    list_lvt_data.append(total_lvt_data*1./(24*60*60))
    
## For MC
list_lvt_mc = []
for run_list_mc in run_lists_mc:
    # Load ASCII column files with run and livetime info:
    run_lvt = jsondb(base_dir+sub_dir+"/run_ymd_hms_dur_lvt.txt",columns=["run","ymd","hms","dur","lvt"]).df()[['run','lvt']]
    
    sel_run_mc = jsondb(base_dir+sub_dir+'/'+dir_mc+'/'+run_list_mc,columns=["run"]).df()
    # Calculate LT now
    total_lvt_mc = run_lvt.loc[run_lvt['run'].isin(sel_run_mc['run']),'lvt'].convert_objects(convert_numeric=True).sum()
    
    print "------------------------------------------------------------------------"
    print "WARNING! It's doubling the value!!! Reduce that effect by dividing by 2!"
    
    print "LT calc MC: ",total_lvt_mc
    total_lvt_mc=total_lvt_mc/2
    print "LT corr MC: ",total_lvt_mc
    
    print "                                                                        "
    print "  | Livetime in seconds for run list MC   \"{}\" is:".format(run_list_mc)
    print "  | {0} years and {1} days".format(int(total_lvt_mc*1./(365.25*24*60*60)),
                                                total_lvt_mc*1./(24*60*60)-365.25*int(total_lvt_mc*1./(365.25*24*60*60)))
    print "  | {:.3f} years".format(total_lvt_mc*1./(365.25*24*60*60))
    print "  | {:.3f} days".format(total_lvt_mc*1./(24*60*60))
    print "  | {:.3f} hours".format(total_lvt_mc*1./(60*60))
    print "  | {:.2f} seconds".format(total_lvt_mc)
    
    list_lvt_mc.append(total_lvt_mc*1./(24*60*60))
    

print "LT[DATA] in days:", list_lvt_data
print "LT[ MC ] in days:", list_lvt_mc

list_years = []
for year in range(2008,2015+1):
    list_years.append(year)

## Let's form one file to write output info when DATA/MC are calculated separately. 
powdata=open(base_dir+'/'+'calc_lvt_total.txt', 'w')
for val in zip(list_years,list_lvt_data,list_lvt_mc):
    powdata.write('{} {} {} {}\n'.format(val[0], val[1], val[2], val[1]/val[2]))
powdata.close()

os._exit(0)







