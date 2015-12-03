# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 20:22:24 2015

@author: erblast
"""
import time
import threading
import pandas as pd
import sqlite3

from lastify_v6 import *
db_path='%s\\test.db' % path
createDB(db_path)
con=sqlite3.connect(db_path)

#Modulate
load_testcharts=False
test_function=False
test_thread=True
close=False


# load testcharts

if load_testcharts:
    for category in ['artist','track','album']:
    
        chart,attr=get_chart(attr={'category':category},period='overall', limit=50, user='erblast')
        
        enter_chart(con,chart,attr)
    
    con.close()
    print 'test chart loaded'



# create controler instances and threading lock
out_cont=output_controler(1)
out_cont.start()
out=out_cont.create_outvar()

call_cont=call_controler(5,1,out_cont)
call_cont.start()


db_lock=threading.Lock()


# test load_toptracks function independent of TOPTRACKS tHREAD

if test_function:
    load_toptracks(db_path, db_lock, call_cont, out, limit=10,n_entries=5000)

# test TOPTRACKS thread the increased limit parameter causes the toptrack information
# to reload and overwrite data from load_toptracks function, where limit was lower

if test_thread:
    cond_event=threading.Event()
    thr_toptracks=TOPTRACKS(db_path,'toptracks', call_cont, out_cont, db_lock, 25, 5000,cond_event)
    cond_event.set()
    thr_toptracks.start()

# thread object signal_event will be set when loading is finished. Main thread will
# pause until then and stop call- and output controller

if close:
    thr_toptracks.signal_event.wait()
    out_cont.stop()
    call_cont.stop()

