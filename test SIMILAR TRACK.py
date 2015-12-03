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
    
        chart,attr=get_chart(attr={'category':category},period='overall', limit=10, user='erblast')
        
        enter_chart(con,chart,attr)
    
    con.close()
    print 'test chart loaded'


# create controler instances and threading lock

out_cont=output_controler(1)
out_cont.start()
out=out_cont.create_outvar()

call_cont=call_controler(5,1, out_cont)
call_cont.start()

db_lock=threading.Lock()


# test load_similar_track function independent of THREAD
if test_function:
    load_similar_track(db_path, db_lock, call_cont, out, user='erblast',limit=10,n_entries=100)

# test  thread the increased limit parameter causes the toptrack information
# to reload and overwrite data from load_toptracks function, where limit was lower
if test_thread:
    cond_event=threading.Event()

    thr_similar_tracks=SIMILAR_TRACK(db_path=db_path,name='SIMILAR_TRACK', \
                                     call_controler=call_cont, output_controler=out_cont, \
                                     db_lock=db_lock, user='erblast',limit=100,no_entries=100,
                                     condition_event=cond_event)
    cond_event.set()
    thr_similar_tracks.start()

#TROUBLE SHOOTING REMARK in order to debug functions called in threads use log object

# thread object signal_event will be set when loading is finished. Main thread will
# pause until then and stop call- and output controller

if close:
    thr_similar_tracks.signal_event.wait()
    out_cont.stop()
    call_cont.stop()


