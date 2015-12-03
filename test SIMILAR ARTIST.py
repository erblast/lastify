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
load_testcharts=True
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
call_cont=call_controler(5,1)
call_cont.start()
call_cont.output(0.2)

out_cont=output_controler(1)
out_cont.start()
out=out_cont.create_outvar()

db_lock=threading.Lock()


# test load_similar_artist function independent of SIMILAR_ARTIST THREAD
if test_function:
    load_similar_artist(db_path, db_lock, call_cont, out, user='erblast',limit=10,n_entries=100)

# test SIMILAR_ARTIST thread the increased limit parameter causes the toptrack information
# to reload and overwrite data from load_toptracks function, where limit was lower
if test_thread:
    cond_event=threading.Event()
    thr_similar_artist=SIMILAR_ARTIST(db_path,'SIMILAR_ARTIST', \
                                 call_cont, out_cont, \
                                 db_lock, 'erblast',100,5000, \
                                 cond_event)
    cond_event.set()
    thr_similar_artist.start()


#TROUBLE SHOOTING REMARK in order to debug functions called in threads use log object

# thread object signal_event will be set when loading is finished. Main thread will
# pause until then and stop call- and output controller
if close:
    thr_similar_artist.signal_event.wait()
    out_cont.stop()
    call_cont.stop()


