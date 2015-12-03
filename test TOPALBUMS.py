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
test_function=True
test_thread=True
close=True

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


# test load_topalbums function independent of TOPTRACKS tHREAD

if test_function:
    load_topalbums(db_path, db_lock, call_cont, out, limit=10,n_entries=100)

# test TOPALBUMS thread the increased limit parameter causes the topalbum information
# to reload and overwrite data from load_topalbumss function, where limit was lower

if test_thread:
    cond_event=threading.Event()
    thr_topalbums=TOPALBUMS(db_path,'topalbums', call_cont, out_cont, db_lock, 100, 5000, cond_event)
    cond_event.set()
    thr_topalbums.start()

# thread object signal_event will be set when loading is finished. Main thread will
# pause until then and stop call- and output controller

if close:
    thr_topalbums.signal_event.wait()
    out_cont.stop()
    call_cont.stop()

