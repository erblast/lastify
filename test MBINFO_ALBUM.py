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
call_cont=call_controler(17,20)
call_cont.start()
call_cont.output(0.2)

out_cont=output_controler(1)
out_cont.start()
out=out_cont.create_outvar()

db_lock=threading.Lock()


# test load_toptags_album function independent of thread

if test_function:
    load_mbinfo_album(db_path, db_lock, call_cont, out, n_entries=1)

if test_thread:
    thr_mbinfo_album=MBINFO_ALBUM(db_path,'mbinfo_album', call_cont, out_cont, db_lock, 100)
    thr_mbinfo_album.start()
    thr_mbinfo_album.activate()    
# thread object signal_event will be set when loading is finished. Main thread will
# pause until then and stop call- and output controller
if close:
    thr_toptracks.signal_event.wait()
    out_cont.stop()
    call_cont.stop()

