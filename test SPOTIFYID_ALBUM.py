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
test_thread=False
close=False

# load testcharts
if load_testcharts:
    for category in ['artist','track','album']:
    
        chart,attr=get_chart(attr={'category':category},period='overall', limit=50, user='erblast')
        
        enter_chart(con,chart,attr)
    
    con.close()
    print 'test chart loaded'


# create controler instances and threading lock
call_cont=call_controler(50,10)
call_cont.start()
call_cont.output(0.2)

out_cont=output_controler(1)
out_cont.start()
out=out_cont.create_outvar()

db_lock=threading.Lock()


# test load_spotifyID_track function independent of thread

if test_function:
    load_spotifyID_album(db_path, db_lock, call_cont, out, n_entries=100)

if test_thread:
    thr_spotifyID_album=SPOTIFYID_ALBUM(db_path,'spotifyID_album', call_cont, out_cont, db_lock, 100)
    thr_spotifyID_album.start()
    thr_spotifyID_album.activate()
# thread object signal_event will be set when loading is finished. Main thread will
# pause until then and stop call- and output controller
if close:
    thr_spotifyID_album.signal_event.wait()
    out_cont.stop()
    call_cont.stop()

