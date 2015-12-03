# -*- coding: utf-8 -*-
"""
Created on Sun Nov 08 21:27:08 2015

@author: erblast
"""

import time
import threading
import pandas as pd
import sqlite3

from lastify_v6 import *

user='erblast'

db_path='%s\\test.db' % path
createDB(db_path)
con=sqlite3.connect(db_path)


# load testcharts

for category in ['artist','track','album']:

    chart,attr=get_chart(attr={'category':category},period='overall', limit=10, user=user)
    
    enter_chart(con,chart,attr)

con.close()
print 'test chart loaded'

# create controler instances and threading lock
out_cont=output_controler(0.1)


lastfm_cont=call_controler(1500,300,out_cont, 'lastfm controller')
lastfm_cont.start()

spotify_cont=call_controler(1500,300,out_cont,'spotify controller')
spotify_cont.start()

mb_cont=call_controler(18,20,out_cont,'musicbrainz controller')
mb_cont.start()


db_lock=threading.Lock()

# create THREADS
thr_similar_artist=SIMILAR_ARTIST(db_path,'SIMILAR_ARTIST', lastfm_cont, out_cont, db_lock, user,5, 5000)
thr_similar_track=SIMILAR_TRACK(db_path,'SIMILAR_TRACK', lastfm_cont, out_cont, db_lock, user, 5, 5000)

thr_toptracks=TOPTRACKS(db_path,'TOPTRACKS', lastfm_cont, out_cont, db_lock, 5, 5000)
thr_topalbums=TOPALBUMS(db_path,'TOPALBUMS', lastfm_cont, out_cont, db_lock, 5, 5000)

thr_toptags_artist=TOPTAGS_ARTIST(db_path,'TOPTAGS_ARTIST', lastfm_cont, out_cont, db_lock, 5000)
thr_toptags_album=TOPTAGS_ALBUM(db_path,'TOPTAGS_ALBUM', lastfm_cont, out_cont, db_lock, 5000)
thr_toptags_track=TOPTAGS_TRACK(db_path,'TOPTAGS_TRACK', lastfm_cont, out_cont, db_lock, 5000)

thr_mbinfo_album=MBINFO_ALBUM(db_path,'MBINFO_ALBUM', mb_cont, out_cont, db_lock, 5000)
thr_spotifyID_album=SPOTIFYID_ALBUM(db_path,'SPOTIFYID ALBUM', spotify_cont, out_cont, db_lock, 5000)
thr_spotifyID_track=SPOTIFYID_TRACK(db_path,'SPOTIFYID TRACK', spotify_cont, out_cont, db_lock, 5000)

# CONTROLER THREAD 1
# toptags tracks gets activated when SIMILAR_TRACK and TOPTRACKS are done

def f_1():
    thr_toptags_track.deactivate()
    
thr_cont_toptags_track=CONTROLER([thr_similar_track.signal_event, thr_toptracks.signal_event],
                                 f_1,name='TOPTAGS TRACK CONTROLER' )
thr_cont_toptags_track.start()

# CONTROLER THREAD 2
# restarting threads are deactivated when all other track and album collecting threads are done
                                 
def f_2():
    thr_mbinfo_album.deactivate()
    thr_spotifyID_album.deactivate()                              
    thr_spotifyID_track.deactivate() 

thr_cont_restart=CONTROLER([thr_similar_track.signal_event, thr_toptracks.signal_event, thr_topalbums.signal_event],
                            f_2,name='RESTART CONTROLER' )
thr_cont_restart.start()

# CONTROLER THREAD 3
# call and output controler are stopped when all threads are finished

def f_3():
    lastfm_cont.stop()
    spotify_cont.stop()
    mb_cont.stop()
    out_cont.stop()

thr_cont=CONTROLER([thr_spotifyID_album.signal_event, \
                    thr_spotifyID_track.signal_event, \
                    thr_mbinfo_album.signal_event, \
                    thr_toptags_artist.signal_event, \
                    thr_toptags_album.signal_event, \
                    thr_toptags_track.signal_event, ], \
                    f_3,name='THREAD CONTROLER ALL' )

thr_cont.start()

# CONTROLER THREAD 4

def f_4():
    thr_toptags_artist.deactivate()
    thr_toptracks.deactivate()
    thr_topalbums.deactivate()

thr_cont_sim_artist=CONTROLER([thr_similar_artist.signal_event], \
                    f_4,name='THREAD CONTROLER TOPTAGS_ARTIST, TOPTRACKS, TOPALBUMS' )

thr_cont_sim_artist.start()

# CONTROLER THREAD 5

def f_5():
    thr_toptags_album.deactivate()

thr_cont_topalbum=CONTROLER([thr_topalbums.signal_event], \
                    f_5,name='THREAD CONTROLER TOPTAGS_ALBUM' )

thr_cont_topalbum.start()

# activate starting threads

thr_similar_artist.activate()
thr_similar_track.activate()
thr_toptracks.activate()
thr_topalbums.activate()
thr_toptags_artist.activate()
thr_toptags_album.activate()
thr_toptags_track.activate()
thr_mbinfo_album.activate()
thr_spotifyID_album.activate()
thr_spotifyID_track.activate()


# start threads

thr_similar_artist.start()
thr_similar_track.start()
thr_toptracks.start()
thr_topalbums.start()
thr_toptags_artist.start()
thr_toptags_album.start()
thr_toptags_track.start()
thr_mbinfo_album.start()
thr_spotifyID_album.start()
thr_spotifyID_track.start()

out_cont.start()


