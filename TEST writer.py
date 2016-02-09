# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 18:00:08 2015

@author: erblast
"""
path = r'd:\Dropbox\work\python\lastify'

import sys
if '%s\github' %path not in sys.path:
    sys.path.append('%s\github' %path)
import time
import threading
import pandas as pd
import sqlite3
import requests
from copy import deepcopy
from createDB import *
from lastify_v6 import cleanup_db_dates, calc_interest_score
from call_controler_v2 import *
from output_controler import *
from Writer import *
import random

url = 'http://ws.audioscrobbler.com/2.0/'

baserequest = {'api_key': '43e826d47e1fc381ac3686f374ee34b5', 'format': 'json'}

path = r'd:\Dropbox\work\python\lastify'

sk = str()

secret = 'e47ef29dda1b81c1f865d12a89ad28b8'

# Database Connection

db_path = '%s\\lastify.db' % path
createDB(db_path)
con = sqlite3.connect(db_path)
c = con.cursor()

db_lock = threading.RLock()

user = 'erblast'

# optional
# cleanup_db_dates(db_path,db_lock) # takes 1 s on large db
# print 'dates cleaned up'
#
# calc_interest_score(user,db_path,db_lock) # takes 1,5 min on large db
# print 'interest_score calculated'

# fill temp_track and temp_artist with random data


c.execute('SELECT count(trackID) FROM ID_track')
n_trackIDs = [i for i in c][0][0]
track_pop = range(1,n_trackIDs)
track_sample = random.sample(track_pop,200)
new_tup = [ ( i,) for i in track_sample]

c.execute('DROP TABLE IF EXISTS temp_track')
c.execute('CREATE TABLE temp_track (trackID)')
c.executemany('INSERT INTO temp_track (trackID) VALUES (?)',new_tup)

c.execute('SELECT count(albumID) FROM ID_album')
n_albumIDs = [i for i in c][0][0]
album_pop = range(1,n_albumIDs)
album_sample = random.sample(album_pop,200)
new_tup = [ ( i,) for i in album_sample]

c.execute('DROP TABLE IF EXISTS temp_album')
c.execute('CREATE TABLE temp_album (albumID)')
c.executemany('INSERT INTO temp_album (albumID) VALUES (?)',new_tup)

con.commit()
# instantiate writer track

wr=Writer(category='track',user='erblast',db_path=db_path,db_lock=db_lock)
print 'writer instantiated'
# playslist selection is limited to 5000 track takes around 5 min to assemble

wr.limit(rank_by='plays_track_total',topX=2, con=con, asc=False)

sort_by = ['date_artist','date_track','i_score_artist','i_score_track']
asc     = [ True        , True       , False          , False]

wr.sort( sort_by=sort_by, asc=asc, con=con )

wr.write_playlist('test_track',path, con=con)

# instantiate writer album

wr=Writer(category='album',user='erblast',db_path=db_path,db_lock=db_lock)
print 'writer instantiated'
# playslist selection is limited to 5000 album takes around 4 min to assemble

wr.limit(rank_by='plays_album_total',topX=2, con=con)

sort_by = ['date_artist','date_album','i_score_artist']
asc     = [ True        , True       , False          ]

wr.sort( sort_by= sort_by, asc=asc, con=con )

wr.write_playlist('test_album',path, con=con)
