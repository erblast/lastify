# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 18:00:08 2015

@author: erblast
"""
import time
import threading
import pandas as pd
import sqlite3
import requests
from copy import deepcopy
from createDB import *
from call_controler_v2 import *
from output_controler import *
from Filter import *

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

# Create Manager


filt_manager_album = FilterManager(db_path=db_path, db_lock=db_lock, category='album')

filt_manager_artist = FilterManager(db_path=db_path, db_lock=db_lock, category='artist')

TRACK Filters

filt_manager_track = FilterManager(db_path=db_path, db_lock=db_lock, category='track')

# tag filter 1, no threshold
f1 = FilterTrackTag('f1', 'indie', 0, con, db_lock)
print 'n filtered tracks by tag_track: %d' % len(f1.IDs)
filt_manager_track.log_filter(f1)

# test if filtering was successfull
freq_init=filt_manager_track.freq_tag_init
count_init=freq_init[ freq_init['tagName'] == 'indie' ]['count']
assert len(f1.IDs) == count_init.iloc[0]

# delete filter and check for successful deletion
assert 'f1' in filt_manager_track.filter_log
filt_manager_track.del_filter(f1)
assert 'f1' not in filt_manager_track.filter_log

# 2nd tag filter with threshold
f1_1 = FilterTrackTag('f1_1', 'alternative', 75, con, db_lock)
print 'n filtered tracks by tag_track: %d' % len(f1_1.IDs)
filt_manager_track.log_filter(f1_1)

# test if thresholded filtering was successfull
freq_75=filt_manager_track.calc_freq_tag(75)
count_init=freq_75[ freq_75['tagName'] == 'alternative' ]['count']
assert len(f1_1.IDs) == count_init.iloc[0]

# add artisttag filter
f2 = FilterArtistTag('f2', 'alternative', 75, category='track', con=con, db_lock=db_lock)
print 'n filtered tracks by tag_artist: %d' % len(f2.IDs)
filt_manager_track.log_filter(f2)

DF = pd.read_sql('SELECT ID_tag.tagName, COUNT (ID_tag.tagName) AS count, ID_tag.url\
                FROM tag_artist \
                INNER JOIN temp_artist ON temp_artist.artistID=tag_artist.artistID \
                LEFT JOIN ID_tag ON ID_tag.tagID=tag_artist.tagID \
                WHERE tag_artist.count>=? \
                GROUP BY ID_tag.tagID' , con, params=(75,))


filt_manager_track.update_filterIDs()
print 'n all filters combined: %d' % len(filt_manager_track.filtered_IDs)


In[5]: filt_manager_track.del_filter(f1)





f3 = FilterPlaysTrack('f3', user, con, db_lock, 0, 30)
print 'n filtered track by plays : %d' % len(f3.IDs)
filt_manager_track.log_filter(f3)

f3_1 = FilterPlaysArtist(name='f3_1', user=user, con=con, db_lock=db_lock, minim=10, maxim=None, category='track')
print 'n filtered track by plays : %d' % len(f3_1.IDs)
filt_manager_track.log_filter(f3_1)

f4 = FilterDateTrack('f4', con, db_lock, 2008, 2010, include_undated=True)
print 'n filtered track by date : %d' % len(f4.IDs)
filt_manager_track.log_filter(f4)

filt_manager_track.update_filterIDs()
print 'n all filters combined: %d' % len(filt_manager_track.filtered_IDs)

filt_manager_track.calc_freq_date()
print filt_manager_track.freq_date.head(50)

filt_manager_track.calc_freq_tag(75)
print filt_manager_track.freq_tag.head(50)

# ALBUM FILTERS

f1 = FilterAlbumTag('f1', 'indie', 75, con, db_lock)
print 'n filtered albums by tag_album: %d' % len(f1.IDs)
filt_manager_album.log_filter(f1)

f1_1 = FilterAlbumTag('f1_1', 'alternative', 75, con, db_lock)
print 'n filtered albums by tag_album: %d' % len(f1_1.IDs)
filt_manager_album.log_filter(f1_1)

f2 = FilterArtistTag('f2', 'alternative', 75, category='album', con=con, db_lock=db_lock)
print 'n filtered albums by tag_artist: %d' % len(f2.IDs)
filt_manager_album.log_filter(f2)

f3 = FilterPlaysAlbum('f3', user, con, db_lock, 0, 30)
print 'n filtered album by plays : %d' % len(f3.IDs)
filt_manager_album.log_filter(f3)


f3_1 = FilterPlaysArtist(name='f3_1', user=user, con=con, db_lock=db_lock, minim=10, maxim=None, category='album')
print 'n filtered album by plays : %d' % len(f3_1.IDs)
filt_manager_album.log_filter(f3_1)

f4 = FilterDateAlbum('f4', con, db_lock, 2008, 2010, include_undated=True)
print 'n filtered album by date : %d' % len(f4.IDs)
filt_manager_album.log_filter(f4)
#
filt_manager_album.update_filterIDs()
print 'n all filters combined: %d' % len(filt_manager_album.filtered_IDs)

filt_manager_album.calc_freq_date()
print filt_manager_album.freq_date.head(50)

filt_manager_album.calc_freq_tag(75)
print filt_manager_album.freq_tag.head(50)
