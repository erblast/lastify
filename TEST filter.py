

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
from lastify_v6 import cleanup_db_dates, calc_interest_score
from call_controler_v2 import *
from output_controler import *
from Filter import *

url = 'http://ws.audioscrobbler.com/2.0/'

baserequest = {'api_key': '43e826d47e1fc381ac3686f374ee34b5', 'format': 'json'}

path = r'd:\Dropbox\work\python\lastify'

sk = str()

secret = 'e47ef29dda1b81c1f865d12a89ad28b8'

# Database Connection

db_path = '%s\\test.db' % path
createDB(db_path)
con = sqlite3.connect(db_path)
c = con.cursor()

db_lock = threading.RLock()

user = 'erblast'

cleanup_db_dates(db_path,db_lock) # takes 1 s on large db
print 'dates cleaned up'

calc_interest_score(user,db_path,db_lock) # takes 1,5 min on large db
print 'interest_score calculated'

# TRACK Filters

filt_manager_track = FilterManager(db_path=db_path, db_lock=db_lock,user= user, category='track',freq_init=True)

# tag filter 1, no threshold
f1 = FilterTrackTag(name='f1',tag= 'indie', thresh=0, db_path=db_path, db_lock=db_lock)
print 'n filtered tracks by tag_track: %d' % len(f1.IDs)
filt_manager_track.log_filter(f1)

# test if filtering was successfull
freq_init=filt_manager_track.calc_freq_tag(thresh=0,con=con)
count_init=freq_init[ freq_init['tagName'] == 'indie' ]['count']
assert len(f1.IDs) == count_init.iloc[0]

# delete filter and check for successful deletion
assert 'f1' in filt_manager_track.filter_log
filt_manager_track.del_filter(f1)
assert 'f1' not in filt_manager_track.filter_log

# 2nd tag filter with threshold
f1_1 = FilterTrackTag(name='f1_1',tag= 'alternative',thresh= 75, db_path=db_path, db_lock=db_lock)
print 'n filtered tracks by tag_track: %d' % len(f1_1.IDs)
filt_manager_track.log_filter(f1_1)

# test if thresholded filtering was successfull
freq_75=filt_manager_track.calc_freq_tag(thresh=75, con=con)
count_init=freq_75[ freq_75['tagName'] == 'alternative' ]['count']
assert len(f1_1.IDs) == count_init.iloc[0]

# add artisttag filter
f2 = FilterArtistTag(name='f2', tag='alternative', thresh=75, category='track', db_path=db_path, db_lock=db_lock)
print 'n filtered tracks by tag_artist: %d' % len(f2.IDs)
filt_manager_track.log_filter(f2)

# test if filter is correct
DF = pd.read_sql('SELECT ID_tag.tagName, COUNT (ID_tag.tagName) AS count, ID_tag.url\
                FROM tag_artist \
                LEFT JOIN ID_track ON tag_artist.artistID=ID_track.artistID\
                LEFT JOIN ID_tag ON ID_tag.tagID=tag_artist.tagID \
                WHERE tag_artist.count>=? \
                GROUP BY ID_tag.tagID' , con, params=(75,))

assert len(f2.IDs) == DF['count'][ DF['tagName']=='alternative'].iloc[0]

# add plays filter
f3_1 = FilterPlaysTrack(name='f3_1',user= user, db_path=db_path,db_lock= db_lock, minim=10)
print 'n filtered track by plays : %d' % len(f3_1.IDs)
filt_manager_track.log_filter(f3_1)

f3_2 = FilterPlaysTrack(name='f3_2', user=user, db_path=db_path,db_lock= db_lock, minim=10, maxim=100)
print 'n filtered track by plays : %d' % len(f3_2.IDs)
filt_manager_track.log_filter(f3_2)

f3_3 = FilterPlaysTrack(name='f3_3', user=user, db_path=db_path, db_lock=db_lock, minim=0, maxim=100)
print 'n filtered track by plays : %d' % len(f3_3.IDs)
filt_manager_track.log_filter(f3_3)

assert len(f3_1.IDs) >=  len(f3_2.IDs)
assert len(f3_3.IDs) >=  len(f3_2.IDs)

# add artist plays filter
f4_1 = FilterPlaysArtist(name='f4_1', user=user, db_path=db_path, db_lock=db_lock, category='track', minim=10)
print 'n filtered tracks by artist plays : %d' % len(f4_1.IDs)
filt_manager_track.log_filter(f4_1)

f4_2 = FilterPlaysArtist(name='f4_2',user= user, db_path=db_path, db_lock=db_lock, category='track', minim=10, maxim=900)
print 'n filtered tracks by artist plays : %d' % len(f4_2.IDs)
filt_manager_track.log_filter(f4_2)

f4_3 = FilterPlaysArtist(name='f4_3', user=user, db_path=db_path, db_lock=db_lock, category='track', minim=0, maxim=900)
print 'n filtered tracks by artist plays : %d' % len(f4_3.IDs)
filt_manager_track.log_filter(f4_3)

assert len(f4_1.IDs) >=  len(f4_2.IDs)
assert len(f4_3.IDs) >=  len(f4_2.IDs)

#add date filter

f5_1 = FilterDateTrack(name='f5_1', db_path=db_path, db_lock=db_lock, minim=1990,include_undated=False)
print 'n filtered Tracks by date : %d' % len(f5_1.IDs)
filt_manager_track.log_filter(f5_1)

f5_2 = FilterDateTrack(name='f5_2', db_path=db_path, db_lock=db_lock, minim=1990, maxim=2014, include_undated=False)
print 'n filtered Tracks by date : %d' % len(f5_2.IDs)
filt_manager_track.log_filter(f5_2)

f5_3 = FilterDateTrack(name='f5_3', db_path=db_path, db_lock=db_lock, minim=1990, maxim=2014, include_undated=True)
print 'n filtered Tracks by date : %d' % len(f5_3.IDs)
filt_manager_track.log_filter(f5_3)

assert len(f5_1.IDs) >= len(f5_2.IDs)
assert len(f5_3.IDs) >= len(f5_2.IDs)

#add date filter artist

f6_1 =FilterDateArtist(name='f6_1', db_path=db_path, db_lock=db_lock, category='track', minim=1990, include_undated=False)
print 'n filtered tracks by artist date : %d' % len(f6_1.IDs)
filt_manager_track.log_filter(f6_1)

f6_2 =FilterDateArtist(name='f6_2', db_path=db_path, db_lock=db_lock, category='track', minim=1990, maxim=2014, include_undated=False)
print 'n filtered tracks by artist date : %d' % len(f6_2.IDs)
filt_manager_track.log_filter(f6_2)

f6_3 =FilterDateArtist(name='f6_3', db_path=db_path, db_lock=db_lock, category='track', minim=1990,  maxim=2014, include_undated=True)
print 'n filtered tracks by artist date : %d' % len(f6_3.IDs)
filt_manager_track.log_filter(f6_3)

assert len(f6_1.IDs) >= len(f6_2.IDs)
assert len(f6_3.IDs) >= len(f6_2.IDs)

assert len(f6_1.IDs) != len(f5_1.IDs)
assert len(f6_2.IDs) != len(f5_2.IDs)
assert len(f6_3.IDs) != len(f5_3.IDs)


filt_manager_track.update_filterIDs(con=con)
print 'n all filters combined: %d' % len(filt_manager_track.filtered_IDs)

print 'deleting tag filters'
filt_manager_track.del_filter(f1_1)
filt_manager_track.del_filter(f2)

filt_manager_track.update_filterIDs(con=con)
print 'n all filters combined: %d' % len(filt_manager_track.filtered_IDs)


# ALBUM FILTERS
filt_manager_album = FilterManager(db_path=db_path, db_lock=db_lock,user= user, category='album',freq_init=True)

# tag filter 1, no threshold
f1 = FilterAlbumTag(name='f1',tag= 'indie', thresh=0, db_path=db_path, db_lock=db_lock)
print 'n filtered albums by tag_album: %d' % len(f1.IDs)
filt_manager_album.log_filter(f1)

# test if filtering was successfull
freq_init=filt_manager_album.calc_freq_tag(thresh=0, con=con)
count_init=freq_init[ freq_init['tagName'] == 'indie' ]['count']
assert len(f1.IDs) == count_init.iloc[0]

# delete filter and check for successful deletion
assert 'f1' in filt_manager_album.filter_log
filt_manager_album.del_filter(f1)
assert 'f1' not in filt_manager_album.filter_log

# 2nd tag filter with threshold
f1_1 = FilterAlbumTag(name='f1_1',tag= 'alternative',thresh= 75, db_path=db_path, db_lock=db_lock)
print 'n filtered albums by tag_album: %d' % len(f1_1.IDs)
filt_manager_album.log_filter(f1_1)

# test if thresholded filtering was successfull
freq_75=filt_manager_album.calc_freq_tag(thresh=75, con=con)
count_init=freq_75[ freq_75['tagName'] == 'alternative' ]['count']
assert len(f1_1.IDs) == count_init.iloc[0]

# add artisttag filter
f2 = FilterArtistTag(name='f2',tag= 'alternative', thresh=75, category='album', db_path=db_path, db_lock=db_lock)
print 'n filtered albums by tag_artist: %d' % len(f2.IDs)
filt_manager_album.log_filter(f2)

# test if filter is correct
DF = pd.read_sql('SELECT ID_tag.tagName, COUNT (ID_tag.tagName) AS count, ID_tag.url\
                FROM tag_artist \
                LEFT JOIN ID_album ON tag_artist.artistID=ID_album.artistID\
                LEFT JOIN ID_tag ON ID_tag.tagID=tag_artist.tagID \
                WHERE tag_artist.count>=? \
                GROUP BY ID_tag.tagID' , con, params=(75,))

assert len(f2.IDs) == DF['count'][ DF['tagName']=='alternative'].iloc[0]

# add plays filter
f3_1 = FilterPlaysAlbum(name='f3_1', user=user, db_path=db_path, db_lock=db_lock, minim=10)
print 'n filtered album by plays : %d' % len(f3_1.IDs)
filt_manager_album.log_filter(f3_1)

f3_2 = FilterPlaysAlbum(name='f3_2',user= user, db_path=db_path, db_lock= db_lock, minim=400, maxim=550)
print 'n filtered album by plays : %d' % len(f3_2.IDs)
filt_manager_album.log_filter(f3_2)

f3_3 = FilterPlaysAlbum(name='f3_3', user=user, db_path=db_path, db_lock=db_lock, minim=400, maxim=550)
print 'n filtered album by plays : %d' % len(f3_3.IDs)
filt_manager_album.log_filter(f3_3)

assert len(f3_1.IDs) >=  len(f3_2.IDs)
assert len(f3_3.IDs) >=  len(f3_2.IDs)

# add artist plays filter
f4_1 = FilterPlaysArtist(name='f4_1',user= user, db_path=db_path, db_lock=db_lock, category='album', minim=10)
print 'n filtered albums by artist plays : %d' % len(f4_1.IDs)
filt_manager_album.log_filter(f4_1)

f4_2 = FilterPlaysArtist(name='f4_2', user=user, db_path=db_path, db_lock=db_lock, category='album', minim=10, maxim=900)
print 'n filtered albums by artist plays : %d' % len(f4_2.IDs)
filt_manager_album.log_filter(f4_2)

f4_3 = FilterPlaysArtist(name='f4_3', user=user, db_path=db_path, db_lock=db_lock, category='album', minim=0, maxim=900)
print 'n filtered albums by artist plays : %d' % len(f4_3.IDs)
filt_manager_album.log_filter(f4_3)

assert len(f4_1.IDs) >=  len(f4_2.IDs)
assert len(f4_3.IDs) >=  len(f4_2.IDs)

#add date filter

f5_1 = FilterDateAlbum(name='f5_1', db_path=db_path, db_lock=db_lock, minim=1990,include_undated=False)
print 'n filtered album by date : %d' % len(f5_1.IDs)
filt_manager_album.log_filter(f5_1)

f5_2 = FilterDateAlbum(name='f5_2', db_path=db_path, db_lock=db_lock, minim=1990, maxim=2014, include_undated=False)
print 'n filtered album by date : %d' % len(f5_2.IDs)
filt_manager_album.log_filter(f5_2)

f5_3 = FilterDateAlbum(name='f5_3', db_path=db_path, db_lock=db_lock, minim=1990, maxim=2014, include_undated=True)
print 'n filtered album by date : %d' % len(f5_3.IDs)
filt_manager_album.log_filter(f5_3)

assert len(f5_1.IDs) >= len(f5_2.IDs)
assert len(f5_3.IDs) >= len(f5_2.IDs)

#add date filter artist

f6_1 =FilterDateArtist(name='f6_1', db_path=db_path, db_lock=db_lock, category='album', minim=1990, include_undated=False)
print 'n filtered albums by artist date : %d' % len(f6_1.IDs)
filt_manager_album.log_filter(f6_1)

f6_2 =FilterDateArtist(name='f6_2', db_path=db_path, db_lock=db_lock, category='album', minim=1990, maxim=2014, include_undated=False)
print 'n filtered albums by artist date : %d' % len(f6_2.IDs)
filt_manager_album.log_filter(f6_2)

f6_3 =FilterDateArtist(name='f6_3', db_path=db_path, db_lock=db_lock, category='album', minim=1990,  maxim=2014, include_undated=True)
print 'n filtered albums by artist date : %d' % len(f6_3.IDs)
filt_manager_album.log_filter(f6_3)

assert len(f6_1.IDs) >= len(f6_2.IDs)
assert len(f6_3.IDs) >= len(f6_2.IDs)

assert len(f6_1.IDs) != len(f5_1.IDs)
assert len(f6_2.IDs) != len(f5_2.IDs)
assert len(f6_3.IDs) != len(f5_3.IDs)


filt_manager_album.update_filterIDs(con=con)
print 'n all filters combined: %d' % len(filt_manager_album.filtered_IDs)

print 'deleting tag filters'
filt_manager_album.del_filter(f1_1)
filt_manager_album.del_filter(f2)

filt_manager_album.update_filterIDs(con=con)
print 'n all filters combined: %d' % len(filt_manager_album.filtered_IDs)

# Track initial frequencies
print '\n'
print 'initial track tag frequency'
print filt_manager_track.freq_tag_init[['tagName','count']].sort_values(by='count',ascending=False ).head(5)
print '\n'
print 'initial track release date frequency'
print filt_manager_track.freq_date_init.sort_values(by='count',ascending=False).head(5)
print '\n'
print 'initial artist tag frequency'
print filt_manager_track.freq_tag_artist_init[['tagName','count']].sort_values(by='count',ascending=False).head(5)
print '\n'
print 'initial artist release date frequency'
print filt_manager_track.freq_date_artist_init.sort_values(by='count',ascending=False).head(5)

# Track filtered frequencies
print '\n'
print 'filtered track tag frequency'
print filt_manager_track.calc_freq_tag(con=con)[['tagName','count']].sort_values(by='count',ascending=False).head(5)
print '\n'
print 'filtered track release date frequency'
print filt_manager_track.calc_freq_date(con=con).sort_values(by='count',ascending=False).head(5)
print '\n'
print 'filtered artist tag frequency'
print filt_manager_track.calc_freq_tag(artist=True, con=con)[['tagName','count']].sort_values(by='count',ascending=False).head(5)
print '\n'
print 'filtered artist release date frequency'
print filt_manager_track.calc_freq_date(artist=True, con=con).sort_values(by='count',ascending=False).head(5)


# Album initial frequencies
print '\n'
print 'initial album tag frequency'
print filt_manager_album.freq_tag_init[['tagName','count']].sort_values(by='count',ascending=False ).head(5)
print '\n'
print 'initial album release date frequency'
print filt_manager_album.freq_date_init.sort_values(by='count',ascending=False).head(5)
print '\n'
print 'initial artist tag frequency'
print filt_manager_album.freq_tag_artist_init[['tagName','count']].sort_values(by='count',ascending=False).head(5)
print '\n'
print 'initial artist release date frequency'
print filt_manager_album.freq_date_artist_init.sort_values(by='count',ascending=False).head(5)

# Album filtered frequencies
print '\n'
print 'filtered album tag frequency'
print filt_manager_album.calc_freq_tag(con=con)[['tagName','count']].sort_values(by='count',ascending=False).head(5)
print '\n'
print 'filtered album release date frequency'
print filt_manager_album.calc_freq_date().sort_values(by='count',ascending=False).head(5)
print '\n'
print 'filtered artist tag frequency'
print filt_manager_album.calc_freq_tag(artist=True, con=con)[['tagName','count']].sort_values(by='count',ascending=False).head(5)
print '\n'
print 'filtered artist release date frequency'
print filt_manager_album.calc_freq_date(artist=True).sort_values(by='count',ascending=False).head(5)


