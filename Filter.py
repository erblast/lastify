import time
import threading
import pandas as pd
import sqlite3

def aggr_date_data(con, category, refresh=False, db_lock=threading.RLock()):
    '''aggregates date data in several temporary tables which can be used by filters or date frequency calculations'''
    c=con.cursor()
    db_lock.acquire()

    if refresh and category=='track' or category=='album':

        c.execute('DROP TABLE IF EXISTS date_joins_track')

        # Creates temporary joined table containing all date data connecting album and tracks
        c.execute('CREATE  TABLE IF NOT EXISTS date_joins_track AS \
                   SELECT ID_track.trackID, date_album.albumID, date_album.date_int  \
                   FROM ID_track \
                   LEFT JOIN track_rel_album ON track_rel_album.trackID = ID_track.trackID \
                   LEFT JOIN date_album      ON date_album.albumID      = track_rel_album.albumID ')

    if refresh and category=='track':

        c.execute('DROP TABLE IF EXISTS date_track')

        # Groups and aggregates date_track data in new temporary table, selects first release date of track
        c.execute('CREATE TABLE IF NOT EXISTS date_track AS \
                   SELECT trackID, MIN(date_int) AS date_int FROM date_joins_track \
                   GROUP BY trackID')

        c.execute('CREATE INDEX track ON date_track (trackID)')

    if refresh and category=='artist':

        c.execute('DROP TABLE IF EXISTS date_joins_artist')
        c.execute('DROP TABLE IF EXISTS date_artist')

        # Creates temporary joined table containing all date data connecting album and artist + album plays
        c.execute('CREATE  TABLE IF NOT EXISTS date_joins_artist AS \
                   SELECT ID_artist.artistID, plays_album.plays, date_album.date_int  \
                   FROM ID_artist \
                   LEFT JOIN ID_album    ON ID_album.artistID   = ID_artist.artistID \
                   LEFT JOIN date_album  ON date_album.albumID  = ID_album.albumID \
                   LEFT JOIN plays_album ON plays_album.albumID = ID_album.albumID \
                   LEFT JOIN ID_user     ON ID_user.userID      = plays_album.userID \
                   WHERE ID_user.userName = "total" ')


        # Groups and aggregates date_artist data in new temporary table. selects release date of most played release
        c.execute('CREATE TABLE IF NOT EXISTS date_artist AS \
                   SELECT artistID, date_int, MAX(plays) AS date_int FROM date_joins_artist \
                   GROUP BY artistID')

        c.execute('CREATE INDEX artist ON date_artist (artistID)')

    con.commit()
    db_lock.release()

class FilterManager(object):
    """
    creates and deletes filter instances. Collects all filtered items from all filters and makes an inner join, which
    is saved as a temporary table in the database.

    calc_freq_tag= calculates tag frequencies of items in temporary table. When filtermanager instance is created tag
                frequencies of all items in database will be calculated and stored.

    calc_freq_date= calculates release date frequency of all items in database and of temporary table

    """

    def __init__(self, db_path, category='track', thresh=0, db_lock=threading.RLock()):
        self.db_path = db_path
        self.lock = db_lock
        self.category = category
        self.con = sqlite3.connect(db_path)
        self.c = self.con.cursor()
        self.unfiltered_IDs = []
        self.thresh = thresh
        self.filter_log={}
        self.filtered_IDs=pd.DataFrame()

        self.load_unfiltered_IDs()
        self.feed_table(self.unfiltered_IDs)
        print 'unfiltered IDs loaded'
        self.freq_tag_init = self.calc_freq_tag(self.thresh)
        print 'initial tag frequency loaded'
        self.freq_date_init = self.calc_freq_date()
        print 'initial date frequency loaded'

        self.freq_tag=pd.DataFrame()
        self.freq_date=pd.DataFrame()

    def create_table(self):

        self.lock.acquire()

        if self.category == 'track':
            self.c.execute('CREATE TABLE IF NOT EXISTS temp_track \
                            (trackID INTEGER PRIMARY KEY)')

            self.c.execute('CREATE VIEW IF NOT EXISTS view_temp_track \
                            AS SELECT temp_track.trackID, ID_track.trackName, ID_artist.artistName, date_album.date_int,\
                            plays_track.plays AS plays_track, plays_track.userID AS plays_track_userID, \
                            plays_artist.plays AS plays_artist, plays_artist.userID AS plays_artist_userID,\
                            i_score_track.i_score AS i_score_track, i_score_artist.i_score AS i_score_artist,\
                            date_album.albumID\
                            FROM temp_track \
                            LEFT JOIN ID_track ON ID_track.trackID=temp_track.trackID \
                            LEFT JOIN track_rel_album ON track_rel_album.trackID=temp_track.trackID \
                            LEFT JOIN date_album ON track_rel_album.albumID=date_album.albumID \
                            LEFT JOIN ID_artist ON ID_artist.artistID=ID_track.artistID \
                            LEFT JOIN plays_track ON plays_track.trackID=temp_track.trackID\
                            LEFT JOIN plays_artist ON ID_track.artistID=plays_artist.artistID \
                            LEFT JOIN i_score_track ON i_score_track.trackID =temp_track.trackID\
                            LEFT JOIN i_score_artist ON i_score_artist.artistID=ID_track.artistID'
                            )

        if self.category == 'album':
            self.c.execute('CREATE TABLE IF NOT EXISTS temp_album \
                            (albumID INTEGER PRIMARY KEY)')

            self.c.execute('CREATE VIEW IF NOT EXISTS view_temp_album \
                            AS SELECT temp_album.albumID, ID_album.albumName, ID_artist.artistName, date_album.date_int, \
                            plays_album.plays AS plays_album, plays_album.userID AS plays_album_userID, \
                            i_score_artist.i_score AS i_score_artist, plays_artist.plays AS plays_artist, \
                            plays_artist.userID AS plays_artist_userID \
                            FROM temp_album \
                            LEFT JOIN ID_album ON ID_album.albumID=temp_album.albumID \
                            LEFT JOIN date_album ON temp_album.albumID=date_album.albumID \
                            LEFT JOIN ID_artist ON ID_artist.artistID=ID_album.artistID \
                            LEFT JOIN plays_artist ON (ID_album.artistID=plays_artist.artistID AND plays_artist.userID=plays_album.userID)\
                            LEFT JOIN plays_album ON plays_album.albumID=temp_album.albumID\
                            LEFT JOIN i_score_artist ON i_score_artist.artistID=ID_album.artistID'
                            )


        if self.category == 'artist':
            self.c.execute('CREATE TABLE IF NOT EXISTS temp_artist \
                            (artistID INTEGER PRIMARY KEY)')
        self.con.commit()
        self.lock.release()

    def drop_table(self):

        self.lock.acquire()
        if self.category == 'track':
            self.c.execute('DROP TABLE IF EXISTS temp_track ')

        if self.category == 'album':
            self.c.execute('DROP TABLE IF EXISTS temp_album ')

        if self.category == 'artist':
            self.c.execute('DROP TABLE IF EXISTS temp_artist ')

        self.con.commit()
        self.lock.release()

    def load_unfiltered_IDs(self):

        self.lock.acquire()
        if self.category == 'track':
            DF = pd.read_sql('SELECT trackID FROM ID_track', self.con)

        if self.category == 'album':
            DF = pd.read_sql('SELECT albumID FROM ID_album', self.con)

        if self.category == 'artist':
            DF = pd.read_sql('SELECT trackID FROM ID_track', self.con)
        self.lock.release()

        self.unfiltered_IDs = [(int(ID),) for i, ID in DF.itertuples()]

    def feed_table(self, IDs):

        assert type(IDs)==list and type(IDs[0])==tuple

        self.lock.acquire()
        self.drop_table()
        self.create_table()

        if self.category == 'track':
            self.c.executemany('INSERT OR IGNORE INTO temp_track (trackID) VALUES (?)', IDs)

        if self.category == 'album':
            self.c.executemany('INSERT OR IGNORE INTO temp_album (albumID) VALUES (?)', IDs)

        if self.category == 'artist':
            self.c.executemany('INSERT OR IGNORE INTO temp_artist (artistID) VALUES (?)', IDs)

        self.con.commit()
        self.lock.release()

    def log_filter(self,Filter_instance):

        if hasattr(Filter_instance, 'category'):
            assert Filter_instance.category==self.category

        self.filter_log[Filter_instance.name]=Filter_instance

    def del_filter(self,Filter_instance):

        del self.filter_log[Filter_instance.name]
        del Filter_instance

    def update_filterIDs(self):

        for filter in self.filter_log.values():

            if self.filtered_IDs.shape[0]==0:
                self.filtered_IDs=self.filtered_IDs.append(filter.IDs)

            else:
                self.filtered_IDs=pd.merge(self.filtered_IDs, filter.IDs, 'inner',
                                             on=[ self.filtered_IDs.columns[0], filter.IDs.columns[0] ] )


        IDs_tup=[(int(ID),) for i,ID in self.filtered_IDs.itertuples()]

        self.feed_table(IDs_tup)

    def calc_freq_tag(self, thresh):

        self.lock.acquire()
        if self.category == 'track':

            DF = pd.read_sql('SELECT ID_tag.tagName, COUNT (ID_tag.tagName) AS count, ID_tag.url \
                            FROM tag_track \
                            INNER JOIN temp_track ON temp_track.trackID=tag_track.trackID \
                            LEFT JOIN ID_tag ON ID_tag.tagID=tag_track.tagID \
                            WHERE tag_track.count>=? \
                            GROUP BY ID_tag.tagID' , self.con, params=(thresh,))

        if self.category == 'album':
            DF = pd.read_sql('SELECT ID_tag.tagName, COUNT (ID_tag.tagName) AS count, ID_tag.url\
                            FROM tag_album \
                            INNER JOIN temp_album ON temp_album.albumID=tag_album.albumID \
                            LEFT JOIN ID_tag ON ID_tag.tagID=tag_album.tagID \
                            WHERE tag_album.count>=? \
                            GROUP BY ID_tag.tagID' , self.con, params=(thresh,))

        if self.category == 'artist':
            DF = pd.read_sql('SELECT ID_tag.tagName, COUNT (ID_tag.tagName) AS count, ID_tag.url\
                            FROM tag_artist \
                            INNER JOIN temp_artist ON temp_artist.artistID=tag_artist.artistID \
                            LEFT JOIN ID_tag ON ID_tag.tagID=tag_artist.tagID \
                            WHERE tag_artist.count>=? \
                            GROUP BY ID_tag.tagID' , self.con, params=(thresh,))

        self.lock.release()
        self.freq_tag=DF
        return DF

    def calc_freq_date(self):

        self.lock.acquire()

        c=self.con.cursor()

        aggr_date_data(self.con, self.category, self.lock)

        if self.category == 'track':
            DF = pd.read_sql('SELECT date_track.date_int, COUNT(date_track.date_int) as count \
                            FROM temp_track \
                            LEFT JOIN date_track ON date_track.trackID=temp_track.trackID \
                            GROUP BY date_track.date_int', self.con)

        if self.category == 'album':
            DF = pd.read_sql('SELECT date_album.date_int, COUNT(date_album.date_int) as count \
                            FROM temp_album \
                            LEFT JOIN date_album ON date_album.albumID=temp_album.albumID \
                            GROUP BY date_album.date_int', self.con)

        if self.category == 'artist':
            DF = pd.read_sql('SELECT date_artist.date_int, COUNT(date_artist.date_int) as count \
                            FROM temp_artist \
                            LEFT JOIN date_artist  ON date_artist.artistID=temp_artist.artistID \
                            GROUP BY date_artist.date_int', self.con)
        self.lock.release()
        self.freq_date=DF['date_int'].value_counts()
        return DF['date_int'].value_counts()

class Filter(object):

    def __init__(self, name, con, db_lock):
        self.name = name
        self.lock = db_lock
        self.con=con

#FILTER TAGS
#________________________________________________________________________________________________________________________
class FilterTrackTag(Filter):
    def __init__(self, name, tag, thresh, con, db_lock):
        Filter.__init__(self,name, con, db_lock)

        self.tag = tag
        self.thresh=thresh
        self.lock.acquire()
        self.IDs=pd.read_sql('SELECT tag_track.trackID FROM tag_track LEFT JOIN ID_tag ON ID_tag.tagID=tag_track.tagID \
                              WHERE ID_tag.tagName=? AND tag_track.count>=?',self.con, params=(self.tag,self.thresh))
        self.lock.release()

class FilterAlbumTag(Filter):

    def __init__(self, name, tag, thresh, con, db_lock):
        Filter.__init__(self, name, con, db_lock)

        self.tag = tag
        self.thresh=thresh
        self.lock.acquire()
        self.IDs=pd.read_sql('SELECT tag_album.albumID FROM tag_album LEFT JOIN ID_tag ON ID_tag.tagID=tag_album.tagID \
                              WHERE ID_tag.tagName=? AND tag_album.count>=?',self.con, params=(self.tag,self.thresh))
        self.lock.release()

class FilterArtistTag(Filter):

    def __init__(self, name, tag, thresh, category, con, db_lock):
        Filter.__init__(self, name, con, db_lock)

        self.tag = tag
        self.thresh=thresh
        self.category=category
        assert self.category=='track' or self.category=='album'

        self.lock.acquire()

        if self.category=='track':
            self.IDs=pd.read_sql('SELECT ID_track.trackID \
                                  FROM tag_artist LEFT JOIN ID_tag ON ID_tag.tagID=tag_artist.tagID \
                                  LEFT JOIN ID_track ON ID_track.artistID=tag_artist.artistID\
                                  WHERE ID_tag.tagName=? AND tag_artist.count>=?',
                                  self.con, params=(self.tag,self.thresh))

        if self.category=='album':
            self.IDs=pd.read_sql('SELECT ID_album.albumID \
                                  FROM tag_artist LEFT JOIN ID_tag ON ID_tag.tagID=tag_artist.tagID \
                                  LEFT JOIN ID_album ON ID_album.artistID=tag_artist.artistID\
                                  WHERE ID_tag.tagName=? AND tag_artist.count>=?',
                                  self.con, params=(self.tag,self.thresh))

        self.lock.release()

#FILTER PLAYS
#________________________________________________________________________________________________________________________
class FilterPlaysTrack(Filter):

    def __init__(self, name, user, con, db_lock, minim=None, maxim=None):
        Filter.__init__(self, name, con, db_lock)

        self.user=user
        self.minim=minim
        self.maxim=maxim

        if (maxim and min):
            assert self.minim<self.maxim

        self.lock.acquire()
        if self.maxim and self.minim:
            self.IDs=pd.read_sql('SELECT ID_track.trackID \
                                  FROM ID_track \
                                  LEFT JOIN plays_track ON plays_track.trackID=ID_track.trackID \
                                  LEFT JOIN ID_user ON ID_user.userID=plays_track.userID\
                                  WHERE ID_user.userName=? AND plays_track.plays>=? AND plays_track.plays<=?',
                                  self.con, params=(self.user, self.minim, self.maxim))

        if self.maxim and not self.minim:
            self.IDs=pd.read_sql('SELECT ID_track.trackID \
                                  FROM ID_track \
                                  LEFT JOIN plays_track ON plays_track.trackID=ID_track.trackID \
                                  LEFT JOIN ID_user ON ID_user.userID=plays_track.userID\
                                  WHERE (ID_user.userName=? AND  plays_track.plays<=?) OR ID_user.userName ISNULL',
                                  self.con, params=(self.user, self.maxim))

        if not self.maxim and self.minim:
            self.IDs=pd.read_sql('SELECT ID_track.trackID \
                                  FROM ID_track \
                                  LEFT JOIN plays_track ON plays_track.trackID=ID_track.trackID \
                                  LEFT JOIN ID_user ON ID_user.userID=plays_track.userID\
                                  WHERE ID_user.userName=? AND plays_track.plays>=? )',
                                  self.con, params=(self.user, self.minim))
        self.lock.release()

class FilterPlaysAlbum(Filter):

    def __init__(self, name, user, con, db_lock, minim=None, maxim=None):
        Filter.__init__(self, name, con, db_lock)

        self.user=user
        self.minim=minim
        self.maxim=maxim

        if (maxim and min):
            assert self.minim<self.maxim

        self.lock.acquire()
        if self.maxim and self.minim:
            self.IDs=pd.read_sql('SELECT ID_album.albumID \
                                  FROM ID_album \
                                  LEFT JOIN plays_album ON plays_album.albumID=ID_album.albumID \
                                  LEFT JOIN ID_user ON ID_user.userID=plays_album.userID\
                                  WHERE ID_user.userName=? AND plays_album.plays>=? AND plays_album.plays<=?',
                                  self.con, params=(self.user, self.minim, self.maxim))

        if self.maxim and not self.minim:
            self.IDs=pd.read_sql('SELECT ID_album.albumID \
                                  FROM ID_album \
                                  LEFT JOIN plays_album ON plays_album.albumID=ID_album.albumID \
                                  LEFT JOIN ID_user ON ID_user.userID=plays_album.userID\
                                  WHERE (ID_user.userName=? AND  plays_album.plays<=?) OR ID_user.userName ISNULL',
                                  self.con, params=(self.user, self.maxim))

        if not self.maxim and self.minim:
            self.IDs=pd.read_sql('SELECT ID_album.albumID \
                                  FROM ID_album \
                                  LEFT JOIN plays_album ON plays_album.albumID=ID_album.albumID \
                                  LEFT JOIN ID_user ON ID_user.userID=plays_album.userID\
                                  WHERE ID_user.userName=? AND plays_album.plays>=? )',
                                  self.con, params=(self.user, self.minim))
        self.lock.release()

class FilterPlaysArtist(Filter):

    def __init__(self, name, user, con, db_lock, category, minim=None, maxim=None):
        Filter.__init__(self, name, con, db_lock)

        self.user=user
        self.minim=minim
        self.maxim=maxim
        self.category=category

        if (maxim and min):
            assert self.minim<self.maxim

        self.lock.acquire()

        # not every artistID relates to track or album ID !!!!!!!!!!

        if category=='track':

            if self.maxim and self.minim:


                self.IDs=pd.read_sql('SELECT ID_track.trackID \
                                      FROM ID_track \
                                      LEFT JOIN plays_artist ON plays_artist.artistID=ID_track.artistID \
                                      LEFT JOIN ID_artist ON plays_artist.artistID=ID_artist.artistID \
                                      LEFT JOIN ID_user ON ID_user.userID=plays_artist.userID \
                                      WHERE ID_user.userName=? AND plays_artist.plays>=? \
                                      AND plays_artist.plays<=?' ,self.con, params=(self.user, self.minim, self.maxim))


            if self.maxim and not self.minim:
                self.IDs=pd.read_sql('SELECT ID_track.trackID \
                                      FROM ID_track \
                                      LEFT JOIN plays_artist ON plays_artist.artistID=ID_track.artistID \
                                      LEFT JOIN ID_artist ON plays_artist.artistID=ID_artist.artistID \
                                      LEFT JOIN ID_user ON ID_user.userID=plays_artist.userID \
                                      WHERE ((ID_user.userName=? AND plays_artist.plays<=? ) OR ID_user.userName ISNULL)\
                                      AND ID_track.trackID NOTNULL',
                                      self.con, params=(self.user,  self.maxim))

            if not self.maxim and self.minim:
                self.IDs=pd.read_sql('SELECT ID_track.trackID \
                                      FROM ID_track \
                                      LEFT JOIN plays_artist ON plays_artist.artistID=ID_track.artistID \
                                      LEFT JOIN ID_artist ON plays_artist.artistID=ID_artist.artistID \
                                      LEFT JOIN ID_user ON ID_user.userID=plays_artist.userID \
                                      WHERE (ID_user.userName=? AND plays_artist.plays>=? ) \
                                      AND ID_track.trackID NOTNULL',
                                      self.con, params=(self.user, self.minim))

        if category=='album':
            if self.maxim and self.minim:
                self.IDs=pd.read_sql('SELECT ID_album.albumID \
                                      FROM ID_album \
                                      LEFT JOIN plays_artist ON plays_artist.artistID=ID_album.artistID \
                                      LEFT JOIN ID_artist ON plays_artist.artistID=ID_artist.artistID \
                                      LEFT JOIN ID_user ON ID_user.userID=plays_artist.userID \
                                      WHERE ID_user.userName=? AND plays_artist.plays>=? \
                                      AND plays_artist.plays<=?' ,self.con, params=(self.user, self.minim, self.maxim))


            if self.maxim and not self.minim:
                self.IDs=pd.read_sql('SELECT ID_album.albumID \
                                      FROM ID_album \
                                      LEFT JOIN plays_artist ON plays_artist.artistID=ID_album.artistID \
                                      LEFT JOIN ID_artist ON plays_artist.artistID=ID_artist.artistID \
                                      LEFT JOIN ID_user ON ID_user.userID=plays_artist.userID \
                                      WHERE ((ID_user.userName=? AND plays_artist.plays<=? ) OR ID_user.userName ISNULL)\
                                      AND ID_album.albumID NOTNULL',
                                      self.con, params=(self.user,  self.maxim))

            if not self.maxim and self.minim:
                self.IDs=pd.read_sql('SELECT ID_album.albumID \
                                      FROM ID_album \
                                      LEFT JOIN plays_artist ON plays_artist.artistID=ID_album.artistID \
                                      LEFT JOIN ID_artist ON plays_artist.artistID=ID_artist.artistID \
                                      LEFT JOIN ID_user ON ID_user.userID=plays_artist.userID \
                                      WHERE (ID_user.userName=? AND plays_artist.plays>=? ) \
                                      AND ID_album.albumID NOTNULL',
                                      self.con, params=(self.user, self.minim))
        self.lock.release()

#FILTER DATE
#________________________________________________________________________________________________________________________
class FilterDateTrack(Filter):

    def __init__(self, name, con, db_lock, minim=0, maxim=time.strftime('%Y', time.localtime()), include_undated=True):
        Filter.__init__(self, name, con, db_lock)

        self.minim=minim
        self.maxim=maxim

        if (maxim and min):
            assert self.minim<self.maxim

        self.lock.acquire()

        c=con.cursor()

        aggr_date_data(con, 'track', self.lock)

        if not include_undated:
            self.IDs = pd.read_sql('SELECT trackID \
                                    FROM date_track \
                                    WHERE date_int>=? AND date_int<=?',
                                    self.con, params=(self.minim, self.maxim))

        if include_undated:
            self.IDs = pd.read_sql('SELECT trackID\
                                    FROM date_track \
                                    WHERE (date_int>=? AND date_int<=?) \
                                    OR date_int ISNULL',
                                    self.con, params=(self.minim, self.maxim))

        self.lock.release()

class FilterDateAlbum(Filter):

    def __init__(self, name, con, db_lock, minim=0, maxim=time.strftime('%Y', time.localtime()), include_undated=True):
        Filter.__init__(self, name, con, db_lock)

        self.minim=minim
        self.maxim=maxim

        if (maxim and min):
            assert self.minim<self.maxim

        self.lock.acquire()
        if not include_undated:
            self.IDs = pd.read_sql('SELECT ID_album.albumID\
                                    FROM ID_album \
                                    INNER JOIN date_album ON date_album.albumID=ID_album.albumID \
                                    WHERE date_album.date_int>=? AND date_album.date_int<=?',
                                    self.con, params=(self.minim, self.maxim))

        if include_undated:
            self.IDs = pd.read_sql('SELECT ID_album.albumID\
                                    FROM ID_album \
                                    LEFT JOIN date_album ON date_album.albumID=ID_album.albumID \
                                    WHERE date_album.date_int>=? AND date_album.date_int<=? \
                                    OR date_album.date_int ISNULL',
                                    self.con, params=(self.minim, self.maxim))

        self.lock.release()

class FilterDateArtist(Filter):

    def __init__(self, name, con, db_lock, minim=0, maxim=time.strftime('%Y', time.localtime()), include_undated=True):
        Filter.__init__(self, name, con, db_lock)

        self.minim=minim
        self.maxim=maxim

        if (maxim and min):
            assert self.minim<self.maxim

        self.lock.acquire()

        c=con.cursor()

        aggr_date_data(con, 'artist', self.lock)

        if not include_undated:
            self.IDs = pd.read_sql('SELECT artistID \
                                    FROM artist_track \
                                    WHERE date_int>=? AND date_int<=?',
                                    self.con, params=(self.minim, self.maxim))

        if include_undated:
            self.IDs = pd.read_sql('SELECT artistID\
                                    FROM artist_track \
                                    WHERE (date_int>=? AND date_int<=?) \
                                    OR date_int ISNULL',
                                    self.con, params=(self.minim, self.maxim))

        self.lock.release()
