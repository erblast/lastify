
# Creates a view table from temp_track or temp_albums and applies different sorts
# Data assembled by view consists of the variations of iscore, plays and release date that are relevant for sorting
# Limits number of tracks/ albums from one artist
# Writes a Spotify Playlist as txt file


# create view feature can take very long 10s for 100 entries it should refuse more than 5000 items in corresponding temp table

import time
import threading
import pandas as pd
import sqlite3

class Writer(object):

    def __init__(self, category, user, db_path, db_lock):
        self.category     = category
        self.con          = sqlite3.connect(db_path)
        self.lock         = db_lock
        self.out          = str()
        self.user         = user

        if self.category == 'track':
            self.sort_by_list = ['i_score_track','i_score_artist', 'plays_track_user','plays_track_total', 'plays_artist_user',
                                 'date_track', 'date_artist']

        if self.category == 'album':
            self.sort_by_list = ['i_score_artist', 'plays_album_user','plays_album_total', 'plays_artist_user',
                                 'date_album', 'date_artist']
        self.c = self.con.cursor()
        self.create_view()

    def create_view(self):

        self.lock.acquire()

        if self.category=='track':

            self.c.execute('DROP TABLE IF EXISTS view_temp_track')

            self.c.execute('CREATE TABLE view_temp_track \
                    AS SELECT temp_track.trackID, ID_track.trackName, ID_artist.artistName, \
                    date_track.date_int    AS date_track, \
                    date_artist.date_int   AS date_artist,\
                    sub_track_plays        AS plays_track_user, \
                    sub_artist_plays       AS plays_artist_user, \
                    sub_track_plays_tot    AS plays_track_total, \
                    i_score_track.i_score  AS i_score_track, \
                    i_score_artist.i_score AS i_score_artist\
                    FROM temp_track \
                    LEFT JOIN ID_track       ON ID_track.trackID        = temp_track.trackID \
                    LEFT JOIN date_track     ON date_track.trackID      = ID_track.trackID \
                    LEFT JOIN ID_artist      ON ID_artist.artistID      = ID_track.artistID \
                    LEFT JOIN date_artist    ON date_artist.artistID    = ID_artist.artistID \
                    LEFT JOIN (\
                                SELECT plays_track.plays   AS sub_track_plays, \
                                       plays_track.trackID AS sub_trackID \
                                FROM plays_track \
                                LEFT JOIN ID_user ON ID_user.userID = plays_track.userID \
                                WHERE ID_user.userName = ?  \
                                            )ON sub_trackID             = ID_track.trackID \
                    LEFT JOIN (\
                                SELECT plays_artist.plays    AS sub_artist_plays, \
                                       plays_artist.artistID AS sub_artistID \
                                FROM plays_artist \
                                LEFT JOIN ID_user ON ID_user.userID = plays_artist.userID \
                                WHERE ID_user.userName = ?  \
                                            )ON sub_artistID            = ID_artist.artistID \
                    LEFT JOIN (\
                                SELECT plays_track.plays   AS sub_track_plays_tot, \
                                       plays_track.trackID AS sub_trackID_tot \
                                FROM plays_track \
                                LEFT JOIN ID_user ON ID_user.userID = plays_track.userID \
                                WHERE ID_user.userName = ?  \
                                            )ON sub_trackID_tot         = ID_track.trackID \
                    LEFT JOIN i_score_track  ON i_score_track.trackID   = temp_track.trackID\
                    LEFT JOIN i_score_artist ON i_score_artist.artistID = ID_track.artistID  \
                    LIMIT 5000', (self.user, self.user, 'total'))


        if self.category=='album':

            self.c.execute('DROP TABLE IF EXISTS view_temp_album')

            self.c.execute('CREATE TABLE view_temp_album \
                    AS SELECT temp_album.albumID, ID_album.albumName, ID_artist.artistName, \
                    date_album.date_int    AS date_album, \
                    date_artist.date_int   AS date_artist,\
                    sub_album_plays        AS plays_album_user, \
                    sub_artist_plays       AS plays_artist_user, \
                    sub_album_plays_tot    AS plays_album_total, \
                    i_score_artist.i_score AS i_score_artist\
                    FROM temp_album \
                    LEFT JOIN ID_album       ON ID_album.albumID        = temp_album.albumID \
                    LEFT JOIN date_album     ON date_album.albumID      = ID_album.albumID \
                    LEFT JOIN ID_artist      ON ID_artist.artistID      = ID_album.artistID \
                    LEFT JOIN date_artist    ON date_artist.artistID    = ID_artist.artistID \
                    LEFT JOIN (\
                                SELECT plays_album.plays   AS sub_album_plays, \
                                       plays_album.albumID AS sub_albumID \
                                FROM plays_album \
                                LEFT JOIN ID_user ON ID_user.userID = plays_album.userID \
                                WHERE ID_user.userName = ?  \
                                            )ON sub_albumID             = ID_album.albumID \
                    LEFT JOIN (\
                                SELECT plays_artist.plays    AS sub_artist_plays, \
                                       plays_artist.artistID AS sub_artistID \
                                FROM plays_artist \
                                LEFT JOIN ID_user ON ID_user.userID = plays_artist.userID \
                                WHERE ID_user.userName = ?  \
                                            )ON sub_artistID            = ID_artist.artistID \
                    LEFT JOIN (\
                                SELECT plays_album.plays   AS sub_album_plays_tot, \
                                       plays_album.albumID AS sub_albumID_tot \
                                FROM plays_album \
                                LEFT JOIN ID_user ON ID_user.userID = plays_album.userID \
                                WHERE ID_user.userName = ?  \
                                            )ON sub_albumID_tot         = ID_album.albumID \
                    LEFT JOIN i_score_artist ON i_score_artist.artistID = ID_album.artistID \
                    LIMIT 5000', (self.user, self.user, 'total'))

        self.con.commit()
        self.lock.release()

    def limit(self, rank_by, topX , con, asc):
        # takes data from view_temp_track or view_temp_album groups by artist, sorts the groups and selects topX
        # SQLLITE does not offer an easy solution (convoluted SELECT statements, subselection, no TOP selection)
        # therefore pandas will perform this task

        assert rank_by in self.sort_by_list
        if rank_by == 'date_artist': # sorting by artist_date makes no sense if data will be grouped by artist
            rank_by = 'date_%s' % self.category
        if rank_by == 'i_score_artist':
            rank_by = 'i_score_track'

        self.lock.acquire()

        if self.category == 'track':
            df = pd.read_sql('SELECT * FROM view_temp_track', con)

        if self.category == 'album':
            df = pd.read_sql('SELECT * FROM view_temp_album', con)

        self.lock.release()

        df_sorted = df.sort_values(by=rank_by, ascending=asc)

        df_group  = df_sorted.groupby('artistName')

        df_top    = df_group.head(topX).sort_values(by = ['artistName',rank_by], ascending=[False,asc])

        self.lock.acquire()

        if self.category == 'track':

            df_top.to_sql('view_temp_track',con, if_exists='replace', index=False)

        if self.category == 'album':
            df_top.to_sql('view_temp_album',con, if_exists='replace', index=False)

        con.commit()
        self.lock.release()

    def sort(self,sort_by, con, asc=True):
        #sort_by == list of strings with column names
        #asc     == corresponding list of booleans True for ascending sorting (default)

        self.lock.acquire()

        if self.category == 'track':
            df = pd.read_sql('SELECT * FROM view_temp_track', con)

        if self.category == 'album':
            df = pd.read_sql('SELECT * FROM view_temp_album', con)

        self.lock.release()

        df_sorted = df.sort_values(by=sort_by, ascending=asc)

        self.lock.acquire()

        if self.category == 'track':
            # sqlite does not have a drop table command, writing tables with pandas leaves a weird level_o column in
            # the table when kwarg if_exists='replace (apparently this stems from trying to write the index, works if index set to false)'

            df_sorted.to_sql('view_temp_track',con, if_exists='replace', index=False)

        if self.category == 'album':
            df_sorted.to_sql('view_temp_album',con, if_exists='replace', index=False)

        con.commit()
        self.lock.release()


    def write_playlist (self, name, path, con):

        f_names=open('%s\%s_name.txt' % (path, name), 'w')
        f_spotify=open('%s\%s_spotify.txt' % (path, name), 'w')

        self.lock.acquire()

        df=pd.read_sql('SELECT trackName,artistName, spotifyID_track.spotifyID FROM view_temp_track \
                        LEFT JOIN spotifyID_track ON spotifyID_track.trackID = view_temp_track.trackID', con)
        self.lock.release()

        for row in df.itertuples(index=False):
            trackName, artistName, spotifyID = row

            if spotifyID :
                f_names.write('%s - %s\n' % ( trackName.encode('ascii', 'replace'), artistName.encode('ascii', 'replace') ))

                f_spotify.write('spotify:%s:%s\n' % (self.category,spotifyID) )

        f_names.close()
        f_spotify.close()


