# -*- coding: utf-8 -*-
"""
Created on Sun Sep 20 23:00:24 2015

@author: erblast
"""

def createDB(dbpath):
    
    import sqlite3
    from os import path as pth
    
    if pth.isfile(dbpath):
        return

    con=sqlite3.connect(dbpath)
    c=con.cursor()

    c.execute('CREATE TABLE "ID_artist" (\
            	`artistID`	INTEGER,\
            	`artistName`	TEXT UNIQUE,\
            	`mbID`	TEXT,\
            	`url`	TEXT,\
            	PRIMARY KEY(artistID)\
                )')
                
    c.execute('CREATE TABLE "ID_album" (\
            	`albumID`	INTEGER,\
            	`albumName`	TEXT,\
            	`artistID`	INTEGER,\
            	`mbID`	TEXT,\
            	`url`	TEXT,\
            	PRIMARY KEY (albumID)\
                 FOREIGN KEY (artistID) REFERENCES ID_artist (artistID)\
                 UNIQUE (artistID,albumName)\
                )')
                
    c.execute('CREATE INDEX mbid ON ID_album (mbID)')
                
    c.execute('CREATE TABLE "ID_track" (\
            	`trackID`	INTEGER,\
            	`trackName`	TEXT,\
            	`artistID`	INTEGER,\
            	`mbID`	TEXT,\
            	`url`	TEXT,\
            	PRIMARY KEY (trackID)\
                 FOREIGN KEY (artistID) REFERENCES ID_artist (artistID)\
                 UNIQUE (artistID,trackName)\
                )')
                
    c.execute('CREATE TABLE "ID_tag" (\
            	`tagID`	INTEGER,\
            	`tagName`	TEXT UNIQUE,\
            	`mbID`	TEXT,\
            	`url`	TEXT,\
            	PRIMARY KEY (tagID)\
                )')
                
    c.execute('CREATE TABLE "ID_user" (\
            	`userID`	INTEGER,\
            	`userName`	TEXT UNIQUE,\
            	`usersk`	TEXT UNIQUE,\
            	PRIMARY KEY (userID)\
                )')
                
    c.execute("INSERT INTO ID_user (userName) VALUES('total')")
    
    c.execute('CREATE TABLE "date_album" (\
            	`albumID`	INTEGER UNIQUE,\
            	`date`	TEXT,\
                 FOREIGN KEY (albumID) REFERENCES ID_album (albumID)\
                )')

    c.execute('CREATE TABLE "image_album" (\
            	`albumID`	INTEGER,\
            	`image`	TEXT UNIQUE, \
            	`size`	TEXT,\
                 FOREIGN KEY (albumID) REFERENCES ID_album (albumID)\
                )')

    c.execute('CREATE TABLE "image_artist" (\
            	`artistID`	INTEGER,\
            	`image`	TEXT UNIQUE, \
            	`size`	TEXT,\
                 FOREIGN KEY (artistID) REFERENCES ID_artist (artistID)\
                )')
    
    c.execute('CREATE TABLE "loaded_similar_artist" (\
            	`artistID`	INTEGER UNIQUE,\
            	`lim`	INTEGER,\
                 FOREIGN KEY (artistID) REFERENCES ID_artist (artistID)\
                )')

    c.execute('CREATE TABLE "loaded_similar_track" (\
            	`trackID`	INTEGER UNIQUE,\
            	`lim`	INTEGER,\
                 FOREIGN KEY (trackID) REFERENCES ID_track (trackID)\
                )')

    c.execute('CREATE TABLE "loaded_toptracks" (\
            	`artistID`	INTEGER UNIQUE,\
            	`lim`	INTEGER,\
                 FOREIGN KEY (artistID) REFERENCES ID_artist (artistID)\
                )')

    c.execute('CREATE TABLE "loaded_topalbums" (\
            	`artistID`	INTEGER UNIQUE,\
            	`lim`	INTEGER,\
                 FOREIGN KEY (artistID) REFERENCES ID_artist (artistID)\
                )')
                
    c.execute('CREATE TABLE "loaded_toptags_artist" (\
            	`artistID`	INTEGER UNIQUE,\
                 FOREIGN KEY (artistID) REFERENCES ID_artist (artistID)\
                )')
                
    c.execute('CREATE TABLE "loaded_toptags_track" (\
            	`trackID`	INTEGER UNIQUE,\
                 FOREIGN KEY (trackID) REFERENCES ID_track (trackID)\
                )')

    c.execute('CREATE TABLE "loaded_toptags_album" (\
            	`albumID`	INTEGER UNIQUE,\
                 FOREIGN KEY (albumID) REFERENCES ID_album (albumID)\
                )')

    c.execute('CREATE TABLE "loaded_SpotifyID_track" (\
            	`trackID`	INTEGER UNIQUE,\
                 FOREIGN KEY (trackID) REFERENCES ID_track (trackID)\
                )')

    c.execute('CREATE TABLE "loaded_SpotifyID_album" (\
            	`albumID`	INTEGER UNIQUE,\
                 FOREIGN KEY (albumID) REFERENCES ID_album (albumID)\
                )')

    c.execute('CREATE TABLE "loaded_mbinfo_album" (\
            	`albumID`	INTEGER UNIQUE,\
                 FOREIGN KEY (albumID) REFERENCES ID_album (albumID)\
                )')

    c.execute('CREATE TABLE "plays_track" (\
            	`trackID`	INTEGER,\
            	`userID`	INTEGER,\
            	`plays`	INTEGER,\
                 FOREIGN KEY (trackID) REFERENCES ID_track (trackID)\
                 FOREIGN KEY (userID) REFERENCES ID_user (userID)\
                 UNIQUE (trackID,userID)\
                )')

    c.execute('CREATE TABLE "plays_artist" (\
            	`artistID`	INTEGER,\
            	`userID`	INTEGER,\
            	`plays`	INTEGER,\
                 FOREIGN KEY (artistID) REFERENCES ID_artist (artistID)\
                 FOREIGN KEY (userID) REFERENCES ID_user (userID)\
                 UNIQUE (artistID,userID)\
                )')

    c.execute('CREATE TABLE "plays_album" (\
            	`albumID`	INTEGER,\
            	`userID`	INTEGER,\
            	`plays`	INTEGER,\
                 FOREIGN KEY (albumID) REFERENCES ID_album (albumID)\
                 FOREIGN KEY (userID) REFERENCES ID_user (userID)\
                 UNIQUE (albumID,userID)\
                )')

    c.execute('CREATE TABLE "similar_track" (\
            	`trackID_1`	INTEGER,\
            	`trackID_2`	INTEGER,\
            	`score`	NUMERIC,\
                 FOREIGN KEY (trackID_1) REFERENCES ID_track (trackID)\
                 FOREIGN KEY (trackID_2) REFERENCES ID_track (trackID)\
                 UNIQUE (trackID_1,trackID_2)\
                )')

    c.execute('CREATE TABLE "similar_artist" (\
            	`artistID_1`	INTEGER,\
            	`artistID_2`	INTEGER,\
            	`score`	NUMERIC,\
                 FOREIGN KEY (artistID_1) REFERENCES ID_artist (artistID)\
                 FOREIGN KEY (artistID_2) REFERENCES ID_artist (artistID)\
                 UNIQUE (artistID_1,artistID_2)\
                )')

    c.execute('CREATE TABLE "spotifyID_track" (\
            	`trackID`	INTEGER UNIQUE,\
            	`spotifyID`	TEXT UNIQUE,\
                 FOREIGN KEY (trackID) REFERENCES ID_track (trackID)\
                )')

    c.execute('CREATE TABLE "spotifyID_album" (\
            	`albumID`	INTEGER UNIQUE,\
            	`spotifyID`	TEXT UNIQUE,\
                 FOREIGN KEY (albumID) REFERENCES ID_album (albumID)\
                )')

    c.execute('CREATE TABLE "tag_artist" (\
            	`tagID`	INTEGER,\
            	`artistID`	INTEGER,\
            	`count`	INTEGER,\
                 FOREIGN KEY (tagID) REFERENCES ID_tag (tagID)\
                 FOREIGN KEY (artistID) REFERENCES ID_artist (artistID)\
                 UNIQUE (tagID,artistID)\
                )')

    c.execute('CREATE TABLE "tag_track" (\
            	`tagID`	INTEGER,\
            	`trackID`	INTEGER,\
            	`count`	INTEGER,\
                 FOREIGN KEY (tagID) REFERENCES ID_tag (tagID)\
                 FOREIGN KEY (trackID) REFERENCES ID_track (trackID)\
                 UNIQUE (tagID,trackID)\
                )')

    c.execute('CREATE TABLE "tag_album" (\
            	`tagID`	INTEGER,\
            	`albumID`	INTEGER,\
            	`count`	INTEGER,\
                 FOREIGN KEY (tagID) REFERENCES ID_tag (tagID)\
                 FOREIGN KEY (albumID) REFERENCES ID_album (albumID)\
                 UNIQUE (tagID,albumID)\
                )')

    c.execute('CREATE TABLE "track_rel_album" (\
            	`trackID`	INTEGER,\
            	`albumID`	INTEGER,\
                 FOREIGN KEY (trackID) REFERENCES ID_track (trackID)\
                 FOREIGN KEY (albumID) REFERENCES ID_album (albumID)\
                 UNIQUE (trackID,albumID)\
                )')
                
    c.execute('CREATE TABLE "i_score_artist" (\
                `userID`        INTEGER,\
                `artistID`	INTEGER,\
                `i_score`	REAL,\
                PRIMARY KEY(artistID)\
                )')
                
    c.execute('CREATE TABLE "i_score_track" (\
                `userID`        INTEGER,\
                `trackID`	INTEGER,\
                `i_score`	REAL,\
                PRIMARY KEY(trackID)\
                )')
            
    con.commit()