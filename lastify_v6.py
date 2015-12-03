# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 22:51:18 2015
This program
@author: erblast

To do:

Authorisated Requests
User interface


"""

#update version 4, this will be a complete overhaul using sqlite3

#for each API REST get method there will be one function which can handle:
#artist, track and album variants
#
#There will be one SQL function that handles data entry for one group of tables:
#all ID tables, all tag tables, all similar tables etc
#
#Dataframe functions should be as general as possible

#Import

import time
import threading
import pandas as pd
import sqlite3
import requests
from copy import deepcopy
from createDB import *
from call_controler_v2 import *
from output_controler import *


# Errors

class timeout(Exception):
    pass

class json(Exception):
    pass

#Functions Basic

def save(variable,filename):
    
    import pickle
    pickle.dump(variable, open(path +'\\' + filename + '.p','wb'))
    print filename + ' pickled'
    
def load(filename):

    import pickle
    from os import path as pth
    
    if pth.isfile(path +'\\' + filename + '.p'):
        print filename + ' depickled'
        return pickle.load(open(path +'\\' + filename + '.p','rb'))
        
def lookupkey(dictionary, key):
    """this recursive function finds a key in a nested dictionary and returns its
    value"""
    
    def function(dictionary, key):

        if key in dictionary:
            
            return dictionary[key]
        
        else:
            
            dictsindict=[item for item in dictionary.values() if type(item)==dict ]

            for item in dictsindict:
                
                return function(item, key)                         
    
    try: 
        
        return function(dictionary, key)
        
    except:
        
        print key + ' not found in dictionary'         

def find_dict(ls_item,IDname,ID,dict_items=[]):
    '''
    recursive function, will search nested list for dictionaries and ad an ID to
    each dictionary. Format: {IDname:ID}
    
    returns list of dicts or a single dict if original ls_item input is dict
    dict_items will not change if function is called mutliple times and function
    is not called as dict_items=[]
    '''
    
    # recursion stops if dict is found
    if type(ls_item)==dict:
        
        # ID is added here
        if IDname in ls_item:
            ls_item['%s 2' %IDname]=ID
        else:
            ls_item[IDname]=ID
        
        return ls_item
        
    # cycles through all items in iterable and applys recursion, passing IDname,ID, und
    # dict_items collecting variable
    
    if type(ls_item)==list or type(ls_item)==tuple or type(ls_item)==set:
    
        for i in range(len(ls_item)+1):
            
            if i<len(ls_item):            
                
                dict_items.append(find_dict(ls_item[i],IDname,ID,dict_items))
            
            if i==len(ls_item):
                
                return dict_items
#___________________________________________________________________________________                
#Dataframe Functions
                
def json2df(json):
    """takes json dict and converts it to dataframes"""
    attr=lookupkey(json, '@attr')
    chart=()
    if lookupkey(json,'topartists'):
        chart=pd.DataFrame(lookupkey(json,'artist'))
        attr['category']='artist'
        attr['subcategory']='topartists'
        
    if lookupkey(json,'toptracks'):
        chart=pd.DataFrame(lookupkey(json,'track'))
        attr['category']='track'
        attr['subcategory']='toptracks'
        
    if lookupkey(json,'topalbums'):
        chart=pd.DataFrame(lookupkey(json,'album'))
        attr['category']='album'
        attr['subcategory']='topalbums'
        
    if lookupkey(json,'toptags'):
        chart=pd.DataFrame(lookupkey(json,'tag'))
        attr['category']='tag'
        
    if 'similar' in str(json.keys()):  
        attr['subcategory']='similar'

        if lookupkey(json,'track'):
            chart=pd.DataFrame(lookupkey(json,'track'))
            attr['category']='track'
                        
        if lookupkey(json,'artist') and not lookupkey(json,'track') :
                
            chart=pd.DataFrame(lookupkey(json,'artist'))
            attr['category']='artist'
    else:
        attr['subcategory']=str()
    
    return chart, attr       

def unpackdf(df,ID_col, nested_col):
    '''this function unpacks fields in a dataframe in which dictionaries are stored
        in nested list structures. It searches all list nodes and returns all key value
        pairs found in dicts as DataFrame
    '''
    #necessary because otherwise data in original dataframe will change
    from copy import deepcopy
    df=deepcopy(df)    
    
    import pandas as pd    
    
    dct=[]
    
    for i,ID,item in df[[ID_col,nested_col]].itertuples():
        
        # find_dict can return a single dict or a list of dicts, append needs to 
        # be used for single dicts and append for list of dicts
        
        temp=find_dict(item,ID_col,ID,[])
        
        if type(temp)==dict:
            dct.append(temp)
        
        else:
            dct.extend(temp) 
        
    return pd.DataFrame(dct)
        

def returnID(con,DF,attr={'category':'artist'}):
    '''returns a List with IDs, category artist DF= name (can be Series), category track/album 
    DF=artistID, name'''
    
    
    try:
        DF=DF.to_frame()
    except:
        pass

    if attr['category']=='artist':
        
        return [lookup_artistID(con,name) for i,name in DF.itertuples()]
        
    if attr['category']=='tag':
        
        return [lookup_tagID(con,name) for i,name in DF.itertuples()]
        
    if attr['category']=='track':
        return [lookup_trackID(con,name,artistID) for i,artistID,name \
                in DF.itertuples()]
                
    if attr['category']=='album':
        return [lookup_albumID(con,name,artistID) for i,artistID,name \
                in DF.itertuples()]

def addIDcol(con,DF,attr):
    '''adds ID of item described in attr to DF as columns
    used if DF contains data related to single item described in attr'''

    
    artist=attr['artist']
    artistID=lookup_artistID(con,artist)
    
    if attr['category']=='artist' or attr['subcategory']=='artist':
        ID=artistID
    
    if attr['category']=='track' or attr['subcategory']=='track':
        track=attr['track']
        ID=lookup_trackID(con,track,artistID)        
    if attr['category']=='album' or attr['subcategory']=='album':
        album=attr['album']
        ID=lookup_albumID(con,album,artistID)        
        
    DF['ID']=[ID for i in range(len(DF))]
    
    return DF
            
#def images2df(nestedImDF):
#    '''takes charts ['artistID','image'] converts it to DF'''
#    
#    newList=[]
#    
#    for i,ID,images in nestedImDF.itertuples():
#        for item in images:
#            newList.append((ID,item['#text'],item['size']))
#            
#    return pd.DataFrame(newList, columns=['artistID','image','size'])       

#def artist2df(nestedartist):
#    '''takes charts['artist'] converts it to DF '''
#    
#    newList=[]
#    
#    for i,artist in nestedartist.iteritems():
#        newList.append((artist['name'],artist['mbid'],artist['url']))
#            
#    return pd.DataFrame(newList, columns=['name','mbid','url'])       

#____________________________________________________________________________________
#Request Functions

def get_userinfo(user='erblast'):
    
    request=deepcopy(baserequest)
    request['method']='user.gettoptracks'
    request['user']=user
    
    answer=requests.get(url, timeout=(50,10000),params=request)
    return answer.json()

def get_chart(attr={'category':'artist'},period='overall', limit=1000, user='erblast'):
    """makes a REST API request requesting user charts
    limit for track and album should not be higher than 3000
    for artist the limit can be set to 4000
    Returns JsonDict"""
    
    request=deepcopy(baserequest)
    
    if attr['category']=='artist':
        request['method']='user.gettopartists'
        
    if attr['category']=='track':
        if limit==1000:
            limit=5000
            
        request['method']='user.gettoptracks'
        
    if attr['category']=='album':
        request['method']='user.gettopalbums'            

    request['period']=period
    request['limit']=str(limit)
    request['user']=user

                        
    
    import requests
    
    try:
        answer=requests.get(url, timeout=(50,10000),params=request)
        chart,New_attr=json2df(answer.json())
        return chart,New_attr
        
        
    except: 
        raise timeout()
        
def get_token():
    '''gets a token from the lastfm API'''  
    
    request=deepcopy(baserequest)
    
    request['method']='auth.getToken'
    answer=requests.get(url, timeout=(50,10000),params=request)
    return answer.json()['token']


def authentication(token):
    '''opens the lastfm webpage which requests the user to authenticate lastify'''
    
    import webbrowser
    webbrowser.open('http://www.last.fm/api/auth/?api_key=%s&token=%s'%(baserequest['api_key'],token))

def get_sk(token):
    
    request=deepcopy(baserequest)
    del request['format']
    request['method']='auth.getSession'
    request['token']=token
    request['api_sig']=md5(request)
    answer=requests.get(url, timeout=(50,10000),params=request)
    return answer.text.split(r'<key>')[1].split(r'</key>')[0]
    
    
def md5(req):
    
    req=pd.Series(req).sort_index()
    
    import hashlib
    m=hashlib.md5()
    [m.update(('%s%s' %(i[0],i[1])).encode('utf-8')) for i in req.iteritems()]
    m.update(secret.encode('utf-8'))
    md5=m.hexdigest()
    
    del m
    return md5
    
def get_rec(sk,limit=1000):

    request=deepcopy(baserequest)
    request['method']='user.getRecommendedArtists'
    request['limit']=str(limit)
    del request['format']

    request['sk']=sk
    request['api_sig']=md5(request)
    answer=requests.get(url, timeout=(50,10000),params=request)

    return answer
    
def get_toptracks(artist,limit=100):

    request=deepcopy(baserequest)
    
    request['method']='artist.getTopTracks'
    request['limit']=str(limit)
    request['artist']=artist
    
    try:
        answer=requests.get(url, timeout=(50,10000),params=request)
    
    except:
        raise timeout()
        
    return answer.json()
        
def get_topalbums(artist,limit=100):

    request=deepcopy(baserequest)
    
    request['method']='artist.getTopAlbums'
    request['limit']=str(limit)
    request['artist']=artist
    
    try:
        answer=requests.get(url, timeout=(50,10000),params=request)
    
    except:
        raise timeout()
        
    return answer.json()
    
def get_similar( attr={'category':'track','artist':'James Blake','track':'limit to your love'},
                 limit=100):

    request=deepcopy(baserequest)
    request['method']='%s.getSimilar' % attr['category']
    request['artist']=attr['artist']
    request['limit']=limit
    
    if attr['category']=='track':
        request['track']=attr['track']
        
    try:
        answer=requests.get(url, timeout=(50,10000),params=request)
    except:
        raise timeout
    
    chart,New_attr=json2df(answer.json())
    
    return chart,New_attr
    
def get_toptags(attr={'category':'track','artist':'James Blake','track':'limit to your love'}):
    
    request=deepcopy(baserequest)
    request['method']='%s.getTopTags' % attr['category']
    request['artist']=attr['artist']
    
    if attr['category']=='track':
        request['track']=attr['track']
        
    if attr['category']=='album':
        request['album']=attr['album']
        
    try:
        answer=requests.get(url, timeout=(50,10000),params=request)
    except:
        raise timeout
        
    chart,New_attr=json2df(answer.json())
    
    New_attr['subcategory']=attr['category']
    
    return chart,New_attr
    
def get_info(attr={'category':'track','artist':'James Blake','track':'limit to your love'}):
    
    request=deepcopy(baserequest)
    request['method']='%s.getInfo' % attr['category']
    request['artist']=attr['artist']
    
    if attr['category']=='track':
        request['track']=attr['track']
        
    if attr['category']=='album':
        request['album']=attr['album']
        
    try:
        answer=requests.get(url, timeout=(50,10000),params=request)
    except:
        raise timeout
        
    chart=answer.json()
        
    return chart,attr

def get_album_mbinfo(con,mbID,albumID,artistID):
    '''makes REST request at musicbrainz API to get tracks on album and release date'''

    url='http://musicbrainz.org/ws/2/release/%s?fmt=json&inc=recordings' % mbID
    
    try:
        answer=requests.get(url, timeout=(50,10000))

    except:
        raise timeout
    
    try:
        date=(str(lookupkey(answer.json(),'date')),int(albumID))
        
        if date[0].lower()=='none':
            
            print 'date not found'
            
    except:
        print 'date not found'
        date='none'
        
    try:    
        media=lookupkey(answer.json(),'media')
        
        media=[lookupkey(cd,'tracks') for cd in media]
    except:
        print 'tracks not found'
        print answer.text
        return date,pd.DataFrame()
    try:    
        tracks=[]
        
        for cd in media:
            tracks.extend(cd)
            
        tracks=pd.DataFrame([lookupkey(track,'recording') for track in tracks])
        
        tracks=tracks['title'].to_frame()
    except:
        print 'recording not found'
        return date,pd.DataFrame()
    
    
    tracks['artistID']=[artistID for i in range(len(tracks))]
    
    tracks['albumID']=[albumID for i in range(len(tracks))]
    
    tracks['trackID']=[lookup_trackID(con,title,int(ID)) for i,title,ID in
                        tracks.loc[:,['title','artistID']].itertuples()]
                            
    try:
#   lookup track ID returns nan if no tack ID could be found, this is returned
#   in the corresponding dataframe the field is a numpyfloat64 variable. Corresponding
#   lines will not be returned
        tracks=tracks[tracks['trackID']!='nan']
        
    except:
        pass
        
    return date,tracks

def get_spotifyID(ID,attr={'category':'track','artist':'James Blake','track':'limit to your love'}):
    
    url='https://api.spotify.com/v1/search'
    
    params={'type':attr['category'], 
            'limit':'1'}
    
    if attr['category']=='track':        
        params['q']='%s %s' % (attr['track'], attr['artist'])
    if attr['category']=='album':        
        params['q']='%s %s' % (attr['album'], attr['artist'])   
    
    try:
        answer=requests.get(url, timeout=(50,10000),params=params)
            
    except:
        print answer.text
        raise timeout
            
    try:
        spotifyID=lookupkey(answer.json(),'items')[0]['id']
        return (int(ID),spotifyID)
    except:
        return 'nan'
#_________________________________________________________________________________       
# SQLite functions
# some of the lookup functions will try to find string with a modification in letter case
# this should be obselete with new database, appopriate fields should be case insensitive
    
def lookup_userID(con,user):
    '''if user does not exist user will be assigned new ID'''
    c=con.cursor()
    c.execute('INSERT OR IGNORE INTO ID_user (userName) VALUES (?)',(user,))
    con.commit()
    c.execute('SELECT userID FROM ID_user WHERE userName=?',(user,))
    return c.fetchone()[0]

def lookup_artistID(con,artist):
    c=con.cursor()
    c.execute('SELECT artistID FROM ID_artist WHERE artistName=?',(artist,))
    return c.fetchone()[0]
    
def lookup_trackID(con,track,artistID):
    c=con.cursor()
    artistID=int(artistID)
    
    try:
        c.execute('SELECT trackID FROM ID_track WHERE trackName=? AND artistID=?',(track,artistID))
        return int(c.fetchone()[0])
    
    except TypeError:
        
        try:
            c.execute('SELECT trackID FROM ID_track WHERE trackName=? AND artistID=?',(track.lower(),artistID))
            return int(c.fetchone()[0])
        except TypeError:
            
            try:
                c.execute('SELECT trackID FROM ID_track WHERE trackName=? AND artistID=?',(track.title(),artistID))
                return int(c.fetchone()[0])
            except TypeError:
                
                try:
                    c.execute('SELECT trackID FROM ID_track WHERE trackName=? AND artistID=?',(track.capitalize(),artistID))
                    return int(c.fetchone()[0])
                except TypeError:
                    return 'nan'
                    
def lookup_albumID(con,album,artistID):
    c=con.cursor()
    artistID=int(artistID)
    
    try:
        c.execute('SELECT albumID FROM ID_album WHERE albumName=? AND artistID=? ',(album,artistID))
        return c.fetchone()[0]
    except TypeError:
        try:
            c.execute('SELECT albumID FROM ID_album WHERE albumName=? AND artistID=? ',(album.lower(),artistID))
            return c.fetchone()[0]
        except TypeError:
            return
            
def lookup_tagID(con,tag):
    c=con.cursor()
    c.execute('SELECT tagID FROM ID_tag WHERE tagName=?',(tag,))
    return c.fetchone()[0]
    
    
def enter_IDitem(con,DF,attr={'category':'artist'}):
    '''takes Dataframe containing 
    
    chart.loc[:,['name','mbid','url']] for category=artist
    chart.loc[:,[artistID,'name','mbid','url']] for category track or album 
    chart.loc[:,[name,url]] for tags
    
    '''
    
    c=con.cursor()

    if attr['category']=='artist':
        newTuple=((name,mbID,url) for i,name,mbID,url in 
                  DF.itertuples() 
                  )
        
        c.executemany('INSERT OR IGNORE INTO ID_artist (artistName,mbID,url) \
                        VALUES (?,?,?)',newTuple)
    
    if attr['category']=='track':
        newTuple=((int(artistID),name.lower(),mbID,url) for i,artistID,name,mbID,url in 
                  DF.itertuples() 
                  )
                      
        c.executemany('INSERT OR IGNORE INTO ID_track (artistID,trackName,mbID,url) \
                        VALUES (?,?,?,?)',newTuple)

    if attr['category']=='album':
        newTuple=((int(artistID),name.lower(),mbID,url) for i,artistID,name,mbID,url in 
                  DF.itertuples() 
                  )
                      
        c.executemany('INSERT OR IGNORE INTO ID_album (artistID,albumName,mbID,url) \
                        VALUES (?,?,?,?)',newTuple)
            
    if attr['category']=='tag':
        newTuple=((name,url) for i,name,url in 
                  DF.itertuples() 
                  )
                      
        c.executemany('INSERT OR IGNORE INTO ID_tag (tagName,url) \
                        VALUES (?,?)',newTuple)
        
    con.commit()


def enter_Images(con,imageDF,attr={'category':'artist'}):
    '''takes image Dataframe [:,['ID','image','size']]
    should be called by enter_chart'''
    
    c=con.cursor()
    
    newTuple=((int(ID),image,size) for i,image,ID,size in imageDF.itertuples())
        
    if attr['category']=='artist' or attr['category']=='track':
        
        c.executemany('INSERT OR IGNORE INTO image_artist (artistID, image, size) \
                       VALUES (?,?,?)',newTuple)
                       
    if attr['category']=='album' :
        
        c.executemany('INSERT OR IGNORE INTO image_album (albumID, image, size) \
                       VALUES (?,?,?)',newTuple)
    con.commit()

def enter_tags(con,tagDF,attr={'category':'tag','subcategory':'artist','artist':'James Blake'}):
    '''tagDF.loc[:,['ID','tag','count']]'''
    
    c=con.cursor()
    newTuple=( ( int(ID),int(lookup_tagID(con,tag)),int(count) ) for 
                i,ID,tag,count in
                tagDF.loc[:,['ID','name','count']].itertuples()
                )
                
    if attr['subcategory']=='artist':
        c.executemany('INSERT OR IGNORE INTO tag_artist (artistID, tagID, count) \
                       VALUES (?,?,?)',newTuple)
                       
    if attr['subcategory']=='track':
        c.executemany('INSERT OR IGNORE INTO tag_track (trackID, tagID, count) \
                       VALUES (?,?,?)',newTuple)
                       
    if attr['subcategory']=='album':
        c.executemany('INSERT OR IGNORE INTO tag_album (albumID, tagID, count) \
                       VALUES (?,?,?)',newTuple)
                       
    con.commit()
    
def enter_similar(con,simDF,attr={'category':'track','subcategory':'similar','artist':'James Blake','track':'limit to your love'}):
    '''artistID or trackID must be part of input DF '''
    
    c=con.cursor()
    
    if attr['category']=='artist':
                
        newTuple=( (int(ID),int(artistID),float(match)) \
                    for i,artistID,ID,match in \
                    simDF.loc[:,['artistID','ID','match']].itertuples() \
                    )
                    
        c.executemany('INSERT OR IGNORE INTO similar_artist (artistID_1, artistID_2, score) \
                       VALUES (?,?,?)',newTuple)
                    
    if attr['category']=='track':

        newTuple=( (int(trackID),int(ID),float(match)) \
                    for i,trackID,ID,match in \
                    simDF.loc[:,['trackID','ID','match']].itertuples() \
                    )
                    
        c.executemany('INSERT OR IGNORE INTO similar_track (trackID_1, trackID_2, score) \
                       VALUES (?,?,?)',newTuple)
    
    con.commit()
    
def enter_userplays(con,playDF,user='erblast',attr={'category':'artist'}):
    '''enters user plays into appropriate database takes DF['xID','playcount']
    should be called by enter_chart'''
    
    
    c=con.cursor()
    
    userID=lookup_userID(con,user)

    newTuple=((int(userID),int(ID),int(plays)) for i,ID,plays 
                in playDF.itertuples()
                )
                        
    if attr['category']=='artist':
        c.executemany('INSERT OR REPLACE INTO plays_artist (userID, artistID , plays) \
                       VALUES (?,?,?)',newTuple)
                       
    if attr['category']=='track':
        c.executemany('INSERT OR REPLACE INTO plays_track (userID, trackID , plays) \
                       VALUES (?,?,?)',newTuple)
                       
    if attr['category']=='album':
        c.executemany('INSERT OR REPLACE INTO plays_album (userID, albumID , plays) \
                       VALUES (?,?,?)',newTuple)
                       
    con.commit()
                   
def enter_chart(con,chart,attr={'category':'artist'}):
    """this function takes a json derived chartfile and artist toptrack and topalbum files
        and enters all the information into the appropriate db charts"""
    
    
    if 'user' in attr:
        user=attr['user']
    else:
        user='total'
    
    if attr['category']=='artist':
        
        
        enter_IDitem(con,chart.loc[:,['name','mbid','url']],attr)
        
        chart['artistID']=returnID(con,chart['name'])
                       
        imagedf=unpackdf(chart,'artistID','image')
        
        enter_Images(con,imagedf)


        if attr['subcategory']!='similar':       
            enter_userplays(con,chart.loc[:,['artistID','playcount']],user,attr)
        
    if attr['category']=='track' or attr['category']=='album':
        
        artistdf=unpackdf(chart,'name','artist')
        
        if attr['subcategory']!='toptracks' and attr['subcategory']!='topalbums':
            enter_IDitem(con,artistdf.loc[:,['name','mbid','url']],{'category':'artist'})
        
        if len(artistdf.columns)==4:
            artistdf.columns=['mbid artist','name artist','name track','url artist']
            
        if len(artistdf.columns)==3:
            artistdf.columns=['name artist','name track','url artist']

        chart['artistID']=returnID(con,artistdf['name artist'])
        
        enter_IDitem(con,chart.loc[:,['artistID','name','mbid','url']], attr)
        
        chart['ID']=returnID(con,chart.loc[:,['artistID','name']],attr)
        
        if attr['category']=='album':
            imagedf=unpackdf(chart,'ID','image')
        if attr['category']=='track':
            imagedf=unpackdf(chart,'artistID','image')
        
        if attr['subcategory']!='toptracks':
            enter_Images(con,imagedf,attr)
                
        enter_userplays(con,chart.loc[:,['ID','playcount']],user,attr)

def enter_mbinfo_album(con,date,tracks):
    '''saves mbinfo_album to database'''
    
    c=con.cursor()
    
    if len(date)>1:
        c.executemany('INSERT OR IGNORE INTO date_album (date,albumID) VALUES (?,?)',date)
        
    if len(date)==1:
        c.execute('INSERT OR IGNORE INTO date_album (date,albumID) VALUES (?,?)',date[0])
        
    try:
        newTuple=((int(trackID),int(albumID)) for i,trackID,albumID in tracks.loc[:,['trackID','albumID']].itertuples() )
        c.executemany('INSERT OR IGNORE INTO track_rel_album (trackID,albumID) VALUES (?,?)',newTuple)
        
    except:
        
        try:
            newTuple=((int(trackID),int(albumID)) for i,trackID,albumID in tracks.loc[:,['trackID','albumID']].itertuples() )
            c.execute('INSERT OR IGNORE INTO track_rel_album (trackID,albumID) VALUES (?,?)',newTuple)
            
        except:
            pass
            
    con.commit()
    
def enter_spotifyID(con,tup,attr):
    '''saves spotifyID to database'''
    
    c=con.cursor()

    if attr['category']=='track' and len(tup)>1:
        c.executemany('INSERT OR IGNORE INTO spotifyID_track (trackID,spotifyID) VALUES (?,?)',tup)
        
    if attr['category']=='track' and len(tup)==1:
        c.execute('INSERT OR IGNORE INTO spotifyID_track (trackID,spotifyID) VALUES (?,?)',tup)

    if attr['category']=='album' and len(tup)>1:
        c.executemany('INSERT OR IGNORE INTO spotifyID_album (albumID,spotifyID) VALUES (?,?)',tup)
        
    if attr['category']=='album' and len(tup)==1:
        c.execute('INSERT OR IGNORE INTO spotifyID_album (albumID,spotifyID) VALUES (?,?)',tup)
        
#Load functions

def load_toptracks(db_path, lock, call_controler, out, limit=100, n_entries=20000):
    """
    goes through artists and loads toptracks loading progress is displayed and
    saved in loaded_toptracks. 
    SQL is faster when many inserts are made at once, enter_chart will be used
    to enter data as soon as at least n_entries are collected.     
    The enterchart function is used to save data. 
    Timeout errors will result in artist not being entered in loaded_toptracks, while 
    key errors which spring from insufficient data in the json response will enter
    artist in loaded thus permanently skipping it.
    Toptracks will be reloaded if the function is being called with a higher limit 
    than in previous runs
    """
    con=sqlite3.connect(db_path)    
    c=con.cursor()
    log=call_controler.create_log()
    
    lock.acquire()
    loaded=pd.read_sql('SELECT artistID FROM loaded_toptracks WHERE lim>=?', con, params=(limit,) )
    loaded=set(loaded['artistID'].tolist())
    insuflim=pd.read_sql('SELECT artistID FROM loaded_toptracks WHERE lim<?', con, params=(limit,) )
    insuflim=set(insuflim['artistID'].tolist())
    artists=pd.read_sql('SELECT artistID,artistName FROM ID_artist',con)
    lock.release()
    
    for i,ID,name in artists.itertuples():
        
        if i==0:
            save_loaded_update=list()
            save_loaded_insert=list()
            reset_chart_save=True
            chart_save=pd.DataFrame()
            
        
        if ID not in loaded:

            call_controler.timer_event.wait()
            
            try:
                toptracks,attr=json2df(get_toptracks(name,limit=limit))
                log.log_event()
                
                if toptracks.shape[0]>1: #accounts for empty table
                
                    if reset_chart_save==True:
                        chart_save=toptracks
                        reset_chart_save=False
                        
                    else:
                        chart_save=chart_save.append(toptracks)
                    
            except timeout:
                continue
            except:
                pass
            
            if ID in insuflim:
                save_loaded_update.append((limit,int(ID)))
            else:
                save_loaded_insert.append((int(ID),limit))
        
        if chart_save.shape[0]>=n_entries or (i==len(artists)-1 and chart_save.shape[0]>0):
            lock.acquire()
            try:
                out.save( 'TOPTRACKS is saving to database')
                enter_chart(con,chart_save,attr)
                reset_chart_save=True
                
                if save_loaded_update:
                    c.executemany('UPDATE loaded_toptracks SET lim=? WHERE artistID=?',save_loaded_update)
                    save_loaded_update=list()
                    
                if save_loaded_insert:
                    c.executemany('INSERT OR IGNORE INTO loaded_toptracks (artistID,lim) VALUES(?,?)',save_loaded_insert)
                    save_loaded_insert=list()
                    
                con.commit()
                
            except Exception as e:
                save(chart_save,'toptrack_error_chart')
                save(attr,'toptrack_error_attr')
                out.save(e)
                lock.release()
                raise json
                
            lock.release()

        out.save( 'TOPTRACKS \t\t%.3f\t percent loaded' % (float(i+1)/len(artists)*100))
            
def load_topalbums(db_path, lock, call_controler, out,limit=100,n_entries=20000):
    """see load_toptracks for description"""
    
    con=sqlite3.connect(db_path)    
    c=con.cursor()
    log=call_controler.create_log()

    lock.acquire()
    loaded=pd.read_sql('SELECT artistID FROM loaded_topalbums WHERE lim>=?', con, params=(limit,) )
    loaded=set(loaded['artistID'].tolist())
    insuflim=pd.read_sql('SELECT artistID FROM loaded_topalbums WHERE lim<?', con, params=(limit,) )
    insuflim=set(insuflim['artistID'].tolist())
    artists=pd.read_sql('SELECT artistID,artistName FROM ID_artist',con)
    lock.release()
    
    for i,ID,name in artists.itertuples():
        
        if i==0:
            save_loaded_update=list()
            save_loaded_insert=list()
            reset_chart_save=True
            chart_save=pd.DataFrame()
            
        if ID not in loaded:
            
            call_controler.timer_event.wait()
            
            try:
                topalbums,attr=json2df(get_topalbums(name,limit=limit))
                log.log_event()
                
                if topalbums.shape[0]>1: #accounts for empty table
                
                    if reset_chart_save==True:
                        chart_save=topalbums
                        reset_chart_save=False
                        
                    else:
                        chart_save=chart_save.append(topalbums)
                    
            except timeout:
                continue
            except:
                pass
            
            if ID in insuflim:
                save_loaded_update.append((limit,int(ID)))
            else:
                save_loaded_insert.append((int(ID),limit))
        
        if chart_save.shape[0]>=n_entries or (i==len(artists)-1 and chart_save.shape[0]>0):
            lock.acquire()
            try:
                out.save( 'TOPALBUMS is saving to database')
                enter_chart(con,chart_save,attr)
                reset_chart_save=True
                
                if save_loaded_update:
                    c.executemany('UPDATE loaded_topalbums SET lim=? WHERE artistID=?',save_loaded_update)
                    save_loaded_update=list()
                    
                if save_loaded_insert:
                    c.executemany('INSERT OR IGNORE INTO loaded_topalbums (artistID,lim) VALUES(?,?)',save_loaded_insert)
                    save_loaded_insert=list()
                    
                con.commit()
            except Exception as e:
                save(chart_save,'topalbum_error_chart')
                save(attr,'topalbum_error_attr')
                out.save(e)
                lock.release()
                raise json
                
            lock.release()
            
        out.save( 'TOPALBUMS \t\t%.3f\t percent loaded' % (float(i+1)/len(artists)*100))
            
    
def load_similar_artist(db_path, lock, call_controler, out, user='erblast',limit=100, n_entries=20000):
    """see load_toptracks for description
        only loads similar artist information if user plays are greater then 0"""

    con=sqlite3.connect(db_path)    
    c=con.cursor()
    attr={'category':'artist'}
    log=call_controler.create_log()
    
    lock.acquire()
    loaded=pd.read_sql('SELECT artistID FROM loaded_similar_artist WHERE lim>=?', con, params=(limit,) )
    loaded=set(loaded['artistID'].tolist())
    insuflim=pd.read_sql('SELECT artistID FROM loaded_similar_artist WHERE lim<?', con, params=(limit,) )
    insuflim=set(insuflim['artistID'].tolist())
    
    artists=pd.read_sql('SELECT plays_artist.artistID,artistName \
                         FROM plays_artist\
                         LEFT JOIN ID_artist\
                         ON plays_artist.artistID=ID_artist.artistID\
                         WHERE plays_artist.userID=? and plays_artist.plays>0'
                         ,con, params=(lookup_userID(con,user),)
                         )
    lock.release()
    
    for i,ID,artist in artists.itertuples():

        if i==0:
            save_loaded_update=list()
            save_loaded_insert=list()
            reset_chart_save=True
            chart_save=pd.DataFrame()
        
        if ID not in loaded:
            
            call_controler.timer_event.wait()
            
            try:
                attr['artist']=artist
                similar,attrNew=get_similar(attr,limit)
                log.log_event()
                
                if similar.shape[0]>1: #accounts for empty table
                    lock.acquire()
                    similar=addIDcol(con,similar,attrNew)
                    lock.release()
                
                    if reset_chart_save==True:
                        chart_save=similar
                        reset_chart_save=False
                        
                    else:
                        chart_save=chart_save.append(similar)
                        
            except timeout:
                continue
            
            except:
                try:
                    lock.release()
                except:
                    pass
            
            if ID in insuflim:
                save_loaded_update.append((limit,int(ID)))
            else:
                save_loaded_insert.append((int(ID),limit))
                
        if chart_save.shape[0]>=n_entries or (i==len(artists)-1 and chart_save.shape[0]>0):
            # in very rare cases addIDcol cannot find the correct ID
            boolean=[False if ID=='nan' else True for i,ID in chart_save['ID'].iteritems()]
            chart_save=chart_save[boolean]

            lock.acquire()
            try:
                out.save('SIMILAR ARTIST is saving to database')
                enter_chart(con,chart_save,attrNew)
                enter_similar(con,chart_save,attrNew)
                reset_chart_save=True
                
                if save_loaded_update:
                    c.executemany('UPDATE loaded_similar_artist SET lim=? WHERE artistID=?',save_loaded_update)
                    save_loaded_update=list()
                    
                if save_loaded_insert:
                    c.executemany('INSERT OR IGNORE INTO loaded_similar_artist (artistID,lim) VALUES(?,?)',save_loaded_insert)
                    save_loaded_insert=list()
                    
                con.commit()
            except Exception as e:
                save(chart_save,'similar_artist_error_chart')
                save(attrNew,'similar_artist_error_attr')
                out.save(e)
                lock.release()
                raise json
                
            lock.release()
            
        out.save( 'SIMILAR_ARTIST \t\t%.3f\t percent loaded' % (float(i+1)/len(artists)*100))

def load_similar_track(db_path, lock, call_controler, out, user='erblast',limit=100, n_entries=20000):
    """see load_toptracks for description"""
    
    con=sqlite3.connect(db_path)    
    c=con.cursor()
    attr={'category':'track'}
    log=call_controler.create_log()
    
    lock.acquire()
    loaded=pd.read_sql('SELECT trackID FROM loaded_similar_track WHERE lim>=?', con, params=(limit,) )
    loaded=set(loaded['trackID'].tolist())
    
    insuflim=pd.read_sql('SELECT trackID FROM loaded_similar_track WHERE lim<?', con, params=(limit,) )
    insuflim=set(insuflim['trackID'].tolist())
    
                        
    tracks=pd.read_sql('SELECT \
                         plays_track.trackID,\
                         trackName, \
                         ID_track.mbID,\
                         artistName\
                         FROM plays_track\
                         LEFT JOIN ID_track\
                         ON plays_track.trackID=ID_track.trackID\
                         LEFT JOIN ID_artist\
                         ON ID_track.artistID=ID_artist.artistID\
                         WHERE plays_track.userID=? and plays_track.plays>0'
                         ,con, params=(lookup_userID(con,user),)
                         )
    lock.release()
    
    for i,ID,track,mbID,artist in tracks.itertuples():

        if i==0:
            save_loaded_update=list()
            save_loaded_insert=list()
            reset_chart_save=True
            chart_save=pd.DataFrame()
        
        if ID not in loaded:
            
            call_controler.timer_event.wait()
            
            try:
                attr['track']=track
                attr['artist']=artist
                similar,attrNew=get_similar(attr,limit)
                log.log_event()
                
                if similar.shape[0]>1: #accounts for empty table
                    attrNew['track']=track
                    
                    lock.acquire()
                    similar=addIDcol(con,similar,attrNew)
                    lock.release()
                    
                    if reset_chart_save==True:
                        chart_save=similar
                        reset_chart_save=False
                        
                    else:
                        chart_save=chart_save.append(similar)
                        
            except timeout:
                continue
            except:
                try:
                    lock.release()
                except:
                    pass
            
            if ID in insuflim:
                save_loaded_update.append((limit,int(ID)))
            else:
                save_loaded_insert.append((int(ID),limit))
                
        if chart_save.shape[0]>=n_entries or (i==len(tracks)-1 and chart_save.shape[0]>0):
            # in very rare cases addIDcol cannot find the correct ID
            boolean=[False if ID=='nan' else True for i,ID in chart_save['ID'].iteritems()]
            chart_save=chart_save[boolean]

            lock.acquire()
            try:
                out.save('SIMILAR_TRACK is saving to database')
                enter_chart(con,chart_save.loc[:,['artist','image','mbid','name','playcount','url']],attrNew)
                artistDF=unpackdf(chart_save,'ID','artist')
                chart_save['artistID']=returnID(con,artistDF['name'],{'category':'artist'})
                chart_save['trackID']=returnID(con, chart_save.loc[:,['artistID','name']],{'category':'track'})
                enter_similar(con,chart_save,attrNew)
    
                reset_chart_save=True
                
                if save_loaded_update:
                    c.executemany('UPDATE loaded_similar_track SET lim=? WHERE trackID=?',save_loaded_update)
                    save_loaded_update=list()
                    
                if save_loaded_insert:
                    c.executemany('INSERT OR IGNORE INTO loaded_similar_track (trackID,lim) VALUES(?,?)',save_loaded_insert)
                    save_loaded_insert=list()
                    
                    
                con.commit()
            except Exception as e:
                save(chart_save,'similar_track_error_chart')
                save(attrNew,'similar_track_error_attr')
                out.save(e)
                lock.release()
                raise json
                
            lock.release()

        out.save('SIMILAR_TRACK \t\t%.3f\t percent loaded' % (float(i+1)/len(tracks)*100))

        
    
def load_toptags_artist(db_path, lock, call_controler, out, n_entries=20000):
    """see load_toptracks for description"""

    con=sqlite3.connect(db_path)    
    c=con.cursor()
    log=call_controler.create_log()
    attr={'category':'artist'}
    
    lock.acquire()
    loaded=pd.read_sql('SELECT artistID FROM loaded_toptags_artist', con, )
    loaded=set(loaded['artistID'].tolist())
    artists=pd.read_sql('SELECT artistID,artistName FROM ID_artist',con)
    lock.release()
    
    for i,ID,name in artists.itertuples():

        if i==0:
            save_loaded=list()
            reset_chart_save=True
            chart_save=pd.DataFrame()
        
        if ID not in loaded:
            
            call_controler.timer_event.wait()
            
            try:
                attr['artist']=name
                toptags,attrNew=get_toptags(attr)
                log.log_event()
                
                if toptags.shape[0]>1: #accounts for empty table
                
                    lock.acquire()
                    toptags=addIDcol(con,toptags,attrNew) #ID info from attrNew is added to DF
                    lock.release()
                    
                    if reset_chart_save==True:
                        chart_save=toptags
                        reset_chart_save=False
                        
                    else:
                        chart_save=chart_save.append(toptags)
                    
            except timeout:
                continue
            except:
                try:
                    lock.release()
                except:
                    pass
            
            save_loaded.append((int(ID),))

        if chart_save.shape[0]>=n_entries or (i==len(artists)-1 and chart_save.shape[0]>0):
            # in very rare cases addIDcol cannot find the correct ID
            boolean=[False if ID=='nan' else True for i,ID in chart_save['ID'].iteritems()]
            chart_save=chart_save[boolean]
            
            lock.acquire()
            try:
                out.save( 'TOPTAGS_ARTIST is saving to database')
                enter_IDitem(con,chart_save.loc[:,['name','url']],attrNew)
                enter_tags(con,chart_save,attrNew)
                reset_chart_save=True
                if save_loaded:
                    c.executemany('INSERT OR IGNORE INTO loaded_toptags_artist (artistID) VALUES(?)',save_loaded)
                    save_loaded=list()
                    
                con.commit()
            except Exception as e:
                save(chart_save,'toptags_artist_error_chart')
                save(attrNew,'toptags_artist_error_attr')
                out.save(e)
                lock.release()
                raise json
                
            lock.release()
            
        out.save( 'TOPTAGS_ARTIST \t\t%.3f\t percent loaded' % (float(i+1)/len(artists)*100))
            
    
def load_toptags_track(db_path, lock, call_controler, out, n_entries=20000):
    """see load_toptracks for description
        will only load tags of tracks with mbID"""

    con=sqlite3.connect(db_path)    
    c=con.cursor()
    log=call_controler.create_log()
    attr={'category':'track'}
    
    lock.acquire()
    loaded=pd.read_sql('SELECT trackID FROM loaded_toptags_track', con, )
    loaded=set(loaded['trackID'].tolist())
    
    tracks=pd.read_sql('SELECT ID_track.trackID,ID_track.trackName,\
                               ID_artist.artistName \
                        FROM ID_track \
                        LEFT JOIN ID_artist \
                        ON ID_track.artistID=ID_artist.artistID\
                        WHERE ID_track.mbID IS NOT NULL AND\
                        ID_track.mbID!=?',con,params=('',))
    lock.release()
    
    for i,ID,track,artist in tracks.itertuples():
        
        if i==0:
            save_loaded=list()
            reset_chart_save=True
            chart_save=pd.DataFrame()
        
        if ID in loaded:
            continue
        
        call_controler.timer_event.wait()
    
        try:
            attr['track']=track
            attr['artist']=artist
            toptags,attrNew=get_toptags(attr)
            log.log_event()
            
            if toptags.shape[0]>1: #accounts for empty table
            
                lock.acquire()
                toptags=addIDcol(con,toptags,attrNew) #ID info from attrNew is added to DF
                lock.release()
                
                if reset_chart_save==True:
                    chart_save=toptags
                    reset_chart_save=False
                    
                else:
                    chart_save=chart_save.append(toptags)
                    
        except timeout:
            continue
        except:
            try:
                lock.release()
            except:
                pass
            
        save_loaded.append((int(ID),))
        
        if chart_save.shape[0]>=n_entries or (i==len(tracks)-1 and chart_save.shape[0]>0):
            # in very rare cases addIDcol cannot find the correct ID
            boolean=[False if ID=='nan' else True for i,ID in chart_save['ID'].iteritems()]
            chart_save=chart_save[boolean]
            lock.acquire()
            try:
                out.save( 'TOPTAGS_TRACKS is saving to database')
                enter_IDitem(con,chart_save.loc[:,['name','url']],attrNew)
                enter_tags(con,chart_save,attrNew)
                reset_chart_save=True
                
                if save_loaded:
                    c.executemany('INSERT OR IGNORE INTO loaded_toptags_track (trackID) VALUES(?)',save_loaded)
                    save_loaded=list()
                    
                con.commit()
            except Exception as e:
                save(chart_save,'toptags_track_error_chart')
                save(attrNew,'toptags_track_error_attr')
                out.save(e)
                lock.release()
                raise json
                
            lock.release()
            
        out.save( 'TOPTAGS_TRACKS \t\t%.3f\t percent loaded' % (float(i+1)/len(tracks)*100))
        

def load_toptags_album(db_path, lock, call_controler, out,n_entries=20000):
    """see load_toptracks for description
        will only load tags of tracks with mbID"""

    con=sqlite3.connect(db_path)    
    c=con.cursor()
    log=call_controler.create_log()
    attr={'category':'album'}
    
    lock.acquire()
    loaded=pd.read_sql('SELECT albumID FROM loaded_toptags_album', con, )
    loaded=set(loaded['albumID'].tolist())
    
    albums=pd.read_sql('SELECT ID_album.albumID, ID_album.albumName, \
                               ID_artist.artistName \
                        FROM ID_album \
                        LEFT JOIN ID_artist \
                        ON ID_album.artistID=ID_artist.artistID \
                        WHERE ID_album.mbID IS NOT NULL AND\
                        ID_album.mbID!=?',con,params=('',))
    lock.release()
    
    for i,ID,album,artist in albums.itertuples():
        
        if i==0:
            save_loaded=list()
            reset_chart_save=True
            chart_save=pd.DataFrame()
            
        if ID in loaded:
            continue
        
        call_controler.timer_event.wait()

        try:
            attr['album']=album
            attr['artist']=artist
            toptags,attrNew=get_toptags(attr)
            log.log_event()
            
            if toptags.shape[0]>1: #accounts for empty table
            
                lock.acquire()
                toptags=addIDcol(con,toptags,attrNew) #ID info from attrNew is added to DF
                lock.release()
                
                if reset_chart_save==True:
                    chart_save=toptags
                    reset_chart_save=False
                    
                else:
                    chart_save=chart_save.append(toptags)
                
        except timeout:
            continue
        
        except:
            try:
                lock.release()
            except:
                pass
        
        save_loaded.append((int(ID),))
            
        if chart_save.shape[0]>=n_entries or (i==len(albums)-1 and chart_save.shape[0]>0):
            # in very rare cases addIDcol cannot find the correct ID
            boolean=[False if ID=='nan' else True for i,ID in chart_save['ID'].iteritems()]
            chart_save=chart_save[boolean]
            
            lock.acquire()
            try:
                out.save( 'TOPTAGS_ALBUM is saving to database')
                enter_IDitem(con,chart_save.loc[:,['name','url']],attrNew)
                enter_tags(con,chart_save,attrNew)
                reset_chart_save=True
                
                if save_loaded:
                    c.executemany('INSERT OR IGNORE INTO loaded_toptags_album (albumID) VALUES(?)',save_loaded)
                    save_loaded=list()
    
                con.commit()
            except Exception as e:
                save(chart_save,'toptags_album_error_chart')
                save(attrNew,'toptags_album_error_attr')
                out.save(e)
                lock.release()
                raise json
                
            lock.release()
            
        out.save( 'TOPTAGS_ALBUM \t\t%.3f\t percent loaded' % (float(i+1)/len(albums)*100))
        
def load_mbinfo_album(db_path, lock, call_controler,out,n_entries=1000): 

    con=sqlite3.connect(db_path)    
    c=con.cursor()
    log=call_controler.create_log()
    
    lock.acquire()
    loaded=pd.read_sql('SELECT albumID FROM loaded_mbinfo_album', con)
    loaded=set(loaded['albumID'].tolist())
    
    albums=pd.read_sql('SELECT ID_album.albumID, ID_album.artistID, ID_album.mbID\
                    FROM ID_album \
                    WHERE ID_album.mbID IS NOT NULL AND\
                    ID_album.mbID!=?',con, params=('',))
    lock.release()
    
    for i,albumID,artistID,mbID in albums.itertuples():
        
        if i==0:
            save_loaded=list()
            reset_chart_save=[True,True]
            chart_save_tracks=pd.DataFrame()
            chart_save_date=[]
            
        if albumID in loaded:
            continue
        call_controler.timer_event.wait()
        try:
            lock.acquire()
            date,tracks=get_album_mbinfo(con,mbID,albumID,artistID)
            log.log_event()
            lock.release()
            if tracks.shape[0]>1: #accounts for empty table
            
                if reset_chart_save[0]==True:
                    chart_save_tracks=tracks
                    reset_chart_save[0]=False
                    
                else:
                    chart_save_tracks=chart_save_tracks.append(tracks)
                    
            if str(date[0]).lower()!='none':
            
                if reset_chart_save[1]==True:
                    chart_save_date=[date]
                    reset_chart_save[1]=False
                    
                else:
                    chart_save_date.append(date)
                
        except timeout:
            out.save('mbinfo timeout')
            
            try:
                lock.release()
            except:
                pass
            continue
        
        except json:
            out.save('mbinfo json error')
            
            try:
                lock.release()
            except:
                pass
        
        save_loaded.append((int(albumID),))
            
        if chart_save_tracks.shape[0]>=n_entries or (i==len(albums)-1 and chart_save_tracks.shape[0]>0):
            lock.acquire()
            try:
                out.save( 'MBINFO_ALBUM is saving to database')
                enter_mbinfo_album(con,chart_save_date,chart_save_tracks)
                reset_chart_save=[True,True]
                
                if save_loaded:
                    c.executemany('INSERT OR IGNORE INTO loaded_mbinfo_album (albumID) VALUES(?)',save_loaded)
                    save_loaded=list()
                    
                con.commit()
            except Exception as e:
                save(chart_save_tracks,'mbinfo_album_error_chart_tracks')
                save(chart_save_date,'mbinfo_album_error_chart_date')
                save(attr,'mbinfo_album_error_attr')
                out.save(e)
                lock.release()
                raise json
                
            lock.release()
            
        out.save( 'MBINFO_ALBUM \t\t%.3f\t percent loaded' % (float(i+1)/len(albums)*100))
    
    
def load_spotifyID_track(db_path, lock, call_controler, out, n_entries=500):
    '''see load_toptracks for description'''

    con=sqlite3.connect(db_path)    
    c=con.cursor()
    log=call_controler.create_log()
    attr={'category':'track'}
    
    lock.acquire()
    loaded=pd.read_sql('SELECT trackID FROM loaded_spotifyID_track', con, )
    loaded=set(loaded['trackID'].tolist())
    
    tracks=pd.read_sql('SELECT ID_track.trackID,ID_track.trackName,\
                               ID_artist.artistName \
                        FROM ID_track \
                        LEFT JOIN ID_artist \
                        ON ID_track.artistID=ID_artist.artistID',con)
    lock.release()
            
    for i,ID,name,artist in tracks.itertuples():
        
        if i==0:
            save_loaded=list()
            reset_chart_save=True
            chart_save=[]
        
        if ID in loaded:
            continue

        call_controler.timer_event.wait()
        
        try:
            attr['track']=name
            attr['artist']=artist
            spotifyID_tup=get_spotifyID(ID,attr)
            log.log_event()
            
            if spotifyID_tup!='nan': #accounts for empty tuple
            
                if reset_chart_save==True:
                    chart_save=[spotifyID_tup]
                    reset_chart_save=False
                    
                else:
                    chart_save.append(spotifyID_tup)
                    
        except timeout:
            continue
        except:
            pass
            
        save_loaded.append((int(ID),))
        
        if len(chart_save)>=n_entries or ( i==len(tracks)-1 and len(chart_save)>0 ):
            lock.acquire()
            try:
                out.save( 'SPOTIFYID_TRACKS is saving to database')
                enter_spotifyID(con,chart_save,attr)
                reset_chart_save=True
                
                if save_loaded:
                    c.executemany('INSERT OR IGNORE INTO loaded_spotifyID_track (trackID) VALUES(?)',save_loaded)
                    save_loaded=list()
                    
                con.commit()
            except Exception as e:
                save(chart_save,'spotifyID_track_error_chart')
                save(attr,'spotifyID_track_error_attr')
                out.save(e)
                lock.release()
                raise json
                
            lock.release()
            
        out.save( 'SPOTIFYID_TRACK \t%.4f\t percent loaded' % (float(i+1)/len(tracks)*100))

def load_spotifyID_album(db_path, lock, call_controler, out, n_entries=500):
    '''see load_toptracks for description'''

    con=sqlite3.connect(db_path)    
    c=con.cursor()
    log=call_controler.create_log()
    attr={'category':'album'}
    
    lock.acquire()
    loaded=pd.read_sql('SELECT albumID FROM loaded_spotifyID_album', con, )
    loaded=set(loaded['albumID'].tolist())
    
    albums=pd.read_sql('SELECT ID_album.albumID,ID_album.albumName,\
                               ID_artist.artistName \
                        FROM ID_album \
                        LEFT JOIN ID_artist \
                        ON ID_album.artistID=ID_artist.artistID',con)
    lock.release()
    
    for i,ID,name,artist in albums.itertuples():
        
        if i==0:
            save_loaded=list()
            reset_chart_save=True
            chart_save=[]
        
        if ID in loaded:
            continue
        
        call_controler.timer_event.wait()
        
        try:
            attr['album']=name
            attr['artist']=artist
            spotifyID_tup=get_spotifyID(ID,attr)
            log.log_event()
            
            if spotifyID_tup!='nan': #accounts for empty tuple
            
                if reset_chart_save==True:
                    chart_save=[spotifyID_tup]
                    reset_chart_save=False
                    
                else:
                    chart_save.append(spotifyID_tup)
                    
        except timeout:
            continue
        except:
            pass
            
        save_loaded.append((int(ID),))
        
        if len(chart_save)>=n_entries or ( i==len(albums)-1 and len(chart_save)>0 ):
            lock.acquire()
            try:
                out.save( 'SPOTIFYID_ALBUM is saving to database')
                enter_spotifyID(con,chart_save,attr)
                reset_chart_save=True
                
                if save_loaded:
                    c.executemany('INSERT OR IGNORE INTO loaded_spotifyID_album (albumID) VALUES(?)',save_loaded)
                    save_loaded=list()
                    
                con.commit()
            except Exception as e:
                save(chart_save,'spotifyID_album_error_chart')
                save(attr,'spotifyID_album_error_attr')
                out.save(e)
                lock.release()
                raise json
                
            lock.release()
            
        out.save( 'SPOTIFYID_ALBUM \t%.4f\t percent loaded' % (float(i+1)/len(albums)*100))
        
def calc_interest_score(user, db_path, lock=threading.RLock()):
    '''
    all artist and track items from initial user charts have been used to request 
    similar items, which are stored in the similar_* tables. 
    The srength of each similartiy connection is expressed as the similarity score
    
    i_score(itemX)=Sum of all( (sim_score itemX--itemY connection)*itemY_plays/total_plays )
    '''

    con=sqlite3.connect(db_path)    
    c=con.cursor()
    userID=lookup_userID(con,user)

    lock.acquire()

        
# i_score artist
    i_score_artist=pd.read_sql('SELECT artistID FROM ID_artist ', con) 

    i_score_artist.index=i_score_artist['artistID'] 
    
    i_score_artist['i_score']=0
    
    del i_score_artist['artistID']

    userplays_artist=pd.read_sql('SELECT artistID, plays \
                                  FROM plays_artist \
                                  WHERE userID=? AND plays>0', con, params=(userID,))

    sim_score=pd.read_sql('SELECT artistID_1, artistID_2, score \
                                  FROM similar_artist ', con)
                                  
    total_plays=userplays_artist['plays'].sum()

    for i,artistID_1,plays in userplays_artist.itertuples():
        
        boolean=sim_score['artistID_1']==int(artistID_1)
        
        sim_score_select=sim_score[boolean]
        
        for i,artistID_2, score in sim_score_select.loc[:,['artistID_2','score']].itertuples():
            
            i_score=float(score)*int(plays)/int(total_plays)
            i_score_artist.loc[artistID_2,'i_score']+=i_score
            i_score_artist.loc[artistID_1,'i_score']+=i_score
            
# i_score track
    i_score_track=pd.read_sql('SELECT trackID FROM ID_track ', con) 

    i_score_track.index=i_score_track['trackID']
    
    i_score_track['i_score']=0
    
    del i_score_track['trackID']

    userplays_track=pd.read_sql('SELECT trackID, plays \
                                  FROM plays_track \
                                  WHERE userID=? AND plays>0', con, params=(userID,))

    sim_score=pd.read_sql('SELECT trackID_1, trackID_2, score \
                                  FROM similar_track ', con)
                                  
    total_plays=userplays_track['plays'].sum()

    for i,trackID_1,plays in userplays_track.itertuples():

# contrary to similar_artist table, trackIDs with user plays (1-5000) can be 
# found in trackID2 column, hence the trackID specifications are switched
        
        boolean=sim_score['trackID_2']==int(trackID_1)
        
        sim_score_select=sim_score[boolean]
        
        for i,trackID_2, score in sim_score_select.loc[:,['trackID_1','score']].itertuples():
            
            i_score=float(score)*int(plays)/int(total_plays)
            i_score_track.loc[trackID_2,'i_score']+=i_score
            i_score_track.loc[trackID_1,'i_score']+=i_score
            
# enter tables
    newTuple=( (int(userID),int(artistID),float(i_score)) for artistID,i_score in i_score_artist.itertuples() 
                if i_score > 0)
    
    c.executemany('INSERT OR REPLACE INTO i_score_artist (userID,artistID,i_score) \
                    VALUES(?,?,?)',newTuple)
                    
    newTuple=( (int(userID),int(trackID),float(i_score)) for trackID,i_score in i_score_track.itertuples() 
                if i_score > 0)
    
    c.executemany('INSERT OR REPLACE INTO i_score_track (userID,trackID,i_score) \
                    VALUES(?,?,?)',newTuple)

    con.commit()                    
    lock.release()
    
    return (i_score_artist, i_score_track)


def cleanup_db_dates(db_path, lock=threading.RLock()):
    """
    i)release dates are stored as strings in date_album.date containing differently formatted dates. This function creates
    a new column containing only the years of the release as integers
    
    ii)a track ID can be associated with several albumIDs. A SQL SELECT statement would require several table joins and
    subselect statements which are expensive. Therefore a temp table will be created with all the necessary joins. And
    a regular table will be created from the temp join using the sub select query.

    """
    con=sqlite3.connect(db_path)
    c=con.cursor()
    lock.acquire()

    # i)
    #deletes empty string entries from database
    c.execute('DELETE FROM date_album WHERE date="" ')
    con.commit()

    dates_df=pd.read_sql('SELECT date, albumID FROM date_album', con)

    tup=[ (  int( date.split('-')[0]),int(ID) ) for i,date,ID in dates_df.itertuples()]

    try:
        c.execute('ALTER TABLE date_album ADD COLUMN date_int INTEGER')
    except:
        pass

    c.executemany('UPDATE date_album SET date_int=? WHERE albumID=?',tup)

    # ii)

    c.execute('CREATE TEMP TABLE IF NOT EXISTS date_joins AS \
               SELECT track_rel_album.trackID,date_album.albumID,date_album.date_int \
               FROM track_rel_album\
               LEFT JOIN date_album ON date_album.albumID = track_rel_album.albumID ')

    c.execute('DROP TABLE IF EXISTS date_track ')

    c.execute('CREATE TABLE date_track AS SELECT main.date_int, main.trackID FROM date_joins AS main\
                  WHERE main.date_int= ( \
                                          SELECT min(sub.date_int) FROM date_joins AS sub \
                                          WHERE sub.trackID=main.trackID \
                                        )' )
    con.commit()
    lock.release()

#----------------------------------------------------------------------------------
#THREADS

    
class LOAD(threading.Thread):
    
    def __init__(self, db_path, name, call_controler, output_controler, db_lock,
                 condition_event):
                     
        threading.Thread.__init__(self)
        self.condition_event=condition_event
        self.signal_event=threading.Event()
        self.lock=db_lock
        self.name=name
        self.call_cont=call_controler
        self.db_path=db_path
        self.output_cont=output_controler
        self.out=output_controler.create_outvar()
        self.out.save('%s not started' % self.name)
        
        
    def run(self):
        
        self.out.save('%s waiting for start condition' % self.name)
        self.condition_event.wait()
        
        self.load()
            
        self.signal()
        
    def activate(self):
        self.condition_event.set()
        
    def deactivate(self):
        self.condition_event.clear()
        
    def signal(self):
        self.signal_event.set()
        self.out.save('%s finished' % self.name)
        
class RESTART(LOAD):
    '''creates subclass of LOAD overwrting run function for selfrestarting threads'''

    def run(self):
        
        self.out.save('%s waiting for start condition' % self.name)
        self.condition_event.wait()
        while self.condition_event.isSet()==True:
        
            self.load()
            self.out.save('%s sleeping' % self.name)
            time.sleep(10)
            
        self.out.save('%s waiting for last run' % self.name)
#        once thread is signalled to finish it attempts one last run after waiting for 1 min
        time.sleep(60)
        self.load()
        self.signal()
        
class SIMILAR_ARTIST(LOAD):

    def __init__(self, db_path, name, call_controler, output_controler, db_lock, user, limit, no_entries,
                 condition_event=threading.Event()):
        LOAD.__init__(self, db_path, name, call_controler, output_controler, db_lock, condition_event)
        self.limit=limit
        self.no_entries=no_entries
        self.user=user
        
    def load(self):
        
        load_similar_artist(self.db_path, self.lock, self.call_cont, self.out, self.user, self.limit, self.no_entries)

class SIMILAR_TRACK(LOAD):

    def __init__(self, db_path, name, call_controler, output_controler, db_lock, user, limit, no_entries,
                 condition_event=threading.Event()):
        LOAD.__init__(self, db_path, name, call_controler, output_controler, db_lock, condition_event)
        self.limit=limit
        self.no_entries=no_entries
        self.user=user
        
    def load(self):
        
        load_similar_track(self.db_path, self.lock, self.call_cont, self.out, self.user, self.limit, self.no_entries)
    
class TOPTRACKS(RESTART):
    
    def __init__(self, db_path, name, call_controler, output_controler, db_lock, limit, no_entries,
                 condition_event=threading.Event()):
        LOAD.__init__(self, db_path, name, call_controler, output_controler, db_lock, condition_event)
        self.limit=limit
        self.no_entries=no_entries
        
    def load(self):
        load_toptracks(self.db_path, self.lock, self.call_cont, self.out, self.limit, self.no_entries)
        
class TOPALBUMS(RESTART):
    
    def __init__(self, db_path, name, call_controler, output_controler, db_lock, limit, no_entries,
                 condition_event=threading.Event()):
        LOAD.__init__(self, db_path, name, call_controler, output_controler, db_lock, condition_event)
        self.limit=limit
        self.no_entries=no_entries
        
    def load(self):
        load_topalbums(self.db_path, self.lock, self.call_cont, self.out, self.limit, self.no_entries)
        

class TOPTAGS_ARTIST(RESTART):

    def __init__(self, db_path, name, call_controler, output_controler, db_lock, no_entries,
                 condition_event=threading.Event()):
        LOAD.__init__(self, db_path, name, call_controler, output_controler, db_lock, condition_event)
        self.no_entries=no_entries
        
    def load(self):
        
        load_toptags_artist(self.db_path, self.lock, self.call_cont, self.out, self.no_entries)

class TOPTAGS_TRACK(RESTART):

    def __init__(self, db_path, name, call_controler, output_controler, db_lock, no_entries,
                 condition_event=threading.Event()):
        LOAD.__init__(self, db_path, name, call_controler, output_controler, db_lock, condition_event)
        self.no_entries=no_entries
        
    def load(self):
        
        load_toptags_track(self.db_path, self.lock, self.call_cont, self.out, self.no_entries)

class TOPTAGS_ALBUM(RESTART):

    def __init__(self, db_path, name, call_controler, output_controler, db_lock, no_entries,
                 condition_event=threading.Event()):
        LOAD.__init__(self, db_path, name, call_controler, output_controler, db_lock, condition_event)
        self.no_entries=no_entries
        
    def load(self):
        
        load_toptags_album(self.db_path, self.lock, self.call_cont, self.out, self.no_entries)

class MBINFO_ALBUM(RESTART):

    def __init__(self, db_path, name, call_controler, output_controler, db_lock, no_entries,
                 condition_event=threading.Event()):
        LOAD.__init__(self, db_path, name, call_controler, output_controler, db_lock, condition_event)
        self.no_entries=no_entries

    def load(self):
        
        load_mbinfo_album(self.db_path, self.lock, self.call_cont, self.out, self.no_entries)

class SPOTIFYID_TRACK(RESTART):

    def __init__(self, db_path, name, call_controler, output_controler, db_lock, no_entries,
                 condition_event=threading.Event()):
        LOAD.__init__(self, db_path, name, call_controler, output_controler, db_lock, condition_event)
        self.no_entries=no_entries
        
    def load(self):
        
        load_spotifyID_track(self.db_path, self.lock, self.call_cont, self.out, self.no_entries)

class SPOTIFYID_ALBUM(RESTART):

    def __init__(self, db_path, name, call_controler, output_controler, db_lock, no_entries,
                 condition_event=threading.Event()):
        LOAD.__init__(self, db_path, name, call_controler, output_controler, db_lock, condition_event)
        self.no_entries=no_entries
        
    def load(self):
        
        load_spotifyID_album(self.db_path, self.lock, self.call_cont, self.out, self.no_entries)

class CONTROLER(threading.Thread):
    '''takes a list of events and a function, when all events are set function will be executed '''
    
    def __init__(self, event_list, function, output_controler=output_controler(1), name='controler'):
        threading.Thread.__init__(self)
        self.events=event_list
        self.function=function
        self.output_cont=output_controler
        self.out=output_controler.create_outvar()
        self.name=name
        self.out.save('%s not started' % self.name)
        
    def run(self):
        for event in self.events:
            self.out.save('%s waiting' % self.name)
            event.wait()
            
        self.function()
        self.out.save('%s finished' % self.name)

class LOCKOWNER(threading.Thread):
    '''creates outputvar that displays lock ownership'''

    def __init__(self,out_cont,lock):
        threading.Thread.__init__(self)
        self.lock=lock
        self.out_cont=out_cont
        self.out=self.out_cont.create_outvar()
        self.condition_event=threading.Event()
        self.condition_event.set()

    def run(self):

        while self.condition_event.isSet()==True:
            self.out.save(str(self.lock))


# EXECUTE
           
if __name__ == "__main__":
    
    #Import
    import time
    import threading
    import pandas as pd
    import sqlite3
    import requests
    from copy import deepcopy
    from createDB import *
    from call_controler_v2 import *
    from output_controler import *
    
    
    # Global Variables
    
    url='http://ws.audioscrobbler.com/2.0/'
    
    baserequest={'api_key':'43e826d47e1fc381ac3686f374ee34b5','format':'json'} 
    
    path=r'd:\Dropbox\work\python\lastify'
    
    sk=str()
    
    secret='e47ef29dda1b81c1f865d12a89ad28b8'
    
    # Database Connection
    
    db_path='%s\\lastify.db' % path
    createDB(db_path)
    con=sqlite3.connect(db_path)
        
    # load charts
    
    for category in ['artist','track','album']:
    
        chart,attr=get_chart(attr={'category':category},period='overall', limit=1000, user='erblast')
        
        enter_chart(con,chart,attr)
    
    con.close()
    print 'charts loaded'
    
    # create controler instances and threading lock
    
    out_cont=output_controler(0.1)
    out_cont.start()
    
    lastfm_cont=call_controler(1500,300, out_cont, 'LASTFM CONTROLLER\t')
    lastfm_cont.start()
    
    spotify_cont=call_controler(1500,300, out_cont,'SPOTIFY CONTROLLER\t')
    spotify_cont.start()
    
    mb_cont=call_controler(17,20, out_cont,'MUSICBRAINZ CONTROLLER\t')
    mb_cont.start()
    
    
    db_lock=threading.RLock()
    
    thr_lockowner=LOCKOWNER(out_cont,db_lock)
    thr_lockowner.start()
    
    
#    thr_lockreleaser=LOCKRELEASER([lastfm_cont,spotify_cont,mb_cont],db_lock,out_cont)
#    thr_lockreleaser.start()
    
    # create THREADS
    thr_similar_artist=SIMILAR_ARTIST(db_path,'SIMILAR_ARTIST', lastfm_cont, out_cont, db_lock, 'erblast',25, 5000)
    thr_similar_track=SIMILAR_TRACK(db_path,'SIMILAR_TRACK', lastfm_cont, out_cont, db_lock, 'erblast', 25, 5000)
    
    thr_toptracks=TOPTRACKS(db_path,'TOPTRACKS', lastfm_cont, out_cont, db_lock, 25, 5000)
    thr_topalbums=TOPALBUMS(db_path,'TOPALBUMS', lastfm_cont, out_cont, db_lock, 25, 5000)
    
    thr_toptags_artist=TOPTAGS_ARTIST(db_path,'TOPTAGS_ARTIST', lastfm_cont, out_cont, db_lock, 20000)
    thr_toptags_album=TOPTAGS_ALBUM(db_path,'TOPTAGS_ALBUM', lastfm_cont, out_cont, db_lock, 20000)
    thr_toptags_track=TOPTAGS_TRACK(db_path,'TOPTAGS_TRACK', lastfm_cont, out_cont, db_lock, 20000)
    
    thr_mbinfo_album=MBINFO_ALBUM(db_path,'MBINFO_ALBUM', mb_cont, out_cont, db_lock, 500)
    thr_spotifyID_album=SPOTIFYID_ALBUM(db_path,'SPOTIFYID ALBUM', spotify_cont, out_cont, db_lock, 1000)
    thr_spotifyID_track=SPOTIFYID_TRACK(db_path,'SPOTIFYID TRACK', spotify_cont, out_cont, db_lock, 1000)
    
    # CONTROLER THREAD 1
    # toptags tracks gets deactivated when SIMILAR_TRACK and TOPTRACKS are done
    
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
    
    thr_cont_sim_artist=CONTROLER([thr_similar_artist.signal_event,thr_similar_track.signal_event], \
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
    
    
    
