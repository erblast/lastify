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

from lastify_v6 import *


url='http://ws.audioscrobbler.com/2.0/'

baserequest={'api_key':'43e826d47e1fc381ac3686f374ee34b5','format':'json'} 

path=r'd:\Dropbox\work\python\lastify'

sk=str()

secret='e47ef29dda1b81c1f865d12a89ad28b8'

# Database Connection

db_path='%s\\lastify.db' % path
createDB(db_path)
con=sqlite3.connect(db_path)
c=con.cursor()

user= 'erblast'

i_score_artist,i_score_track=calc_interest_score(user, db_path)
t1=i_score_artist[i_score_artist['i_score']>0]
t2=i_score_track[i_score_track['i_score']>0]

