            IDs=pd.read_sql('SELECT ID_track.trackID, plays_track.plays, ID_user.userName\
                                  FROM ID_track \
                                  LEFT JOIN plays_track ON ID_track.trackID = plays_track.trackID\
                                  LEFT JOIN ID_user ON ID_user.userID=plays_track.userID\
                                  EXCEPT \
                                        SELECT plays_track.trackID,  plays_track.plays, ID_user.userName  FROM plays_track\
                                        LEFT JOIN ID_user ON ID_user.userID=plays_track.userID\
                                        WHERE (ID_user.userName=? AND plays_track.plays<=?)',\
                                  con, params=(user, 10 ))


            IDs_select= pd.read_sql('SELECT plays_track.trackID,  plays_track.plays, ID_user.userName  FROM plays_track\
                                        LEFT JOIN ID_user ON ID_user.userID=plays_track.userID\
                                        WHERE (ID_user.userName=? AND plays_track.plays<=?)',\
                                        con, params=(user, 50 ))


            IDs_select= pd.read_sql('SELECT plays_track.trackID,  plays_track.plays, ID_user.userName  FROM plays_track\
                                        LEFT JOIN ID_user ON ID_user.userID=plays_track.userID\
                                        WHERE (ID_user.userName=? AND plays_track.plays<=?)',\
                                        con, params=(user, 50 ))
