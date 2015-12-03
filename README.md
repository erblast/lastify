Lastify

Lastify allows you to generate Spotify playlists based on chart data of lastfm user accounts. It is intended for lastfm users that 
regularly scrooble to their lastfm accounts and also use spotify. Lastify not only collects information on music scroobled to the 
account but also on music similar to what has been scroobled. To this end it uses the lastfm, musicbrainz and spotify APIs.

Playlists can be generated by filtering and sorting the data collected. Specifically: tags (music genre), user plays ( played vs 
unplayed), first release date and interest score (based on similarity links between artists and user plays).

The programm is in development:

What works:
  Core section (latify_v6.py, createDB.py, call_controler_v2.py, output_controler). Uses threading, locking to make simultaneous API 
  requests to three different APIs efficiently maxing out the API request restrictions. Stores all data in a sqllite3 database.

  Run: TEST ALL THREADS in python 2.7 , set global path variable to desired database location, change global variable user to use 
  different lastfm user account. This creates a small test.db with examplatory data. The run should finish in a few minutes.
  
  Run: lastify_v6.py . Collects a larger set of data from the above mentioned APIs, takes around 48h to complete, collects ca 670 mb 
  of data. Indifferent to interruptions of the run, picks up collecting data where interrupted when restarted. Adjust global path 
  variable to existing directory before running

In testing:
  Filter.py Creates and manages different filters to select data from the database. 

To do:
  GUI



