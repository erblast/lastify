# To do: Handle {"error":"The MusicBrainz web server is currently busy. Please try again later."}


import PyQt4.uic
from PyQt4 import QtGui, QtCore
import sys
from lastify_v6 import *
from Filter import *
import gui
from Writer import *
import threading
import pandas as pd
import sqlite3
import time
import re

# variables

db_path = '%s\\test.db' % path
createDB(db_path)
con = sqlite3.connect(db_path)
db_lock = threading.RLock()

# update gui.py
with open('gui.py','w') as f:
	PyQt4.uic.compileUi('gui.ui',f)

class QTHR_FILTER(QtCore.QThread):
    signal_text = QtCore.pyqtSignal('QString', name='text')
    signal_filtman_track = QtCore.pyqtSignal(bool ,'PyQt_PyObject', name='filtman_track')
    signal_filtman_album = QtCore.pyqtSignal(bool ,'PyQt_PyObject', name='filtman_album')

    def __init__(self, user):
        super(QTHR_FILTER, self).__init__()
        self.user = user

    def run(self):
        self.signal_text.emit('cleaning up database')
        cleanup_db_dates(db_path, db_lock)

        self.signal_text.emit('calculating interest score')
        calc_interest_score(self.user, db_path, db_lock)

        self.signal_text.emit('creating track filter manager')
        track_filtman = FilterManager(db_path=db_path, user=self.user, category='track', thresh=0, db_lock=threading.RLock(), freq_init= True)
        self.signal_filtman_track.emit(True, track_filtman)

        self.signal_text.emit('creating album filter manager')
        album_filtman = FilterManager(db_path=db_path, user=self.user, category='album', thresh=0, db_lock=threading.RLock(), freq_init= True)
        self.signal_filtman_album.emit(True, album_filtman)

        self.signal_text.emit('filter manager created')

class QTHR_OUTPUT_2_GUI(QtCore.QThread):
    '''takes tuple of out variables and events. Creates a Signal and emits outvariables in regular intervall 2 per second
    to slot'''

    signal = QtCore.pyqtSignal('PyQt_PyObject', name='Signal')

    def __init__(self, out_vars, events):
        super(QTHR_OUTPUT_2_GUI, self).__init__()
        self.out_vars = out_vars
        self.events = events

    def run(self):
        while not all([event.isSet() for event in self.events]):
            self.signal.emit(self.out_vars)
            time.sleep(1 / 0.5)

	def __del__(self):  # this seems to make the thread more stable and prevent it from crashing
		self.wait()

class WindowApp(QtGui.QMainWindow, gui.Ui_MainWindow):
    def __init__(self):
        super(WindowApp, self).__init__()  # loads the constructors of the parent classes
        self.setupUi(self)  # setupUi is defined in the Gui.Ui_MainWindow class in the Gui.py module


        # connect tab selection
        self.tabWidget.currentChanged.connect(self.select_tab)

        # connect buttons load tab
        self.btn_load.clicked.connect(self.load)

        # fill combobox filter tab
        for category in ('track','album'):
            self.select_list_category.addItem(category)

        for category in ('artist','track'):
            self.select_plays_category.addItem(category)
            self.select_date_category.addItem(category)

        for category in ('artist','track','album'):
            self.select_tag_category.addItem(category)

    def select_tab(self):
        if self.tabWidget.currentWidget().objectName() == 'tab_filter':
            print 'filter tab selected'
            thr_select_tab_filter = QTHR_FILTER(str(self.in_txt_user.text()))
            thr_select_tab_filter.signal_text.connect(self.set_filter_status)
            thr_select_tab_filter.run()

        if self.tabWidget.currentWidget().objectName() == 'tab_write':
            print 'writer tab selected'

        if self.tabWidget.currentWidget().objectName() == 'tab_stats':
            print 'stats tab selected'

    def set_filter_status(self,text):
        self.lab_filter_status.setText(text)

    def get_input_load(self):
        return (str(self.in_txt_user.text()), \
                int(self.in_box_limit_charts_artist.value()), \
                int(self.in_box_limit_charts_track.value()), \
                int(self.in_box_limit_charts_album.value()), \
                int(self.in_box_limit_similar_artist.value()), \
                int(self.in_box_limit_similar_track.value()), \
                int(self.in_box_limit_toptracks.value()), \
                int(self.in_box_limit_topalbums.value())
                )

    def load(self):
        '''this function loads database information same structure as in lastify_v6 main and TEST ALL THREADS this variant
		connects it to the GUI, form = GUI class'''

        user, lim_charts_artist, lim_charts_track, lim_charts_album, lim_similar_artist, lim_similar_track, lim_toptracks, \
        lim_topalbums = self.get_input_load()

        # load charts

        chart, attr = get_chart(attr={'category': 'artist'}, period='overall', limit=lim_charts_artist, user=user)
        enter_chart(con, chart, attr)
        chart, attr = get_chart(attr={'category': 'track'}, period='overall', limit=lim_charts_track, user=user)
        enter_chart(con, chart, attr)
        chart, attr = get_chart(attr={'category': 'album'}, period='overall', limit=lim_charts_album, user=user)
        enter_chart(con, chart, attr)

        print 'charts loaded'

        # load output, call controlers and lockowner
        out_cont = output_controler(0.1)  # output controler in combination with GUI serves only logging purposes
        out_cont.start()

        lastfm_cont = call_controler(1500, 300, out_cont, 'LASTFM CONTROLLER\t')
        lastfm_cont.start()

        spotify_cont = call_controler(1500, 300, out_cont, 'SPOTIFY CONTROLLER\t')
        spotify_cont.start()

        mb_cont = call_controler(17, 20, out_cont, 'MUSICBRAINZ CONTROLLER\t')
        mb_cont.start()

        thr_lockowner = LOCKOWNER(out_cont, db_lock)  # also only for logging
        thr_lockowner.start()

        # create THREADS
        thr_similar_artist = SIMILAR_ARTIST(db_path, 'SIMILAR_ARTIST', lastfm_cont, out_cont, db_lock, user,
                                            lim_similar_artist, 5000)
        thr_similar_track = SIMILAR_TRACK(db_path, 'SIMILAR_TRACK', lastfm_cont, out_cont, db_lock, user,
                                          lim_similar_track, 5000)

        thr_toptracks = TOPTRACKS(db_path, 'TOPTRACKS', lastfm_cont, out_cont, db_lock, lim_toptracks, 5000)
        thr_topalbums = TOPALBUMS(db_path, 'TOPALBUMS', lastfm_cont, out_cont, db_lock, lim_topalbums, 5000)

        thr_toptags_artist = TOPTAGS_ARTIST(db_path, 'TOPTAGS_ARTIST', lastfm_cont, out_cont, db_lock, 20000)
        thr_toptags_album = TOPTAGS_ALBUM(db_path, 'TOPTAGS_ALBUM', lastfm_cont, out_cont, db_lock, 20000)
        thr_toptags_track = TOPTAGS_TRACK(db_path, 'TOPTAGS_TRACK', lastfm_cont, out_cont, db_lock, 20000)

        thr_mbinfo_album = MBINFO_ALBUM(db_path, 'MBINFO_ALBUM', mb_cont, out_cont, db_lock, 500)
        thr_spotifyID_album = SPOTIFYID_ALBUM(db_path, 'SPOTIFYID ALBUM', spotify_cont, out_cont, db_lock, 1000)
        thr_spotifyID_track = SPOTIFYID_TRACK(db_path, 'SPOTIFYID TRACK', spotify_cont, out_cont, db_lock, 1000)

        # CONTROLER THREAD 1
        # toptags tracks gets deactivated when SIMILAR_TRACK and TOPTRACKS are done

        def f_1():
            thr_toptags_track.deactivate()

        thr_cont_toptags_track = CONTROLER([thr_similar_track.signal_event, thr_toptracks.signal_event],
                                           f_1, name='TOPTAGS TRACK CONTROLER')
        thr_cont_toptags_track.start()

        # CONTROLER THREAD 2
        # restarting threads are deactivated when all other track and album collecting threads are done

        def f_2():
            thr_mbinfo_album.deactivate()
            thr_spotifyID_album.deactivate()
            thr_spotifyID_track.deactivate()

        thr_cont_restart = CONTROLER(
            [thr_similar_track.signal_event, thr_toptracks.signal_event, thr_topalbums.signal_event],
            f_2, name='RESTART CONTROLER')
        thr_cont_restart.start()

        # CONTROLER THREAD 3
        # call and output controler are stopped when all threads are finished

        def f_3():
            lastfm_cont.stop()
            spotify_cont.stop()
            mb_cont.stop()
            out_cont.stop()

        thr_cont = CONTROLER([thr_spotifyID_album.signal_event, \
                              thr_spotifyID_track.signal_event, \
                              thr_mbinfo_album.signal_event, \
                              thr_toptags_artist.signal_event, \
                              thr_toptags_album.signal_event, \
                              thr_toptags_track.signal_event, ], \
                             f_3, name='THREAD CONTROLER ALL')

        thr_cont.start()

        # CONTROLER THREAD 4

        def f_4():
            thr_toptags_artist.deactivate()
            thr_toptracks.deactivate()
            thr_topalbums.deactivate()

        thr_cont_sim_artist = CONTROLER([thr_similar_artist.signal_event, thr_similar_track.signal_event], \
                                        f_4, name='THREAD CONTROLER TOPTAGS_ARTIST, TOPTRACKS, TOPALBUMS')

        thr_cont_sim_artist.start()

        # CONTROLER THREAD 5

        def f_5():
            thr_toptags_album.deactivate()

        thr_cont_topalbum = CONTROLER([thr_topalbums.signal_event], \
                                      f_5, name='THREAD CONTROLER TOPTAGS_ALBUM')

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

        # return

        out_vars = (thr_similar_artist.out, \
                    thr_similar_track.out, \
                    thr_toptracks.out, \
                    thr_topalbums.out, \
                    thr_toptags_artist.out, \
                    thr_toptags_track.out, \
                    thr_toptags_album.out, \
                    thr_mbinfo_album.out, \
                    thr_spotifyID_track.out, \
                    thr_spotifyID_album.out, \
                    lastfm_cont.out, \
                    spotify_cont.out, \
                    mb_cont.out, \
                    thr_lockowner.out)

        events = [thr_spotifyID_album.signal_event, \
                  thr_spotifyID_track.signal_event, \
                  thr_mbinfo_album.signal_event, \
                  thr_toptags_artist.signal_event, \
                  thr_toptags_album.signal_event, \
                  thr_toptags_track.signal_event, ]

        thr_output_2_gui = QTHR_OUTPUT_2_GUI(out_vars, events)
        thr_output_2_gui.signal.connect(self.manage_output)
        thr_output_2_gui.start()

    def manage_output(self, out_vars):

        # loading threads

        bars = (self.bar_similar_artist, \
                self.bar_similar_track, \
                self.bar_toptracks, \
                self.bar_topalbums, \
                self.bar_toptags_artist, \
                self.bar_toptags_track, \
                self.bar_toptags_album, \
                self.bar_mbinfo, \
                self.bar_spotifyID_track, \
                self.bar_spotifyID_album, \
                self.bar_call_lastfm, \
                self.bar_call_spotify, \
                self.bar_call_mbinfo
                )

        labels = (self.lab_thr_status_similar_artist, \
                  self.lab_thr_status_similar_track, \
                  self.lab_thr_status_toptracks, \
                  self.lab_thr_status_topalbums, \
                  self.lab_thr_status_toptags_artist, \
                  self.lab_thr_status_toptags_track, \
                  self.lab_thr_status_toptags_album, \
                  self.lab_thr_status_mbinfo, \
                  self.lab_thr_status_spotifyID_track, \
                  self.lab_thr_status_spotifyID_album, \
                  self.lab_lastfm, \
                  self.lab_spotify, \
                  self.lab_mbinfo,
                  self.lab_dblock \
 \
                  )
        # Status loading threads
        for tripplet in zip(out_vars[:10], bars[:10], labels[:10]):
            outvar, bar, label = tripplet

            m = re.search('\d+.\d+', outvar.out)
            if m:
                bar.setValue(int(float(m.group())))
                label.setText('%.3f %%' % float(m.group()))
            else:
                label.setText(outvar.out)

            if 'finished' in outvar.out:
                bar.setValue(100)

        # Status call controlers
        for tripplet in zip(out_vars[10:13], bars[10:13], labels[10:13]):
            outvar, bar, label = tripplet
            text = outvar.out
            text = text.replace('\t', '').replace('CONTROLLER', 'API')
            label.setText(text)

            if ':' in outvar.out:
                m = re.search('\d+', text.split(':')[1])
                bar.setValue(int(m.group()))

        # Lock owner
        outvar, label = (out_vars[13], labels[13])
        m = re.search('=(.+) ', outvar.out)
        text = m.group().rstrip().lstrip('=')
        label.setText('Database access: %s' % text)


if __name__ == '__main__':
    # execute Gui
    app = QtGui.QApplication([])  # starts GUI app
    form = WindowApp()  # instanciates Gui class
    form.show()
    app.exec_()

    user = form.in_txt_user.text