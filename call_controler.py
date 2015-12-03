# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 17:17:26 2015

The call controler controls the number of calls that can be made by multiple threads
to the same source. Create an instance of call_controler and start it.

Define the number of allowed calls (no_calls) during a specific period in seconds

A thread making controled calls should create a log in the call_controler instance
log=controler.create_log()

The controler event time_event is set to False if no_calls exceeds the limit
Before making a call the thread checks the event

controler.time_event.wait()

then the call should be logged

log.log_event()

then make the call

The number of calls during the last period can be displayed using the output function,
freq= Frequency of print outputs [1/s].
Which can be stopped using the output_stop function

@author: erblast
"""

import time
import threading

class call_controler(threading.Thread):
    '''call_controler can create different logs, when the number of events in the past period
    loged in child logs exceeds no_calls timer_event will be set to False
    
    call controler becomes active when Thread is started it can be stopped using stop function'''
    
    def __init__(self,no_calls,period,name='call controler'):
        threading.Thread.__init__(self)
        self.no_calls=no_calls
        self.period=period
        self.logs=[]
        self.timer_event=threading.Event()
        self.stop_event=threading.Event()
        self.output_stop_event=threading.Event()
        self.output_str='%s empty' % self.name
        self.name=name
    def run(self):
        
        self.timer_event.set()
        self.stop_event.set()
                
        while self.stop_event.isSet()==True:
            
            history=[event for log in self.logs for event in log.log 
                     if time.time()-self.period < event]
                         
            if len(history)>=self.no_calls:
                self.timer_event.clear()
                self.output_str= '%s calls during last %ds: %s limit exceeded'  %(self.name, self.period, len(history))
            else:
                self.output_str= '%s calls during last %ds: %s' %(self.name, self.period, len(history))
                
            if len(history)<self.no_calls and not self.timer_event.isSet():
                self.timer_event.set()
                
    def create_log(self):
        
        self.logs.append(log(self.period))
        return self.logs[-1]
        
    def stop(self):
        self.stop_event.clear()
    
    def output(self,freq=0.5):
        self.output_thread=threading.Thread(target=self.output_restart, args=(freq,))
        self.output_thread.start()
        
    def output_restart(self,freq):
        
        self.output_stop_event.set()
        while self.stop_event.isSet()==True and self.output_stop_event.isSet()==True:
            print self.output_str
            time.sleep(1/float(freq))
            
    def output_stop(self):
        self.output_stop_event.clear()
        

class log():
    '''important class for call_controler'''
    
    def __init__(self,period):
        self.period=period
        self.log=[time.time()]
        
    def log_event(self):
        self.log.append(time.time())
        self.log=[event for event in self.log if time.time()-self.period < event]
        


class caller(threading.Thread):

    def __init__(self,name,controler,freq):
        threading.Thread.__init__(self)
        self.name=name
        self.controler=controler
        self.log=self.controler.create_log()
        self.freq=freq
        self.stop=False
        
    def run(self):
        
        while not self.stop:
            if not self.controler.timer_event.isSet():
                print '%s is waiting' %self.name
            
            self.controler.timer_event.wait()
            self.log.log_event()
#            print '%s calls' %self.name
            time.sleep((float(1)/self.freq))
            
    def stop(self):
        self.stop=True

if __name__ == "__main__":            
            
    controler=call_controler(50,10)
    controler.start()
    
    c1=caller('caller_1',controler,2)
    c1.start()
    
    c2=caller('caller_2',controler,2.5)
    c2.start()
    
    controler.output(0.5)
