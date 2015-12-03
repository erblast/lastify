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

v2 uses the output_controler to make periodic outputs

@author: erblast
"""

import time
import threading
from output_controler import *

class call_controler(threading.Thread):
    '''call_controler can create different logs, when the number of events in the past period
    loged in child logs exceeds no_calls timer_event will be set to False
    
    call controler becomes active when Thread is started it can be stopped using stop function'''
    
    def __init__(self,no_calls,period,out_cont=output_controler(0.1),name='call controler'):
        threading.Thread.__init__(self)
        self.no_calls=no_calls
        self.period=period
        self.logs=[]
        self.timer_event=threading.Event()
        self.stop_event=threading.Event()
        self.out_cont=out_cont
        self.out=out_cont.create_outvar()
        self.name=name
        
    def run(self):
        
        self.timer_event.set()
        self.stop_event.set()
                
        while self.stop_event.isSet()==True:
            
            history=[event for log in self.logs for event in log.log 
                     if time.time()-self.period < event]
                         
            if len(history)>=self.no_calls:
                self.timer_event.clear()
                self.out.save( '%s calls during last %ds: %s limit exceeded'  %(self.name, self.period, len(history)))
            else:
                self.out.save(  '%s calls during last %ds: %s' %(self.name, self.period, len(history)))
                
            if len(history)<self.no_calls and not self.timer_event.isSet():
                self.timer_event.set()
                
    def create_log(self):
        
        self.logs.append(log(self.period))
        return self.logs[-1]
        
    def stop(self):
        self.stop_event.clear()
    
        

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
    
    out_cont=output_controler(0.2)     
    out_cont.start()
    
    controler1=call_controler(50,10,out_cont, 'CONT 1')
    controler1.start()
    
    c1=caller('caller_1',controler1,2)
    c1.start()
    
    c2=caller('caller_2',controler1,2.5)
    c2.start()
    
    controler2=call_controler(50,10,out_cont, 'CONT 2')
    controler2.start()
    
    c3=caller('caller_3',controler2,2)
    c3.start()
    
    c4=caller('caller_4',controler2,2.5)
    c4.start()
