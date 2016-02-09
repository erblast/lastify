# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 14:00:57 2015

The output_controler can store strings and prints them at a given frequency
-create an instance of the output controler
start the output controler
create output variable instance

controler.create_outvar()

outvar.save('string')

the controler posses functions that stop and start output and a stop function which
stops the controler code from executing

the controler.output function returns a list of all output strings and prints them

@author: erblast
"""

import time
import threading

class output_controler(threading.Thread):
    ''' '''
    
    def __init__(self,freq=0.1):
        threading.Thread.__init__(self)
        self.stop_event=threading.Event()
        self.output_stop_event=threading.Event()
        self.log=[]
        self.freq=freq
        self.string_list=[]
        self.f=open('output_log.csv','w')

    def run(self):
        import time
        self.stop_event.set()
        self.output_stop_event.set()      

        while self.stop_event.isSet()==True:
            
            self.output_stop_event.wait()
            
            self.string=''
            self.string_list=[]
            
            for i in self.log:
                try: # simultaneous printing of two threads will result in Value error
                    i.lock.acquire()
                    print i.out
                    i.lock.release()
                    
                except ValueError:
                    pass
                
                self.string_list.append(i.out)
            
            self.f.write('%s \n' % str(self.string_list).rstrip(']').lstrip('['))
                
                
            time.sleep(1/float(self.freq))
            
    def create_outvar(self):
        self.log.append(outvar())
        return self.log[-1]
        
    def stop(self):
        self.stop_event.clear()
        self.f.close()
        
    def output_stop(self):
        self.output_stop_event.clear()

    def output_start(self):
        self.output_stop_event.set()
        
    def output(self):
        
        for i in self.log:
            print i.out
            self.string_list.append(i.out)
            
        return self.string_list
        

class outvar():
    
    def __init__(self):
        self.out=''
        self.lock = threading.RLock()
        
    def save(self,text):
        self.lock.acquire()
        self.out=text
        self.lock.release()

    def read(self):
        self.lock.acquire()
        text = self.out
        self.lock.release()
        return text
        

    
    
if __name__ == "__main__":   
         
    out_cont=output_controler(0.5)
    out_cont.start()    
    
    print 'controler starts'
    
    out1=out_cont.create_outvar()
    out2=out_cont.create_outvar()
    out3=out_cont.create_outvar()
    
    out1.save('trail 1')
    out2.save('trail 2')
    out3.save('trail 3')
    
    time.sleep(3)

    out1.save('trail 1-1')
    out2.save('trail 2-2')
    out3.save('trail 3-3')

    time.sleep(3)
    
    out_cont.output_stop()
    
    time.sleep(3)
    
    out1.save('trail 1-2')
    out2.save('trail 2-3')
    out3.save('trail 3-4')
    
    print 'output stops'
    time.sleep(3)
    
    out_cont.output()
    
    out_cont.stop()
    
    print 'controler stops'

