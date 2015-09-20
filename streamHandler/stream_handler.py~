from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

import os
import math

import random

import rpyc


now_year = 2014
now_month = 10
now_day = 6
minute_lower_band = 830
minute_upper_band = 2000

class MyService(rpyc.Service):
    def on_connect(self):
        # code that runs when a connection is created
        # (to init the serivce, if needed)
        pass

    def on_disconnect(self):
        # code that runs when the connection has already closed
        # (to finalize the service, if needed)
        pass

    #request for data that come from entrance mechines
    def exposed_get_ent_data(self, station_a): # this is an exposed method
    
        #from 2014 10 6 Addison Road
        real_time_data = [(830, 2856), (845, 2405), (900, 2183), (915, 1570), (930, 1583), (945, 1168), (1000, 930), (1015, 947), (1030, 761), (1045, 729), (1100, 881), (1115, 690), (1130, 566), (1145, 723), (1200, 845), (1215, 619), (1230, 593), (1245, 727), (1300, 918), (1315, 518), (1330, 659), (1345, 613), (1400, 843), (1415, 510), (1430, 701), (1445, 616), (1500, 680), (1515, 451), (1530, 704), (1545, 624), (1600, 618), (1615, 475), (1630, 577), (1645, 708), (1700, 468), (1715, 478), (1730, 462), (1745, 426), (1800, 380), (1815, 396), (1830, 371), (1845, 417), (1900, 330), (1915, 376), (1930, 346), (1945, 305), (2000, 273)]
        
        size = len(real_time_data)
        
        #new[i+offset][1] = old[i][1]
        #new[i][1] = old[i-offset][1]
        offset = -1
        
        a = offset
        b = size+offset
        
        if a < 0:
            a = 0
        
        if b > size:
            b = size
        
        output_real_time_data = [list(i) for i in real_time_data]
        
        for idx, val in enumerate(output_real_time_data):
            if idx >=a and idx < b:
                old_idx = idx-offset
                val[1] = real_time_data[old_idx][1]*1#random.uniform(0.98, 1.02)

        output_real_time_data = output_real_time_data[a:b]


        return output_real_time_data
        
        
    #request for data that come from a camera    
    def exposed_get_C1_at_time(self, enter_time, enter_station, direction):
        return None


    def get_question(self):  # while this method is not exposed
        return "what is the airspeed velocity of an unladen swallow?"
   
        
def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0

    new_count =  int(newValues[0][1])

    return runningCount+new_count
    
def pre_process_1(item):
    items = item.split(',')
    return ((items[0]),(items[1], items[2]))
    
def threaded_function(arg):
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MyService, port = 18862)
    t.start()


checkpointDirectory = r'/home/sklaw/Desktop/experiment/spark/ex/3/v1/data/streaming_data/checkpoint_data'

def functionToCreateContext():
    sc = SparkContext("local[*]", "streaming_part")
    sc.setLogLevel("WARN")
    
    ssc = StreamingContext(sc, 10)
    
    lines = ssc.socketTextStream("localhost", 9999)
    
    #meat
    lines.map(pre_process_1).updateStateByKey(updateFunction).pprint()
    
    ssc.checkpoint(checkpointDirectory)   # set checkpoint directory
    return ssc

# Get StreamingContext from checkpoint data or create a new one

if __name__ == "__main__":
    from threading import Thread
    
    thread = Thread(target = threaded_function, args=(None,))
    thread.start()


 
    conf = SparkConf().setMaster("localhost")
    ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()

    

