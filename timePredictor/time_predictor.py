from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

import os
import math


import lib.getTwinedDay

import rpyc
class MyService(rpyc.Service):
    def on_connect(self):
        # code that runs when a connection is created
        # (to init the serivce, if needed)
        pass

    def on_disconnect(self):
        # code that runs when the connection has already closed
        # (to finalize the service, if needed)
        pass

    def exposed_do_prediction(self, year, month, day, hour, minute, station_a, station_b): # this is an exposed method
        return get_estimated_time(year, month, day, hour, minute, station_a, station_b)

    def get_question(self):  # while this method is not exposed
        return "what is the airspeed velocity of an unladen swallow?"



def get_estimated_time(year, month, day, hour, minute, station_a, station_b):
    global sc
    print lib.getTwinedDay.get_twined_day(year, month, day, station_a, sc)
        
     
    #file_data = sc.textFile(r"/home/sklaw/Desktop/experiment/spark/ex/3/v0/data/metro data from LK.csv").map(pre_process)
    
    #func_filter = filter_generator(year, month, day, hour, minute, station_a, station_b)
    #result = file_data.filter(func_filter).map(lambda w: w[1][2]).reduce(lambda a, b: (a + b) / 2)
    #if result == None:
    #    result = -1
    
    return 10;    


dbtalk = None
sc = None

def threaded_function(arg):
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MyService, port = 18861)
    t.start()


if __name__ == "__main__":
    global dbtalk
    
    from threading import Thread
    
    thread = Thread(target = threaded_function, args=(None,))
    thread.start()
    
    
    

    
    

    conf = SparkConf().setMaster("localhost")

    sc = SparkContext("local[*]", "streaming_part")
    
    
    from pymongo import MongoClient
    dbtalk = MongoClient("localhost:27017")
    

    









#######fuck themselves
def pre_process(line):
    raw = line.split(",")
    cnt = 0
    key = []
    data = []
    for e in raw:
        cnt += 1
        if cnt == 1:
            #record_num
            data.append(int(e))
        elif cnt == 2:
            #is_holiday
            key.append(e.strip('"'))
        elif cnt == 3:
            #date
            key.append(e.strip('"'))
        elif cnt == 4:
            #period
            key.append(e.strip('"'))
        elif cnt == 5:
            #ent
            key.append(e.strip('"'))
        elif cnt == 6:
            #ext
            key.append(e.strip('"'))
        elif cnt == 7:
            #avg_ridership
            #data.append(float(e),)
            pass
        elif cnt == 8:
            #year_month
            key.append(int(e))
        elif cnt == 9:
            #travel_minutes
            data.append(int(e))
        elif cnt == 10:
            #avg_minutes
            data.append(float(e))
    return (tuple(key), tuple(data))
   
   

   
def filter_generator(year, month, day, hour, minute, station_a, station_b):
    def func_filter(data):
        if data[0][3] == station_a and data[0][4] == station_b:
            return True
        else:
            return False
    return func_filter


