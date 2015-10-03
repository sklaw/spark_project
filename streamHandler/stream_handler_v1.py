from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

import os
import math

import random

import rpyc

import datetime

checkpointDirectory = r'/home/sklaw/Desktop/experiment/spark/ex/3/v1/data/streaming_data/checkpoint_data'

ticket_mechine_data_dict = {}
camera_mechine_data_dict = {}

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
    def exposed_get_ticket_mechine_data(self, station_name):
        if station_name not in ticket_mechine_data_dict.keys():
            return None
        
        return_l = list(ticket_mechine_data_dict[station_name].iteritems())
        return_l = sorted(return_l, key=lambda x: x[0])
        
        return return_l
        
        
    #request for data that come from a camera    
    def exposed_get_camera_mechine_data(self, station_name, direction):
        if station_name not in camera_mechine_data_dict.keys():
            return None
        
        if direction not in camera_mechine_data_dict[station_name].keys():
            return None
        
        return_l = list(camera_mechine_data_dict[station_name][direction].iteritems())
        return_l = sorted(return_l, key=lambda x: x[0])
        
        return return_l

    def exposed_get_ticket_mechine_data_dict(self):
        return ticket_mechine_data_dict


    def get_question(self):  # while this method is not exposed
        return "what is the airspeed velocity of an unladen swallow?"
   













def camera_mechine_RDD_handler(rdd):
    global camera_mechine_data_dict
    
    rdd_list = rdd.collect()

    for i in rdd_list:
        station_name = i[0][0]
        direction = i[0][1]
        
        data = i[1]
        while len(data) != 0 and data[-1][2] != -1:
            data.pop()
    
    for i in rdd_list:
        station_name = i[0][0]
        direction = i[0][1]
        
        data_list = i[1]
        
        if len(data_list)  == 0:
            continue
        
        if station_name not in camera_mechine_data_dict.keys():
            camera_mechine_data_dict[station_name] = {}
        
        if direction not in camera_mechine_data_dict[station_name].keys():
            camera_mechine_data_dict[station_name][direction] = {-1:data_list[0][3]}
            
        for j in data_list:
            time = j[0]
            count = j[1]
            date = j[3]
            if j[3] == camera_mechine_data_dict[station_name][direction][-1]:
                camera_mechine_data_dict[station_name][direction][time] = count
            elif j[3] > camera_mechine_data_dict[station_name][direction][-1]:
                camera_mechine_data_dict[station_name][direction] = {-1:j[3]}
                camera_mechine_data_dict[station_name][direction][time] = count

    print '-'*20
    print sorted(list(camera_mechine_data_dict.iteritems()), key=lambda x:x[0])[0]






def ticket_mechine_RDD_handler(rdd):
    global ticket_mechine_data_dict
    
    rdd_list = rdd.collect()

    for i in rdd_list:
        station_name = i[0]
        data = i[1]
        while len(data) != 0 and data[-1][2] != -1:
            data.pop()

    
    for i in rdd_list:
        station_name = i[0]
        data_list = i[1]
        
        if len(data_list)  == 0:
            continue
        
        
        
        if station_name not in ticket_mechine_data_dict.keys():
            ticket_mechine_data_dict[station_name] = {-1:data_list[0][3]}
    
        for j in data_list:
            time = j[0]
            count = j[1]
            date = j[3]
            if j[3] == ticket_mechine_data_dict[station_name][-1]:
                ticket_mechine_data_dict[station_name][time] = count
            elif j[3] > ticket_mechine_data_dict[station_name][-1]:
                ticket_mechine_data_dict[station_name] = {-1:j[3]}
                ticket_mechine_data_dict[station_name][time] = count

    
    print '+'*20
    to_print = sorted(list(ticket_mechine_data_dict.iteritems()), key=lambda x:x[0])[0]
    print sorted(list(to_print[1].iteritems()), key=lambda x:x[0])
    
def updateFunction(newValues, oldValues):
    if oldValues is None:
       oldValues = []
    
    data = [i for i in oldValues if i[2] != -1]
    oldValues = data
    
    for i in newValues:
    
        time = int(i[0])
        new_count =  float(i[1])
        date_items = i[2].split('-')
        date = datetime.datetime(int(date_items[0]), int(date_items[1]), int(date_items[2]))
        
        if len(oldValues) != 0:
            recent_period_begin = oldValues[-1][0]
        else:
            recent_period_begin = -15
        
        period_end = recent_period_begin+15
        

        if time >= period_end or time == 0:
            time = (time/100)*100+((time%100)/15)*15
            tmp_l = [time, 0, 0, date]
            if len(oldValues) != 0:
                tmp_l[1] = oldValues[-1][2]
                tmp_l[2] = new_count
                count = int(oldValues[-1][2]-oldValues[-1][1])
                oldValues[-1][1] = count
                oldValues[-1][2] = -1
            else:
                tmp_l[1] = 0
                tmp_l[2] = new_count
                
            oldValues.append(tmp_l)
        else:
            oldValues[-1][2] = new_count
    
    return oldValues
            

def ticket_mechine_pre_process(item):
    items = item.split(',')
    return ((items[0]),(items[1], items[2], items[3]))
    
def camera_mechine_pre_process(item):
    items = item.split(',')
    return ((items[0], items[1]),(items[2], items[3], items[4]))

# Get StreamingContext from checkpoint data or create a new one
def functionToCreateContext():
    sc = SparkContext("local[*]", "streaming_part")
    sc.setLogLevel("ERROR")
    
    ssc = StreamingContext(sc, 5)
    
    data_from_ticket_mechine = ssc.socketTextStream("localhost", 9999)
    data_from_camera_mechine = ssc.socketTextStream("localhost", 9998)
    
    
    #meat
    data_from_ticket_mechine.map(ticket_mechine_pre_process).updateStateByKey(updateFunction).foreachRDD(ticket_mechine_RDD_handler)
    data_from_camera_mechine.map(camera_mechine_pre_process).updateStateByKey(updateFunction).foreachRDD(camera_mechine_RDD_handler)
    
    ssc.checkpoint(checkpointDirectory)   # set checkpoint directory
    return ssc


    
def threaded_function(arg):
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MyService, port = 18862, protocol_config = {"allow_all_attrs" : True})
    t.start()

if __name__ == "__main__":
    from threading import Thread
    
    thread = Thread(target = threaded_function, args=(None,))
    thread.start()

    conf = SparkConf().setMaster("localhost")
    ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()


    

