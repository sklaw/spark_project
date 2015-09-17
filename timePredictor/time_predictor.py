from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

import os
import math


import lib.getTwinedDay

import rpyc
stream_handler = rpyc.connect("localhost", 18862)

import datetime

import numpy

import math

from pymongo import MongoClient
dbtalk = MongoClient("localhost:27017")
routes_similar_to_google_map = dbtalk.IBM_contest.routes_similar_to_google_map
entrance_delay_function_parameter = dbtalk.IBM_contest.entrance_delay_function_parameter
station_destinations_by_directions = dbtalk.IBM_contest.station_destinations_by_directions

conf = SparkConf().setMaster("localhost")
sc = SparkContext("local[*]", "time_predictor")
sc.setLogLevel("WARN")

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


def pre_process_1(line):
    elem_list = line.split(",")
    return (elem_list[0], int(elem_list[1]), int(elem_list[2]))
    

def get_C1_at_time(enter_time, enter_station, direction):
    print ' '

    from_streaming_part = stream_handler.root.get_C1_at_time(enter_time, enter_station, direction)
    if from_streaming_part != None:
        return from_streaming_part

    year = enter_time.year
    month = enter_time.month
    day = enter_time.day
    
    twined_day_result =  lib.getTwinedDay.get_twined_day(year, month, day, enter_station, sc)

    if twined_day_result[0][0] != None:
        print 'the best fitted day is determined'
        twined_day_path = twined_day_result[0][0]
        twined_day_offset = twined_day_result[0][1]
    else:
        print 'the best fitted day will be the most recent day'
        twined_day_result[1] = sorted(twined_day_result[1], key = lambda x: datetime.datetime(*[int(i) for i in x.split('/')[-1].split('-')]))
        
        twined_day_path = twined_day_result[1][-1]
        twined_day_offset = 0
        
    print 'twined_day_path:', twined_day_path
    print 'twined_day_offset:', twined_day_offset

    print 'enter_time:', enter_time

    enter_time = enter_time-twined_day_offset*datetime.timedelta(minutes=15)
    
    print 'enter_time with offset:', enter_time
    
    destinations_by_directions = station_destinations_by_directions.find_one({"station_name":enter_station})['destinations_by_directions']
    
    for i in destinations_by_directions:
        if i[0] == direction:
            destinations = i[1]

    

    file_data = sc.textFile(twined_day_path)

    int_time = enter_time.hour*100+enter_time.minute

     
    C1s = file_data.map(pre_process_1).filter(lambda x: x[1] == int_time and x[0] in destinations).map(lambda x: x[2]).collect()
    
    print C1s
    if C1s == []:
        
        return None
    C1 = reduce(lambda a,b: a+b, C1s)

    print 'C1:',C1
    return C1

def get_entrance_delay(enter_time, enter_station, direction):
    print ' '
    print 'enter_time before 15min ajustment:', enter_time
    enter_time = enter_time.replace(minute = 15*int(enter_time.minute/15))
    print 'enter_time after 15min ajustment:', enter_time

    int_time = enter_time.hour*100+enter_time.minute
    day_type = enter_time.weekday()+1
    parameters = entrance_delay_function_parameter.find_one({"station_name":enter_station, "time":int_time, "direction":direction, "day_type":day_type})
    if parameters is None:
        print 'no parameters for', enter_station,'to', direction,'at',int_time,'day_type',day_type
        return (8, False)
        
    parameters = parameters['parameters']
        
    print 'parameters:' ,parameters
    
    C1 = get_C1_at_time(enter_time, enter_station, direction)
    
    if C1 == None:
        print 'C1 failed to get'
        return (8, False)
    else:
        delay = int(math.floor(0.5+parameters[0]+parameters[1]*(numpy.log(C1+1))))
    
        #delay = round(parameters[0]+parameters[1]*(numpy.log(C1+1)), 2)
    
    
    return (delay, True)
    
def clean_interchange_time(route):
    to_subtract = 0
    for i in route['route_by_line']:
        i['subroute'] = [[j[0], j[1]-to_subtract] for j in i['subroute']]
        to_subtract += 8
    return route
    
def get_estimated_time(year, month, day, hour, minute, station_a, station_b):
    
    route = routes_similar_to_google_map.find_one({'enter_station':station_a, 'exit_station':station_b})

    route = clean_interchange_time(route)

    
    current_time = datetime.datetime(year,month,day,hour,minute)

    for idx, val in enumerate(route['route_by_line']):
        enter_time = current_time+datetime.timedelta(minutes=val['subroute'][0][1])

        enter_station = val['subroute'][0][0]
        
        direction = val['subroute'][1][0]
        
        enter_delay_tuple = get_entrance_delay(enter_time, enter_station, direction)

        print 'enter_delay_tuple:',enter_delay_tuple



        enter_delay = enter_delay_tuple[0]
        enter_delay_accurate = enter_delay_tuple[1]

    
        for i in range(idx, len(route['route_by_line'])):
            for j in route['route_by_line'][i]['subroute']:
                j[1] += enter_delay
                
    print '-'*10
    for j in route['route_by_line']:
        print '*'*10
        print j['line'],j['subroute']
    
    return route['route_by_line'];    

def threaded_function(arg):
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MyService, port = 18861)
    t.start()


if __name__ == "__main__":
    from threading import Thread
    
    thread = Thread(target = threaded_function, args=(None,))
    thread.start()
    #for i in range(0,24):
    #    print i
    #    print get_estimated_time(2014, 10, 6, i, 0, "Addison Road", "Huntington")
    












