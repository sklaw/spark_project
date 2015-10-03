import rpyc
stream_handler = rpyc.connect("localhost", 18862)

import os
import math


import lib.getTwinedDay

import datetime

import numpy

import math

import time

from pymongo import MongoClient
dbtalk = MongoClient("localhost:27017")
routes_similar_to_google_map = dbtalk.IBM_contest.routes_similar_to_google_map
entrance_delay_function_parameter = dbtalk.IBM_contest.entrance_delay_function_parameter
station_destinations_by_directions = dbtalk.IBM_contest.station_destinations_by_directions

sc = None


def pre_process_1(line):
    elem_list = line.split(",")
    return (elem_list[0], int(elem_list[1]), int(elem_list[2]))
    

def get_C1_at_time(enter_time, enter_station, direction):
    if lib.getTwinedDay.is_today(enter_time):
        from_streaming_part = stream_handler.root.get_camera_mechine_data(enter_station, direction)
        if from_streaming_part != None:
            from_streaming_part = list(from_streaming_part)
            print 'C1 from_streaming_part', from_streaming_part
            date = from_streaming_part.pop(0)
            most_recent_C1_record = from_streaming_part[-1]
            most_recent_C1_time = datetime.datetime(year=2000, month=1, day = 1, hour=most_recent_C1_record[0]/100, minute=most_recent_C1_record[0]%100)
            
            adjusted_enter_time = datetime.datetime(year=2000, month=1, day = 1, hour=enter_time.hour, minute=enter_time.minute)

            

            print 'most_recent_C1_time', most_recent_C1_time
            print 'adjusted_enter_time', adjusted_enter_time

            if adjusted_enter_time >= most_recent_C1_time and adjusted_enter_time <= (most_recent_C1_time+datetime.timedelta(minutes=15)):
                print 'C1 is from streaming part'
                C1 = most_recent_C1_record[1]
                print 'C1', C1
                return C1
    print 'C1 is based on prediction'

    year = enter_time.year
    month = enter_time.month
    day = enter_time.day
    
    twined_day_result =  lib.getTwinedDay.get_twined_day(enter_time, enter_station, sc)

    print 'twined_day_result', twined_day_result
    
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
    
    print 'C1s', C1s
    if C1s == []:
        return None
    C1 = reduce(lambda a,b: a+b, C1s)

    print 'C1:',C1
    return C1

def get_entrance_delay(enter_time, enter_station, direction):
    enter_time = enter_time.replace(minute = 15*int(enter_time.minute/15))


    int_time = enter_time.hour*100+enter_time.minute
    day_type = enter_time.weekday()+1
    if day_type <= 5:
        day_type = 1
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
    
def get_route_with_time(year, month, day, hour, minute, station_a, station_b, _sc):
    global sc
    sc = _sc
    
    route = routes_similar_to_google_map.find_one({'enter_station':station_a, 'exit_station':station_b})

    if route is None:
        return None

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

