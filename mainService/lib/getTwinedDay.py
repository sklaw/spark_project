import os

#return file paths for the files that store daya per day

import rpyc
stream_handler = rpyc.connect("localhost", 18862)

import datetime

import getDistance

import time
import threading


from pymongo import MongoClient
dbtalk = MongoClient("localhost:27017")
station_names = dbtalk.IBM_contest.station_names
stations = station_names.find()[0]['data']

package_directory = os.path.dirname(os.path.abspath(__file__))
data_dir_path = package_directory+'/../../../data/'

#today = datetime.datetime.now()
today = datetime.datetime(2014, 10, 15)


def get_possible_days(year, month, day, station):
    base_path = data_dir_path+r"v0/"
    import datetime
    wday = datetime.date(year, month, day).weekday()+1
    files = os.listdir(base_path+station+'/'+str(wday)) 
    result = []  
    for i in files:
        result.append(base_path+station+'/'+str(wday)+'/'+i)
    return result

def is_today(time):
    #judge if the time is today, which may find data in streaming part
    year = time.year
    month = time.month
    day = time.day

    

    if year == today.year and month == today.month and day == today.day:
        return True
    else:
        return False
    
today_twined_day_info = {}

def twined_day_auto_run(sc):
    global today_twined_day_info

    tmp_today_twined_day_info = {}

    year = today.year
    month = today.month
    day =today.day

    for station in stations:
        twined_day = None
        twined_offset = None
        min_distance = None


        tmp_today_twined_day_info[station] = {}

        possible_days = get_possible_days(year, month, day, station)

        real_time_data = stream_handler.root.get_ticket_mechine_data(station)
	print real_time_data


        if real_time_data != None:
            real_time_data = list(real_time_data)
            date = real_time_data.pop(0)
            for i in possible_days:
                offset, distance =  getDistance.cmp_two_days(i, real_time_data, sc)
        
                print i, offset, distance
        
                if offset == None:
                    continue
        
                if min_distance == None or distance < min_distance:
                    twined_day = i
                    min_distance = distance
                    twined_offset = offset

        tmp_today_twined_day_info[station]['twined_day'] = twined_day
        tmp_today_twined_day_info[station]['min_distance'] = min_distance
        tmp_today_twined_day_info[station]['twined_offset'] = twined_offset

    today_twined_day_info = tmp_today_twined_day_info

    print 'finished one run'
    threading.Timer(100, twined_day_auto_run, [sc]).start()

def get_twined_day(enter_time, station, sc):
    year = enter_time.year
    month = enter_time.month
    day = enter_time.day
    
    possible_days = get_possible_days(year, month, day, station)
    
    twined_day = None
    twined_offset = None
    min_distance = None

    
    if is_today(enter_time):
        if station in today_twined_day_info.keys():
            twined_day = today_twined_day_info[station]['twined_day']
            min_distance = today_twined_day_info[station]['min_distance']
            twined_offset = today_twined_day_info[station]['twined_offset']
                
    
    
    return [[twined_day, twined_offset, min_distance] , possible_days]

    
    
