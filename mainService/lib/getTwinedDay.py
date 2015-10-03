import os

#return file paths for the files that store daya per day

import rpyc
stream_handler = rpyc.connect("localhost", 18862)

import datetime

import getDistance

import time

package_directory = os.path.dirname(os.path.abspath(__file__))
data_dir_path = package_directory+'/../../../data/'

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

    today = datetime.datetime.now()

    if year == today.year and month == today.month and day == today.day:
        return True
    else:
        return False
    
today_twined_day_info = {}
update_lapse = 15*60

def get_twined_day(enter_time, station, sc):
    year = enter_time.year
    month = enter_time.month
    day = enter_time.day
    
    possible_days = get_possible_days(year, month, day, station)
    
    now = datetime.datetime.now()
    
    twined_day = None
    twined_offset = None
    min_distance = None
    
    
    to_print = []
    
    if is_today(enter_time):
        if today_twined_day_info != {} and time.time() <= today_twined_day_info['update_time']+update_lapse:
            twined_day = today_twined_day_info['twined_day']
            min_distance = today_twined_day_info['min_distance']
            twined_offset = today_twined_day_info['twined_offset']
        else:    
            real_time_data = stream_handler.root.get_ticket_mechine_data(station)
            if real_time_data != None:
                real_time_data = list(real_time_data)
                date = real_time_data.pop(0)
                print 'ticket mechine data from streaming part', real_time_data
                for i in possible_days:
                    offset, distance =  getDistance.cmp_two_days(i, real_time_data, sc)
            
                    print i, offset, distance
            
                    if offset == None:
                        continue
            
                    if min_distance == None or distance < min_distance:
                        twined_day = i
                        min_distance = distance
                        twined_offset = offset
                today_twined_day_info['update_time'] = time.time()
                today_twined_day_info['twined_day'] = twined_day
                today_twined_day_info['min_distance'] = min_distance
                today_twined_day_info['twined_offset'] = twined_offset
    
    
    return [[twined_day, twined_offset, min_distance] , possible_days]

    
    
