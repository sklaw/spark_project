import os

#return file paths for the files that store daya per day

import rpyc
stream_handler = rpyc.connect("localhost", 18862)

import datetime

import getDistance 

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
    
    
def get_twined_day(year, month, day, station, sc):
    possible_days = get_possible_days(year, month, day, station)
    
    now = datetime.datetime.now()
    
    twined_day = None
    twined_offset = None
    min_distance = None
    
    
    to_print = []
    
    #if year == now.year and month == now.month and day == now.day:
    if False:
        real_time_data = stream_handler.root.get_ent_data(station)
	#print real_time_data
        for i in possible_days:
            offset, distance =  getDistance.cmp_two_days(i, real_time_data, sc)
            
            print i, offset, distance
            
            if offset == None:
            	continue
            
            if min_distance == None or distance < min_distance:
                twined_day = i
                min_distance = distance
                twined_offset = offset

    
    
    return [[twined_day, twined_offset, min_distance] , possible_days]

    
    
