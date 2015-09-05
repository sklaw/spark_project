import os

#return file paths for the files that store daya per day

import rpyc
stream_handler = rpyc.connect("localhost", 18862)

import datetime

import getDistance 



def get_possible_days(year, month, day, station):
    base_path = r"/home/sklaw/Desktop/experiment/spark/ex/3/v1/timePredictor/data/v0/"
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
    
    to_print = []
    
    #if year == now.year and month == now.month and day == now.day:
    if True:
        real_time_data = stream_handler.root.get_ent_data(station)

        

        for i in possible_days:
            distance =  getDistance.cmp_two_days(real_time_data , i, sc)
            to_print.append((i.split('/')[-1], distance));

    
    return to_print
    
    #return possible_days[0]