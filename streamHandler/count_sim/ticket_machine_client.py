import os,sys,inspect
package_directory = os.path.dirname(os.path.abspath(__file__))
lib_dir = package_directory+'/../../mainService/lib'


sys.path.insert(0,lib_dir)
import getDistance

import socket
import random
import time

import os

data_dir_path = package_directory+'/../../../data/'

from pymongo import MongoClient
dbtalk = MongoClient("localhost:27017")
data_used_by_ticket_mechine_gen = dbtalk.IBM_contest.data_used_by_ticket_mechine_gen

import datetime


stations = ['White Flint', 'Capitol South', 'Morgan Blvd.', 'Arlington Cemetery', 'Waterfront', 'Vienna', 'Franconia-Springfield', 'Crystal City', "Prince George's Plaza", 'Forest Glen', 'Branch Avenue', 'Grosvenor', 'West Hyattsville', 'Minnesota Avenue', 'Dunn Loring', 'McPherson Square', 'Glenmont', 'Metro Center', 'Shaw-Howard University', 'Benning Road', 'Reagan Washington National Airport', 'Takoma', 'New Carrollton', 'Tenleytown-AU', 'Foggy Bottom', 'Huntington', 'Cleveland Park', 'U Street-Cardozo', 'Naylor Road', 'Brookland', 'Mt. Vernon Square-UDC', 'Farragut North', 'Georgia Avenue-Petworth', 'Gallery Place-Chinatown', 'Stadium-Armory', 'Wiehle', 'Potomac Avenue', 'Braddock Road', 'East Falls Church', 'Wheaton', 'Spring Hill', 'Archives-Navy Memorial', 'Court House', 'Virginia Square-GMU', 'King Street', 'Congress Heights', 'Pentagon City', 'Landover', 'Cheverly', 'Woodley Park-Zoo', 'Farragut West', 'Bethesda', 'Columbia Heights', 'Greenbelt', 'Van Ness-UDC', 'Addison Road', 'Silver Spring', 'Navy Yard', 'Largo Town Center', 'Dupont Circle', 'McLean', 'Tysons Corner', 'Friendship Heights', 'Twinbrook', 'Smithsonian', 'Medical Center', 'Ballston', 'Suitland', 'Rhode Island Avenue', 'Van Dorn Street', 'Federal Center SW', 'Eastern Market', "L'Enfant Plaza", 'Deanwood', 'Rockville', 'College Park-U of MD', 'Capitol Heights', 'Greensboro', 'Union Station', 'Shady Grove', 'Judiciary Square', 'Federal Triangle', 'New York Ave', 'Clarendon', 'Anacostia', 'Southern Avenue', 'West Falls Church', 'Pentagon', 'Rosslyn', 'Fort Totten', 'Eisenhower Avenue']

def save_data_to_db():
    from pyspark import SparkContext, SparkConf
    from pyspark.streaming import StreamingContext

    conf = SparkConf().setMaster("localhost")
    sc = SparkContext("local[*]", "tikcket_mechine_gen")
    sc.setLogLevel("WARN")
    sc.addFile(lib_dir+'/getDistance.py')

    data_used_by_ticket_mechine_gen.drop()
    path = '/3/2014-10-15'
    for s in stations:
        full_path = data_dir_path+'v0/'+s+path
        print full_path
        data_to_save = getDistance.get_one_day_group_by_time(full_path, sc)
        for item in data_to_save:
            data_used_by_ticket_mechine_gen.insert({'station_name':s, 'time':item[0], 'data':item[1]})




if __name__ == "__main__":
    
    
    host = "localhost"
    port = 19999
    

    
    
    count_dict = {}
    
    now_time = datetime.datetime(2015,9,16,23,0)
    
    while True:
        for i in stations:
            int_time = now_time.hour*100+now_time.minute
        
            if i not in count_dict.keys():
                count_dict[i] = 0
            
            int_time_to_query = now_time.hour*100+15*int(now_time.minute/15)    
            
            acc =  data_used_by_ticket_mechine_gen.find_one({'station_name':i, 'time':int_time_to_query})
        
            if acc == None:
                acc = 0
            else:
                acc = float(acc['data'])/15
        
            count_dict[i] += acc

            
            soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            soc.connect((host, port))
            soc.send(i+','+str(int_time)+','+str(count_dict[i])+','+str(now_time.date())+'\n')
            #print i+','+str(int_time)+','+str(count_dict[i])+'\n'
            soc.close()
        
        print now_time
        
        print int_time_to_query
        
        now_time += datetime.timedelta(minutes=1)
        
        time.sleep(1)
        

        
        
 
   
