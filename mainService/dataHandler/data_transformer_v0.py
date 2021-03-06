from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os

import random

package_directory = os.path.dirname(os.path.abspath(__file__))
data_dir_path = package_directory+'/../../../data/'

def map_yearmonth(raw):
    year = int(raw[:4])
    month = int(raw[4:])
    return [year,month]
    
def map_serveice_type(raw):
    if raw == "Weekday":
        return 1
    elif raw == "Saturday":
        return 6
    elif raw == "Sunday":
        return 7

def map_quarter_interval(raw):
    sentence_list = raw.split(' ')
    time = int(sentence_list[0].replace(":", ""))
    if sentence_list[1] == "PM":
        if time/100 != 12:
            time += 1200
    
    if time/100 == 12 and sentence_list[1] == "AM":
        time -= 1200;
    
    return time
    

def pre_process(line):
    global mapping
    elem_list = line.split(",")
    elem_list = [i.strip('"') for i in elem_list]
    
    
    
    year, month = map_yearmonth(elem_list[mapping["Year Month"]])
    
    day = 1
    wday = map_serveice_type( elem_list[mapping["Ent Date Service Type"]] )
    if wday == 1:
        day = 3
    elif wday == 6:
        day = 4
    elif wday == 7:
        day = 5
    
    #0 -- Ent Station
    #1 -- year
    #2 -- month
    #3 -- day
    #4 -- serveice type
    key = [elem_list[mapping["Ent Station"]],\
           year,\
           month,\
           day,\
           wday\
           ]
    
    #0 -- Ext Station 
    #1 -- Quarter Hour Interval
    #2 -- Number Rider SUM
    #3 -- AvgTravelMinutes
 
    data = [elem_list[mapping["Ext Station"]],\
            map_quarter_interval(elem_list[mapping["Ent Quarter Hour Interval"]]),\
            int(elem_list[mapping["Number Rider SUM"]]),\
            float(elem_list[mapping["AvgTravelMinutes"]])\
            ]
        
    
    return (tuple(key), tuple(data))


#elem = ( (key) (data_list) )
def output_by_station(elem):
    key = elem[0]
    data_list = elem[1]
    
    data_list = sorted(data_list, key=lambda x: x[1])
    
    
    year = key[1]
    month = key[2]
    
    if key[4] == 1:
        for i in range(5):
            path = data_dir_path+"v0/"+key[0]+'/'+str(i+1);
            if not os.path.exists(path):
                os.makedirs(path)
        
            
            
            begin_day = [6, 7, 1, 2, 3]
            
            day = begin_day[i]
            fake_data(year, month, day, data_list, path)
            
                    
    elif key[4] == 6:
        path = data_dir_path+"v0/"+key[0]+'/6';
        if not os.path.exists(path):
            os.makedirs(path)
        day = 4
        fake_data(year, month, day, data_list, path)
    
    elif key[4] == 7:
        path = data_dir_path+"v0/"+key[0]+'/7';
        if not os.path.exists(path):
            os.makedirs(path)
        day = 5
        fake_data(year, month, day, data_list, path)
        
def fake_data(year, month, day, data_list, path):
    while day <= 31:
        filename = str(year)+'-'+str(month)+'-'+str(day)
                
        day += 7
                
        f = open(path+'/'+filename, 'a')
    
        offset = random.randint(-2, 2)*15;
    
        #k is a tuple
        for k in data_list:
            vibration_1 = random.uniform(0.8, 1.2)
            vibration_2 = vibration_1*random.uniform(0.9, 1.1)
            
            hour = k[1]/100
            minute = k[1]%100+offset
            
            if minute >= 60:
                hour += 1
                minute -= 60
                if hour >= 24:
                    hour -= 24
            
            if minute < 0:
                hour -= 1
                minute += 60
                if hour < 0:
                    hour += 24
            
            
            
           
            
            time = hour*100+minute
            
            tmp = [k[0], time, int(k[2]*vibration_1+1), k[3]*vibration_2]
                    
            s = ','.join([str(l) for l in tmp])
            f.write(s+'\n')
    
        f.close()

conf = SparkConf().setMaster("localhost")

sc = SparkContext("local[*]", "data_transformer")

file_data = sc.textFile(data_dir_path+"2014 October OD Quarter Hour.csv")

header = file_data.first()

data = file_data.filter(lambda x: x!=header)

mapping = dict()
mapping["Number Rider SUM"] = 0
mapping["Ent Date Service Type"] = 1
mapping["Ent Time Period"] = 2
mapping["Ent Quarter Hour Interval"] = 3
mapping["Ent Station"] = 4
mapping["Ext Station"] = 5
mapping["AvgRidership"] = 6
mapping["Year Month"] = 7
mapping["Travel Minutes SUM"] = 8
mapping["AvgTravelMinutes"] = 9
mapping["Ent Date Holiday"] = 10



new_data = data.map(pre_process).groupByKey().foreach(output_by_station)


