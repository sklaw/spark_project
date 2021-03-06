import math

from pymongo import MongoClient
dbtalk = MongoClient("localhost:27017")

one_day_group_by_time = dbtalk.IBM_contest.one_day_group_by_time

import datetime

def pre_process_2(line):
    elem_list = line.split(",")
    
    #assume list[1] is time interval, list[2] is rider sum 
    return ((int(elem_list[1])), (int(elem_list[2])))

def get_one_day_group_by_time(path, sc):
    data = one_day_group_by_time.find_one({'path':path})
    if data != None:
        return data['data']



    file_data = sc.textFile(path)
    data = file_data.map(pre_process_2).reduceByKey(lambda a,b: a+b).collect()
    data = sorted(data, key=lambda x: x[0])
    
    one_day_group_by_time.update({'path': path},{'$set': {'data': data}}, upsert=True)
    
    return data

def get_avg_distance(day1, x1, day2, x2, length):
    result = 0
    i = 0
    
    
    while (i < length):
        result += math.fabs(day1[x1][1] - day2[x2][1])
        x1 += 1
        x2 += 1
        i += 1
    return float(result)/length


#get the offset (which makes these two days fit most) and the distance between those two curves under that offset

#day1+offset = day2

def try_match(day1, day2):
    to_return = None
    min_distance = None

    day1 = [[datetime.datetime(2000, 1, 1, i[0]/100, i[0]%100), i[1]] for i in day1]
    day2 = [[datetime.datetime(2000, 1, 1, i[0]/100, i[0]%100), i[1]] for i in day2]

    day1 = dict(day1)
    day2 = dict(day2)



    #may change over different range of offsets in test
    offset_list = [-4, -3, -2, -1, 0, 1, 2, 3, 4]

    for i in offset_list:
        length = 0
        distance = 0
        for j in day2.keys():
            day2_time = j
            
            day1_time = day2_time-i*datetime.timedelta(minutes=15)
            
            if day1_time in day1.keys():
                length += 1
                distance += math.fabs(day1[day1_time] - day2[day2_time])
            
        
        if length != 0:
            distance = float(distance)/length
        tmp = distance
        if min_distance == None:
            min_distance = tmp
            to_return = i
        elif tmp < min_distance:
            min_distance = tmp
            to_return = i

    return (to_return, min_distance)
   
#API to time predictor
def cmp_two_days(path_to_day1, cooked_day2, _sc):
    global sc
    sc = _sc
    cooked_day1 = get_one_day_group_by_time(path_to_day1, sc)
    return try_match(cooked_day1, cooked_day2)
    

