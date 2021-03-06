import math

from pymongo import MongoClient
dbtalk = MongoClient("localhost:27017")

one_day_group_by_time = dbtalk.IBM_contest.one_day_group_by_time

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
def try_match(day1, day2):
    to_return = None
    min_distance = None


    x1 = 0
    x2 = [idx for idx, val in enumerate(day2) if val[0]==day1[x1][0]]
    if x2 == []:
        x2 = 0
        x1 = [idx for idx, val in enumerate(day1) if val[0]==day2[x2][0]]
        if x1 == []:
            return (to_return, min_distance)
        else:
            x1 = x1[0]
    else:
        x2 = x2[0]
    
    print 'day1', day1
    print 'day2', day2
    print 'x1',x1, 'x2', x2


    #may change over different range of offsets in test
    offset_list = [-4, -3, -2, -1, 0, 1, 2, 3, 4]


    

    for i in offset_list:
        now_x1 = x1
        now_x2 = x2+i
        
        if now_x2 < 0 or now_x2 >= len(day2):
            now_x1 = x1-i
            now_x2 = x2
        
        
    
        l1 = len(day1) - now_x1
        l2 = len(day2) - now_x2
    
        if l1 < l2:
            length = l1
        else:
            length = l2
 
 
        if length == 0:
            continue
 
        tmp = get_avg_distance(day1, now_x1, day2, now_x2, length)
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
    

