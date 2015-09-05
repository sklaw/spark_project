from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

sc = None


import math

def pre_process_2(line):
    elem_list = line.split(",")
    
    #assume list[1] is time interval, list[2] is rider sum 
    return ((int(elem_list[1])), (int(elem_list[2])))

def get_one_day_group_by_time(path):
    global sc
    file_data = sc.textFile(path)
    data = file_data.map(pre_process_2).reduceByKey(lambda a,b: a+b).collect()
    data = sorted(data, key=lambda x: x[0])
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
    x1 = 0
    x2 = [idx for idx, val in enumerate(day2) if val[0]==day1[x1][0]]
    if x2 == []:
        x2 = 0
        x1 = [idx for idx, val in enumerate(day1) if val[0]==day2[x2][0]][0]
    else:
        x2 = x2[0]
    
    
    #may change over different range of offsets in test
    offset_list = [-4, -3, -2, -1, 0, 1, 2, 3, 4]
    
    min_distance = -1
    
    
    print "x1:"+str(x1)+" x2:"+str(x2)

    for i in offset_list:
        if (i < 0):
            now_x1 = x1
            now_x2 = x2-i
        else:
            now_x1 = x1+i
            now_x2 = x2
        
    
        l1 = len(day1) - now_x1
        l2 = len(day2) - now_x2
    
        if l1 < l2:
            length = l1
        else:
            length = l2
 
        tmp = get_avg_distance(day1, now_x1, day2, now_x2, length)
        if min_distance == -1:
            min_distance = tmp
            to_return = i
        elif tmp < min_distance:
            min_distance = tmp
            to_return = i

    return (to_return, min_distance)
   
#API to time predictor
def cmp_two_days(day1 , path_to_day2, _sc):
    global sc
    sc = _sc
    day2 = get_one_day_group_by_time(path_to_day2)
    return try_match(day1, day2)
    

